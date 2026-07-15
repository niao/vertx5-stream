package com.example.service;

import com.example.Item;
import com.example.MainVerticle;
import com.example.util.ProtobufUtils;
import io.minio.*;
import io.minio.http.Method;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.zip.GZIPOutputStream;

public class MinioUploadService {
  private final Vertx vertx;
  private final MinioClient minioClient;
  private final Pool pgPool;
  private final String bucket;
  private final int totalRecords;

  private static final Logger logger = LoggerFactory.getLogger(MinioUploadService.class);


  public MinioUploadService(Vertx vertx, MinioClient minioClient, Pool pgPool, String bucket, int totalRecords) {
    this.vertx = vertx;
    this.minioClient = minioClient;
    this.pgPool = pgPool;
    this.bucket = bucket;
    this.totalRecords = totalRecords;
  }

  public Future<String> uploadProtobufGzipToMinio() {
    String fileName = "items-" + UUID.randomUUID() + ".pbstream.gz";

    Promise<String> urlPromise = Promise.promise();
    Promise<Void> promise = Promise.promise();

    try {
      String presignedUrl = minioClient.getPresignedObjectUrl(
        GetPresignedObjectUrlArgs.builder()
          .method(Method.GET)
          .bucket(bucket)
          .object(fileName)
          .expiry(7, java.util.concurrent.TimeUnit.DAYS)
          .build()
      );

      PipedInputStream pipeIn = new PipedInputStream(8192);
      PipedOutputStream pipeOut = new PipedOutputStream(pipeIn);
      GZIPOutputStream gzipStream = new GZIPOutputStream(pipeOut, true);

      vertx.executeBlocking(() -> generateData(promise, gzipStream, pipeOut))
        .onSuccess(v -> uploadToMinio(urlPromise, pipeIn, fileName, presignedUrl))
        .onFailure(urlPromise::fail);

      return urlPromise.future();
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }

  private Callable<Void> generateData(Promise<Void> promise, GZIPOutputStream gzipStream, PipedOutputStream pipeOut) {
    try {
      pgPool.getConnection().onComplete(connRes -> {
        if (connRes.failed()) {
          promise.fail(connRes.cause());
          closeResources(gzipStream, pipeOut);
          return;
        }
        var conn = connRes.result();

        conn.prepare("SELECT id, name, sequence_number FROM items ORDER BY id LIMIT $1")
          .onComplete(pqRes -> {
            if (pqRes.failed()) {
              conn.close();
              promise.fail(pqRes.cause());
              closeResources(gzipStream, pipeOut);
              return;
            }
            var pq = pqRes.result();

            conn.begin().onComplete(txRes -> {
              if (txRes.failed()) {
                pq.close();
                conn.close();
                promise.fail(txRes.cause());
                closeResources(gzipStream, pipeOut);
                return;
              }
              var tx = txRes.result();

              RowStream<Row> stream = pq.createStream(50, Tuple.of(totalRecords));

              stream.exceptionHandler(err -> {
                tx.rollback();
                closeResources(gzipStream, pipeOut);
                pq.close();
                conn.close();
                promise.fail(err);
              });

              stream.endHandler(v -> {
                try {
                  gzipStream.finish();
                  gzipStream.flush();
                } catch (IOException ignored) {}
                closeResources(gzipStream, pipeOut);
                tx.commit();
                pq.close();
                conn.close();
                promise.complete();
              });

              stream.handler(row -> {
                try {
                  byte[] data = Item.newBuilder()
                    .setId(row.getLong("id"))
                    .setName(row.getString("name"))
                    .setSequenceNumber(row.getInteger("sequence_number"))
                    .build()
                    .toByteArray();

                  gzipStream.write(ProtobufUtils.encodeVarint(data.length).getBytes());
                  gzipStream.write(data);
                  gzipStream.flush();
                } catch (Exception e) {
                  logger.error("Error writing data", e);
//                  stream.exceptionHandler().handle(e);
                }
              });
            });
          });
      });
    } catch (Exception e) {
      promise.fail(e);
    }
    return null;
  }

  private void uploadToMinio(Promise<String> urlPromise, PipedInputStream pipeIn, String fileName, String presignedUrl) {
    Promise<Void> promise = Promise.promise();
    vertx.executeBlocking(() -> {
      try {
        minioClient.putObject(
          PutObjectArgs.builder()
            .bucket(bucket)
            .object(fileName)
            .stream(pipeIn, -1, 10485760)
            .contentType("application/gzip")
            .build()
        );
        promise.complete();
      } catch (Exception e) {
        promise.fail(e);
      } finally {
        try { pipeIn.close(); } catch (Exception ignored) {}
      }
        return null;
      }).onSuccess(v -> urlPromise.complete(presignedUrl))
      .onFailure(urlPromise::fail);
  }

  private void closeResources(GZIPOutputStream gzipStream, PipedOutputStream pipeOut) {
    try { gzipStream.close(); } catch (Exception ignored) {}
    try { pipeOut.close(); } catch (Exception ignored) {}
  }
}
