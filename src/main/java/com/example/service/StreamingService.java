package com.example.service;

import com.example.Item;
import com.example.ItemDto;
import com.example.MainVerticle;
import com.example.util.ProtobufUtils;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import io.vertx.sqlclient.Tuple;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

public class StreamingService {
  private final Pool pgPool;
  private final int totalRecords;
  private final Vertx vertx;

  private static final Logger logger = LoggerFactory.getLogger(StreamingService.class);

  public StreamingService(Vertx vertx, Pool pgPool, int totalRecords) {
    this.vertx = vertx;
    this.pgPool = pgPool;
    this.totalRecords = totalRecords;
  }

  public void handleStream(RoutingContext ctx) {
    var response = ctx.response();
    response.setChunked(true);
    response.putHeader(HttpHeaders.CONTENT_TYPE, "application/x-ndjson");
    response.putHeader("X-Stream-Info", "max=" + totalRecords);

    pgPool.getConnection().onSuccess(conn -> {
      conn.prepare("SELECT id, name, sequence_number FROM items ORDER BY id LIMIT $1")
        .onSuccess(pq -> {
          RowStream<Row> stream = pq.createStream(50, Tuple.of(totalRecords));
          stream.handler(row -> {
            ItemDto dto = new ItemDto(
              row.getLong("id"),
              row.getString("name"),
              row.getInteger("sequence_number")
            );
            response.write(dto.toJson().encode() + "\n");
          });
          stream.endHandler(v -> {
            response.end();
            pq.close();
            conn.close();
          });
          stream.exceptionHandler(err -> handleError(response, pq, conn, err));
        })
        .onFailure(err -> {
          if (!response.ended()) response.setStatusCode(500).end();
          conn.close();
        });
    }).onFailure(err -> ctx.fail(500));
  }

  public void handleStreamProtobuf(RoutingContext ctx) {
    var response = ctx.response();
    response.setChunked(true);
    response.putHeader(HttpHeaders.CONTENT_TYPE, "application/x-protobuf-stream");

    pgPool.getConnection().onSuccess(conn -> {
      conn.prepare("SELECT id, name, sequence_number FROM items ORDER BY id LIMIT $1")
        .onSuccess(pq -> {
          RowStream<Row> stream = pq.createStream(50, Tuple.of(totalRecords));
          stream.handler(row -> {
            try {
              Item item = Item.newBuilder()
                .setId(row.getLong("id"))
                .setName(row.getString("name"))
                .setSequenceNumber(row.getInteger("sequence_number"))
                .build();

              byte[] data = item.toByteArray();
              response.write(ProtobufUtils.encodeVarint(data.length));
              response.write(Buffer.buffer(data));
            } catch (Exception e) {
              ctx.fail(500);
            }
          });
          stream.endHandler(v -> {
            response.end();
            pq.close();
            conn.close();
          });
          stream.exceptionHandler(err -> handleError(response, pq, conn, err));
        });
    }).onFailure(err -> ctx.fail(500));
  }

  public void handleStreamProtobufGzip(RoutingContext ctx) {
    var response = ctx.response();
    response.setChunked(true);
    response.putHeader(HttpHeaders.CONTENT_TYPE, "application/gzip");
    response.putHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"items.pbstream.gz\"");

    pgPool.getConnection().onSuccess(conn -> {
      conn.prepare("SELECT id, name, sequence_number FROM items ORDER BY id LIMIT $1")
        .onSuccess(pq -> {
          RowStream<Row> stream = pq.createStream(50, Tuple.of(totalRecords));

          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          GZIPOutputStream gzos;
          try {
            gzos = new GZIPOutputStream(baos, true);
          } catch (IOException e) {
            ctx.fail(500);
            conn.close();
            return;
          }

          stream.exceptionHandler(err -> {
            closeQuietly(gzos);
            handleError(response, pq, conn, err);
          });

          stream.endHandler(v -> {
            finishGzipAndEnd(gzos, response, baos, pq, conn);
          });

          stream.handler(row -> {
            try {
              Item item = Item.newBuilder()
                .setId(row.getLong("id"))
                .setName(row.getString("name"))
                .setSequenceNumber(row.getInteger("sequence_number"))
                .build();

              byte[] data = item.toByteArray();
              gzos.write(ProtobufUtils.encodeVarint(data.length).getBytes());
              gzos.write(data);
              gzos.flush();

              if (baos.size() >= 8192) {
                response.write(Buffer.buffer(baos.toByteArray()));
                baos.reset();
              }
            } catch (Exception e) {
              logger.error("Error writing item", e);
//              stream.exceptionHandler().handle(e);
            }
          });
        });
    }).onFailure(err -> ctx.fail(500));
  }

  private void finishGzipAndEnd(GZIPOutputStream gzos, io.vertx.core.http.HttpServerResponse response,
                                ByteArrayOutputStream baos, io.vertx.sqlclient.PreparedStatement pq,
                                io.vertx.sqlclient.SqlConnection conn) {
    try {
      gzos.finish();
      if (baos.size() > 0) {
        response.write(Buffer.buffer(baos.toByteArray()));
      }
      gzos.close();
    } catch (Exception ignored) {}
    response.end();
    pq.close();
    conn.close();
  }

  private void closeQuietly(GZIPOutputStream gzos) {
    try { gzos.close(); } catch (Exception ignored) {}
  }

  private void handleError(io.vertx.core.http.HttpServerResponse response,
                           io.vertx.sqlclient.PreparedStatement pq,
                           io.vertx.sqlclient.SqlConnection conn,
                           Throwable err) {
    if (!response.ended()) response.end();
    pq.close();
    conn.close();
  }
}
