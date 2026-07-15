package com.example;

import com.example.service.MinioUploadService;
import com.example.service.StreamingService;
import io.minio.GetPresignedObjectUrlArgs;
import io.minio.http.Method;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.VerticleBase;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.pgclient.PgBuilder;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


import java.util.UUID;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;


import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import static com.example.util.ProtobufUtils.encodeVarint;

public class MainVerticle extends VerticleBase {

  private static final Logger logger = LoggerFactory.getLogger(MainVerticle.class);

  private Pool pgPool;
  private int totalRecords;

  private MinioClient minioClient;
  private final AppConfig config = AppConfig.load();

  private StreamingService streamingService;
  private MinioUploadService minioUploadService;

  @Override
  public Future<?> start()  {

    AppConfig config = AppConfig.load();

    totalRecords = config.totalRecords();

    PgConnectOptions connectOptions = new PgConnectOptions()
      .setHost(config.dbHost())
      .setPort(config.dbPort())
      .setDatabase(config.dbName())
      .setUser(config.dbUser())
      .setPassword(config.dbPassword())
      .setCachePreparedStatements(true)
      .setPreparedStatementCacheMaxSize(256);

    PoolOptions poolOptions = new PoolOptions()
      .setMaxSize(10);
    //for testing purposes - .setEventLoopSize(4);


// Create the pooled client
    pgPool = PgBuilder
      .pool()
      .with(poolOptions)
      .connectingTo(connectOptions)
      .using(vertx)
      .build();

    this.minioClient = MinioClient.builder()
      .endpoint(config.minioEndpoint())
      .credentials(config.minioAccessKey(), config.minioSecretKey())
      .build();

    Future<Void> dbFuture = initializeDatabase();
    Future<Void> httpFuture = setupRouter(config.httpPort());

    return Future.all(dbFuture, httpFuture);
  }

  private Future<Void> initializeDatabase() {
    Promise<Void> promise = Promise.promise();

    final String createTable = """
      CREATE TABLE IF NOT EXISTS items (
          id BIGSERIAL PRIMARY KEY,
          name VARCHAR(255) NOT NULL,
          description TEXT NOT NULL,
          sequence_number INTEGER NOT NULL
      )
      """;

    final String truncateTable = "TRUNCATE TABLE items RESTART IDENTITY";

    // Генерация N записей через generate_series (PostgreSQL feature)
    final String insertData = """
      INSERT INTO items (name, description, sequence_number)
      SELECT
          'Item_' || g,
          'Description for item ' || g || ' - Lorem ipsum dolor sit amet',
          g
      FROM generate_series(1, $1) AS g
      """;

    pgPool.query(createTable).execute()
      .compose(v -> pgPool.query(truncateTable).execute())
      .compose(v -> pgPool.preparedQuery(insertData).execute(Tuple.of(totalRecords)))
      .onSuccess(v -> {
        logger.info("Database initialized with {} records", totalRecords);
        promise.complete();
      })
      .onFailure(promise::fail);

    return promise.future();
  }

  private Future<Void> setupRouter(int httpPort) {
    Promise<Void> promise = Promise.promise();
    Router router = Router.router(vertx);

    router.route().handler(ctx->{
      var headers = ctx.request().headers().toString();
      JsonObject resp = new JsonObject().put("headers", headers);
      logger.info("Incoming request: {}", ctx.request().uri());
      ctx.next();
    });

    // Статика (клиент)
    router.route("/").handler(StaticHandler.create("webroot"));
    router.route("/api/v1/vertx-stream").handler(StaticHandler.create("webroot"));

    router.route().last().handler(ctx->{
      var headers = ctx.request().headers().toString();
      var route =  ctx.request().uri();
      logger.error("Route missing: {} \nHeaders: {}", route, headers);
      ctx.response().setStatusCode(404).end("#Path: " + route +"\n#Req headers:\n"+ headers);
    });


    // Стриминг эндпоинт
    router.get("/api/v1/vertx-stream/stream").handler(this::handleStream);
    router.get("/api/v1/vertx-stream/stream-protobuf").handler(this::handleStreamProtobuf);
    router.get("/api/v1/vertx-stream/stream-protobuf-gzip").handler(this::handleStreamProtobufGzip);
    router.get("/api/v1/vertx-stream/stream-protobuf-gzip-minio").handler(this::handleStreamProtobufGzipToMinio);
    // Эндпоинт для проверки статуса
    router.get("/api/v1/vertx-stream/status").handler(this::handleStatus);

    vertx.createHttpServer()
      .requestHandler(router)
      .listen(httpPort)
      .onSuccess(server -> {
        logger.info("Server started on http://localhost:{}",httpPort);
        promise.complete();
      })
      .onFailure(f->{
        logger.error("Server starting fail with: {}", f.getMessage());
        promise.fail("http: " + f);
      });
    return promise.future();
  }

  private void handleStream(RoutingContext ctx) {
    var response = ctx.response();
    response.setChunked(true);
    response.putHeader(HttpHeaders.CONTENT_TYPE, "application/x-ndjson");
    response.putHeader("X-Stream-Info", "max=" + totalRecords);

    String sql = "SELECT id, name, sequence_number FROM items ORDER BY id LIMIT $1";

    pgPool.getConnection().onSuccess(conn -> {
      conn.prepare(sql)
        .onSuccess(pq -> {
          conn.begin()
            .onSuccess(tx -> {
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
                tx.commit();
                response.end();
                logger.info("Stream completed");
                pq.close();
                conn.close();
              });

              stream.exceptionHandler(err -> {
                logger.error("Stream error: {}", err.getMessage());
                tx.rollback();
                if (!response.ended()) {
                  response.end();
                }
                pq.close();
                conn.close();
              });
            })
            .onFailure(err -> {
              logger.error("Transaction begin failed: {}", err.getMessage());
              if (!response.ended()) {
                response.setStatusCode(500).end("Database error");
              }
              pq.close();
              conn.close();
            });
        })
        .onFailure(err -> {
          logger.error("Query preparation failed: {}", err.getMessage());
          if (!response.ended()) {
            response.setStatusCode(500).end("Database error");
          }
          conn.close();
        });
    }).onFailure(err -> {
      logger.error("Failed to get connection: {}", err.getMessage());
      ctx.response().setStatusCode(500).end("Connection error");
    });
  }

  private void handleStreamProtobuf(RoutingContext ctx) {
    var response = ctx.response();
    response.setChunked(true);
    response.putHeader(HttpHeaders.CONTENT_TYPE, "application/x-protobuf-stream");
    // Можно также использовать: application/octet-stream

    String sql = "SELECT id, name, sequence_number FROM items ORDER BY id LIMIT $1";

    pgPool.getConnection().onSuccess(conn -> {
      conn.prepare(sql).onSuccess(pq -> {
        conn.begin().onSuccess(tx -> {
          RowStream<Row> stream = pq.createStream(50, Tuple.of(totalRecords));

          stream.handler(row -> {
            try {
              Item item = Item.newBuilder()
                .setId(row.getLong("id"))
                .setName(row.getString("name"))
                .setSequenceNumber(row.getInteger("sequence_number"))
                .build();

              byte[] data = item.toByteArray();
              int len = data.length;

              // Записываем длину (varint) + данные
              response.write(encodeVarint(len));
              response.write(Buffer.buffer(data));
            } catch (Exception e) {
              logger.error("Protobuf serialization failed", e);
              ctx.fail(500);
            }
          });

          stream.endHandler(v -> {
            tx.commit();
            response.end();
            logger.info("Protobuf stream completed");
            pq.close();
            conn.close();
          });

          stream.exceptionHandler(err -> {
            logger.error("Protobuf stream error: {}", err.getMessage());
            tx.rollback();
            if (!response.ended()) response.end();
            pq.close();
            conn.close();
          });
        }).onFailure(err -> {
          logger.error("Tx begin failed: {}", err.getMessage());
          if (!response.ended()) response.setStatusCode(500).end();
          conn.close();
        });
      }).onFailure(err -> {
        logger.error("Prepare failed: {}", err.getMessage());
        if (!response.ended()) response.setStatusCode(500).end();
        conn.close();
      });
    }).onFailure(err -> {
      logger.error("Connection failed: {}", err.getMessage());
      ctx.response().setStatusCode(500).end();
    });
  }

  private void handleStreamProtobufGzip(RoutingContext ctx) {
    var response = ctx.response();
    response.setChunked(true);
    response.putHeader(HttpHeaders.CONTENT_TYPE, "application/gzip");
    response.putHeader(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"items.pbstream.gz\"");

    String sql = "SELECT id, name, sequence_number FROM items ORDER BY id LIMIT $1";

    pgPool.getConnection().onSuccess(conn -> {
      conn.prepare(sql).onSuccess(pq -> {
        conn.begin().onSuccess(tx -> {
          RowStream<Row> stream = pq.createStream(50, Tuple.of(totalRecords));

          // Буфер для сборки GZIP-потока
          ByteArrayOutputStream baos = new ByteArrayOutputStream();
          GZIPOutputStream gzos;
          try {
            gzos = new GZIPOutputStream(baos, true); // true = небуферизированный режим
          } catch (Exception e) {
            logger.error("Failed to create GZIP stream", e);
            ctx.fail(500);
            conn.close();
            return;
          }

          stream.exceptionHandler(err -> {
            try { gzos.close(); } catch (Exception ignored) {}
            logger.error("GZIP stream error: {}", err.getMessage());
            tx.rollback();
            if (!response.ended()) response.end();
            pq.close();
            conn.close();
          });

          stream.endHandler(v -> {
            try {
              gzos.finish(); // завершаем GZIP, но не закрываем — чтобы прочитать остаток
              if (baos.size() > 0) {
                response.write(Buffer.buffer(baos.toByteArray()));
                baos.reset();
              }
              // Теперь можно закрыть
              gzos.close();
            } catch (Exception e) {
              logger.error("Error finishing GZIP", e);
            } finally {
              tx.commit();
              response.end();
              logger.info("Protobuf-GZIP stream completed");
              pq.close();
              conn.close();
            }
          });

          stream.handler(row -> {
            try {
              Item item = Item.newBuilder()
                .setId(row.getLong("id"))
                .setName(row.getString("name"))
                .setSequenceNumber(row.getInteger("sequence_number"))
                .build();

              byte[] data = item.toByteArray();
              byte[] lenBytes = encodeVarint(data.length).getBytes();

              // Пишем [varint][protobuf] в GZIP
              gzos.write(lenBytes);
              gzos.write(data);
              gzos.flush(); // важно!

              // Отправляем накопленные сжатые данные
              if (baos.size() >= 8192) {
                response.write(Buffer.buffer(baos.toByteArray()));
                baos.reset();
              }
            } catch (Exception e) {
              logger.error("Error during GZIP serialization", e);
//            stream.exceptionHandler(e).handle(e);
            }
          });

        }).onFailure(handleFailure(ctx, conn));
      }).onFailure(handleFailure(ctx, conn));
    }).onFailure(err -> ctx.fail(500, err));
  }

// Promise<Void> promise = Promise.promise();

  private void handleStreamProtobufGzipToMinio(RoutingContext ctx) {
    String fileName = "items-" + UUID.randomUUID() + ".pbstream.gz";
    Promise<Void> promise = Promise.promise();
    String objectUrl;
    try {
      objectUrl = minioClient.getPresignedObjectUrl(
        GetPresignedObjectUrlArgs.builder()
          .method(Method.GET)
          .bucket(config.minioBucket())
          .object(fileName)
          .expiry(7, TimeUnit.DAYS)
          .build()
      );
    } catch (Exception e) {
      ctx.fail(500, new RuntimeException("Failed to generate presigned URL", e));
      return;
    }

    PipedInputStream pipeIn = new PipedInputStream(8192);
    PipedOutputStream pipeOut;

    try {
      pipeOut = new PipedOutputStream(pipeIn);
    } catch (IOException e) {
      ctx.fail(500, new RuntimeException("Failed to create pipe", e));
      return;
    }

    GZIPOutputStream gzipStream;
    try {
      gzipStream = new GZIPOutputStream(pipeOut, true); // true = небуферизированный режим
    } catch (IOException e) {
      ctx.fail(500, new RuntimeException("Failed to create GZIP stream", e));
      try { pipeIn.close(); } catch (IOException ignored) {}
      try { pipeOut.close(); } catch (IOException ignored) {}
      return;
    }

    vertx.executeBlocking(() -> {
      try {
        pgPool.getConnection().onComplete(connRes -> {
          if (connRes.failed()) {
            promise.fail(connRes.cause());
            closeResources(gzipStream, pipeIn, pipeOut);
            return;
          }
          SqlConnection conn = connRes.result();

          conn.prepare("SELECT id, name, sequence_number FROM items ORDER BY id LIMIT $1")
            .onComplete(pqRes -> {
              if (pqRes.failed()) {
                conn.close();
                promise.fail(pqRes.cause());
                closeResources(gzipStream, pipeIn, pipeOut);
                return;
              }
              PreparedStatement pq = pqRes.result();

              conn.begin().onComplete(txRes -> {
                if (txRes.failed()) {
                  pq.close();
                  conn.close();
                  promise.fail(txRes.cause());
                  closeResources(gzipStream, pipeIn, pipeOut);
                  return;
                }
                Transaction tx = txRes.result();

                RowStream<Row> stream = pq.createStream(50, Tuple.of(config.totalRecords()));

                stream.exceptionHandler(err -> {
                  logger.error("Stream error during GZIP+Protobuf generation", err.getMessage());
                  tx.rollback();
                  closeResources(gzipStream, pipeIn, pipeOut);
                  pq.close();
                  conn.close();
                  promise.fail(err);
                });

                stream.endHandler(v -> {
                  try {
                    gzipStream.finish(); // завершаем GZIP
                    gzipStream.flush();
                  } catch (IOException ignored) {}
                  closeResources(gzipStream, pipeIn, pipeOut);
                  tx.commit();
                  pq.close();
                  conn.close();
                  promise.complete();
                });

                stream.handler(row -> {
                  try {
                    Item item = Item.newBuilder()
                      .setId(row.getLong("id"))
                      .setName(row.getString("name"))
                      .setSequenceNumber(row.getInteger("sequence_number"))
                      .build();

                    byte[] data = item.toByteArray();
                    byte[] lenBytes = encodeVarint(data.length).getBytes();

                    gzipStream.write(lenBytes);
                    gzipStream.write(data);
                    gzipStream.flush();

                    // Опционально: сбрасываем буфер при накоплении
                    if (pipeIn.available() > 8192) {
                      pipeOut.flush();
                    }
                  } catch (Exception e) {
                    logger.error("Error during GZIP+Protobuf generation", e);
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
    }, false).onSuccess(v -> {
      // Успешно сгенерировано → загружаем в MinIO
      uploadGzipStreamToMinio(ctx, pipeIn, fileName, objectUrl);
    }).onFailure(err -> {
      logger.error("Failed to generate GZIP stream", err);
      ctx.fail(500, err);
    });
  }

  private void uploadGzipStreamToMinio(RoutingContext ctx, PipedInputStream pipeIn, String fileName, String objectUrl) {
    Promise<Void> promise = Promise.promise();
    vertx.executeBlocking(() -> {
      try {
        minioClient.putObject(
          PutObjectArgs.builder()
            .bucket(config.minioBucket())
            .object(fileName)
            .stream(pipeIn, -1, 10485760) // input stream, size=-1, partSize=10MB
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
    }, false).onComplete(ar -> {
      if (ar.succeeded()) {
        ctx.json(Map.of("url", objectUrl, "filename", fileName));
      } else {
        logger.error("Upload to MinIO failed", ar.cause());
        ctx.fail(500, ar.cause());
      }
    });
  }


  private void closeResources(GZIPOutputStream gzipStream, PipedInputStream pipeIn, PipedOutputStream pipeOut) {
    try { gzipStream.close(); } catch (Exception ignored) {}
    try { pipeOut.close(); } catch (Exception ignored) {}
    try { pipeIn.close(); } catch (Exception ignored) {}
  }

  private void handleStatus(RoutingContext ctx) {
    pgPool.query("SELECT COUNT(*) FROM items").execute().onSuccess(rows -> {
      long count = rows.iterator().next().getLong(0);
      ctx.json(Map.of("status", "ok", "records", count));
    }).onFailure(err -> {
      ctx.response().setStatusCode(500).end(new JsonObject(Map.of("status", "error", "message", err.getMessage())).toBuffer());
    });
  }

  private Handler<Throwable> handleFailure(RoutingContext ctx, SqlConnection conn) {
    return err -> {
      logger.error("DB error: {}", err.getMessage());
      if (!ctx.response().ended()) {
        ctx.response().setStatusCode(500).end();
      }
      conn.close();
    };
  }

  @Override
  public Future<?> stop() {
    if (pgPool != null) {
      return pgPool.close();
    }
    return Future.succeededFuture();
  }
}
