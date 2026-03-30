package com.example;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.VerticleBase;
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

import java.util.Map;

public class MainVerticle extends VerticleBase {

  private static final Logger logger = LoggerFactory.getLogger(MainVerticle.class);

  private Pool pgPool;
  private int totalRecords;

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

    // Статика (клиент)
    router.route("/").handler(StaticHandler.create("webroot"));
    router.route("/api/v1/vertx-stream/").handler(StaticHandler.create("webroot"));


    // Стриминг эндпоинт
    router.get("/api/v1/vertx-stream/stream").handler(this::handleStream);

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

  private void handleStatus(RoutingContext ctx) {
    pgPool.query("SELECT COUNT(*) FROM items").execute()
      .onSuccess(rows -> {
        long count = rows.iterator().next().getLong(0);
        ctx.json(Map.of("status", "ok", "records", count));
      })
      .onFailure(err -> {
        ctx.response().setStatusCode(500).end(new JsonObject(Map.of("status", "error", "message", err.getMessage())).toBuffer());
      });
  }

  @Override
  public Future<?> stop() {
    if (pgPool != null) {
      return pgPool.close();
    }
    return Future.succeededFuture();
  }
}
