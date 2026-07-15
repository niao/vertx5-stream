//package com.example;
//
//import org.junit.jupiter.api.*;
//import org.testcontainers.containers.PostgreSQLContainer;
//import org.testcontainers.junit.jupiter.Container;
//import org.testcontainers.junit.jupiter.Testcontainers;
//import io.vertx.core.Vertx;
//import io.vertx.core.http.HttpClient;
//import io.vertx.core.http.HttpClientOptions;
//import io.vertx.core.http.RequestOptions;
//import io.vertx.junit5.VertxTestContext;
//
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicInteger;
//
//import static org.junit.jupiter.api.Assertions.*;
//
//@Testcontainers
//@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
//public class StreamingVerticleTest {
//
//    @Container
//    public static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
//        .withDatabaseName("testdb")
//        .withUsername("postgres")
//        .withPassword("postgres")
//        .withExposedPorts(5432);
//
//    private static Vertx vertx;
//    private static HttpClient httpClient;
//
//    @BeforeAll
//    static void setUpAll(VertxTestContext testContext) {
//        // Установка переменных окружения для подключения к TestContainers
//        System.setProperty("DB_HOST", postgres.getHost());
//        System.setProperty("DB_PORT", String.valueOf(postgres.getFirstMappedPort()));
//        System.setProperty("DB_NAME", postgres.getDatabaseName());
//        System.setProperty("DB_USER", postgres.getUsername());
//        System.setProperty("DB_PASSWORD", postgres.getPassword());
//
//        vertx = Vertx.vertx();
//
//        vertx.deployVerticle(new MainVerticle())
//            .onSuccess(id -> testContext.completeNow())
//            .onFailure(testContext::failNow);
//    }
//
//    @BeforeEach
//    void setUp() {
//        httpClient = vertx.createHttpClient(new HttpClientOptions());
//    }
//
//    @AfterEach
//    void tearDown() {
//        if (httpClient != null) {
//            httpClient.close();
//        }
//    }
//
//    @AfterAll
//    static void tearDownAll(VertxTestContext testContext) {
//        vertx.close()
//            .onSuccess(v -> testContext.completeNow())
//            .onFailure(testContext::failNow);
//    }
//
//    @Test
//    @Order(1)
//    @DisplayName("Проверка статуса БД")
//    void testDatabaseStatus(VertxTestContext testContext) {
//        httpClient.request(new RequestOptions()
//                .setPort(8080)
//                .setHost("localhost")
//                .setURI("/api/status"))
//            .compose(req -> req.send())
//            .onSuccess(resp -> {
//                assertEquals(200, resp.statusCode());
//                resp.bodyHandler(body -> {
//                    System.out.println("Status response: " + body.toString());
//                    testContext.completeNow();
//                });
//            })
//            .onFailure(testContext::failNow);
//
//        testContext.awaitCompletion(5, TimeUnit.SECONDS);
//    }
//
//    @Test
//    @Order(2)
//    @DisplayName("Тест стриминга данных")
//    void testStreamingEndpoint(VertxTestContext testContext) {
//        AtomicInteger recordCount = new AtomicInteger(0);
//        AtomicInteger errorCount = new AtomicInteger(0);
//
//        httpClient.request(new RequestOptions()
//                .setPort(8080)
//                .setHost("localhost")
//                .setURI("/api/stream"))
//            .compose(req -> req.send())
//            .onSuccess(resp -> {
//                assertEquals(200, resp.statusCode());
//                assertEquals("application/x-ndjson", resp.getHeader("content-type"));
//
//                resp.handler(chunk -> {
//                    String[] lines = chunk.toString().split("\n");
//                    for (String line : lines) {
//                        if (line.trim().isEmpty()) continue;
//                        try {
//                            // Простая валидация JSON
//                            line = line.trim();
//                            if (line.startsWith("{") && line.endsWith("}")) {
//                                recordCount.incrementAndGet();
//                            }
//                        } catch (Exception e) {
//                            errorCount.incrementAndGet();
//                        }
//                    }
//                });
//
//                resp.endHandler(v -> {
//                    System.out.println("Stream completed. Records: " + recordCount.get());
//                    assertTrue(recordCount.get() > 0, "Should receive at least one record");
//                    assertEquals(0, errorCount.get(), "Should not have parsing errors");
//                    testContext.completeNow();
//                });
//            })
//            .onFailure(testContext::failNow);
//
//        testContext.awaitCompletion(10, TimeUnit.SECONDS);
//    }
//}
