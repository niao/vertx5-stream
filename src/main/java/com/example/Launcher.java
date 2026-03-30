package com.example;

import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Launcher {
  private static final Logger logger = LoggerFactory.getLogger(Launcher.class);
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        vertx.deployVerticle(new MainVerticle())
            .onSuccess(id -> logger.info("Verticle deployed: {}", id))
            .onFailure(err -> {
                logger.error("Failed to deploy: {}", err.getMessage());
            });
    }
}
