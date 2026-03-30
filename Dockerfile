# 1st Docker build stage: build the project with Maven
FROM maven:3.9.9-eclipse-temurin-21 AS builder
WORKDIR /project
COPY . /project/
RUN mvn package -DskipTests -B

# 2nd Docker build stage: copy builder output and configure entry point
FROM eclipse-temurin:21-jre
ENV APP_DIR=/application
ENV APP_FILE=app.jar

EXPOSE 8080

# Create non-root user
RUN addgroup --system javauser && adduser --system --ingroup javauser javauser
USER javauser

WORKDIR $APP_DIR
COPY --from=builder /project/target/*-fat.jar $APP_DIR/$APP_FILE

# Use single ENTRYPOINT with variable expansion
ENTRYPOINT ["sh", "-c", "exec java -jar $APP_DIR/$APP_FILE"]

# HEALTHCHECK (optional but recommended)
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
  CMD wget --quiet --spider http://localhost:8080/api/v1/vertx-stream/status || exit 1
