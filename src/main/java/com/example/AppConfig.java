package com.example;

public record AppConfig(
  String dbHost,
  int dbPort,
  String dbName,
  String dbUser,
  String dbPassword,
  int httpPort,
  int totalRecords
) {

  public static AppConfig load() {
    // Конфигурация подключения (из переменных окружения или defaults)
    String dbHost = System.getenv("DB_HOST") != null ?
      System.getenv("DB_HOST") : "localhost";
    int dbPort = System.getenv("DB_PORT") != null ?
      Integer.parseInt(System.getenv("DB_PORT")) : 5432;
    String dbName = System.getenv("DB_NAME") != null ?
      System.getenv("DB_NAME") : "mp";
    String dbUser = System.getenv("DB_USER") != null ?
      System.getenv("DB_USER") : "mp";
    String dbPassword = System.getenv("DB_PASSWORD") != null ?
      System.getenv("DB_PASSWORD") : "mp";
    int httpPort = System.getenv("HTTP_PORT") != null ?
      Integer.parseInt(System.getenv("HTTP_PORT")) : 8080;
    int totalRecords = System.getenv("TOTAL_RECORDS") != null ?
      Integer.parseInt(System.getenv("TOTAL_RECORDS")) : 3500;

    // Возвращаем новый экземпляр записи
    return new AppConfig(dbHost, dbPort, dbName, dbUser, dbPassword, httpPort, totalRecords);
  }
}
