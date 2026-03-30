# Vert.x 5 Streaming Service

Реактивное приложение на **Vert.x 5**, которое:
- Инициализирует PostgreSQL таблицу с тестовыми данными.
- Потоково отдаёт данные в формате **ND-JSON** через HTTP.
- Поддерживает асинхронную работу и низкое потребление памяти.

Используется для демонстрации потоковой передачи больших объёмов данных без загрузки в память.

---

## 🚀 Функциональность

| Эндпоинт | Метод | Описание |
|--------|-------|--------|
| `/api/v1/vertx-stream/stream` | `GET` | Потоковый вывод записей в формате ND-JSON |
| `/api/v1/vertx-stream/status` | `GET` | Статус сервиса (всегда 200 OK) |

Формат данных в стриме:
```json
{"id":1,"name":"Item_1","sequenceNumber":1}
{"id":2,"name":"Item_2","sequenceNumber":2}
...
```

---

## ⚙️ Конфигурация

Параметры задаются через переменные окружения:

| Переменная | По умолчанию | Описание |
|-----------|-------------|---------|
| `DB_HOST` | `localhost` | Хост PostgreSQL |
| `DB_PORT` | `5432` | Порт PostgreSQL |
| `DB_NAME` | `mp` | Имя базы данных |
| `DB_USER` | `mp` | Пользователь БД |
| `DB_PASSWORD` | `mp` | Пароль пользователя |
| `HTTP_PORT` | `8080` | Порт HTTP-сервера |
| `TOTAL_RECORDS` | `3500` | Количество записей для генерации |

---

## 📦 Запуск

### 1. Через Docker (рекомендуется)

Соберите образ:
```bash
docker build -t vertx-stream .
```

Запустите контейнер:
```bash
docker run --rm -p 8080:8080 \
  -e DB_HOST=192.168.232.4 \
  -e TOTAL_RECORDS=5000 \
  vertx-stream
```

> Убедитесь, что PostgreSQL доступен по указанному `DB_HOST`.

---

### 2. Локальная сборка и запуск

Требуется: **Java 17+**, **Maven**

Соберите проект:
```bash
mvn clean package
```

Запустите:
```bash
java -jar target/vertx5-stream-1.0.0-SNAPSHOT-fat.jar
```

---

## 🧪 Тестирование

Запрос к стриму:
```bash
curl http://localhost:8080/api/v1/vertx-stream/stream
```

Пример вывода:
```
{"id":1,"name":"Item_1","sequenceNumber":1}
{"id":2,"name":"Item_2","sequenceNumber":2}
...
```

Проверка статуса:
```bash
curl -I http://localhost:8080/api/v1/vertx-stream/status
```

---

## 🐳 Docker Compose (опционально)

Создайте `docker-compose.yml`:

```yaml
version: '3.8'
services:
  db:
    image: postgres:15
    environment:
      POSTGRES_DB: mp
      POSTGRES_USER: mp
      POSTGRES_PASSWORD: mp
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U mp"]
      interval: 10s
      timeout: 5s
      retries: 5

  app:
    build: .
    ports:
      - "8080:8080"
    environment:
      DB_HOST: db
      DB_PORT: 5432
      DB_NAME: mp
      DB_USER: mp
      DB_PASSWORD: mp
      HTTP_PORT: 8080
      TOTAL_RECORDS: 3500
    depends_on:
      db:
        condition: service_healthy
```

Запустите:
```bash
docker-compose up --build
```

---

## 🧩 Архитектура

- **`MainVerticle`** — основной компонент: инициализация БД, HTTP-роутинг.
- **`AppConfig`** — централизованная конфигурация.
- **`ItemDto`** — DTO без `description` для оптимизации стрима.
- **`RowStream`** — потоковое чтение из PostgreSQL без нагрузки на память.
- **`Dockerfile`** — многоступенчатая сборка, non-root пользователь, healthcheck.

---

## 📁 Структура проекта

```
src/
├── main/java/com/example/
│   ├── MainVerticle.java     # Основная логика
│   ├── AppConfig.java        # Конфигурация
│   ├── Item.java             # JPA-сущность (не используется)
│   ├── ItemDto.java          # DTO для стрима
│   └── Launcher.java         # Точка входа
└── test/                     # Интеграционные тесты (рекомендуется добавить)
```

> 💡 Примечание: `hibernate-reactive` подключён в `pom.xml`, но не используется. Рекомендуется удалить, если не планируется использовать.

---

## 🛡️ Best Practices

- ✅ Non-root пользователь в Docker.
- ✅ Healthcheck.
- ✅ ND-JSON для потоковой передачи.
- ✅ Graceful shutdown.
- ✅ Конфигурация через env.
- ✅ Testcontainers (для тестов).

---

## 📄 Лицензия

MIT
