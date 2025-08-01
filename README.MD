
# Демонстрационный стенд: Kafka + PostgreSQL

Этот проект представляет собой готовый `docker-compose` стенд для демонстрации перегонки данных с использованием Apache Kafka и PostgreSQL.

Включает в себя генератор событий, брокер сообщений Kafka, сервис-потребитель, который обрабатывает события, и базу данных PostgreSQL для их хранения.

## Архитектура

Процесс обработки данных выглядит следующим образом:

```
[Event Emitter] -> [Kafka Topics] -> [Kafka Consumer] -> [PostgreSQL]                         
```

## Структура проекта

```
.
├── docker-compose.yml      # Главный файл, описывающий все сервисы и их конфигурацию
├── kafka-entrypoint.sh     # Скрипт для автоматической генерации ID кластера Kafka
│
├── postgres_init/
│   └── init.sql          # SQL-схема для создания таблиц в PostgreSQL
│
├── kafka_consumer/
│   ├── Dockerfile          # Dockerfile для сборки образа потребителя
│   ├── consumer.py         # Python-скрипт, читающий из Kafka и пишущий в Postgres
│   └── requirements.txt    # Зависимости для потребителя
│
└── emitter/
    ├── Dockerfile          # Dockerfile для сборки образа генератора событий
    ├── emitter.py          # Python-скрипт, генерирующий и отправляющий события
    └── requirements.txt    # Зависимости для генератора
```

## Сервисы

  * **`kafka`**: Брокер сообщений **Bitnami Kafka**, работающий в режиме **KRaft** .
  * **`postgres`**: Реляционная база данных PostgreSQL для хранения обработанных событий.
  * **`event-emitter`**: Продюсер, который генерирует три типа событий и отправляет их в Kafka.
  * **`kafka-consumer`**: Потребитель, который читает события из Kafka и записывает их в PostgreSQL

## Ключевые особенности

  * **Разделение компонентов**: Четкое разделение продюсера, потребителя и брокера сообщений.
  * **Автоматическая инициализация**: Схемы и таблицы в PostgreSQL создаются автоматически при первом запуске.
  * **Гибкая конфигурация**: Ключевые параметры Kafka, продюсера и потребителя вынесены в переменные окружения в `docker-compose.yml`.


## Необходимые условия

  * **Docker**
  * **Docker Compose v2**

## Запуск проекта

1.  Склонируйте репозиторий:
    ```bash
    git clone https://github.com/TheLastManSleeping/kafka-clickhouse-demo.git
    ```
2.  Запустите все сервисы:
    ```bash
    docker compose up -d --build
    ```

## Остановка проекта

  * Для остановки всех сервисов и удаления контейнеров:
    ```bash
    docker compose down
    ```
  * Для **полной очистки** (включая все данные):
    ```bash
    docker compose down -v
    ```

## Полезные команды

  * **Просмотр статуса сервисов:** `docker compose ps`
  * **Доступ к PostgreSQL:**
    ```bash
    docker compose exec postgres psql -U user -d events_db
    ```
    Пример запроса внутри `psql`: `SELECT * FROM orders LIMIT 10;`
  * **Доступ к Kafka:**
    ```bash
    docker compose exec kafka bash
    ```
  * **Просмотр логов:** `docker compose logs -f [имя_сервиса]`