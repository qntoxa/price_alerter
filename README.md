# Crypto / Stock Price Alerter — Агрегатор котировок и система ценовых уведомлений в реальном времени

Высоконагруженный backend-сервис на Go для потокового сбора котировок из нескольких источников, in-memory сопоставления цен с пользовательскими триггерами с минимальной задержкой и рассылки уведомлений в реальном времени.

## Основные архитектурные решения

1. **Конкурентный сбор потоков (Fan-In / Fan-Out + Worker Pool)**
   * Несколько WebSocket-источников котировок (`binance-ws`, `coinbase-ws`, `kraken-ws`) агрегируются в единый неблокирующий поток с помощью паттерна **Fan-In** (`internal/stream/pipeline.go`).
   * Пул воркеров обрабатывает поток котировок и рассылает сработавшие события всем подключённым потребителям по **Server-Sent Events (SSE)** через неблокирующий **Broadcaster (Fan-Out)**.
2. **Шардированный In-Memory движок сопоставления (`internal/engine/matcher.go`)**
   * Подписки распределены по 16 шардам (`symbolShard`) на основе быстрого неаллоцирующего хеша `FNV-1a` от тикера (`BTCUSDT`, `ETHUSDT` и др.) для устранения конкуренции за мьютексы (lock contention).
   * На горячем пути (`Evaluate`) используется двухфазная проверка (`RLock` для быстрого чтения и переход к эксклюзивному `Lock` только при фактическом пробое порога), что даёт **0 аллокаций в куче (`0 allocs/op`)** при проверке тиков без пробоя.
3. **Защита от Thundering Herd (`singleflight`)**
   * Локальный кеш актуальных котировок (`internal/cache/quote_cache.go`) снабжён обобщённой реализацией `singleflight.Group[V]` (`internal/cache/singleflight.go`), которая схлопывает пачку одновременных запросов по одному тикеру при истечении TTL в единственный поход к внешнему источнику.
4. **Наблюдаемость и профилирование (`Prometheus` + `pprof`)**
   * Эндпоинт `/metrics` отдаёт метрики в формате Prometheus (количество обработанных котировок, сработавших алертов, попаданий/промахов кеша, склеенных запросов `singleflight`, активных горутин и памяти).
   * Подключены профилировщики `/debug/pprof/*` для снятия профилей CPU и кучи под нагрузкой.

## Структура проекта

```text
price-alerter/
├── api/proto/alerter.proto         # Контракт gRPC сервиса
├── cmd/alerter/main.go             # Точка входа, инициализация пайплайна и Graceful Shutdown
├── deployments/prometheus.yml      # Конфигурация сбора метрик Prometheus
├── internal/
│   ├── api/
│   │   ├── http_server.go          # REST API, SSE-стриминг, /metrics и /debug/pprof
│   │   └── grpc_server.go          # RPC-сервер межсервисного взаимодействия
│   ├── cache/
│   │   ├── quote_cache.go          # In-memory кеш с TTL
│   │   ├── quote_cache_test.go     # Тесты singleflight и бенчмарк кеша
│   │   └── singleflight.go         # Обобщённый пакет подавления дублирующих запросов
│   ├── domain/models.go            # Доменные модели Quote, Subscription, AlertEvent
│   ├── engine/
│   │   ├── matcher.go              # Шардированный in-memory движок сопоставления цен
│   │   └── matcher_test.go         # Unit-тесты, race-тесты и Go-бенчмарки горячего пути
│   ├── metrics/prometheus.go       # Экспортер метрик Prometheus
│   └── stream/pipeline.go          # Провайдеры потоков, Fan-In и Fan-Out Broadcaster
├── migrations/001_init.sql         # SQL-схема таблиц подписок и истории срабатываний
├── Dockerfile
├── docker-compose.yml
└── Makefile
```

## Быстрый старт

### Запуск через Docker Compose
```bash
docker compose up --build -d
```
После запуска доступны:
* **HTTP API & SSE & pprof:** `http://localhost:8080`
* **RPC сервер:** `localhost:9090`
* **Prometheus UI:** `http://localhost:9091`

### Локальный запуск и тестирование
```bash
# Запуск сервиса
make run

# Запуск тестов с детектором гонок (Race Detector)
make race

# Запуск бенчмарков с отчётом по аллокациям памяти
make bench
```

## Примеры запросов к HTTP API

### 1. Создание подписки на пробой цены
```bash
curl -X POST http://localhost:8080/api/v1/subscriptions \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user-101",
    "symbol": "BTCUSDT",
    "condition": "GT",
    "threshold": 68100.0
  }'
```

### 2. Список активных подписок пользователя
```bash
curl "http://localhost:8080/api/v1/subscriptions?user_id=user-101"
```

### 3. Ручная отправка котировки (для мгновенной проверки триггера)
```bash
curl -X POST http://localhost:8080/api/v1/quotes/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "symbol": "BTCUSDT",
    "price": 68500.0,
    "source": "manual-test"
  }'
```

### 4. Подписка на живой поток срабатываний (SSE)
```bash
curl -N http://localhost:8080/api/v1/stream/alerts
```

### 5. История сработавших уведомлений и метрики
```bash
curl "http://localhost:8080/api/v1/alerts/history?limit=20"
curl http://localhost:8080/metrics
```
