# ⚡ StreamForge: High-Efficiency Kafka Mock Data Generator

> A production-grade mock data streaming generator powered by an asynchronous **Rust engine (Axum, Tokio, `rdkafka`)**, **Apache Kafka in KRaft mode**, **Kafka UI**, and a **Modern Dark-Mode Web Dashboard**. Everything runs seamlessly in **Docker**.

---

## 🌟 Key Features

- **🚀 Ultra-High Efficiency Rust Backend**:
  - Built with Tokio async runtimes and `librdkafka` C-bindings for maximum zero-copy socket throughput and minimal memory usage (< 25MB RAM).
  - Configurable rate throttling: Target throughput (`messages/second`) or interval delay (`ms`).
  - Supports continuous stream mode (infinite) or fixed message limits.
  - State machine with real-time controls: **Start**, **Pause**, **Resume**, **Stop**, and **Reset**.
- **🎨 Dynamic Mock Data Engine**:
  - Dynamic token interpolation supporting:
    - `{{uuid}}` / `{{uuid_v4}}`
    - `{{timestamp}}` (epoch ms), `{{timestamp_s}}` (epoch sec), `{{timestamp_iso}}` (UTC ISO-8601)
    - `{{random_int(min, max)}}`
    - `{{random_float(min, max, decimals)}}`
    - `{{choose(['OPT1', 'OPT2', 'OPT3'])}}`
    - `{{name}}`, `{{first_name}}`, `{{last_name}}`, `{{email}}`, `{{ipv4}}`, `{{country_code}}`, `{{city}}`
    - `{{phone}}`, `{{credit_card}}`, `{{boolean}}`, `{{sequence}}`
  - Ready-to-use industry presets: **🛒 E-Commerce Orders**, **📡 IoT Sensor Telemetry**, **💳 Financial Transactions**, **🖱️ User Clickstream**.
- **🔧 Deep Kafka Parameterization**:
  - Custom topic and partition key strategies: None (Round-Robin), Static, Random UUID, or Dynamic JSON field extraction (e.g. `order_id`).
  - Custom Kafka Record Headers (key-value pairs, trace IDs).
  - Producer tuning: `acks` (`0`, `1`, `all`), compression (`snappy`, `lz4`, `zstd`, `gzip`, `none`), `linger.ms`, `batch.size`.
  - Live Broker ping & topic discovery checker.
- **📊 Real-Time Telemetry & Observability**:
  - Server-Sent Events (SSE) telemetry stream updating throughput gauges, delivery error rates, and canvas sparkline trends.
  - Real-time Message Stream Inspector showing partition IDs, offsets, and copyable payloads.
  - Included **Kafka UI** for inspecting Kafka topics, partitions, and consumer groups visually.

---

## 🏗️ Architecture

```
┌─────────────────────────────────┐
│     Web Dashboard (Port 3000)   │  <--- Frontend (Nginx, Vanilla JS, CSS)
└──────────────┬──────────────────┘
               │ HTTP / SSE Stream
┌──────────────▼──────────────────┐
│   Rust Async Engine (Port 5000) │  <--- Backend (Axum, Tokio, librdkafka)
└──────────────┬──────────────────┘
               │ rdkafka TCP Batching
┌──────────────▼──────────────────┐       ┌───────────────────────────────┐
│   Apache Kafka KRaft (Port 9092)│ <---> │   Kafka UI (Port 8080)        │
└─────────────────────────────────┘       └───────────────────────────────┘
```

---

## 🚀 Quick Start (Docker)

Ensure Docker Desktop is running, then execute:

```bash
docker compose up --build -d
```

### Access URLs:

| Service | URL | Description |
|---|---|---|
| **Web Dashboard** | **http://localhost:3000** | Interactive control center & payload studio |
| **Kafka UI** | **http://localhost:8080** | Web UI to inspect topics, partitions & records |
| **Rust Backend API** | **http://localhost:5000** | REST API & SSE telemetry endpoints |
| **Kafka Broker** | `localhost:9092` (Host) / `kafka:9092` (Docker) | Apache Kafka 3.8.0 KRaft cluster |

To stop the services:
```bash
docker compose down
```

---

## 🛠️ API Reference

- `POST /api/jobs/start`: Launch data generator job
- `POST /api/jobs/pause`: Pause active generation
- `POST /api/jobs/resume`: Resume paused generation
- `POST /api/jobs/stop`: Cancel active job
- `GET /api/jobs/status`: Snapshot of job metrics
- `GET /api/jobs/stream`: Server-Sent Events (SSE) real-time metric stream
- `POST /api/template/preview`: Render sample payloads without pushing to Kafka
- `POST /api/kafka/test-connection`: Ping Kafka broker and fetch topic list
