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
| **Kafka Broker (Internal)** | `kafka:29092` | Used inside Docker network |
| **Kafka Broker (External)** | `192.168.1.100:9092` / `localhost:9092` | Used by external consumers (Apache Flink, other PCs) |

To stop the services:
```bash
docker compose down
```

### 🌐 Connecting External Consumers (Apache Flink, Spark, etc.)

StreamForge exposes Kafka on port `9092` with advertised listeners mapped to your machine's LAN IP (`192.168.1.100`), allowing consumers on other computers across your local network to stream events in real time.

#### 1. Network & Firewall Prerequisites
On the host Windows PC running StreamForge, allow inbound connections on port `9092` (run PowerShell as Administrator):
```powershell
New-NetFirewallRule -DisplayName "Kafka 9092 LAN Inbound" -Direction Inbound -LocalPort 9092 -Protocol TCP -Action Allow
```
Verify reachability from the remote computer:
```bash
curl -v telnet://192.168.1.100:9092
# Or: nc -zv 192.168.1.100 9092
```

---

#### 2. Apache Flink Integration Examples

##### Option A: Flink SQL (Table API / SQL Client)
```sql
-- Define Kafka source table
CREATE TABLE streamforge_orders (
    order_id STRING,
    customer STRING,
    amount DOUBLE,
    `timestamp` BIGINT,
    proctime AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'ecommerce-orders',
    'properties.bootstrap.servers' = '192.168.1.100:9092',
    'properties.group.id' = 'flink-sql-consumer',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- Continuous tumbling window aggregation
SELECT 
    customer,
    COUNT(*) AS total_orders,
    ROUND(SUM(amount), 2) AS total_revenue
FROM streamforge_orders
GROUP BY customer;
```

##### Option B: PyFlink (Python)
```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

env = StreamExecutionEnvironment.get_execution_environment()

kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("192.168.1.100:9092") \
    .set_topics("ecommerce-orders") \
    .set_group_id("pyflink-streamforge-group") \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "StreamForgeKafkaSource")
stream.print()

env.execute("StreamForge Flink Ingestion")
```

##### Option C: Java / Scala (DataStream API)
```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamForgeFlinkConsumer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
            .setBootstrapServers("192.168.1.100:9092")
            .setTopics("ecommerce-orders")
            .setGroupId("flink-java-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "StreamForge Source")
           .print();

        env.execute("StreamForge Kafka Pipeline");
    }
}
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
