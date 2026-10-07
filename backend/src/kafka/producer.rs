use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::message::OwnedHeaders;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::{error, info};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaProducerConfig {
    pub bootstrap_servers: String,
    pub topic: String,
    #[serde(default = "default_acks")]
    pub acks: String, // "0", "1", "all"
    #[serde(default = "default_compression")]
    pub compression: String, // "none", "snappy", "gzip", "lz4", "zstd"
    #[serde(default = "default_linger_ms")]
    pub linger_ms: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    pub security_protocol: Option<String>,
    pub sasl_mechanism: Option<String>,
    pub sasl_username: Option<String>,
    pub sasl_password: Option<String>,
}

fn default_acks() -> String {
    "1".to_string()
}
fn default_compression() -> String {
    "snappy".to_string()
}
fn default_linger_ms() -> u64 {
    5
}
fn default_batch_size() -> usize {
    65536
}

impl Default for KafkaProducerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            topic: "mock-events".to_string(),
            acks: "1".to_string(),
            compression: "snappy".to_string(),
            linger_ms: 5,
            batch_size: 65536,
            security_protocol: None,
            sasl_mechanism: None,
            sasl_username: None,
            sasl_password: None,
        }
    }
}

pub fn create_kafka_producer(config: &KafkaProducerConfig) -> Result<FutureProducer, String> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", &config.bootstrap_servers)
        .set("acks", &config.acks)
        .set("compression.type", &config.compression)
        .set("linger.ms", &config.linger_ms.to_string())
        .set("batch.size", &config.batch_size.to_string())
        .set("queue.buffering.max.messages", "200000")
        .set("queue.buffering.max.kbytes", "1048576")
        .set("message.timeout.ms", "5000");

    if let Some(ref sec) = config.security_protocol {
        if !sec.is_empty() {
            client_config.set("security.protocol", sec);
        }
    }
    if let Some(ref mech) = config.sasl_mechanism {
        if !mech.is_empty() {
            client_config.set("sasl.mechanism", mech);
        }
    }
    if let Some(ref user) = config.sasl_username {
        if !user.is_empty() {
            client_config.set("sasl.username", user);
        }
    }
    if let Some(ref pass) = config.sasl_password {
        if !pass.is_empty() {
            client_config.set("sasl.password", pass);
        }
    }

    client_config
        .create()
        .map_err(|e| format!("Failed to create Kafka FutureProducer: {}", e))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KafkaConnectionResult {
    pub success: bool,
    pub latency_ms: u128,
    pub brokers_count: usize,
    pub topics: Vec<String>,
    pub error: Option<String>,
}

pub fn test_kafka_connection(bootstrap_servers: &str) -> KafkaConnectionResult {
    let start = std::time::Instant::now();
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", bootstrap_servers)
        .set("client.id", "kafka-test-client")
        .set("metadata.request.timeout.ms", "4000")
        .set("socket.timeout.ms", "4000");

    let consumer: Result<BaseConsumer, _> = client_config.create();
    match consumer {
        Ok(c) => match c.fetch_metadata(None, Duration::from_millis(4000)) {
            Ok(metadata) => {
                let latency_ms = start.elapsed().as_millis();
                let brokers_count = metadata.brokers().len();
                let topics: Vec<String> = metadata
                    .topics()
                    .iter()
                    .map(|t| t.name().to_string())
                    .filter(|name| !name.starts_with('_'))
                    .collect();
                KafkaConnectionResult {
                    success: true,
                    latency_ms,
                    brokers_count,
                    topics,
                    error: None,
                }
            }
            Err(e) => {
                let latency_ms = start.elapsed().as_millis();
                KafkaConnectionResult {
                    success: false,
                    latency_ms,
                    brokers_count: 0,
                    topics: vec![],
                    error: Some(format!("Metadata query failed: {}", e)),
                }
            }
        },
        Err(e) => KafkaConnectionResult {
            success: false,
            latency_ms: start.elapsed().as_millis(),
            brokers_count: 0,
            topics: vec![],
            error: Some(format!("Client creation failed: {}", e)),
        },
    }
}
