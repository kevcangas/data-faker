pub mod producer;

pub use producer::{create_kafka_producer, test_kafka_connection, KafkaConnectionResult, KafkaProducerConfig};
