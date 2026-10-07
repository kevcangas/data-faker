use crate::generator::TemplateEngine;
use crate::kafka::{create_kafka_producer, KafkaProducerConfig};
use chrono::Utc;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, watch, RwLock};
use tracing::{error, info, warn};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeaderItem {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum KeyStrategy {
    #[serde(rename = "none")]
    None,
    #[serde(rename = "static")]
    Static(String),
    #[serde(rename = "random_uuid")]
    RandomUuid,
    #[serde(rename = "extract_field")]
    ExtractField(String),
}

impl Default for KeyStrategy {
    fn default() -> Self {
        KeyStrategy::None
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    #[serde(default = "default_rate_mode")]
    pub mode: String, // "msg_per_sec" or "interval_ms"
    #[serde(default = "default_rate_value")]
    pub value: f64,
}

fn default_rate_mode() -> String {
    "msg_per_sec".to_string()
}
fn default_rate_value() -> f64 {
    100.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobConfig {
    pub kafka: KafkaProducerConfig,
    pub payload_template: String,
    #[serde(default)]
    pub key_strategy: KeyStrategy,
    #[serde(default)]
    pub headers: Vec<HeaderItem>,
    pub rate_limit: RateLimitConfig,
    pub total_messages: Option<u64>,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
}

fn default_concurrency() -> usize {
    2
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum JobStateEnum {
    Idle,
    Running,
    Paused,
    Stopped,
    Completed,
    Failed(String),
}

impl JobStateEnum {
    pub fn as_str(&self) -> &str {
        match self {
            JobStateEnum::Idle => "Idle",
            JobStateEnum::Running => "Running",
            JobStateEnum::Paused => "Paused",
            JobStateEnum::Stopped => "Stopped",
            JobStateEnum::Completed => "Completed",
            JobStateEnum::Failed(_) => "Failed",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducedMessageSample {
    pub timestamp: String,
    pub key: Option<String>,
    pub payload_preview: String,
    pub partition: i32,
    pub offset: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobStats {
    pub status: String,
    pub sent_count: u64,
    pub failed_count: u64,
    pub target_count: Option<u64>,
    pub current_rate: f64,
    pub elapsed_seconds: f64,
    pub last_error: Option<String>,
    pub recent_messages: Vec<ProducedMessageSample>,
}

struct SharedState {
    status: RwLock<JobStateEnum>,
    sent_count: AtomicU64,
    failed_count: AtomicU64,
    target_count: RwLock<Option<u64>>,
    current_rate: RwLock<f64>,
    start_time: RwLock<Option<Instant>>,
    paused_duration: RwLock<Duration>,
    last_pause_time: RwLock<Option<Instant>>,
    last_error: RwLock<Option<String>>,
    recent_messages: RwLock<VecDeque<ProducedMessageSample>>,
    is_paused: AtomicBool,
}

pub struct JobManager {
    shared: Arc<SharedState>,
    stop_tx: watch::Sender<bool>,
    stats_tx: broadcast::Sender<JobStats>,
    template_engine: Arc<TemplateEngine>,
}

impl JobManager {
    pub fn new() -> Self {
        let (stop_tx, _) = watch::channel(false);
        let (stats_tx, _) = broadcast::channel(100);

        let shared = Arc::new(SharedState {
            status: RwLock::new(JobStateEnum::Idle),
            sent_count: AtomicU64::new(0),
            failed_count: AtomicU64::new(0),
            target_count: RwLock::new(None),
            current_rate: RwLock::new(0.0),
            start_time: RwLock::new(None),
            paused_duration: RwLock::new(Duration::ZERO),
            last_pause_time: RwLock::new(None),
            last_error: RwLock::new(None),
            recent_messages: RwLock::new(VecDeque::with_capacity(10)),
            is_paused: AtomicBool::new(false),
        });

        let manager = Self {
            shared,
            stop_tx,
            stats_tx,
            template_engine: Arc::new(TemplateEngine::new()),
        };

        manager.spawn_metrics_monitor();
        manager
    }

    pub fn subscribe_stats(&self) -> broadcast::Receiver<JobStats> {
        self.stats_tx.subscribe()
    }

    pub async fn get_stats(&self) -> JobStats {
        let status = self.shared.status.read().await.clone();
        let sent_count = self.shared.sent_count.load(Ordering::Relaxed);
        let failed_count = self.shared.failed_count.load(Ordering::Relaxed);
        let target_count = *self.shared.target_count.read().await;
        let current_rate = *self.shared.current_rate.read().await;
        let last_error = self.shared.last_error.read().await.clone();
        let recent = self.shared.recent_messages.read().await.iter().cloned().collect();

        let elapsed = if let Some(start) = *self.shared.start_time.read().await {
            let total = start.elapsed();
            let paused = *self.shared.paused_duration.read().await;
            if total > paused {
                (total - paused).as_secs_f64()
            } else {
                0.0
            }
        } else {
            0.0
        };

        JobStats {
            status: status.as_str().to_string(),
            sent_count,
            failed_count,
            target_count,
            current_rate,
            elapsed_seconds: elapsed,
            last_error,
            recent_messages: recent,
        }
    }

    fn spawn_metrics_monitor(&self) {
        let shared = Arc::clone(&self.shared);
        let stats_tx = self.stats_tx.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(500));
            let mut last_count = 0u64;
            let mut last_check = Instant::now();

            loop {
                interval.tick().await;

                let current_count = shared.sent_count.load(Ordering::Relaxed);
                let now = Instant::now();
                let duration_secs = now.duration_since(last_check).as_secs_f64();
                
                if duration_secs > 0.0 {
                    let diff = current_count.saturating_sub(last_count);
                    let rate = diff as f64 / duration_secs;
                    let is_running = {
                        let st = shared.status.read().await;
                        *st == JobStateEnum::Running
                    };

                    let calculated_rate = if is_running { rate } else { 0.0 };
                    *shared.current_rate.write().await = calculated_rate;
                }

                last_count = current_count;
                last_check = now;

                // Broadcast stats snapshot
                let status = shared.status.read().await.clone();
                let failed_count = shared.failed_count.load(Ordering::Relaxed);
                let target_count = *shared.target_count.read().await;
                let current_rate = *shared.current_rate.read().await;
                let last_error = shared.last_error.read().await.clone();
                let recent = shared.recent_messages.read().await.iter().cloned().collect();

                let elapsed = if let Some(start) = *shared.start_time.read().await {
                    let total = start.elapsed();
                    let paused = *shared.paused_duration.read().await;
                    if total > paused {
                        (total - paused).as_secs_f64()
                    } else {
                        0.0
                    }
                } else {
                    0.0
                };

                let stats = JobStats {
                    status: status.as_str().to_string(),
                    sent_count: current_count,
                    failed_count,
                    target_count,
                    current_rate,
                    elapsed_seconds: elapsed,
                    last_error,
                    recent_messages: recent,
                };

                let _ = stats_tx.send(stats);
            }
        });
    }

    pub async fn start_job(&self, config: JobConfig) -> Result<(), String> {
        {
            let current_status = self.shared.status.read().await;
            if *current_status == JobStateEnum::Running || *current_status == JobStateEnum::Paused {
                return Err("A job is already running or paused. Please stop it first.".to_string());
            }
        }

        // Test and initialize producer
        info!("Initializing Kafka producer for {}", config.kafka.bootstrap_servers);
        let producer = create_kafka_producer(&config.kafka)?;
        let producer = Arc::new(producer);

        // Reset state
        self.shared.sent_count.store(0, Ordering::Relaxed);
        self.shared.failed_count.store(0, Ordering::Relaxed);
        *self.shared.target_count.write().await = config.total_messages;
        *self.shared.start_time.write().await = Some(Instant::now());
        *self.shared.paused_duration.write().await = Duration::ZERO;
        *self.shared.last_pause_time.write().await = None;
        *self.shared.last_error.write().await = None;
        self.shared.recent_messages.write().await.clear();
        self.shared.is_paused.store(false, Ordering::Relaxed);
        self.template_engine.reset_sequence();

        let _ = self.stop_tx.send(false);
        *self.shared.status.write().await = JobStateEnum::Running;

        let shared_clone = Arc::clone(&self.shared);
        let stop_rx = self.stop_tx.subscribe();
        let engine_clone = Arc::clone(&self.template_engine);

        // Spawn master job worker
        tokio::spawn(async move {
            Self::run_job_loop(shared_clone, engine_clone, producer, config, stop_rx).await;
        });

        Ok(())
    }

    pub async fn pause_job(&self) -> Result<(), String> {
        let mut status = self.shared.status.write().await;
        if *status != JobStateEnum::Running {
            return Err("Cannot pause: Job is not currently running.".to_string());
        }
        self.shared.is_paused.store(true, Ordering::SeqCst);
        *self.shared.last_pause_time.write().await = Some(Instant::now());
        *status = JobStateEnum::Paused;
        info!("Job paused.");
        Ok(())
    }

    pub async fn resume_job(&self) -> Result<(), String> {
        let mut status = self.shared.status.write().await;
        if *status != JobStateEnum::Paused {
            return Err("Cannot resume: Job is not paused.".to_string());
        }

        if let Some(pause_start) = *self.shared.last_pause_time.read().await {
            let mut paused_dur = self.shared.paused_duration.write().await;
            *paused_dur += pause_start.elapsed();
        }
        *self.shared.last_pause_time.write().await = None;
        self.shared.is_paused.store(false, Ordering::SeqCst);
        *status = JobStateEnum::Running;
        info!("Job resumed.");
        Ok(())
    }

    pub async fn stop_job(&self) -> Result<(), String> {
        let mut status = self.shared.status.write().await;
        if *status == JobStateEnum::Idle || *status == JobStateEnum::Stopped || *status == JobStateEnum::Completed {
            return Ok(());
        }

        let _ = self.stop_tx.send(true);
        self.shared.is_paused.store(false, Ordering::SeqCst);
        *status = JobStateEnum::Stopped;
        info!("Job stopped by user.");
        Ok(())
    }

    async fn run_job_loop(
        shared: Arc<SharedState>,
        engine: Arc<TemplateEngine>,
        producer: Arc<FutureProducer>,
        config: JobConfig,
        stop_rx: watch::Receiver<bool>,
    ) {
        info!("Starting Kafka data generation loop. Topic: {}", config.kafka.topic);

        let topic = config.kafka.topic.clone();
        let total_target = config.total_messages;
        let rate_mode = config.rate_limit.mode.clone();
        let rate_value = config.rate_limit.value.max(0.1);

        // Calculate timing delay
        let delay_per_message = if rate_mode == "interval_ms" {
            Duration::from_secs_f64(rate_value / 1000.0)
        } else {
            // "msg_per_sec"
            Duration::from_secs_f64(1.0 / rate_value)
        };

        let mut next_send_time = Instant::now();

        loop {
            // Check stop signal
            if *stop_rx.borrow() {
                break;
            }

            // Check pause
            while shared.is_paused.load(Ordering::Relaxed) {
                if *stop_rx.borrow() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            // Check completion count
            let sent_so_far = shared.sent_count.load(Ordering::Relaxed);
            if let Some(target) = total_target {
                if sent_so_far >= target {
                    let mut st = shared.status.write().await;
                    *st = JobStateEnum::Completed;
                    info!("Target count of {} messages reached. Job completed.", target);
                    break;
                }
            }

            // Generate payload
            let rendered_payload = engine.render(&config.payload_template);

            // Determine message key
            let message_key: Option<String> = match &config.key_strategy {
                KeyStrategy::None => None,
                KeyStrategy::Static(k) => Some(k.clone()),
                KeyStrategy::RandomUuid => Some(Uuid::new_v4().to_string()),
                KeyStrategy::ExtractField(field_name) => {
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&rendered_payload) {
                        v.get(field_name).map(|val| {
                            if let Some(s) = val.as_str() {
                                s.to_string()
                            } else {
                                val.to_string()
                            }
                        })
                    } else {
                        None
                    }
                }
            };

            // Build Kafka Headers
            let mut headers = OwnedHeaders::new();
            for h in &config.headers {
                headers = headers.insert(Header {
                    key: &h.key,
                    value: Some(&h.value),
                });
            }

            // Dispatch to Kafka FutureProducer
            let mut record = FutureRecord::to(&topic)
                .payload(&rendered_payload)
                .headers(headers);

            let key_ref = message_key.as_deref();
            if let Some(k) = key_ref {
                record = record.key(k);
            }

            let send_result = producer.send(record, Duration::from_secs(5)).await;

            match send_result {
                Ok((partition, offset)) => {
                    shared.sent_count.fetch_add(1, Ordering::Relaxed);

                    // Add to recent sample preview (limit 10)
                    let sample = ProducedMessageSample {
                        timestamp: Utc::now().to_rfc3339(),
                        key: message_key,
                        payload_preview: rendered_payload,
                        partition,
                        offset,
                    };

                    let mut recents = shared.recent_messages.write().await;
                    if recents.len() >= 10 {
                        recents.pop_front();
                    }
                    recents.push_back(sample);
                }
                Err((e, _)) => {
                    shared.failed_count.fetch_add(1, Ordering::Relaxed);
                    let err_msg = format!("Delivery error: {}", e);
                    error!("{}", err_msg);
                    *shared.last_error.write().await = Some(err_msg);
                }
            }

            // Rate pacing
            next_send_time += delay_per_message;
            let now = Instant::now();
            if next_send_time > now {
                tokio::time::sleep(next_send_time - now).await;
            } else {
                // If we fell behind, catch up without accumulating infinite debt
                if now.duration_since(next_send_time) > Duration::from_millis(500) {
                    next_send_time = now;
                }
            }
        }
    }
}
