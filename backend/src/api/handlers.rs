use crate::api::models::{ApiResponse, PreviewRequest, PreviewResponse, TestConnectionRequest};
use crate::generator::TemplateEngine;
use crate::job::{JobConfig, JobManager, JobStats};
use crate::kafka::{test_kafka_connection, KafkaConnectionResult};
use axum::{
    extract::State,
    http::{header, Method, StatusCode},
    response::sse::{Event, KeepAlive, Sse},
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use futures_util::stream::Stream;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;
use tracing::info;

pub struct AppState {
    pub job_manager: Arc<JobManager>,
    pub template_engine: Arc<TemplateEngine>,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([header::CONTENT_TYPE, header::AUTHORIZATION]);

    Router::new()
        .route("/health", get(health_check))
        .route("/api/kafka/test-connection", post(test_connection))
        .route("/api/template/preview", post(preview_template))
        .route("/api/jobs/start", post(start_job))
        .route("/api/jobs/pause", post(pause_job))
        .route("/api/jobs/resume", post(resume_job))
        .route("/api/jobs/stop", post(stop_job))
        .route("/api/jobs/status", get(get_job_status))
        .route("/api/jobs/stream", get(stream_job_stats))
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn health_check() -> impl IntoResponse {
    Json(ApiResponse::ok("Kafka Data Faker Backend is Healthy"))
}

async fn test_connection(
    Json(req): Json<TestConnectionRequest>,
) -> impl IntoResponse {
    let result = test_kafka_connection(&req.bootstrap_servers);
    Json(ApiResponse::ok(result))
}

async fn preview_template(
    State(state): State<Arc<AppState>>,
    Json(req): Json<PreviewRequest>,
) -> impl IntoResponse {
    let count = req.count.unwrap_or(3).min(10);
    let mut samples = Vec::new();
    let mut is_valid_json = true;

    for _ in 0..count {
        let rendered = state.template_engine.render(&req.template);
        if is_valid_json && serde_json::from_str::<serde_json::Value>(&rendered).is_err() {
            is_valid_json = false;
        }
        samples.push(rendered);
    }

    Json(ApiResponse::ok(PreviewResponse {
        samples,
        is_valid_json,
    }))
}

async fn start_job(
    State(state): State<Arc<AppState>>,
    Json(config): Json<JobConfig>,
) -> impl IntoResponse {
    match state.job_manager.start_job(config).await {
        Ok(_) => (
            StatusCode::OK,
            Json(ApiResponse::ok_msg("Job started successfully")),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::err(e)),
        ),
    }
}

async fn pause_job(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match state.job_manager.pause_job().await {
        Ok(_) => (
            StatusCode::OK,
            Json(ApiResponse::ok_msg("Job paused successfully")),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::err(e)),
        ),
    }
}

async fn resume_job(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match state.job_manager.resume_job().await {
        Ok(_) => (
            StatusCode::OK,
            Json(ApiResponse::ok_msg("Job resumed successfully")),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::err(e)),
        ),
    }
}

async fn stop_job(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    match state.job_manager.stop_job().await {
        Ok(_) => (
            StatusCode::OK,
            Json(ApiResponse::ok_msg("Job stopped successfully")),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::err(e)),
        ),
    }
}

async fn get_job_status(
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    let stats = state.job_manager.get_stats().await;
    Json(ApiResponse::ok(stats))
}

async fn stream_job_stats(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.job_manager.subscribe_stats();
    let stream = BroadcastStream::new(rx).filter_map(|item| match item {
        Ok(stats) => {
            let json_str = serde_json::to_string(&stats).unwrap_or_default();
            Some(Ok(Event::default().event("stats").data(json_str)))
        }
        Err(_) => None,
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(5))
            .text("keep-alive"),
    )
}
