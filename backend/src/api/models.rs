use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct PreviewRequest {
    pub template: String,
    pub count: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct PreviewResponse {
    pub samples: Vec<String>,
    pub is_valid_json: bool,
}

#[derive(Debug, Deserialize)]
pub struct TestConnectionRequest {
    pub bootstrap_servers: String,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub message: String,
    pub data: Option<T>,
}

impl<T> ApiResponse<T> {
    pub fn ok(data: T) -> Self {
        Self {
            success: true,
            message: "Success".to_string(),
            data: Some(data),
        }
    }
}

impl ApiResponse<()> {
    pub fn ok_msg(message: impl Into<String>) -> Self {
        Self {
            success: true,
            message: message.into(),
            data: None,
        }
    }

    pub fn err(message: impl Into<String>) -> Self {
        Self {
            success: false,
            message: message.into(),
            data: None,
        }
    }
}

