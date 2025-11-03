use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RPMAggregation {
    pub window_start: String,
    pub window_end: String,
    pub session_uid: i64,
    pub avg_rpm: f64,
    pub min_rpm: i32,
    pub max_rpm: i32,
    pub sample_count: i64,
}
