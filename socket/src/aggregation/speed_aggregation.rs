use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedAggregation {
    pub window_start: String,
    pub window_end: String,
    pub session_uid: i64,
    pub avg_speed: f64,
    pub min_speed: i32,
    pub max_speed: i32,
    pub sample_count: i64,
}
