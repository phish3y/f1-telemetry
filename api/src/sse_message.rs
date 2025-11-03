use crate::aggregation::{rpm_aggregation::RPMAggregation, speed_aggregation::SpeedAggregation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum SSEMessage {
    #[serde(rename = "speed")]
    Speed(SpeedAggregation),
    #[serde(rename = "rpm")]
    Rpm(RPMAggregation),
}
