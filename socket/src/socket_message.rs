use serde::{Deserialize, Serialize};
use crate::aggregation::{speed_aggregation::SpeedAggregation, rpm_aggregation::RPMAggregation};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum SocketMessage {
    #[serde(rename = "speed")]
    Speed(SpeedAggregation),
    #[serde(rename = "rpm")]
    Rpm(RPMAggregation),
}
