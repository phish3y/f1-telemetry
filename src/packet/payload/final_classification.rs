use bytemuck::{Pod, Zeroable};
use serde::Serialize;

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct FinalClassification {
    pub m_position: u8,
    pub m_num_laps: u8,
    pub m_grid_position: u8,
    pub m_points: u8,
    pub m_num_pit_stops: u8,
    pub m_result_status: u8,
    pub m_result_reason: u8,
    pub m_best_lap_time_in_ms: u32,
    pub m_total_race_time: f64,
    pub m_penalties_time: u8,
    pub m_num_penalties: u8,
    pub m_num_tyre_stints: u8,
    pub m_tyre_stints_actual: [u8; 8],
    pub m_tyre_stints_visual: [u8; 8],
    pub m_tyre_stints_end_laps: [u8; 8],
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct PacketFinalClassification {
    pub m_header: header::PacketHeader,
    pub m_num_cars: u8,
    pub m_classification_data: [FinalClassification; 22],
}
