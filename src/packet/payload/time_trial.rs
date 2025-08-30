use bytemuck::{Pod, Zeroable};
use serde::Serialize;

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct TimeTrialSet {
    pub m_car_idx: u8,
    pub m_team_id: u8,
    pub m_lap_time_in_ms: u32,
    pub m_sector1_time_in_ms: u32,
    pub m_sector2_time_in_ms: u32,
    pub m_sector3_time_in_ms: u32,
    pub m_traction_control: u8,
    pub m_gearbox_assist: u8,
    pub m_anti_lock_brakes: u8,
    pub m_equal_car_performance: u8,
    pub m_custom_setup: u8,
    pub m_valid: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct PacketTimeTrial {
    pub m_header: header::PacketHeader,
    pub m_player_session_best_data_set: TimeTrialSet,
    pub m_personal_best_data_set: TimeTrialSet,
    pub m_rival_data_set: TimeTrialSet,
}
