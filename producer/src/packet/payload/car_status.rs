use bytemuck::{Pod, Zeroable};
use serde::Serialize;

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct CarStatus {
    pub m_traction_control: u8,
    pub m_anti_lock_brakes: u8,
    pub m_fuel_mix: u8,
    pub m_front_brake_bias: u8,
    pub m_pit_limiter_status: u8,
    pub m_fuel_in_tank: f32,
    pub m_fuel_capacity: f32,
    pub m_fuel_remaining_laps: f32,
    pub m_max_rpm: u16,
    pub m_idle_rpm: u16,
    pub m_max_gears: u8,
    pub m_drs_allowed: u8,
    pub m_drs_activation_distance: u16,
    pub m_actual_tyre_compound: u8,
    pub m_visual_tyre_compound: u8,
    pub m_tyres_age_laps: u8,
    pub m_vehicle_fia_flags: i8,
    pub m_engine_power_ice: f32,
    pub m_engine_power_mguk: f32,
    pub m_ers_store_energy: f32,
    pub m_ers_deploy_mode: u8,
    pub m_ers_harvested_this_lap_mguk: f32,
    pub m_ers_harvested_this_lap_mguh: f32,
    pub m_ers_deployed_this_lap: f32,
    pub m_network_paused: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct PacketCarStatus {
    pub m_header: header::PacketHeader,
    pub m_car_status_data: [CarStatus; 22],
}
