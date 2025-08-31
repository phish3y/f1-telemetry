use bytemuck::{Pod, Zeroable};
use serde::Serialize;

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct Lap {
    pub m_last_lap_time_in_ms: u32,
    pub m_current_lap_time_in_ms: u32,
    pub m_sector1_time_ms_part: u16,
    pub m_sector1_time_minutes_part: u8,
    pub m_sector2_time_ms_part: u16,
    pub m_sector2_time_minutes_part: u8,
    pub m_delta_to_car_in_front_ms_part: u16,
    pub m_delta_to_car_in_front_minutes_part: u8,
    pub m_delta_to_race_leader_ms_part: u16,
    pub m_delta_to_race_leader_minutes_part: u8,
    pub m_lap_distance: f32,
    pub m_total_distance: f32,
    pub m_safety_car_delta: f32,
    pub m_car_position: u8,
    pub m_current_lap_num: u8,
    pub m_pit_status: u8,
    pub m_num_pit_stops: u8,
    pub m_sector: u8,
    pub m_current_lap_invalid: u8,
    pub m_penalties: u8,
    pub m_total_warnings: u8,
    pub m_corner_cutting_warnings: u8,
    pub m_num_unserved_drive_through_pens: u8,
    pub m_num_unserved_stop_go_pens: u8,
    pub m_grid_position: u8,
    pub m_driver_status: u8,
    pub m_result_status: u8,
    pub m_pit_lane_timer_active: u8,
    pub m_pit_lane_time_in_lane_in_ms: u16,
    pub m_pit_stop_timer_in_ms: u16,
    pub m_pit_stop_should_serve_pen: u8,
    pub m_speed_trap_fastest_speed: f32,
    pub m_speed_trap_fastest_lap: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct PacketLap {
    pub m_header: header::PacketHeader,
    pub m_lap_data: [Lap; 22],
    pub m_time_trial_pb_car_idx: u8,
    pub m_time_trial_rival_car_idx: u8,
}
