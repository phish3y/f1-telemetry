use bytemuck::{Pod, Zeroable};
use serde::Serialize;

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct CarSetup {
    pub m_front_wing: u8,
    pub m_rear_wing: u8,
    pub m_on_throttle: u8,
    pub m_off_throttle: u8,
    pub m_front_camber: f32,
    pub m_rear_camber: f32,
    pub m_front_toe: f32,
    pub m_rear_toe: f32,
    pub m_front_suspension: u8,
    pub m_rear_suspension: u8,
    pub m_front_anti_roll_bar: u8,
    pub m_rear_anti_roll_bar: u8,
    pub m_front_suspension_height: u8,
    pub m_rear_suspension_height: u8,
    pub m_brake_pressure: u8,
    pub m_brake_bias: u8,
    pub m_engine_braking: u8,
    pub m_rear_left_tyre_pressure: f32,
    pub m_rear_right_tyre_pressure: f32,
    pub m_front_left_tyre_pressure: f32,
    pub m_front_right_tyre_pressure: f32,
    pub m_ballast: u8,
    pub m_fuel_load: f32,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct PacketCarSetups {
    pub m_header: header::PacketHeader,
    pub m_car_setup_data: [CarSetup; 22],
    pub m_next_front_wing_value: f32,
}
