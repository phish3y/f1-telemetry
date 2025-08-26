use bytemuck::{Pod, Zeroable};

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct CarTelemetry {
    pub m_speed: u16,
    pub m_throttle: f32,
    pub m_steer: f32,
    pub m_brake: f32,
    pub m_clutch: u8,
    pub m_gear: i8,
    pub m_engine_rpm: u16,
    pub m_drs: u8,
    pub m_rev_lights_percent: u8,
    pub m_rev_lights_bit_value: u16,
    pub m_brakes_temperature: [u16; 4],
    pub m_tyres_surface_temperature: [u8; 4],
    pub m_tyres_inner_temperature: [u8; 4],
    pub m_engine_temperature: u16,
    pub m_tyres_pressure: [f32; 4],
    pub m_surface_type: [u8; 4],
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct PacketCarTelemetry {
    pub m_header: header::PacketHeader,
    pub m_car_telemetry_data: [CarTelemetry; 22],
    pub m_mfd_panel_index: u8,
    pub m_mfd_panel_index_secondary_player: u8,
    pub m_suggested_gear: i8,
}
