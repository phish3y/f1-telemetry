use bytemuck::{Pod, Zeroable};
use serde::Serialize;

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct CarMotion {
    pub m_world_position_x: f32,
    pub m_world_position_y: f32,
    pub m_world_position_z: f32,

    pub m_world_velocity_x: f32,
    pub m_world_velocity_y: f32,
    pub m_world_velocity_z: f32,

    pub m_world_forward_dir_x: i16,
    pub m_world_forward_dir_y: i16,
    pub m_world_forward_dir_z: i16,

    pub m_world_right_dir_x: i16,
    pub m_world_right_dir_y: i16,
    pub m_world_right_dir_z: i16,

    pub m_g_force_lateral: f32,
    pub m_g_force_longitudinal: f32,
    pub m_g_force_vertical: f32,

    pub m_yaw: f32,
    pub m_pitch: f32,
    pub m_roll: f32,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct PacketMotion {
    pub m_header: header::PacketHeader,
    pub m_car_motion_data: [CarMotion; 22],
}
