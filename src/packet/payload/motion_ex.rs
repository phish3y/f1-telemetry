use bytemuck::{Pod, Zeroable};
use serde::Serialize;

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct PacketMotionEx {
    pub m_header: header::PacketHeader,
    pub m_suspension_position: [f32; 4],
    pub m_suspension_velocity: [f32; 4],
    pub m_suspension_acceleration: [f32; 4],
    pub m_wheel_speed: [f32; 4],
    pub m_wheel_slip_ratio: [f32; 4],
    pub m_wheel_slip_angle: [f32; 4],
    pub m_wheel_lat_force: [f32; 4],
    pub m_wheel_long_force: [f32; 4],
    pub m_height_of_cog_above_ground: f32,
    pub m_local_velocity_x: f32,
    pub m_local_velocity_y: f32,
    pub m_local_velocity_z: f32,
    pub m_angular_velocity_x: f32,
    pub m_angular_velocity_y: f32,
    pub m_angular_velocity_z: f32,
    pub m_angular_acceleration_x: f32,
    pub m_angular_acceleration_y: f32,
    pub m_angular_acceleration_z: f32,
    pub m_front_wheels_angle: f32,
    pub m_wheel_vert_force: [f32; 4],
    pub m_front_aero_height: f32,
    pub m_rear_aero_height: f32,
    pub m_front_roll_angle: f32,
    pub m_rear_roll_angle: f32,
    pub m_chassis_yaw: f32,
    pub m_chassis_pitch: f32,
    pub m_wheel_camber: [f32; 4],
    pub m_wheel_camber_gain: [f32; 4],
}
