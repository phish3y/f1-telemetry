use bytemuck::{Pod, Zeroable};

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct PacketMotionEx {
    pub m_header: header::PacketHeader,
    pub m_suspension_position: [f32; 4],        // RL, RR, FL, FR
    pub m_suspension_velocity: [f32; 4],        // RL, RR, FL, FR
    pub m_suspension_acceleration: [f32; 4],    // RL, RR, FL, FR
    pub m_wheel_speed: [f32; 4],                // Speed of each wheel
    pub m_wheel_slip_ratio: [f32; 4],           // Slip ratio for each wheel
    pub m_wheel_slip_angle: [f32; 4],           // Slip angles for each wheel
    pub m_wheel_lat_force: [f32; 4],            // Lateral forces for each wheel
    pub m_wheel_long_force: [f32; 4],           // Longitudinal forces for each wheel
    pub m_height_of_cog_above_ground: f32,      // Height of centre of gravity above ground
    pub m_local_velocity_x: f32,                // Velocity in local space X - metres/s
    pub m_local_velocity_y: f32,                // Velocity in local space Y
    pub m_local_velocity_z: f32,                // Velocity in local space Z
    pub m_angular_velocity_x: f32,              // Angular velocity x-component - radians/s
    pub m_angular_velocity_y: f32,              // Angular velocity y-component
    pub m_angular_velocity_z: f32,              // Angular velocity z-component
    pub m_angular_acceleration_x: f32,          // Angular acceleration x-component - radians/s/s
    pub m_angular_acceleration_y: f32,          // Angular acceleration y-component
    pub m_angular_acceleration_z: f32,          // Angular acceleration z-component
    pub m_front_wheels_angle: f32,              // Current front wheels angle in radians
    pub m_wheel_vert_force: [f32; 4],           // Vertical forces for each wheel
    pub m_front_aero_height: f32,               // Front plank edge height above road surface
    pub m_rear_aero_height: f32,                // Rear plank edge height above road surface
    pub m_front_roll_angle: f32,                // Roll angle of the front suspension
    pub m_rear_roll_angle: f32,                 // Roll angle of the rear suspension
    pub m_chassis_yaw: f32,                     // Yaw angle of the chassis relative to the direction of motion - radians
    pub m_chassis_pitch: f32,                   // Pitch angle of the chassis relative to the direction of motion - radians
    pub m_wheel_camber: [f32; 4],               // Camber of each wheel in radians
    pub m_wheel_camber_gain: [f32; 4],          // Camber gain for each wheel in radians
}
