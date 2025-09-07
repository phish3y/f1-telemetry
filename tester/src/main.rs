use std::net::UdpSocket;
use bytemuck::{Pod, Zeroable};

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct PacketHeader {
    pub m_packet_format: u16,
    pub m_game_year: u8,
    pub m_game_major_version: u8,
    pub m_game_minor_version: u8,
    pub m_packet_version: u8,
    pub m_packet_id: u8,
    pub m_session_uid: u64,
    pub m_session_time: f32,
    pub m_frame_identifier: u32,
    pub m_overall_frame_identifier: u32,
    pub m_player_car_index: u8,
    pub m_secondary_player_car_index: u8,
}

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
    pub m_header: PacketHeader,
    pub m_car_telemetry_data: [CarTelemetry; 22],
    pub m_mfd_panel_index: u8,
    pub m_mfd_panel_index_secondary_player: u8,
    pub m_suggested_gear: i8,
}

fn main() {
    let packet = PacketCarTelemetry {
        m_header: PacketHeader {
            m_packet_format: 2025,
            m_game_year: 25,
            m_game_major_version: 1,
            m_game_minor_version: 0,
            m_packet_version: 1,
            m_packet_id: 6,
            m_session_uid: 12345,
            m_session_time: 123.45,
            m_frame_identifier: 1000,
            m_overall_frame_identifier: 1000,
            m_player_car_index: 0,
            m_secondary_player_car_index: 255,
        },
        m_car_telemetry_data: [CarTelemetry {
            m_speed: 250,
            m_throttle: 0.8,
            m_steer: 0.0,
            m_brake: 0.0,
            m_clutch: 0,
            m_gear: 7,
            m_engine_rpm: 10500,
            m_drs: 1,
            m_rev_lights_percent: 85,
            m_rev_lights_bit_value: 0b1111100000000000,
            m_brakes_temperature: [450, 450, 420, 420],
            m_tyres_surface_temperature: [95, 95, 90, 90],
            m_tyres_inner_temperature: [105, 105, 100, 100],
            m_engine_temperature: 95,
            m_tyres_pressure: [23.5, 23.5, 23.0, 23.0],
            m_surface_type: [0, 0, 0, 0],
        }; 22],
        m_mfd_panel_index: 255,
        m_mfd_panel_index_secondary_player: 255,
        m_suggested_gear: 7,
    };

    let packet_bytes = bytemuck::bytes_of(&packet);

    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    socket.send_to(packet_bytes, "127.0.0.1:20777").unwrap();
}
