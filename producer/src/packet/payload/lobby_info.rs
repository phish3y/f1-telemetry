use bytemuck::{Pod, Zeroable};
use serde::Serialize;

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct LobbyInfo {
    pub m_ai_controlled: u8,
    pub m_team_id: u8,
    pub m_nationality: u8,
    pub m_platform: u8,
    pub m_name: [u8; 32],
    pub m_car_number: u8,
    pub m_your_telemetry: u8,
    pub m_show_online_names: u8,
    pub m_tech_level: u16,
    pub m_ready_status: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct PacketLobbyInfo {
    pub m_header: header::PacketHeader,
    pub m_num_players: u8,
    pub m_lobby_players: [LobbyInfo; 22],
}
