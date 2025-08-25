use bytemuck::{Pod, Zeroable};

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct LiveryColour {
    pub red: u8,
    pub green: u8,
    pub blue: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct Participant {
    pub m_ai_controlled: u8,
    pub m_driver_id: u8,
    pub m_network_id: u8,
    pub m_team_id: u8,
    pub m_my_team: u8,
    pub m_race_number: u8,
    pub m_nationality: u8,
    pub m_name: [u8; 32],
    pub m_your_telemetry: u8,
    pub m_show_online_names: u8,
    pub m_tech_level: u16,
    pub m_platform: u8,
    pub m_num_colours: u8,
    pub m_livery_colours: [LiveryColour; 4],
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct PacketParticipants {
    pub m_header: header::PacketHeader,
    pub m_num_active_cars: u8,
    pub m_participants: [Participant; 22],
}
