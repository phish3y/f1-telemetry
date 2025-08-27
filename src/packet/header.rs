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

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
pub enum PacketId {
    Motion = 0,
    Session = 1,
    LapData = 2,
    Event = 3,
    Participants = 4,
    CarSetups = 5,
    CarTelemetry = 6,
    CarStatus = 7,
    FinalClassification = 8,
    LobbyInfo = 9,
    CarDamage = 10,
    SessionHistory = 11,
    TyreSets = 12,
    MotionEx = 13,
    TimeTrial = 14,
    LapPositions = 15,
}

impl TryFrom<u8> for PacketId {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(PacketId::Motion),
            1 => Ok(PacketId::Session),
            2 => Ok(PacketId::LapData),
            3 => Ok(PacketId::Event),
            4 => Ok(PacketId::Participants),
            5 => Ok(PacketId::CarSetups),
            6 => Ok(PacketId::CarTelemetry),
            7 => Ok(PacketId::CarStatus),
            8 => Ok(PacketId::FinalClassification),
            9 => Ok(PacketId::LobbyInfo),
            10 => Ok(PacketId::CarDamage),
            11 => Ok(PacketId::SessionHistory),
            12 => Ok(PacketId::TyreSets),
            13 => Ok(PacketId::MotionEx),
            14 => Ok(PacketId::TimeTrial),
            15 => Ok(PacketId::LapPositions),
            _ => Err(()),
        }
    }
}
