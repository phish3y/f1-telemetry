use bytemuck::{Pod, Zeroable};

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct LapHistory {
    pub m_lap_time_in_ms: u32,
    pub m_sector1_time_ms_part: u16,
    pub m_sector1_time_minutes_part: u8,
    pub m_sector2_time_ms_part: u16,
    pub m_sector2_time_minutes_part: u8,
    pub m_sector3_time_ms_part: u16,
    pub m_sector3_time_minutes_part: u8,
    pub m_lap_valid_bit_flags: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct TyreStintHistory {
    pub m_end_lap: u8,
    pub m_tyre_actual_compound: u8,
    pub m_tyre_visual_compound: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug)]
pub struct PacketSessionHistory {
    pub m_header: header::PacketHeader,
    pub m_car_idx: u8,
    pub m_num_laps: u8,
    pub m_num_tyre_stints: u8,
    pub m_best_lap_time_lap_num: u8,
    pub m_best_sector1_lap_num: u8,
    pub m_best_sector2_lap_num: u8,
    pub m_best_sector3_lap_num: u8,
    pub m_lap_history_data: [LapHistory; 100],
    pub m_tyre_stints_history_data: [TyreStintHistory; 8],
}

unsafe impl Zeroable for PacketSessionHistory {}
unsafe impl Pod for PacketSessionHistory {}
