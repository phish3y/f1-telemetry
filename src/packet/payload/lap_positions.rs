use bytemuck::{Pod, Zeroable};

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug)]
pub struct PacketLapPositions {
    pub m_header: header::PacketHeader,
    pub m_num_laps: u8,
    pub m_lap_start: u8,
    pub m_position_for_vehicle_idx: [[u8; 22]; 50],
}

unsafe impl Zeroable for PacketLapPositions {}
unsafe impl Pod for PacketLapPositions {}
