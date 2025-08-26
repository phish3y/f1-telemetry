use bytemuck::{Pod, Zeroable};

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct TyreSet {
    pub m_actual_tyre_compound: u8,
    pub m_visual_tyre_compound: u8,
    pub m_wear: u8,
    pub m_available: u8,
    pub m_recommended_session: u8,
    pub m_life_span: u8,
    pub m_usable_life: u8,
    pub m_lap_delta_time: i16,
    pub m_fitted: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct PacketTyreSets {
    pub m_header: header::PacketHeader,
    pub m_car_idx: u8,
    pub m_tyre_set_data: [TyreSet; 20], // 13 slick + 7 wet weather
    pub m_fitted_idx: u8,
}
