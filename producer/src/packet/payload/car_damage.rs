use bytemuck::{Pod, Zeroable};
use serde::Serialize;

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct CarDamage {
    pub m_tyres_wear: [f32; 4],
    pub m_tyres_damage: [u8; 4],
    pub m_brakes_damage: [u8; 4],
    pub m_tyre_blisters: [u8; 4],
    pub m_front_left_wing_damage: u8,
    pub m_front_right_wing_damage: u8,
    pub m_rear_wing_damage: u8,
    pub m_floor_damage: u8,
    pub m_diffuser_damage: u8,
    pub m_sidepod_damage: u8,
    pub m_drs_fault: u8,
    pub m_ers_fault: u8,
    pub m_gear_box_damage: u8,
    pub m_engine_damage: u8,
    pub m_engine_mguh_wear: u8,
    pub m_engine_es_wear: u8,
    pub m_engine_ce_wear: u8,
    pub m_engine_ice_wear: u8,
    pub m_engine_mguk_wear: u8,
    pub m_engine_tc_wear: u8,
    pub m_engine_blown: u8,
    pub m_engine_seized: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod, Serialize)]
pub struct PacketCarDamage {
    pub m_header: header::PacketHeader,
    pub m_car_damage_data: [CarDamage; 22],
}
