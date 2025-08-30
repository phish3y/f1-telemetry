use bytemuck::{Pod, Zeroable};

use crate::packet::header;

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct FastestLap {
    pub vehicle_idx: u8,
    pub lap_time: f32,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct Retirement {
    pub vehicle_idx: u8,
    pub reason: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct DrsDisabled {
    pub reason: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct TeamMateInPits {
    pub vehicle_idx: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct RaceWinner {
    pub vehicle_idx: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct Penalty {
    pub penalty_type: u8,
    pub infringement_type: u8,
    pub vehicle_idx: u8,
    pub other_vehicle_idx: u8,
    pub time: u8,
    pub lap_num: u8,
    pub places_gained: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct SpeedTrap {
    pub vehicle_idx: u8,
    pub speed: f32,
    pub is_overall_fastest_in_session: u8,
    pub is_driver_fastest_in_session: u8,
    pub fastest_vehicle_idx_in_session: u8,
    pub fastest_speed_in_session: f32,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct StartLights {
    pub num_lights: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct DriveThroughPenaltyServed {
    pub vehicle_idx: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct StopGoPenaltyServed {
    pub vehicle_idx: u8,
    pub stop_time: f32,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct Flashback {
    pub flashback_frame_identifier: u32,
    pub flashback_session_time: f32,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct Buttons {
    pub button_status: u32,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct Overtake {
    pub overtaking_vehicle_idx: u8,
    pub being_overtaken_vehicle_idx: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct SafetyCar {
    pub safety_car_type: u8,
    pub event_type: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct Collision {
    pub vehicle1_idx: u8,
    pub vehicle2_idx: u8,
}

// TODO serde
#[repr(C)]
#[derive(Clone, Copy)]
pub union EventDetails {
    pub fastest_lap: FastestLap,
    pub retirement: Retirement,
    pub drs_disabled: DrsDisabled,
    pub team_mate_in_pits: TeamMateInPits,
    pub race_winner: RaceWinner,
    pub penalty: Penalty,
    pub speed_trap: SpeedTrap,
    pub start_lights: StartLights,
    pub drive_through_penalty_served: DriveThroughPenaltyServed,
    pub stop_go_penalty_served: StopGoPenaltyServed,
    pub flashback: Flashback,
    pub buttons: Buttons,
    pub overtake: Overtake,
    pub safety_car: SafetyCar,
    pub collision: Collision,
    pub raw: [u8; 16],
}

impl std::fmt::Debug for EventDetails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventDetails").finish()
    }
}

unsafe impl Zeroable for EventDetails {}
unsafe impl Pod for EventDetails {}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct PacketEvent {
    pub m_header: header::PacketHeader,
    pub m_event_string_code: [u8; 4],
    pub m_event_details: EventDetails,
}
