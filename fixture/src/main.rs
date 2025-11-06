use std::net::UdpSocket;
use std::time::Duration;
use std::thread;
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

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct Lap {
    pub m_last_lap_time_in_ms: u32,
    pub m_current_lap_time_in_ms: u32,
    pub m_sector1_time_ms_part: u16,
    pub m_sector1_time_minutes_part: u8,
    pub m_sector2_time_ms_part: u16,
    pub m_sector2_time_minutes_part: u8,
    pub m_delta_to_car_in_front_ms_part: u16,
    pub m_delta_to_car_in_front_minutes_part: u8,
    pub m_delta_to_race_leader_ms_part: u16,
    pub m_delta_to_race_leader_minutes_part: u8,
    pub m_lap_distance: f32,
    pub m_total_distance: f32,
    pub m_safety_car_delta: f32,
    pub m_car_position: u8,
    pub m_current_lap_num: u8,
    pub m_pit_status: u8,
    pub m_num_pit_stops: u8,
    pub m_sector: u8,
    pub m_current_lap_invalid: u8,
    pub m_penalties: u8,
    pub m_total_warnings: u8,
    pub m_corner_cutting_warnings: u8,
    pub m_num_unserved_drive_through_pens: u8,
    pub m_num_unserved_stop_go_pens: u8,
    pub m_grid_position: u8,
    pub m_driver_status: u8,
    pub m_result_status: u8,
    pub m_pit_lane_timer_active: u8,
    pub m_pit_lane_time_in_lane_in_ms: u16,
    pub m_pit_stop_timer_in_ms: u16,
    pub m_pit_stop_should_serve_pen: u8,
    pub m_speed_trap_fastest_speed: f32,
    pub m_speed_trap_fastest_lap: u8,
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct PacketLap {
    pub m_header: PacketHeader,
    pub m_lap_data: [Lap; 22],
    pub m_time_trial_pb_car_idx: u8,
    pub m_time_trial_rival_car_idx: u8,
}

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
    pub m_header: PacketHeader,
    pub m_num_active_cars: u8,
    pub m_participants: [Participant; 22],
}

#[repr(C, packed)]
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
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
#[derive(Clone, Copy, Debug, Zeroable, Pod)]
pub struct PacketLobbyInfo {
    pub m_header: PacketHeader,
    pub m_num_players: u8,
    pub m_lobby_players: [LobbyInfo; 22],
}

fn main() {
    let socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    
    // Send lobby info packet once at start
    let lobby_packet = PacketLobbyInfo {
        m_header: PacketHeader {
            m_packet_format: 2025,
            m_game_year: 25,
            m_game_major_version: 1,
            m_game_minor_version: 0,
            m_packet_version: 1,
            m_packet_id: 9,
            m_session_uid: 12345,
            m_session_time: 0.0,
            m_frame_identifier: 1,
            m_overall_frame_identifier: 1,
            m_player_car_index: 0,
            m_secondary_player_car_index: 255,
        },
        m_num_players: 1,
        m_lobby_players: [LobbyInfo {
            m_ai_controlled: 0,
            m_team_id: 0,
            m_nationality: 0,
            m_platform: 0,
            m_name: [0; 32],
            m_car_number: 0,
            m_your_telemetry: 1,
            m_show_online_names: 1,
            m_tech_level: 0,
            m_ready_status: 1,
        }; 22],
    };
    socket.send_to(bytemuck::bytes_of(&lobby_packet), "127.0.0.1:20777").unwrap();
    println!("sent lobby info packet");

    let mut car_telemetry_timer = 0;
    let mut lap_timer = 0;
    let mut participants_timer = 0;

    loop {
        // Car telemetry every 3 seconds
        if car_telemetry_timer % 3 == 0 {
            let car_packet = PacketCarTelemetry {
                m_header: PacketHeader {
                    m_packet_format: 2025,
                    m_game_year: 25,
                    m_game_major_version: 1,
                    m_game_minor_version: 0,
                    m_packet_version: 1,
                    m_packet_id: 6,
                    m_session_uid: 12345,
                    m_session_time: car_telemetry_timer as f32,
                    m_frame_identifier: car_telemetry_timer,
                    m_overall_frame_identifier: car_telemetry_timer,
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
            socket.send_to(bytemuck::bytes_of(&car_packet), "127.0.0.1:20777").unwrap();
            println!("sent car telemetry packet");
        }

        // Lap packet every 60 seconds
        if lap_timer % 60 == 0 && lap_timer > 0 {
            let lap_packet = PacketLap {
                m_header: PacketHeader {
                    m_packet_format: 2025,
                    m_game_year: 25,
                    m_game_major_version: 1,
                    m_game_minor_version: 0,
                    m_packet_version: 1,
                    m_packet_id: 2,
                    m_session_uid: 12345,
                    m_session_time: lap_timer as f32,
                    m_frame_identifier: lap_timer,
                    m_overall_frame_identifier: lap_timer,
                    m_player_car_index: 0,
                    m_secondary_player_car_index: 255,
                },
                m_lap_data: [Lap {
                    m_last_lap_time_in_ms: 75000,
                    m_current_lap_time_in_ms: 15000,
                    m_sector1_time_ms_part: 500,
                    m_sector1_time_minutes_part: 0,
                    m_sector2_time_ms_part: 750,
                    m_sector2_time_minutes_part: 0,
                    m_delta_to_car_in_front_ms_part: 100,
                    m_delta_to_car_in_front_minutes_part: 0,
                    m_delta_to_race_leader_ms_part: 1500,
                    m_delta_to_race_leader_minutes_part: 0,
                    m_lap_distance: 1250.0,
                    m_total_distance: 25000.0,
                    m_safety_car_delta: 0.0,
                    m_car_position: 1,
                    m_current_lap_num: 5,
                    m_pit_status: 0,
                    m_num_pit_stops: 1,
                    m_sector: 2,
                    m_current_lap_invalid: 0,
                    m_penalties: 0,
                    m_total_warnings: 0,
                    m_corner_cutting_warnings: 0,
                    m_num_unserved_drive_through_pens: 0,
                    m_num_unserved_stop_go_pens: 0,
                    m_grid_position: 1,
                    m_driver_status: 4,
                    m_result_status: 2,
                    m_pit_lane_timer_active: 0,
                    m_pit_lane_time_in_lane_in_ms: 0,
                    m_pit_stop_timer_in_ms: 0,
                    m_pit_stop_should_serve_pen: 0,
                    m_speed_trap_fastest_speed: 320.5,
                    m_speed_trap_fastest_lap: 3,
                }; 22],
                m_time_trial_pb_car_idx: 255,
                m_time_trial_rival_car_idx: 255,
            };
            socket.send_to(bytemuck::bytes_of(&lap_packet), "127.0.0.1:20777").unwrap();
            println!("sent lap packet");
        }

        // Participants packet every 60 seconds
        if participants_timer % 60 == 0 && participants_timer > 0 {
            let participants_packet = PacketParticipants {
                m_header: PacketHeader {
                    m_packet_format: 2025,
                    m_game_year: 25,
                    m_game_major_version: 1,
                    m_game_minor_version: 0,
                    m_packet_version: 1,
                    m_packet_id: 4,
                    m_session_uid: 12345,
                    m_session_time: participants_timer as f32,
                    m_frame_identifier: participants_timer,
                    m_overall_frame_identifier: participants_timer,
                    m_player_car_index: 0,
                    m_secondary_player_car_index: 255,
                },
                m_num_active_cars: 20,
                m_participants: [Participant {
                    m_ai_controlled: 0,
                    m_driver_id: 0,
                    m_network_id: 0,
                    m_team_id: 0,
                    m_my_team: 1,
                    m_race_number: 44,
                    m_nationality: 0,
                    m_name: [0; 32],
                    m_your_telemetry: 1,
                    m_show_online_names: 1,
                    m_tech_level: 0,
                    m_platform: 0,
                    m_num_colours: 0,
                    m_livery_colours: [LiveryColour { red: 0, green: 0, blue: 255 }; 4],
                }; 22],
            };
            socket.send_to(bytemuck::bytes_of(&participants_packet), "127.0.0.1:20777").unwrap();
            println!("sent participants packet");
        }

        thread::sleep(Duration::from_secs(1));
        car_telemetry_timer += 1;
        lap_timer += 1;
        participants_timer += 1;
    }
}
