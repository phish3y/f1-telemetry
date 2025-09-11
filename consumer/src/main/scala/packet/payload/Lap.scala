package packet.payload

import packet.Header

case class Lap(
    m_last_lap_time_in_ms: Long,
    m_current_lap_time_in_ms: Long,
    m_sector1_time_ms_part: Int,
    m_sector1_time_minutes_part: Int,
    m_sector2_time_ms_part: Int,
    m_sector2_time_minutes_part: Int,
    m_delta_to_car_in_front_ms_part: Int,
    m_delta_to_car_in_front_minutes_part: Int,
    m_delta_to_race_leader_ms_part: Int,
    m_delta_to_race_leader_minutes_part: Int,
    m_lap_distance: Float,
    m_total_distance: Float,
    m_safety_car_delta: Float,
    m_car_position: Int,
    m_current_lap_num: Int,
    m_pit_status: Int,
    m_num_pit_stops: Int,
    m_sector: Int,
    m_current_lap_invalid: Int,
    m_penalties: Int,
    m_total_warnings: Int,
    m_corner_cutting_warnings: Int,
    m_num_unserved_drive_through_pens: Int,
    m_num_unserved_stop_go_pens: Int,
    m_grid_position: Int,
    m_driver_status: Int,
    m_result_status: Int,
    m_pit_lane_timer_active: Int,
    m_pit_lane_time_in_lane_in_ms: Int,
    m_pit_stop_timer_in_ms: Int,
    m_pit_stop_should_serve_pen: Int,
    m_speed_trap_fastest_speed: Float,
    m_speed_trap_fastest_lap: Int
)

case class PacketLap(
    m_header: Header,
    m_lap_data: Array[Lap],
    m_time_trial_pb_car_idx: Int,
    m_time_trial_rival_car_idx: Int
)
