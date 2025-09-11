package packet

case class Header(
    m_packet_format: Int,
    m_game_year: Int,
    m_game_major_version: Int,
    m_game_minor_version: Int,
    m_packet_version: Int,
    m_packet_id: Int,
    m_session_uid: Long,
    m_session_time: Float,
    m_frame_identifier: Long,
    m_overall_frame_identifier: Long,
    m_player_car_index: Int,
    m_secondary_player_car_index: Int
)
