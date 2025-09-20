package packet.payload

import packet.Header

case class LobbyInfo(
    m_ai_controlled: Int,
    m_team_id: Int,
    m_nationality: Int,
    m_platform: Int,
    m_name: Array[Int],
    m_car_number: Int,
    m_your_telemetry: Int,
    m_show_online_names: Int,
    m_tech_level: Int,
    m_ready_status: Int
)

case class PacketLobbyInfo(
    m_header: Header,
    m_num_players: Int,
    m_lobby_players: Array[LobbyInfo]
)
