package packet.payload

import packet.Header

case class LiveryColour(
    red: Int,
    green: Int,
    blue: Int
)

case class Participant(
    m_ai_controlled: Int,
    m_driver_id: Int,
    m_network_id: Int,
    m_team_id: Int,
    m_my_team: Int,
    m_race_number: Int,
    m_nationality: Int,
    m_name: Array[Int],
    m_your_telemetry: Int,
    m_show_online_names: Int,
    m_tech_level: Int,
    m_platform: Int,
    m_num_colours: Int,
    m_livery_colours: Array[LiveryColour]
)

case class PacketParticipants(
    m_header: Header,
    m_num_active_cars: Int,
    m_participants: Array[Participant]
)
