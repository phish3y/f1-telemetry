package packet.payload

import packet.Header

case class CarTelemetry(
    m_speed: Int,
    m_throttle: Float,
    m_steer: Float,
    m_brake: Float,
    m_clutch: Int,
    m_gear: Int,
    m_engine_rpm: Int,
    m_drs: Int,
    m_rev_lights_percent: Int,
    m_rev_lights_bit_value: Int,
    m_brakes_temperature: Array[Int],
    m_tyres_surface_temperature: Array[Int],
    m_tyres_inner_temperature: Array[Int],
    m_engine_temperature: Int,
    m_tyres_pressure: Array[Float],
    m_surface_type: Array[Int]
)

case class PacketCarTelemetry(
    m_header: Header,
    m_car_telemetry_data: Array[CarTelemetry],
    m_mfd_panel_index: Int,
    m_mfd_panel_index_secondary_player: Int,
    m_suggested_gear: Int
)
