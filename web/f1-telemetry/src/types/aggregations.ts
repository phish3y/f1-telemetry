export interface SpeedAggregation {
  window_start: string
  window_end: string
  session_uid: number
  avg_speed: number
  min_speed: number
  max_speed: number
  sample_count: number
}

export interface RPMAggregation {
  window_start: string
  window_end: string
  session_uid: number
  avg_rpm: number
  min_rpm: number
  max_rpm: number
  sample_count: number
}

export interface SocketMessage {
  type: 'speed' | 'rpm'
  data: SpeedAggregation | RPMAggregation
}
