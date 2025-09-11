<template>
  <div class="telemetry-dashboard">
    <div class="title-bar">
      <div class="title-text">F1 Telemetry Monitor</div>
      <div class="title-right">
        <div :class="['connection-status', isConnected ? 'connected' : 'disconnected']">
          {{ isConnected ? 'Connected' : 'Disconnected' }}
        </div>
      </div>
    </div>

    <div class="main-content">
      <div class="charts-container">
        <SpeedChart :latest-speed-data="latestSpeedData" />
        <!-- Future charts will go here -->
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'
import SpeedChart from './components/SpeedChart.vue'

interface SpeedAggregation {
  window_start: string
  window_end: string
  session_uid: number
  car_index: number
  avg_speed: number
  min_speed: number
  max_speed: number
  sample_count?: number
}

let websocket: WebSocket | null = null

const isConnected = ref(false)
const connectionError = ref<string | null>(null)
const latestSpeedData = ref<SpeedAggregation | null>(null)

const connectWebSocket = () => {
  try {
    websocket = new WebSocket(`ws://${window.location.host}:8080`)

    websocket.onopen = () => {
      isConnected.value = true
      connectionError.value = null
    }

    websocket.onmessage = (event) => {
      try {
        const rawData = JSON.parse(event.data)

        const speedData: SpeedAggregation = {
          window_start: rawData.window_start,
          window_end: rawData.window_end,
          session_uid: rawData.session_uid,
          car_index: rawData.car_index,
          avg_speed: rawData.avg_speed,
          min_speed: rawData.min_speed,
          max_speed: rawData.max_speed,
          sample_count: rawData.sample_count,
        }

        latestSpeedData.value = speedData
      } catch (error) {
        console.error('failed to parse telemetry data:', error)
        connectionError.value = 'failed to parse telemetry data'
      }
    }

    websocket.onclose = () => {
      isConnected.value = false

      setTimeout(() => {
        connectWebSocket()
      }, 3000)
    }

    websocket.onerror = (error) => {
      console.error('socket error:', error)
      connectionError.value = 'socket connection failed'
      isConnected.value = false
    }
  } catch (error) {
    console.error('failed to create socket connection:', error)
    connectionError.value = 'failed to create socket connection'
  }
}

const disconnectWebSocket = () => {
  if (websocket) {
    websocket.close()
    websocket = null
  }
}

onMounted(() => {
  connectWebSocket()
})

onUnmounted(() => {
  disconnectWebSocket()
})
</script>

<style scoped>
.telemetry-dashboard {
  min-height: 100vh;
  background: linear-gradient(135deg, #c0c0c0 0%, #808080 100%);
  font-family: 'Geneva', 'Tahoma', sans-serif;
  font-size: 11px;
  display: flex;
  flex-direction: column;
}

.title-bar {
  background: linear-gradient(180deg, #0080ff 0%, #0060df 100%);
  color: white;
  padding: 8px 16px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-weight: bold;
  font-size: 12px;
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.2);
}

.title-text {
  font-size: 12px;
}

.title-right {
  display: flex;
  align-items: center;
  gap: 12px;
}

.connection-status {
  font-size: 10px;
  font-weight: bold;
  padding: 2px 6px;
  border-radius: 2px;
  background: rgba(255, 255, 255, 0.1);
}

.connection-status.connected {
  color: #90EE90;
}

.connection-status.disconnected {
  color: #FFB6C1;
}

.title-version {
  font-size: 10px;
  opacity: 0.9;
}

.main-content {
  flex: 1;
  padding: 16px;
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.charts-container {
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
  justify-content: flex-start;
}

.charts-container > * {
  flex: 1;
  min-width: 300px;
  max-width: calc(50% - 8px);
}

.metrics-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
  gap: 12px;
}

.metric-item {
  text-align: center;
}

.metric-label {
  font-size: 10px;
  color: #666666;
  margin-bottom: 4px;
}

.metric-value {
  font-size: 14px;
  font-weight: bold;
  color: #000080;
}

.waiting-text {
  text-align: center;
  color: #666666;
  font-style: italic;
}

.status-bar {
  background: linear-gradient(180deg, #dfdfdf 0%, #c0c0c0 100%);
  border-top: 1px solid #808080;
  padding: 4px 16px;
  display: flex;
  justify-content: space-between;
  font-size: 10px;
  color: #333333;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.5);
}

@media (max-width: 768px) {
  .main-content {
    padding: 8px;
    gap: 8px;
  }

  .charts-container {
    gap: 8px;
  }

  .charts-container > * {
    max-width: 100%;
    min-width: 280px;
  }
}
</style>
