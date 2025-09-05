<template>
  <div class="telemetry-dashboard">
    <h1>🏎️ F1 Telemetry Dashboard</h1>

    <!-- Connection Status -->
    <div class="connection-status">
      <div :class="['status-indicator', isConnected ? 'connected' : 'disconnected']">
        {{ isConnected ? '🟢 Connected' : '🔴 Disconnected' }}
      </div>
      <div v-if="connectionError" class="error-message">⚠️ {{ connectionError }}</div>
    </div>

    <!-- Speed Data Display -->
    <div v-if="latestSpeedData" class="speed-data">
      <h2>Latest Speed Data</h2>
      <div class="metrics-grid">
        <div class="metric-card">
          <div class="metric-value">{{ latestSpeedData.avg_speed.toFixed(1) }}</div>
          <div class="metric-label">Average Speed (km/h)</div>
        </div>
        <div class="metric-card">
          <div class="metric-value">{{ latestSpeedData.max_speed }}</div>
          <div class="metric-label">Max Speed (km/h)</div>
        </div>
        <div class="metric-card">
          <div class="metric-value">{{ latestSpeedData.min_speed }}</div>
          <div class="metric-label">Min Speed (km/h)</div>
        </div>
        <div class="metric-card">
          <div class="metric-value">{{ latestSpeedData.sample_count }}</div>
          <div class="metric-label">Samples</div>
        </div>
      </div>

      <div class="session-info">
        <p><strong>Session:</strong> {{ latestSpeedData.session_uid }}</p>
        <p>
          <strong>Window:</strong>
          {{ new Date(latestSpeedData.window_start).toLocaleTimeString() }} -
          {{ new Date(latestSpeedData.window_end).toLocaleTimeString() }}
        </p>
      </div>
    </div>

    <div v-else-if="isConnected" class="waiting-message">🔄 Waiting for telemetry data...</div>
  </div>
</template>

<style scoped>
.telemetry-dashboard {
  padding: 20px;
  font-family: 'Segoe UI', system-ui, sans-serif;
  background: linear-gradient(135deg, #1e3c72, #2a5298);
  min-height: 100vh;
  color: white;
}

h1 {
  text-align: center;
  margin-bottom: 30px;
  font-size: 2.5em;
  text-shadow: 2px 2px 4px rgba(0, 0, 0, 0.3);
}

.connection-status {
  text-align: center;
  margin-bottom: 30px;
}

.status-indicator {
  display: inline-block;
  padding: 10px 20px;
  border-radius: 25px;
  font-weight: bold;
  margin-bottom: 10px;
}

.connected {
  background-color: rgba(40, 167, 69, 0.2);
  border: 2px solid #28a745;
}

.disconnected {
  background-color: rgba(220, 53, 69, 0.2);
  border: 2px solid #dc3545;
}

.error-message {
  color: #ff6b6b;
  font-weight: bold;
}

.speed-data {
  max-width: 800px;
  margin: 0 auto;
}

.speed-data h2 {
  text-align: center;
  margin-bottom: 20px;
}

.metrics-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 20px;
  margin-bottom: 30px;
}

.metric-card {
  background: rgba(255, 255, 255, 0.1);
  backdrop-filter: blur(10px);
  border-radius: 15px;
  padding: 20px;
  text-align: center;
  border: 1px solid rgba(255, 255, 255, 0.2);
}

.metric-value {
  font-size: 2em;
  font-weight: bold;
  color: #00d4ff;
  margin-bottom: 5px;
}

.metric-label {
  font-size: 0.9em;
  opacity: 0.8;
}

.session-info {
  background: rgba(255, 255, 255, 0.1);
  border-radius: 10px;
  padding: 20px;
  margin-top: 20px;
}

.waiting-message {
  text-align: center;
  font-size: 1.2em;
  margin-top: 50px;
  opacity: 0.8;
}
</style>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'

interface SpeedAggregation {
  window_start: string
  window_end: string
  session_uid: number
  avg_speed: number
  min_speed: number
  max_speed: number
  sample_count: number
}

let websocket: WebSocket | null = null

const isConnected = ref(false)
const connectionError = ref<string | null>(null)

const latestSpeedData = ref<SpeedAggregation | null>(null)

const connectWebSocket = () => {
  try {
    websocket = new WebSocket(`ws://${window.location.host}:8080`) // TODO

    websocket.onopen = () => {
      console.log('connected to F1 telemetry socket')
      isConnected.value = true
      connectionError.value = null
    }

    websocket.onmessage = (event) => {
      try {
        const rawData = JSON.parse(event.data)
        console.log('Received telemetry data:', rawData)

        // Parse into SpeedAggregation interface
        const speedData: SpeedAggregation = {
          window_start: rawData.window_start,
          window_end: rawData.window_end,
          session_uid: rawData.session_uid,
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

    websocket.onclose = (event) => {
      console.log('socket connection closed:', event.code, event.reason)
      isConnected.value = false

      setTimeout(() => {
        console.log('attempting to reconnect...')
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
