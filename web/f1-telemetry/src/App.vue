<template>
  <div class="telemetry-dashboard">
    <div class="title-bar">
      <div class="title-text">F1 Telemetry Dashboard</div>
      <div class="title-right">
        <div :class="['connection-status', isConnected ? 'connected' : 'disconnected']">
          {{ isConnected ? 'Connected' : 'Disconnected' }}
        </div>
      </div>
    </div>

    <div class="main-content">
      <div class="charts-container">
        <SpeedChart :latest-speed-data="latestSpeedData" />
        <RPMChart :latest-rpm-data="latestRpmData" />
        <!-- Future charts will go here -->
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'
import SpeedChart from './components/SpeedChart.vue'
import RPMChart from './components/RPMChart.vue'
import type { SpeedAggregation, RPMAggregation, SocketMessage } from './types/aggregations'

let websocket: WebSocket | null = null

const isConnected = ref(false)
const connectionError = ref<string | null>(null)

const latestSpeedData = ref<SpeedAggregation | null>(null)
const latestRpmData = ref<RPMAggregation | null>(null)

const connectWebSocket = () => {
  try {
    // Use /ws path - works in both local and Kubernetes
    // Local: nginx proxies /ws to socket:8080
    // Kubernetes: Ingress routes /ws to socket service
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    websocket = new WebSocket(`${protocol}//${window.location.host}/socket`)

    websocket.onopen = () => {
      isConnected.value = true
      connectionError.value = null
    }

    websocket.onmessage = (event) => {
      try {
        const message: SocketMessage = JSON.parse(event.data)

        if (message.type === 'speed') {
          latestSpeedData.value = message.data as SpeedAggregation
        } else if (message.type === 'rpm') {
          latestRpmData.value = message.data as RPMAggregation
        }
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
  color: #90ee90;
}

.connection-status.disconnected {
  color: #ffb6c1;
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
