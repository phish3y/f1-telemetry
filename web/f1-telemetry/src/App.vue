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
      <div class="session-info">
        <div class="session-info-item">
          <span class="session-label">Session ID:</span>
          <span class="session-value">{{ currentSessionUid ?? '--' }}</span>
        </div>
        <div class="session-info-item">
          <span class="session-label">Session Time:</span>
          <span class="session-value">{{ formatSessionTime(currentSessionTime) }}</span>
        </div>
      </div>
      <div class="charts-container">
        <SpeedChart :latest-speed-data="latestSpeedData" />
        <RPMChart :latest-rpm-data="latestRpmData" />
        <!-- Future charts will go here -->
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted } from 'vue'
import SpeedChart from './components/SpeedChart.vue'
import RPMChart from './components/RPMChart.vue'
import type { SpeedAggregation, RPMAggregation, SSEMessage } from './types/aggregations'

let eventSource: EventSource | null = null

const isConnected = ref(false)
const connectionError = ref<string | null>(null)

const latestSpeedData = ref<SpeedAggregation | null>(null)
const latestRpmData = ref<RPMAggregation | null>(null)

const currentSessionUid = computed(() => {
  return latestSpeedData.value?.session_uid ?? latestRpmData.value?.session_uid ?? null
})

const currentSessionTime = computed(() => {
  return latestSpeedData.value?.session_time ?? latestRpmData.value?.session_time ?? null
})

const formatSessionTime = (seconds: number | null): string => {
  if (seconds === null) return '--:--:--'

  const hours = Math.floor(seconds / 3600)
  const minutes = Math.floor((seconds % 3600) / 60)
  const secs = Math.floor(seconds % 60)

  return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`
}

const connectSSE = () => {
  try {
    eventSource = new EventSource('/api/events')

    eventSource.onopen = () => {
      isConnected.value = true
      connectionError.value = null
    }

    eventSource.onmessage = (event) => {
      try {
        const message: SSEMessage = JSON.parse(event.data)

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

    eventSource.onerror = (error) => {
      console.error('SSE error:', error)
      isConnected.value = false

      if (eventSource?.readyState === EventSource.CLOSED) {
        connectionError.value = 'SSE connection failed'
        setTimeout(() => {
          connectSSE()
        }, 3000)
      }
    }
  } catch (error) {
    console.error('failed to create SSE connection:', error)
    connectionError.value = 'failed to create SSE connection'
  }
}

const disconnectSSE = () => {
  if (eventSource) {
    eventSource.close()
    eventSource = null
  }
}

onMounted(() => {
  connectSSE()
})

onUnmounted(() => {
  disconnectSSE()
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

.session-info {
  background: linear-gradient(180deg, #dfdfdf 0%, #c0c0c0 100%);
  border: 1px solid #808080;
  border-radius: 2px;
  padding: 8px 12px;
  display: flex;
  gap: 24px;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.5), 0 1px 2px rgba(0, 0, 0, 0.1);
}

.session-info-item {
  display: flex;
  align-items: center;
  gap: 8px;
}

.session-label {
  font-size: 10px;
  font-weight: bold;
  color: #333333;
  text-transform: uppercase;
}

.session-value {
  font-size: 11px;
  font-weight: bold;
  color: #000080;
  font-family: 'Courier New', monospace;
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
