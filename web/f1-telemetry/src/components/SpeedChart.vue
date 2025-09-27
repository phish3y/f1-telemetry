<template>
  <div class="speed-chart-container">
    <div class="window-header">
      <span class="window-title">
        Speed Analysis{{ currentSessionId ? ` for Session ${currentSessionId}` : '' }}
      </span>
    </div>
    <div class="chart-content">
      <Line :data="chartData" :options="chartOptions" :height="280" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed, watch, ref } from 'vue'
import { Line } from 'vue-chartjs'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  TimeScale,
  type TooltipItem,
} from 'chart.js'
import 'chartjs-adapter-date-fns'
import type { SpeedAggregation } from '../types/aggregations'

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  Title,
  Tooltip,
  Legend,
  TimeScale,
)

const props = defineProps<{
  latestSpeedData: SpeedAggregation | null
}>()

const speedDataHistory = ref<SpeedAggregation[]>([])

watch(
  () => props.latestSpeedData,
  (newData) => {
    if (newData) {
      speedDataHistory.value.push(newData)
      if (speedDataHistory.value.length > 50) {
        speedDataHistory.value.shift()
      }
    }
  },
)

const currentSessionId = computed(() => {
  if (speedDataHistory.value.length > 0) {
    return speedDataHistory.value[speedDataHistory.value.length - 1].session_uid
  }
  return null
})

const maxDataPoints = 20

const chartData = computed(() => {
  const recentData = speedDataHistory.value.slice(-maxDataPoints)

  return {
    datasets: [
      {
        label: 'Min Speed',
        data: recentData.map((point) => ({
          x: new Date(point.window_start).getTime(),
          y: point.min_speed,
        })),
        borderColor: '#6b46c1',
        backgroundColor: 'rgba(107, 70, 193, 0.1)',
        fill: false,
        pointRadius: 3,
        pointHoverRadius: 5,
        pointBackgroundColor: '#6b46c1',
        pointBorderColor: '#ffffff',
        pointBorderWidth: 1,
        tension: 0.2,
        borderWidth: 2,
      },
      {
        label: 'Avg Speed',
        data: recentData.map((point) => ({
          x: new Date(point.window_start).getTime(),
          y: point.avg_speed,
        })),
        borderColor: '#059669',
        backgroundColor: 'rgba(5, 150, 105, 0.1)',
        fill: false,
        pointRadius: 3,
        pointHoverRadius: 5,
        pointBackgroundColor: '#059669',
        pointBorderColor: '#ffffff',
        pointBorderWidth: 1,
        tension: 0.2,
        borderWidth: 2,
      },
      {
        label: 'Max Speed',
        data: recentData.map((point) => ({
          x: new Date(point.window_start).getTime(),
          y: point.max_speed,
        })),
        borderColor: '#dc2626',
        backgroundColor: 'rgba(220, 38, 38, 0.1)',
        fill: false,
        pointRadius: 3,
        pointHoverRadius: 5,
        pointBackgroundColor: '#dc2626',
        pointBorderColor: '#ffffff',
        pointBorderWidth: 1,
        tension: 0.2,
        borderWidth: 2,
      },
    ],
  }
})

const chartOptions = computed(() => {
  const now = Date.now()
  const timeWindow = 60000

  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: {
      duration: 0,
    },
    plugins: {
      title: {
        display: false,
      },
      legend: {
        display: true,
        position: 'bottom' as const,
        labels: {
          color: '#333333',
          font: {
            family: 'Geneva, Tahoma, sans-serif',
            size: 11,
            weight: 'normal' as const,
          },
          usePointStyle: true,
          pointStyle: 'circle',
          boxWidth: 8,
          boxHeight: 8,
          padding: 15,
        },
      },
      tooltip: {
        backgroundColor: '#ffffff',
        titleColor: '#333333',
        bodyColor: '#333333',
        borderColor: '#cccccc',
        borderWidth: 1,
        cornerRadius: 0,
        displayColors: true,
        titleFont: {
          family: 'Geneva, Tahoma, sans-serif',
          size: 11,
          weight: 'bold' as const,
        },
        bodyFont: {
          family: 'Geneva, Tahoma, sans-serif',
          size: 11,
        },
        callbacks: {
          title: function (context: TooltipItem<'line'>[]) {
            return new Date(context[0].parsed.x).toLocaleTimeString()
          },
          label: function (context: TooltipItem<'line'>) {
            return `${context.dataset.label}: ${context.parsed.y.toFixed(1)} km/h`
          },
        },
      },
    },
    interaction: {
      mode: 'nearest' as const,
      axis: 'x' as const,
      intersect: false,
    },
    scales: {
      x: {
        type: 'time' as const,
        time: {
          displayFormats: {
            second: 'HH:mm:ss',
            minute: 'HH:mm:ss',
          },
          unit: 'second' as const,
        },
        min:
          speedDataHistory.value.length > 0
            ? Math.max(
                now - timeWindow,
                new Date(
                  speedDataHistory.value[Math.max(0, speedDataHistory.value.length - maxDataPoints)]
                    ?.window_start || now,
                ).getTime(),
              )
            : now - timeWindow,
        max: now + 5000,
        title: {
          display: true,
          text: 'Time',
          color: '#666666',
          font: {
            family: 'Geneva, Tahoma, sans-serif',
            size: 11,
            weight: 'normal' as const,
          },
        },
        ticks: {
          color: '#666666',
          font: {
            family: 'Geneva, Tahoma, sans-serif',
            size: 10,
          },
          maxTicksLimit: 8,
        },
        grid: {
          color: '#e5e5e5',
          lineWidth: 1,
        },
        border: {
          color: '#cccccc',
          width: 1,
        },
      },
      y: {
        beginAtZero: false,
        title: {
          display: true,
          text: 'Speed (km/h)',
          color: '#666666',
          font: {
            family: 'Geneva, Tahoma, sans-serif',
            size: 11,
            weight: 'normal' as const,
          },
        },
        ticks: {
          color: '#666666',
          font: {
            family: 'Geneva, Tahoma, sans-serif',
            size: 10,
          },
        },
        grid: {
          color: '#e5e5e5',
          lineWidth: 1,
        },
        border: {
          color: '#cccccc',
          width: 1,
        },
      },
    },
  }
})
</script>

<style scoped>
.speed-chart-container {
  background: linear-gradient(135deg, #f0f0f0 0%, #e8e8e8 100%);
  border: 2px outset #c0c0c0;
  margin-bottom: 20px;
  font-family: 'Geneva', 'Tahoma', sans-serif;
  font-size: 11px;
  box-shadow: 2px 2px 4px rgba(0, 0, 0, 0.1);
  width: 100%;
  max-width: 600px;
  flex: 1;
}

.window-header {
  background: linear-gradient(180deg, #0080ff 0%, #0060df 100%);
  color: white;
  padding: 4px 8px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-weight: bold;
  font-size: 11px;
}

.window-title {
  flex: 1;
}

.chart-content {
  padding: 12px;
  background: #ffffff;
  border: 1px inset #c0c0c0;
  margin: 2px;
}
</style>
