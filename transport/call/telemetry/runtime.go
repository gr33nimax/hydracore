package telemetry

import (
	"runtime"
	"time"
)

type ProcessSampler struct {
	lastWall time.Time
	lastCPU  uint64
}

func SampleServerRuntime(accumulator *Accumulator) {
	if accumulator == nil {
		return
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	accumulator.Set(RuntimeGoroutines, float64(runtime.NumGoroutine()))
	accumulator.Set(RuntimeHeapBytes, float64(memory.HeapAlloc))
	accumulator.Set(RuntimeGCPauseSecondsTotal, float64(memory.PauseTotalNs)/1e9)
}

func (s *ProcessSampler) Sample(accumulator *Accumulator) {
	if accumulator == nil {
		return
	}
	now := time.Now()
	cpuNanos, rssBytes, thermalState, thermalAvailable := readProcessUsage()
	if !s.lastWall.IsZero() && cpuNanos >= s.lastCPU {
		wall := now.Sub(s.lastWall)
		if wall > 0 {
			percent := float64(cpuNanos-s.lastCPU) / float64(wall) * 100
			maximum := float64(runtime.NumCPU()) * 100
			if percent > maximum {
				percent = maximum
			}
			accumulator.Set(RuntimeCPUPercent, percent)
		}
	}
	s.lastWall = now
	s.lastCPU = cpuNanos
	accumulator.Set(RuntimeRSSBytes, float64(rssBytes))
	accumulator.Set(RuntimeThermalState, float64(thermalState))
	if thermalAvailable {
		accumulator.Set(RuntimeThermalAvailable, 1)
	} else {
		accumulator.Set(RuntimeThermalAvailable, 0)
	}
}
