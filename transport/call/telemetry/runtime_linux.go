//go:build linux || android

package telemetry

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func readProcessUsage() (cpuNanos uint64, rssBytes uint64, thermalState int, thermalAvailable bool) {
	taskPaths, _ := filepath.Glob("/proc/self/task/*/schedstat")
	taskSamples := 0
	for _, taskPath := range taskPaths {
		content, err := os.ReadFile(taskPath)
		if err != nil {
			continue
		}
		fields := strings.Fields(string(content))
		if len(fields) > 0 {
			value, parseErr := strconv.ParseUint(fields[0], 10, 64)
			if parseErr == nil {
				cpuNanos += value
				taskSamples++
			}
		}
	}
	if taskSamples == 0 {
		if content, err := os.ReadFile("/proc/self/schedstat"); err == nil {
			fields := strings.Fields(string(content))
			if len(fields) > 0 {
				cpuNanos, _ = strconv.ParseUint(fields[0], 10, 64)
			}
		}
	}
	if content, err := os.ReadFile("/proc/self/statm"); err == nil {
		fields := strings.Fields(string(content))
		if len(fields) > 1 {
			residentPages, parseErr := strconv.ParseUint(fields[1], 10, 64)
			if parseErr == nil {
				rssBytes = residentPages * uint64(os.Getpagesize())
			}
		}
	}
	maximumMilliCelsius := int64(0)
	paths, _ := filepath.Glob("/sys/class/thermal/thermal_zone*/temp")
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		value, err := strconv.ParseInt(strings.TrimSpace(string(content)), 10, 64)
		if err != nil || value <= 0 || value > 200_000 {
			continue
		}
		thermalAvailable = true
		if value > maximumMilliCelsius {
			maximumMilliCelsius = value
		}
	}
	switch {
	case !thermalAvailable:
		thermalState = 0
	case maximumMilliCelsius < 45_000:
		thermalState = 1
	case maximumMilliCelsius < 55_000:
		thermalState = 2
	case maximumMilliCelsius < 65_000:
		thermalState = 3
	default:
		thermalState = 4
	}
	return
}
