//go:build !linux && !android

package telemetry

func readProcessUsage() (cpuNanos uint64, rssBytes uint64, thermalState int, thermalAvailable bool) {
	return 0, 0, 0, false
}
