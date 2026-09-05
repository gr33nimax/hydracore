package daemon

import (
	"runtime/metrics"
	"sync"
)

// Классы памяти, из которых складывается то же число, что раньше давал
// memory.Total(): stacks + heap in use + heap idle, не отданный ОС.
var runtimeMemoryClasses = []string{
	"/memory/classes/heap/objects:bytes",
	"/memory/classes/heap/unused:bytes",
	"/memory/classes/heap/free:bytes",
	"/memory/classes/heap/stacks:bytes",
}

var runtimeMemory = struct {
	sync.Mutex
	samples []metrics.Sample
}{}

// processMemoryInUse возвращает занятую память процесса, не останавливая мир.
//
// Раньше это был memory.Total() из sing, который на всём, кроме Darwin, уходит
// в runtime.ReadMemStats — а тот останавливает мир. Его звали на каждый тик
// потока runtime-событий, то есть раз в секунду всё время жизни туннеля: 86 400
// stop-the-world в сутки на процессе, который в это же время пересылает пакеты.
//
// runtime/metrics.Read берёт только metricsLock и даёт те же классы памяти.
func processMemoryInUse() uint64 {
	runtimeMemory.Lock()
	defer runtimeMemory.Unlock()
	if runtimeMemory.samples == nil {
		runtimeMemory.samples = make([]metrics.Sample, len(runtimeMemoryClasses))
		for index, name := range runtimeMemoryClasses {
			runtimeMemory.samples[index].Name = name
		}
	}
	metrics.Read(runtimeMemory.samples)
	var total uint64
	for _, sample := range runtimeMemory.samples {
		if sample.Value.Kind() == metrics.KindUint64 {
			total += sample.Value.Uint64()
		}
	}
	return total
}
