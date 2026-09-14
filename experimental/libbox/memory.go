package libbox

import (
	"math"
	"runtime/debug"

	C "github.com/sagernet/sing-box/constant"
)

// The Go-heap budget a user asks for when the memory limit is on.
//
// iOS network extensions are killed around 50 MB, so the limit stays below it.
// Everywhere else an Android VpnService lives in an ordinary process without a
// hard boundary; the limit is a ceiling there rather than a survival budget.
const (
	memoryLimitGoIOS   = 45 * 1024 * 1024
	memoryLimitGoOther = 128 * 1024 * 1024
)

// SetMemoryLimit bounds the Go heap with a soft limit instead of continuous
// collection.
//
// An enabled limit used to mean GOGC=10, and SetMemoryLimit was applied on iOS
// alone. GOGC=10 targets "heap no larger than 1.1x live", which collects on
// nearly every packet-path allocation; on the measured span (rtpCodec.unwrap) it
// cost 3.2x the time of GOGC=100 for the same work. A soft limit gives the same
// RSS ceiling but only pays for it near the boundary: while the heap is far from
// the limit, collection runs at the ordinary GOGC.
func SetMemoryLimit(enabled bool) {
	debug.SetGCPercent(100)
	if !enabled {
		debug.SetMemoryLimit(math.MaxInt64)
		return
	}
	if C.IsIos {
		debug.SetMemoryLimit(memoryLimitGoIOS)
		return
	}
	debug.SetMemoryLimit(memoryLimitGoOther)
}
