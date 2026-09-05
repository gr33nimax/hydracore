package libbox

import (
	"math"
	runtimeDebug "runtime/debug"

	C "github.com/sagernet/sing-box/constant"
)

// Бюджет Go-heap, когда пользователь просит ограничить память.
//
// iOS: network extension убивают около 50 МБ, поэтому лимит остаётся ниже.
// Остальные платформы: Android-VpnService живёт в обычном процессе, и жёсткой
// границы у него нет — лимит здесь нужен как потолок, а не как выживание.
const (
	memoryLimitGoIOS   = 45 * 1024 * 1024
	memoryLimitGoOther = 128 * 1024 * 1024
)

var memoryLimitEnabled bool

// SetMemoryLimit ограничивает Go-heap мягким лимитом вместо непрерывной сборки.
//
// Раньше включённый лимит означал GOGC=10, а SetMemoryLimit ставился только на
// iOS. GOGC=10 — это цель «heap не больше 1.1× живого», то есть сборка почти на
// каждой аллокации пакетного пути; на измеренном участке (rtpCodec.unwrap) она
// стоила 3.2× времени против GOGC=100 при той же работе. Мягкий лимит даёт ту
// же границу RSS, но платит за неё только рядом с границей: пока heap далеко от
// лимита, сборка идёт по обычному GOGC.
func SetMemoryLimit(enabled bool) {
	memoryLimitEnabled = enabled
	if enabled {
		runtimeDebug.SetGCPercent(100)
		if C.IsIos {
			runtimeDebug.SetMemoryLimit(memoryLimitGoIOS)
		} else {
			runtimeDebug.SetMemoryLimit(memoryLimitGoOther)
		}
		return
	}
	runtimeDebug.SetGCPercent(100)
	runtimeDebug.SetMemoryLimit(math.MaxInt64)
}
