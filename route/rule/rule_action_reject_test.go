package rule

import (
	"testing"
	"time"

	C "github.com/sagernet/sing-box/constant"

	"github.com/stretchr/testify/require"
)

// Счётчик флуда переписан на компактирование по месту, поэтому его поведение
// закреплено тестом: окно тридцать секунд, порог пятьдесят событий, и после
// порога отказ становится тихим.
func TestRejectFloodCounter(t *testing.T) {
	t.Parallel()
	action := &RuleActionReject{Method: C.RuleActionRejectMethodDefault}

	for i := 0; i < 50; i++ {
		err := action.Error(nil)
		require.ErrorIs(t, err, ErrReset, "отказ до порога обязан сбрасывать соединение, событие %d", i)
	}
	require.ErrorIs(t, action.Error(nil), ErrDrop, "за порогом отказ обязан стать тихим")
	require.Len(t, action.dropCounter, 51)

	// Записи старше окна обязаны уходить, и отказ снова становится сбросом.
	action.dropAccess.Lock()
	stale := time.Now().Add(-31 * time.Second)
	for index := range action.dropCounter {
		action.dropCounter[index] = stale
	}
	action.dropAccess.Unlock()
	require.ErrorIs(t, action.Error(nil), ErrReset, "после истечения окна порог обязан сброситься")
	require.Len(t, action.dropCounter, 1)
}

// Компактирование по месту обязано стоить постоянного числа аллокаций, не
// зависящего от размера окна.
//
// Прежняя реализация звала common.Filter, а та растёт из nil, поэтому на каждый
// отброшенный пакет приходился новый срез на всё окно — около девяти аллокаций
// при окне в двести записей. Здесь остаётся только сам возвращаемый error.
func TestRejectFloodCounterAllocationIsConstant(t *testing.T) {
	action := &RuleActionReject{Method: C.RuleActionRejectMethodDefault}
	for i := 0; i < 256; i++ {
		action.Error(nil)
	}
	require.Greater(t, len(action.dropCounter), 200, "окно должно быть заполнено")
	allocations := testing.AllocsPerRun(200, func() {
		action.Error(nil)
	})
	require.LessOrEqual(t, allocations, 2.0,
		"на отброшенный пакет должен приходиться только error, а не копия окна")
}

// NoDrop обходит счётчик целиком: ни окна, ни порога.
func TestRejectNoDropSkipsTheCounter(t *testing.T) {
	t.Parallel()
	action := &RuleActionReject{Method: C.RuleActionRejectMethodDefault, NoDrop: true}
	for i := 0; i < 100; i++ {
		require.ErrorIs(t, action.Error(nil), ErrReset)
	}
	require.Empty(t, action.dropCounter)
}
