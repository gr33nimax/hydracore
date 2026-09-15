package log

import (
	"context"
	"sync"
	"testing"

	"github.com/sagernet/sing-box/option"
	"github.com/stretchr/testify/require"
)

type recordingPlatformWriter struct {
	access   sync.Mutex
	messages []string
}

func (w *recordingPlatformWriter) WriteMessage(_ Level, message string) {
	w.access.Lock()
	defer w.access.Unlock()
	w.messages = append(w.messages, message)
}
func (w *recordingPlatformWriter) count() int {
	w.access.Lock()
	defer w.access.Unlock()
	return len(w.messages)
}

func newDisabledFactoryForTest(t *testing.T, writer *recordingPlatformWriter) Factory {
	t.Helper()
	factory, err := New(Options{
		Context:        context.Background(),
		Options:        option.LogOptions{Disabled: true},
		PlatformWriter: writer,
	})
	require.NoError(t, err)
	require.NoError(t, factory.Start())
	t.Cleanup(func() { _ = factory.Close() })
	return factory
}

// A platform attaches its writer once, usually while logging is still off, and the core asserts the
// factory it was handed to be observable while building Clash. The factory built when logging is
// turned on has to receive that writer, or the platform that asked for messages hears nothing.
func TestDisabledFactoryCarriesPlatformWritersIntoTheEnabledFactory(t *testing.T) {
	initial := new(recordingPlatformWriter)
	factory := newDisabledFactoryForTest(t, initial)
	observableFactory, ok := factory.(ObservableFactory)
	require.True(t, ok, "the factory New returned is not observable")

	late := new(recordingPlatformWriter)
	observableFactory.AttachPlatformWriter(late)
	require.NoError(t, factory.(*disabledFactory).Enable(LevelError))

	factory.Logger().Error("after")
	require.Equal(t, 1, initial.count(), "the writer the options carried stopped receiving lines")
	require.Equal(t, 1, late.count(), "the writer attached while logging was off was lost")
}

func TestDisabledFactoryEnablesExistingLogger(t *testing.T) {
	writer := new(recordingPlatformWriter)
	factory := newDisabledFactoryForTest(t, writer)
	logger := factory.NewLogger("test")
	logger.Error("before")
	require.Equal(t, 0, writer.count(), "disabled factory wrote a line")
	require.NoError(t, factory.(*disabledFactory).Enable(LevelError))
	logger.Error("after")
	require.Equal(t, 1, writer.count(), "existing logger did not use the enabled factory")
	factory.SetLevel(LevelPanic)
	logger.Error("off again")
	require.Equal(t, 1, writer.count(), "quiet level wrote after logging was turned off")
}

// A suppressed line through the enabled wrapper must cost what an ordinary logger's line
// costs: the delegate is resolved once and reused, not rebuilt per call on top of the line.
// The baseline is the raw factory, since every factory New hands out is the switchable one.
func TestEnabledDisabledLoggerAllocatesLikeAnOrdinaryLogger(t *testing.T) {
	writer := new(recordingPlatformWriter)
	ordinary, err := newActiveFactory(Options{
		Context:        context.Background(),
		Options:        option.LogOptions{Level: "error"},
		PlatformWriter: writer,
	})
	require.NoError(t, err)

	wrappedFactory := newDisabledFactoryForTest(t, writer)
	require.NoError(t, wrappedFactory.(*disabledFactory).Enable(LevelError))
	wrapped := wrappedFactory.NewLogger("test")

	ordinaryLogger := ordinary.NewLogger("test")
	ordinaryAllocations := testing.AllocsPerRun(1000, func() { ordinaryLogger.Debug("suppressed") })
	wrappedAllocations := testing.AllocsPerRun(1000, func() { wrapped.Debug("suppressed") })
	require.Equal(t, ordinaryAllocations, wrappedAllocations,
		"a suppressed line through the enabled wrapper allocates more than an ordinary logger's")
}

// The disabled fast path costs no more than the one variadic slice every call builds: no
// lock, no logger construction, no state. (The NOP factory reaches zero only because its
// empty methods inline away entirely.) It is the path every line of a start with logging
// off runs through.
func TestDisabledFastPathCostsNoMoreThanTheArgumentsSlice(t *testing.T) {
	writer := new(recordingPlatformWriter)
	factory := newDisabledFactoryForTest(t, writer)
	logger := factory.NewLogger("test")
	require.LessOrEqual(t, testing.AllocsPerRun(1000, func() { logger.Debug("suppressed") }), float64(1))
}

func TestDisabledFactoryDisableReleasesTheActiveOne(t *testing.T) {
	writer := new(recordingPlatformWriter)
	factory := newDisabledFactoryForTest(t, writer).(*disabledFactory)
	logger := factory.NewLogger("test")

	// OFF on a factory that never came on is a no-op, not a first enable.
	require.NoError(t, factory.Disable())
	require.Nil(t, factory.active.Load(), "OFF built a factory where none was needed")

	require.NoError(t, factory.Enable(LevelError))
	logger.Error("while on")
	require.Equal(t, 1, writer.count())

	require.NoError(t, factory.Disable())
	require.Nil(t, factory.active.Load(), "ON→OFF left the factory built")
	logger.Error("while off")
	require.Equal(t, 1, writer.count(), "a line was written after the factory was disabled")

	// And back on again: the cached delegate must not survive the transition.
	require.NoError(t, factory.Enable(LevelError))
	logger.Error("on again")
	require.Equal(t, 2, writer.count(), "a logger that lived through disable→enable lost its delegate")
}

func TestClosedFactoryCannotBeResurrected(t *testing.T) {
	writer := new(recordingPlatformWriter)
	factory := newDisabledFactoryForTest(t, writer).(*disabledFactory)
	logger := factory.NewLogger("test")

	require.NoError(t, factory.Enable(LevelError))
	require.NoError(t, factory.Close())

	require.ErrorIs(t, factory.Enable(LevelError), ErrClosed, "a closed factory was resurrected by Enable")
	require.ErrorIs(t, factory.Disable(), ErrClosed, "a closed factory was resurrected by Disable")
	require.ErrorIs(t, factory.Start(), ErrClosed)
	logger.Error("after close")
	require.Equal(t, 0, writer.count(), "a logger wrote through a closed factory")
	require.NoError(t, factory.Close(), "closing twice is not an error")
}

// OFF is not reserved for a core that started with it: a core that started at DEBUG is the
// one whose OFF has the most to release, and refusing it there left the app able to turn
// logging up on a running tunnel but never back down.
func TestAFactoryStartedEnabledCanTurnOffAndBackOn(t *testing.T) {
	writer := new(recordingPlatformWriter)
	factory, err := New(Options{
		Context:        context.Background(),
		Options:        option.LogOptions{Level: "debug"},
		PlatformWriter: writer,
	})
	require.NoError(t, err)
	require.NoError(t, factory.Start())
	t.Cleanup(func() { _ = factory.Close() })

	logger := factory.NewLogger("test")
	logger.Error("while on")
	require.Equal(t, 1, writer.count())

	require.NoError(t, factory.(*disabledFactory).Disable(), "a started-enabled factory refused OFF")
	logger.Error("while off")
	require.Equal(t, 1, writer.count(), "a line was written after OFF")

	require.NoError(t, factory.(*disabledFactory).Enable(LevelError))
	logger.Error("on again")
	require.Equal(t, 2, writer.count(), "a logger that lived through OFF→ON lost its delegate")

	// The command server's wire word goes through the same two methods.
	factory.SetLevel(LevelDebug)
	logger.Debug("debug passes at debug")
	require.Equal(t, 3, writer.count())
}
