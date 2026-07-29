package urltest

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/adapter"
	M "github.com/sagernet/sing/common/metadata"
)

type recordingDialer struct {
	calls atomic.Int32
}

func (d *recordingDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	d.calls.Add(1)
	return (&net.Dialer{}).DialContext(ctx, network, destination.String())
}

func (d *recordingDialer) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, errors.New("not implemented")
}

type multiplexRecordingDialer struct {
	recordingDialer
}

func (*multiplexRecordingDialer) Type() string {
	return "test"
}

func (*multiplexRecordingDialer) Tag() string {
	return "test"
}

func (*multiplexRecordingDialer) Network() []string {
	return []string{"tcp"}
}

func (*multiplexRecordingDialer) Dependencies() []string {
	return nil
}

func (*multiplexRecordingDialer) MultiplexEnabled() bool {
	return true
}

func TestURLTestUsesHTTPThroughProvidedDialer(t *testing.T) {
	t.Parallel()
	methods := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		methods <- request.Method
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dialer := new(recordingDialer)
	if _, err := URLTest(context.Background(), server.URL, dialer); err != nil {
		t.Fatal(err)
	}
	if dialer.calls.Load() != 1 {
		t.Fatalf("unexpected dial count: %d", dialer.calls.Load())
	}
	select {
	case method := <-methods:
		if method != http.MethodHead {
			t.Fatalf("unexpected HTTP method: %s", method)
		}
	case <-time.After(time.Second):
		t.Fatal("test endpoint did not receive a request")
	}
}

func TestURLTestWarmsUpMultiplexedOutbound(t *testing.T) {
	t.Parallel()
	methods := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		methods <- request.Method
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	dialer := new(multiplexRecordingDialer)
	if _, err := URLTest(context.Background(), server.URL, dialer); err != nil {
		t.Fatal(err)
	}
	if dialer.calls.Load() != 2 {
		t.Fatalf("unexpected dial count: %d", dialer.calls.Load())
	}
	for range 2 {
		select {
		case method := <-methods:
			if method != http.MethodHead {
				t.Fatalf("unexpected HTTP method: %s", method)
			}
		case <-time.After(time.Second):
			t.Fatal("test endpoint did not receive both requests")
		}
	}
}

func TestHistoryStorageCopiesValues(t *testing.T) {
	t.Parallel()
	storage := NewHistoryStorage()
	history := &adapter.URLTestHistory{Time: time.Now(), Delay: 42, Status: adapter.URLTestStatusAvailable}
	storage.StoreURLTestHistory("proxy", history)
	history.Delay = 99

	loaded := storage.LoadURLTestHistory("proxy")
	if loaded == nil || loaded.Delay != 42 {
		t.Fatalf("stored history was mutated through caller pointer: %#v", loaded)
	}
	loaded.Delay = 7
	loadedAgain := storage.LoadURLTestHistory("proxy")
	if loadedAgain == nil || loadedAgain.Delay != 42 {
		t.Fatalf("stored history was mutated through loaded pointer: %#v", loadedAgain)
	}
}

func TestHistoryStorageConcurrentAccess(t *testing.T) {
	storage := NewHistoryStorage()
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := range 500 {
				tag := string(rune('a' + worker%4))
				storage.StoreURLTestHistory(tag, &adapter.URLTestHistory{
					Time:  time.Now(),
					Delay: uint16(iteration%100 + 1),
				})
				_ = storage.LoadURLTestHistory(tag)
				if iteration%7 == 0 {
					storage.DeleteURLTestHistory(tag)
				}
			}
		}()
	}
	workers.Wait()
}
