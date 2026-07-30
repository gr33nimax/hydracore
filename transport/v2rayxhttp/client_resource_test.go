package xhttp

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json/badoption"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
)

const xhttpResourceSoakIterations = 64

func TestXHTTPResourceSoak(t *testing.T) {
	started := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
		writer.(http.Flusher).Flush()
		started <- struct{}{}
		<-request.Context().Done()
		stopped <- struct{}{}
	}))
	defer server.Close()

	parsedURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	transport, err := NewClient(
		context.Background(),
		logger.NOP(),
		N.SystemDialer,
		M.ParseSocksaddr(parsedURL.Host),
		option.V2RayXHTTPOptions{
			Mode: "packet-up",
			V2RayXHTTPBaseOptions: option.V2RayXHTTPBaseOptions{
				Path:                 "/resource-soak",
				ScMaxEachPostBytes:   &badoption.Range[int]{From: 1024, To: 1024},
				ScMinPostsIntervalMs: &badoption.Range[int]{From: 1, To: 1},
				Xmux:                 &option.V2RayXHTTPXmuxOptions{MaxConcurrency: badoption.Range[int]{From: 1, To: 1}},
				XPaddingBytes:        badoption.Range[int]{From: 100, To: 100},
				XPaddingPlacement:    option.PlacementHeader,
				SessionPlacement:     option.PlacementPath,
				SeqPlacement:         option.PlacementPath,
				UplinkDataPlacement:  option.PlacementBody,
				UplinkHTTPMethod:     http.MethodPost,
				ScMaxBufferedPosts:   1,
				ScStreamUpServerSecs: &badoption.Range[int]{From: 1, To: 1},
				NoGRPCHeader:         true,
				ServerMaxHeaderBytes: 4096,
				XPaddingMethod:       "repeat-x",
				XPaddingHeader:       "X-Padding",
				SessionIDLength:      badoption.Range[int]{From: 16, To: 16},
			},
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	client := transport.(*Client)

	baselineFDs := platformOpenFileDescriptorCount(t)
	baselineGoroutines := runtime.NumGoroutine()
	for index := 0; index < xhttpResourceSoakIterations; index++ {
		connection, dialErr := client.DialContext(context.Background())
		if dialErr != nil {
			t.Fatalf("dial %d: %v", index, dialErr)
		}
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("download %d did not start", index)
		}
		if closeErr := connection.Close(); closeErr != nil {
			t.Fatalf("close %d: %v", index, closeErr)
		}
		select {
		case <-stopped:
		case <-time.After(2 * time.Second):
			t.Fatalf("download %d did not stop", index)
		}
		var active atomic.Int32
		client.active.Range(func(_, _ any) bool {
			active.Add(1)
			return true
		})
		if active.Load() != 0 {
			t.Fatalf("iteration %d left %d active sessions", index, active.Load())
		}
	}
	if err = client.Close(); err != nil {
		t.Fatal(err)
	}

	const tolerance = 8
	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		currentFDs := platformOpenFileDescriptorCount(t)
		currentGoroutines := runtime.NumGoroutine()
		fdsReleased := baselineFDs < 0 || currentFDs <= baselineFDs+tolerance
		if fdsReleased && currentGoroutines <= baselineGoroutines+tolerance {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"resource growth after %d sessions: file descriptors %d -> %d, goroutines %d -> %d",
				xhttpResourceSoakIterations,
				baselineFDs,
				currentFDs,
				baselineGoroutines,
				currentGoroutines,
			)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
