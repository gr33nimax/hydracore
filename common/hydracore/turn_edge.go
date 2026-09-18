package hydracore

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// The last TURN edge a transport actually reached, kept for a client that wants to measure
// reachability without a worker: a single STUN Binding to the edge needs an address to send
// it to, and the only trustworthy one is an edge the transport itself allocated through.
//
// The value outlives the transport. It is written to a small file in the working directory,
// so a client can ask for it from a later process — one that never ran the transport at all
// — and a client that gets an empty string shows "not measured" rather than guessing an
// address, because no VK authorisation is ever performed just to obtain one.
//
// The endpoint alone is not an attribution. It is a global record in a core that can serve
// several tagged transports across runtime generations, and a client that files it under
// whichever server was just announced can attach the previous transport's edge to the next
// server's profile. The record therefore carries the transport tag and the runtime
// generation the allocation happened under, and a client is expected to accept it only when
// both match the outbound it is filing it under.
type TurnEdgeRecord struct {
	Endpoint          string `json:"endpoint,omitempty"`
	TransportTag      string `json:"transport_tag,omitempty"`
	RuntimeGeneration uint64 `json:"runtime_generation,omitempty"`
	UpdatedAt         int64  `json:"updated_at,omitempty"`
}

type turnEdgeStore struct {
	mu     sync.RWMutex
	record TurnEdgeRecord
	path   string
}

var turnEdge turnEdgeStore

// SetTurnEdgeStorePath points the store at its file. Called from setup, before any
// transport exists; an empty path keeps the value in memory for this process only.
func SetTurnEdgeStorePath(path string) {
	turnEdge.mu.Lock()
	turnEdge.path = path
	turnEdge.mu.Unlock()
}

// RecordTurnEdgeEndpoint remembers an endpoint the transport reached together with the
// tag of the transport and the runtime generation the allocation happened under, and
// makes a best-effort attempt to keep it for later processes. A store that cannot be
// written still serves the running one; the transport must not fail over a diagnostic
// hint.
//
// The generation is re-read here, inside the same critical section as the write: a caller
// checks the generation before it starts writing and can be suspended across a runtime
// switch, and without this second look its record would land after the newer runtime's
// and overwrite it — the current transport's edge would go stale until its next
// allocation. The read takes the runtime state's read lock under this store's own lock,
// never the other way round, and the slow file write stays under the store's lock alone.
func RecordTurnEdgeEndpoint(endpoint string, transportTag string, runtimeGeneration uint64) {
	if endpoint == "" {
		return
	}
	turnEdge.mu.Lock()
	defer turnEdge.mu.Unlock()
	if runtimeGeneration != CurrentRuntimeGeneration() {
		return
	}
	turnEdge.record = TurnEdgeRecord{
		Endpoint:          endpoint,
		TransportTag:      transportTag,
		RuntimeGeneration: runtimeGeneration,
		UpdatedAt:         time.Now().UnixMilli(),
	}
	if turnEdge.path == "" {
		return
	}
	content, err := json.Marshal(turnEdge.record)
	if err != nil {
		return
	}
	directory := filepath.Dir(turnEdge.path)
	if err = os.MkdirAll(directory, 0o755); err != nil {
		return
	}
	temporary := turnEdge.path + ".tmp"
	if err = os.WriteFile(temporary, content, 0o644); err != nil {
		return
	}
	_ = os.Rename(temporary, turnEdge.path)
}

// TurnEdgeAttribution answers the full record of the edge a transport last reached. The
// file is re-read on every call when one is configured: the caller is usually a different
// process than the one that recorded the edge, and it may have recorded it after this
// process first looked, so a first empty answer must not be remembered as permanent. The
// file is a few dozen bytes; callers ask when a screen opens, not per packet. An empty
// endpoint means no transport has ever recorded an edge; an empty tag or a zero runtime
// generation means the record predates attribution and belongs to nobody in particular.
func TurnEdgeAttribution() TurnEdgeRecord {
	turnEdge.mu.Lock()
	path := turnEdge.path
	turnEdge.mu.Unlock()
	if path == "" {
		turnEdge.mu.RLock()
		defer turnEdge.mu.RUnlock()
		return turnEdge.record
	}
	if content, err := os.ReadFile(path); err == nil {
		var record TurnEdgeRecord
		if json.Unmarshal(content, &record) == nil && record.Endpoint != "" {
			return record
		}
	}
	return TurnEdgeRecord{}
}

// TurnEdgeEndpoint answers the endpoint a transport last reached, without the
// attribution. Kept for clients that only wanted the address; anything that files the
// edge under a specific server belongs on [TurnEdgeAttribution] instead.
func TurnEdgeEndpoint() string {
	return TurnEdgeAttribution().Endpoint
}

func resetTurnEdgeStore() {
	turnEdge.mu.Lock()
	turnEdge.record = TurnEdgeRecord{}
	turnEdge.mu.Unlock()
}
