package libbox

import (
	"encoding/json"

	H "github.com/sagernet/sing-box/common/hydracore"
)

func HydraCoreTransportState() string {
	payload := struct {
		SchemaVersion int                       `json:"schema_version"`
		Health        H.TransportHealthSnapshot `json:"health"`
		Challenge     *H.RuntimeChallenge       `json:"challenge,omitempty"`
	}{
		SchemaVersion: 2,
		Health:        H.CurrentTransportHealth(),
		Challenge:     H.CurrentRuntimeChallenge(),
	}
	content, err := json.Marshal(payload)
	if err != nil {
		return ""
	}
	return string(content)
}

func HydraCoreCancelRuntimeChallenge(id string) bool {
	return H.CancelRuntimeChallenge(id)
}

// HydraCoreTurnEdgeEndpoint answers the TURN edge a transport last reached, as
// `network://host:port`, or an empty string when none was ever recorded. The client uses it
// for a workerless reachability probe — a single STUN Binding — and shows nothing rather
// than performing VK authorisation just to obtain an address.
func HydraCoreTurnEdgeEndpoint() string {
	return H.TurnEdgeEndpoint()
}

// TurnEdgeAttribution is the edge record with its attribution: which transport reached
// the edge and under which runtime generation the allocation happened.
type TurnEdgeAttribution struct {
	Endpoint          string
	TransportTag      string
	RuntimeGeneration int64
	UpdatedAtMillis   int64
}

// HydraCoreTurnEdgeAttribution answers the full record behind HydraCoreTurnEdgeEndpoint.
// A client that files the edge under a specific server accepts it only when the transport
// tag and the runtime generation match the outbound it is filing it under; an older
// record with no tag belongs to nobody in particular and reads as "not measured".
func HydraCoreTurnEdgeAttribution() *TurnEdgeAttribution {
	record := H.TurnEdgeAttribution()
	return &TurnEdgeAttribution{
		Endpoint:          record.Endpoint,
		TransportTag:      record.TransportTag,
		RuntimeGeneration: int64(record.RuntimeGeneration),
		UpdatedAtMillis:   record.UpdatedAt,
	}
}

func HydraCoreSetNetworkGeneration(generation int64) {
	if generation >= 0 {
		H.SetNetworkGeneration(uint64(generation))
	}
}
