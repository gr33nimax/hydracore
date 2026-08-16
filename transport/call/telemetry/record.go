package telemetry

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"regexp"
	"time"
)

const (
	SchemaVersion    = 1
	MaximumRecordLen = 64 * 1024
)

var slugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`)

type Record struct {
	Schema            int            `json:"schema"`
	Timestamp         float64        `json:"timestamp"`
	OriginTimestamp   float64        `json:"origin_timestamp,omitempty"`
	Scope             string         `json:"scope"`
	Kind              string         `json:"kind"`
	User              string         `json:"user,omitempty"`
	SessionID         string         `json:"session_id,omitempty"`
	SessionGeneration uint64         `json:"session_generation,omitempty"`
	WorkerID          *uint16        `json:"worker_id,omitempty"`
	Event             string         `json:"event,omitempty"`
	Stage             string         `json:"stage,omitempty"`
	Reason            string         `json:"reason,omitempty"`
	Metrics           map[string]any `json:"metrics"`
}

func Snapshot(scope, user, sessionID string, metrics map[string]any) Record {
	return Record{
		Schema:    SchemaVersion,
		Timestamp: float64(time.Now().UnixNano()) / 1e9,
		Scope:     scope,
		Kind:      "snapshot",
		User:      user,
		SessionID: sessionID,
		Metrics:   metrics,
	}
}

func EventRecord(scope, user, sessionID string, event Event) Record {
	return Record{
		Schema:    SchemaVersion,
		Timestamp: event.Timestamp,
		Scope:     scope,
		Kind:      "event",
		User:      user,
		SessionID: sessionID,
		WorkerID:  event.WorkerID,
		Event:     event.Event,
		Stage:     event.Stage,
		Reason:    event.Reason,
		Metrics:   map[string]any{},
	}
}

func DecodeClientRecord(payload []byte) (Record, error) {
	if len(payload) == 0 || len(payload) > MaximumRecordLen {
		return Record{}, errors.New("call telemetry: invalid client record length")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var record Record
	if err := decoder.Decode(&record); err != nil {
		return Record{}, errors.New("call telemetry: invalid client record")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Record{}, errors.New("call telemetry: trailing client record")
	}
	if record.Scope != "client" || record.User != "" || record.SessionID != "" || record.SessionGeneration != 0 || record.OriginTimestamp != 0 {
		return Record{}, errors.New("call telemetry: client supplied server-owned fields")
	}
	if err := ValidateRecord(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func ValidateRecord(record Record) error {
	if record.Schema != SchemaVersion || (record.Scope != "server" && record.Scope != "client") {
		return errors.New("call telemetry: invalid record envelope")
	}
	if record.Kind != "snapshot" && record.Kind != "event" {
		return errors.New("call telemetry: invalid record kind")
	}
	if math.IsNaN(record.Timestamp) || math.IsInf(record.Timestamp, 0) || record.Timestamp < 0 {
		return errors.New("call telemetry: invalid timestamp")
	}
	if math.IsNaN(record.OriginTimestamp) || math.IsInf(record.OriginTimestamp, 0) || record.OriginTimestamp < 0 {
		return errors.New("call telemetry: invalid origin timestamp")
	}
	if len(record.Metrics) > 128 {
		return errors.New("call telemetry: too many metrics")
	}
	for name, value := range record.Metrics {
		if !KnownMetric(name) || !safeMetricValue(value) {
			return errors.New("call telemetry: invalid metric")
		}
	}
	for _, value := range []string{record.Event, record.Stage, record.Reason} {
		if value != "" && !slugPattern.MatchString(value) {
			return errors.New("call telemetry: invalid event label")
		}
	}
	return nil
}

func Marshal(record Record) ([]byte, error) {
	if err := ValidateRecord(record); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(record)
	if err != nil || len(payload) > MaximumRecordLen {
		return nil, errors.New("call telemetry: record exceeds limit")
	}
	return payload, nil
}

func MarshalLine(record Record) ([]byte, error) {
	payload, err := Marshal(record)
	if err != nil {
		return nil, err
	}
	return append(payload, '\n'), nil
}

func safeMetricValue(value any) bool {
	switch typed := value.(type) {
	case bool:
		return true
	case int:
		return typed >= 0
	case int64:
		return typed >= 0
	case uint64:
		return typed <= math.MaxInt64
	case float64:
		return typed >= 0 && typed <= 1e18 && !math.IsNaN(typed) && !math.IsInf(typed, 0)
	default:
		return false
	}
}
