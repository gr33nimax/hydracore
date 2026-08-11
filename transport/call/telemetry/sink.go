package telemetry

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

const (
	DefaultStateDirectory = "/var/lib/hydra/calls/vk/telemetry"
	DefaultOutputPath     = "/run/hydra/calls-telemetry.jsonl"
	maximumStateFileSize  = 64 * 1024
	defaultMaxOutputBytes = 64 * 1024 * 1024
)

var sessionIDPattern = regexp.MustCompile(`^[0-9]{8}T[0-9]{6}Z-[a-f0-9]{8}$`)

type SinkConfig struct {
	StateDirectory string
	OutputPath     string
	MaxOutputBytes int64
}

type Sink struct {
	mu             sync.Mutex
	stateDirectory string
	outputPath     string
	activeSession  string
	file           *os.File
	writtenBytes   int64
	maxOutputBytes int64
	rotations      uint64
}

func NewSink(config SinkConfig) *Sink {
	if config.StateDirectory == "" {
		config.StateDirectory = DefaultStateDirectory
	}
	if config.OutputPath == "" {
		config.OutputPath = DefaultOutputPath
	}
	if config.MaxOutputBytes <= 0 {
		config.MaxOutputBytes = defaultMaxOutputBytes
	}
	return &Sink{
		stateDirectory: config.StateDirectory,
		outputPath:     config.OutputPath,
		maxOutputBytes: config.MaxOutputBytes,
	}
}

func (s *Sink) Sync() (active bool, changed bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sessionID, err := s.readActiveSession()
	if err != nil {
		s.deactivateLocked()
		return false, false, err
	}
	if sessionID == "" {
		changed = s.activeSession != ""
		s.deactivateLocked()
		return false, changed, nil
	}
	if sessionID == s.activeSession && s.file != nil {
		return true, false, nil
	}
	if s.activeSession == "" && s.file == nil {
		resumed, resumeErr := s.resumeLocked(sessionID)
		if resumeErr != nil {
			s.deactivateLocked()
			return false, false, resumeErr
		}
		if resumed {
			return true, true, nil
		}
	}
	if err = s.rotateLocked(sessionID); err != nil {
		s.deactivateLocked()
		return false, false, err
	}
	return true, true, nil
}

func (s *Sink) Write(record Record) error {
	payload, err := MarshalLine(record)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file == nil || s.activeSession == "" {
		return nil
	}
	if s.writtenBytes > 0 && s.writtenBytes+int64(len(payload)) > s.maxOutputBytes {
		sessionID := s.activeSession
		if err = s.rotateLocked(sessionID); err != nil {
			return err
		}
		s.rotations++
	}
	written, err := s.file.Write(payload)
	s.writtenBytes += int64(written)
	return err
}

func (s *Sink) Active() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.file != nil && s.activeSession != ""
}

func (s *Sink) Rotations() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rotations
}

func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	if s.file != nil {
		err = s.file.Close()
	}
	s.file = nil
	s.activeSession = ""
	s.writtenBytes = 0
	return err
}

func (s *Sink) readActiveSession() (string, error) {
	pointerPath := filepath.Join(s.stateDirectory, "active.json")
	pointer, err := readProtectedJSON(pointerPath)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	sessionID, _ := pointer["session_id"].(string)
	if !sessionIDPattern.MatchString(sessionID) {
		return "", errors.New("call telemetry: invalid active session pointer")
	}
	session, err := readProtectedJSON(filepath.Join(s.stateDirectory, sessionID+".json"))
	if err != nil {
		return "", err
	}
	if storedID, _ := session["session_id"].(string); storedID != sessionID {
		return "", errors.New("call telemetry: active session identity mismatch")
	}
	stopped, ok := session["stopped_at"].(float64)
	if !ok || stopped != 0 {
		return "", nil
	}
	return sessionID, nil
}

func readProtectedJSON(path string) (map[string]any, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > maximumStateFileSize {
		return nil, errors.New("call telemetry: unsafe state file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	content, err := io.ReadAll(io.LimitReader(file, maximumStateFileSize+1))
	if err != nil || len(content) > maximumStateFileSize {
		return nil, errors.New("call telemetry: unreadable state file")
	}
	var payload map[string]any
	if err = json.Unmarshal(content, &payload); err != nil {
		return nil, errors.New("call telemetry: invalid state file")
	}
	return payload, nil
}

func (s *Sink) rotateLocked(sessionID string) error {
	if err := os.MkdirAll(filepath.Dir(s.outputPath), 0o700); err != nil {
		return fmt.Errorf("call telemetry: create runtime directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(s.outputPath), ".calls-telemetry-*.jsonl")
	if err != nil {
		return fmt.Errorf("call telemetry: create runtime file: %w", err)
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err = temporary.Chmod(0o600); err != nil {
		return err
	}
	if err = temporary.Sync(); err != nil {
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	preserveCurrent := s.file != nil && s.activeSession == sessionID && s.writtenBytes > 0
	handoffPath := ""
	if preserveCurrent {
		handoffPath, err = s.nextHandoffPathLocked(sessionID)
		if err != nil {
			return err
		}
		if err = s.file.Close(); err != nil {
			return err
		}
		s.file = nil
		if err = os.Rename(s.outputPath, handoffPath); err != nil {
			s.file, _ = os.OpenFile(s.outputPath, os.O_WRONLY|os.O_APPEND, 0o600)
			return fmt.Errorf("call telemetry: preserve runtime segment: %w", err)
		}
	}
	if err = os.Rename(temporaryPath, s.outputPath); err != nil {
		if handoffPath != "" {
			_ = os.Rename(handoffPath, s.outputPath)
			s.file, _ = os.OpenFile(s.outputPath, os.O_WRONLY|os.O_APPEND, 0o600)
		}
		return fmt.Errorf("call telemetry: publish runtime file: %w", err)
	}
	committed = true
	if err = writeSessionMarker(s.outputPath+".session", sessionID); err != nil {
		return err
	}
	if s.file != nil {
		_ = s.file.Close()
	}
	s.file, err = os.OpenFile(s.outputPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	s.activeSession = sessionID
	s.writtenBytes = 0
	return nil
}

func (s *Sink) nextHandoffPathLocked(sessionID string) (string, error) {
	pattern := s.outputPath + "." + sessionID + ".part-*.jsonl"
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return "", fmt.Errorf("call telemetry: enumerate runtime segments: %w", err)
	}
	for index := len(matches) + 1; index <= 99999; index++ {
		candidate := fmt.Sprintf("%s.%s.part-%05d.jsonl", s.outputPath, sessionID, index)
		if _, statErr := os.Lstat(candidate); errors.Is(statErr, os.ErrNotExist) {
			return candidate, nil
		} else if statErr != nil {
			return "", fmt.Errorf("call telemetry: inspect runtime segment: %w", statErr)
		}
	}
	return "", errors.New("call telemetry: runtime segment limit reached")
}

func (s *Sink) resumeLocked(sessionID string) (bool, error) {
	marker, err := readSessionMarker(s.outputPath + ".session")
	if errors.Is(err, os.ErrNotExist) || err != nil || marker != sessionID {
		return false, nil
	}
	info, err := os.Lstat(s.outputPath)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return false, nil
	}
	file, err := os.OpenFile(s.outputPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return false, err
	}
	if err = file.Chmod(0o600); err != nil {
		_ = file.Close()
		return false, err
	}
	s.file = file
	s.activeSession = sessionID
	s.writtenBytes = info.Size()
	return true, nil
}

func readSessionMarker(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > 128 {
		return "", errors.New("call telemetry: unsafe session marker")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sessionID := strings.TrimSpace(string(content))
	if !sessionIDPattern.MatchString(sessionID) {
		return "", errors.New("call telemetry: invalid session marker")
	}
	return sessionID, nil
}

func writeSessionMarker(path, sessionID string) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".calls-telemetry-session-*")
	if err != nil {
		return fmt.Errorf("call telemetry: create session marker: %w", err)
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err = temporary.Chmod(0o600); err != nil {
		return err
	}
	if _, err = temporary.WriteString(sessionID + "\n"); err != nil {
		return err
	}
	if err = temporary.Sync(); err != nil {
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	if err = os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("call telemetry: publish session marker: %w", err)
	}
	committed = true
	return nil
}

func (s *Sink) deactivateLocked() {
	if s.file != nil {
		_ = s.file.Close()
	}
	s.file = nil
	s.activeSession = ""
	s.writtenBytes = 0
}
