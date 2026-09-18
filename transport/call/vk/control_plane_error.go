package vk

import (
	"errors"
	"fmt"
	"time"
)

var ErrVKCaptchaRequired = errors.New("VK captcha required")

type ControlPlaneError struct {
	Stage        string
	Kind         string
	Code         string
	RetryAfter   time.Duration
	ChallengeID string
	Terminal     bool
	Cause        error
}

func (e *ControlPlaneError) Error() string {
	if e == nil {
		return "VK control-plane error"
	}
	message := "VK control-plane error"
	if e.Stage != "" {
		message += " at " + e.Stage
	}
	if e.Kind != "" {
		message += " (" + e.Kind + ")"
	}
	if e.Code != "" {
		message += " code=" + e.Code
	}
	if e.RetryAfter > 0 {
		message += " retry_after=" + e.RetryAfter.Round(time.Second).String()
	}
	if e.ChallengeID != "" {
		message += " challenge_id=" + e.ChallengeID
	}
	if e.Cause != nil {
		message += ": " + e.Cause.Error()
	}
	return message
}

func (e *ControlPlaneError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func newControlPlaneError(stage, kind, code string, cause error) error {
	return &ControlPlaneError{Stage: stage, Kind: kind, Code: code, Cause: cause}
}

func controlPlaneErrorf(stage, kind, code, format string, values ...any) error {
	return newControlPlaneError(stage, kind, code, fmt.Errorf(format, values...))
}

func AsControlPlaneError(err error) (*ControlPlaneError, bool) {
	var controlError *ControlPlaneError
	return controlError, errors.As(err, &controlError)
}
