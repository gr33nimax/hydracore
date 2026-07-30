package adapter

import (
	"testing"
	"time"
)

func TestURLTestHistoryStatus(t *testing.T) {
	t.Parallel()
	if URLTestHistoryStatus(nil) != "" {
		t.Fatal("nil history must not report a status")
	}
	failed := &URLTestHistory{Time: time.Now(), Status: URLTestStatusUnavailable, Error: "timeout"}
	if URLTestHistoryStatus(failed) != URLTestStatusUnavailable {
		t.Fatal("failed history must remain unavailable")
	}
	available := &URLTestHistory{Time: time.Now(), Delay: 42, Status: URLTestStatusUnavailable}
	if URLTestHistoryStatus(available) != URLTestStatusAvailable {
		t.Fatal("a positive delay must remain compatible with legacy available histories")
	}
}
