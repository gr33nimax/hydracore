package wdtt

import (
	"testing"
	"time"
)

func TestRuntimeCredentialLifecycle(t *testing.T) {
	ClearRuntimeCredentials()
	t.Cleanup(ClearRuntimeCredentials)
	if err := SetRuntimeCredential("wdtt:user-1:device-1", "device-1", "opaque-grant"); err != nil {
		t.Fatal(err)
	}
	deviceID, grant, loaded := loadRuntimeCredential("wdtt:user-1:device-1")
	if !loaded || deviceID != "device-1" || grant != "opaque-grant" {
		t.Fatalf("unexpected credential: loaded=%v device=%q grant=%q", loaded, deviceID, grant)
	}
	ClearRuntimeCredentials()
	if _, _, loaded = loadRuntimeCredential("wdtt:user-1:device-1"); loaded {
		t.Fatal("credential survived clear")
	}
}

func TestRuntimeAccountCredentialLifecycle(t *testing.T) {
	ClearRuntimeCredentials()
	t.Cleanup(ClearRuntimeCredentials)
	err := SetRuntimeAccountCredentials(
		"wdtt:user-1:device-1",
		"turn-user",
		"turn-password",
		`["203.0.113.20:3478"]`,
		time.Now().Add(9*time.Minute).Unix(),
	)
	if err != nil {
		t.Fatal(err)
	}
	credentials, loaded := loadRuntimeAccountCredentials("wdtt:user-1:device-1")
	if !loaded || credentials.username != "turn-user" || credentials.password != "turn-password" || len(credentials.urls) != 1 {
		t.Fatalf("unexpected account credentials: loaded=%v credentials=%+v", loaded, credentials)
	}
}

func TestRuntimeCredentialValidation(t *testing.T) {
	for _, test := range []struct {
		ref    string
		device string
		grant  string
	}{
		{ref: "", device: "device", grant: "grant"},
		{ref: "bad/ref", device: "device", grant: "grant"},
		{ref: "ref", device: "bad|device", grant: "grant"},
		{ref: "ref", device: "device", grant: ""},
	} {
		if err := SetRuntimeCredential(test.ref, test.device, test.grant); err == nil {
			t.Fatalf("accepted invalid credential: %+v", test)
		}
	}
}
