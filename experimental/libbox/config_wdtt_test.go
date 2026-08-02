//go:build with_wdtt && with_wireguard

package libbox

import (
	"strings"
	"testing"
)

func TestCheckConfigAcceptsBoundedWDTTEndpoint(t *testing.T) {
	err := CheckConfig(`{
		"endpoints": [{
			"type": "wdtt",
			"tag": "wdtt-test",
			"server": "203.0.113.10",
			"server_port": 56000,
			"credential_ref": "wdtt:user-1:device-1",
			"vk_hashes": ["8UkewARpV0aJoWheFZlR942el6unTZvhneulo-eU8gA"],
			"workers": 18,
			"obfs": "audio",
			"vk_auth": "auto",
			"vk_anon_path": "vkcalls"
		}]
	}`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCheckConfigRejectsUnsafeWDTTOptions(t *testing.T) {
	for _, test := range []struct {
		name   string
		field  string
		config string
	}{
		{
			"too many workers",
			"workers",
			`{"endpoints":[{"type":"wdtt","tag":"wdtt","server":"203.0.113.10","server_port":56000,"credential_ref":"wdtt:user:device","vk_hashes":["hash"],"workers":37}]}`,
		},
		{
			"invalid auth",
			"vk_auth",
			`{"endpoints":[{"type":"wdtt","tag":"wdtt","server":"203.0.113.10","server_port":56000,"credential_ref":"wdtt:user:device","vk_hashes":["hash"],"vk_auth":"browser"}]}`,
		},
		{
			"runtime device id",
			"device_id",
			`{"endpoints":[{"type":"wdtt","tag":"wdtt","server":"203.0.113.10","server_port":56000,"credential_ref":"wdtt:user:device","vk_hashes":["hash"],"device_id":"publisher-owned"}]}`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := CheckConfig(test.config)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), test.field) {
				t.Fatalf("expected %q rejection, got %v", test.field, err)
			}
		})
	}
}
