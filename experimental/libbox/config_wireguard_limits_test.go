//go:build with_wireguard

package libbox

import (
	"strings"
	"testing"
)

func TestCheckConfigRejectsUnsafeWireGuardResourceOptions(t *testing.T) {
	tests := []struct {
		name   string
		config string
		field  string
	}{
		{
			"negative workers",
			`{"endpoints":[{"type":"wireguard","tag":"wg","workers":-1}]}`,
			"workers",
		},
		{
			"too many preallocated buffers per pool",
			`{"endpoints":[{"type":"wireguard","tag":"wg","preallocated_buffers_per_pool":4097}]}`,
			"preallocated_buffers_per_pool",
		},
		{
			"inverted amnezia junk range",
			`{"endpoints":[{"type":"wireguard","tag":"wg","amnezia":{"jc":1,"jmin":2,"jmax":1}}]}`,
			"jmin",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := CheckConfig(test.config)
			if err == nil {
				t.Fatal("expected CheckConfig to reject unsafe WireGuard options")
			}
			if !strings.Contains(err.Error(), test.field) {
				t.Fatalf("expected error to mention %q, got %v", test.field, err)
			}
		})
	}
}
