package wireguard

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/option"
)

func validResourceLimitedOptions() option.WireGuardEndpointOptions {
	return option.WireGuardEndpointOptions{
		Workers:                    64,
		PreallocatedBuffersPerPool: maxWireGuardBuffersPerPool,
		Amnezia: &option.WireGuardAmnezia{
			JC:   120,
			JMin: 23,
			JMax: 911,
			S1:   1,
			S2:   2,
			S3:   3,
			S4:   4,
		},
	}
}

func TestValidateEndpointResourceLimitsAcceptsClassicAmnezia(t *testing.T) {
	for _, options := range []option.WireGuardEndpointOptions{
		{},
		validResourceLimitedOptions(),
		{
			Amnezia: &option.WireGuardAmnezia{
				JC:   128,
				JMax: maxAmneziaHandshakeJunkBytes / 128,
				S1:   maxAmneziaPacketPaddingBytes,
			},
		},
	} {
		if err := validateEndpointResourceLimits(options); err != nil {
			t.Fatalf("expected valid options, got %v", err)
		}
	}
}

func TestValidateEndpointResourceLimitsRejectsUnsafeValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*option.WireGuardEndpointOptions)
	}{
		{"negative workers", func(options *option.WireGuardEndpointOptions) { options.Workers = -1 }},
		{"excessive workers", func(options *option.WireGuardEndpointOptions) { options.Workers = maxWireGuardWorkers + 1 }},
		{"excessive preallocated buffers per pool", func(options *option.WireGuardEndpointOptions) {
			options.PreallocatedBuffersPerPool = maxWireGuardBuffersPerPool + 1
		}},
		{"negative jc", func(options *option.WireGuardEndpointOptions) { options.Amnezia.JC = -1 }},
		{"excessive jc", func(options *option.WireGuardEndpointOptions) { options.Amnezia.JC = maxAmneziaJunkPacketCount + 1 }},
		{"negative jmin", func(options *option.WireGuardEndpointOptions) { options.Amnezia.JMin = -1 }},
		{"excessive jmax", func(options *option.WireGuardEndpointOptions) {
			options.Amnezia.JMax = maxAmneziaPacketPaddingBytes + 1
		}},
		{"inverted junk range", func(options *option.WireGuardEndpointOptions) { options.Amnezia.JMin, options.Amnezia.JMax = 912, 911 }},
		{"excessive junk burst", func(options *option.WireGuardEndpointOptions) {
			options.Amnezia.JC, options.Amnezia.JMax = 128, maxAmneziaHandshakeJunkBytes/128+1
		}},
		{"negative s1", func(options *option.WireGuardEndpointOptions) { options.Amnezia.S1 = -1 }},
		{"excessive s4", func(options *option.WireGuardEndpointOptions) { options.Amnezia.S4 = maxAmneziaPacketPaddingBytes + 1 }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := validResourceLimitedOptions()
			test.mutate(&options)
			if err := validateEndpointResourceLimits(options); err == nil {
				t.Fatal("expected unsafe resource options to be rejected")
			}
		})
	}
}

func TestNewEndpointRejectsUnsafeWorkersBeforeDeviceConstruction(t *testing.T) {
	_, err := NewEndpoint(
		context.Background(),
		nil,
		nil,
		"wg-test",
		option.WireGuardEndpointOptions{Workers: -1},
	)
	if err == nil {
		t.Fatal("expected NewEndpoint to reject negative workers")
	}
}
