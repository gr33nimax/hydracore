package wdtt

import (
	"net"
	"strings"
	"testing"

	"github.com/sagernet/sing-box/option"
)

func validEndpointOptions() option.WDTTEndpointOptions {
	return option.WDTTEndpointOptions{
		Server:     "203.0.113.10",
		ServerPort: 56000,
		Password:   "subscription-secret",
		VKHashes:   []string{"8UkewARpV0aJoWheFZlR942el6unTZvhneulo-eU8gA"},
	}
}

func TestNormalizeAndValidateOptionsDefaults(t *testing.T) {
	options := validEndpointOptions()
	if err := normalizeAndValidateOptions(&options); err != nil {
		t.Fatal(err)
	}
	if options.Workers != defaultWorkers || options.Obfs != "audio" || options.VKAuth != "anonymous" || options.VKAnonPath != "vkcalls" {
		t.Fatalf("unexpected defaults: %+v", options)
	}
}

func TestNormalizeAndValidateOptionsRejectsPublisherControlledRuntimeState(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*option.WDTTEndpointOptions)
	}{
		{"workers", func(options *option.WDTTEndpointOptions) { options.Workers = maximumWorkers + 1 }},
		{"hash count", func(options *option.WDTTEndpointOptions) { options.VKHashes = []string{"a", "b", "c", "d", "e"} }},
		{"hash URL", func(options *option.WDTTEndpointOptions) {
			options.VKHashes = []string{"https://vk.com/call/join/hash"}
		}},
		{"password delimiter", func(options *option.WDTTEndpointOptions) { options.Password = "secret|injected" }},
		{"auth mode", func(options *option.WDTTEndpointOptions) { options.VKAuth = "account" }},
		{"anonymous path", func(options *option.WDTTEndpointOptions) { options.VKAnonPath = "legacy" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := validEndpointOptions()
			test.mutate(&options)
			if err := normalizeAndValidateOptions(&options); err == nil {
				t.Fatal("expected options to be rejected")
			}
		})
	}
}

func TestValidServerName(t *testing.T) {
	for _, server := range []string{"203.0.113.10", "2001:db8::1", "relay.example.com", "relay.example.com."} {
		if !validServerName(server) {
			t.Fatalf("expected %q to be accepted", server)
		}
	}
	for _, server := range []string{"-relay.example", "relay..example", "relay_example", strings.Repeat("a", 64) + ".example"} {
		if validServerName(server) {
			t.Fatalf("expected %q to be rejected", server)
		}
	}
}

func TestNumericRemoteAddressRejectsDNSFallback(t *testing.T) {
	numeric, err := numericRemoteAddress(&net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 3478})
	if err != nil || numeric != "[2001:db8::1]:3478" {
		t.Fatalf("unexpected numeric remote address: %q, %v", numeric, err)
	}
	if _, err = numericRemoteAddress(stringAddress("turn.example:3478")); err == nil {
		t.Fatal("expected a hostname remote address to be rejected")
	}
}

type stringAddress string

func (a stringAddress) Network() string { return "udp" }
func (a stringAddress) String() string  { return string(a) }
