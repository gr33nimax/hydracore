package option

import "github.com/sagernet/sing/common/json/badoption"

type SelectorOutboundOptions struct {
	GroupCommonOption
	Default                   string `json:"default,omitempty" reference:"outbound"`
	InterruptExistConnections bool   `json:"interrupt_exist_connections,omitempty"`
}

type URLTestOutboundOptions struct {
	GroupCommonOption
	URL          string             `json:"url,omitempty"`
	Interval     badoption.Duration `json:"interval,omitempty"`
	Tolerance    uint16             `json:"tolerance,omitempty"`
	IdleTimeout  badoption.Duration `json:"idle_timeout,omitempty"`
	ProbeTimeout badoption.Duration `json:"probe_timeout,omitempty"`
	// Zero leaves the group at its historical budget of ten parallel probes.
	ProbeConcurrency          int  `json:"probe_concurrency,omitempty"`
	InterruptExistConnections bool `json:"interrupt_exist_connections,omitempty"`
}

type FallbackOutboundOptions struct {
	Outbounds        []string           `json:"outbounds"`
	BlacklistTimeout badoption.Duration `json:"blacklist_timeout,omitempty"`
}

type GroupCommonOption struct {
	Outbounds       []string                   `json:"outbounds" reference:"outbound"`
	Providers       badoption.Listable[string] `json:"providers,omitempty"`
	Exclude         *badoption.Regexp          `json:"exclude,omitempty"`
	Include         *badoption.Regexp          `json:"include,omitempty"`
	UseAllProviders bool                       `json:"use_all_providers,omitempty"`
}
