package option

import "github.com/sagernet/sing/common/json/badoption"

type SelectorOutboundOptions struct {
	GroupCommonOption
	Default                   string `json:"default,omitempty" reference:"outbound"`
	InterruptExistConnections bool   `json:"interrupt_exist_connections,omitempty"`
}

type URLTestOutboundOptions struct {
	GroupCommonOption
	URL      string             `json:"url,omitempty"`
	Interval badoption.Duration `json:"interval,omitempty"`
	// UnavailableInterval is how soon a target whose last probe failed is asked again. The general
	// interval is meant for healthy servers; without this one, a server that hiccuped once is
	// reported unreachable for the whole of it, and the setting that names the shorter wait had
	// nowhere to go.
	UnavailableInterval badoption.Duration `json:"unavailable_interval,omitempty"`
	Tolerance           uint16             `json:"tolerance,omitempty"`
	IdleTimeout         badoption.Duration `json:"idle_timeout,omitempty"`
	ProbeTimeout        badoption.Duration `json:"probe_timeout,omitempty"`
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
