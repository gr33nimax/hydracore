package option

import (
	"testing"

	"github.com/sagernet/sing/common/json"

	"github.com/stretchr/testify/require"
)

// The migrated core carries the upstream Snell implementation: a server accepts
// version 5 (obfuscation) or 6 (its own mode) together with the flat obfs_mode /
// mode fields, and a client accepts 4 (the other half of the 5 pair) or 6.
// Hydra Ultimate renders exactly these shapes; a server-side version 4 or a nested
// `obfs` object is what the previous renderer produced, and both are refused.
func TestSnellServerAcceptsTheUpstreamGenerations(t *testing.T) {
	for _, obfsMode := range []string{"none", "http", "tls"} {
		var options SnellInboundOptions
		content := `{"version":5,"listen_port":32000,"psk":"secret","obfs_mode":"` + obfsMode + `"}`
		require.NoError(t, json.UnmarshalDisallowUnknownFields([]byte(content), &options), obfsMode)
		require.Equal(t, 5, options.Version)
		require.Equal(t, obfsMode, options.ObfsOptions.ObfsMode)
	}
	for _, mode := range []string{"default", "unshaped", "unsafe-raw"} {
		var options SnellInboundOptions
		content := `{"version":6,"listen_port":32000,"psk":"secret","mode":"` + mode + `"}`
		require.NoError(t, json.UnmarshalDisallowUnknownFields([]byte(content), &options), mode)
		require.Equal(t, 6, options.Version)
		require.Equal(t, mode, options.V6Options.Mode)
	}
}

func TestSnellClientAcceptsThePairedVersions(t *testing.T) {
	var classic SnellOutboundOptions
	require.NoError(t, json.UnmarshalDisallowUnknownFields(
		[]byte(`{"version":4,"server":"example.com","server_port":32000,"psk":"secret","obfs_mode":"tls","obfs_host":"www.bing.com"}`),
		&classic,
	))
	require.Equal(t, 4, classic.Version)
	require.Equal(t, "tls", classic.ObfsOptions.ObfsMode)
	require.Equal(t, "www.bing.com", classic.ObfsOptions.ObfsHost)

	var modern SnellOutboundOptions
	require.NoError(t, json.UnmarshalDisallowUnknownFields(
		[]byte(`{"version":6,"server":"example.com","server_port":32000,"psk":"secret","mode":"unshaped"}`),
		&modern,
	))
	require.Equal(t, 6, modern.Version)
	require.Equal(t, "unshaped", modern.V6Options.Mode)
}

func TestSnellRefusesThePreviousShape(t *testing.T) {
	// The core Hydra moved away from had a server-side version 4.
	var versionFour SnellInboundOptions
	err := json.UnmarshalDisallowUnknownFields(
		[]byte(`{"version":4,"listen_port":32000,"psk":"secret"}`),
		&versionFour,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported version")

	// Obfuscation is a flat field; the nested object is what the renderer used to send.
	var nestedObfs SnellInboundOptions
	err = json.UnmarshalDisallowUnknownFields(
		[]byte(`{"version":5,"listen_port":32000,"psk":"secret","obfs":{"mode":"tls"}}`),
		&nestedObfs,
	)
	require.Error(t, err)

	// There is no client-side version 5: the classic pair is client 4 against server 5.
	var versionFive SnellOutboundOptions
	err = json.UnmarshalDisallowUnknownFields(
		[]byte(`{"version":5,"server":"example.com","server_port":32000,"psk":"secret"}`),
		&versionFive,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported version")
}
