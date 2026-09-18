package option

import (
	"testing"

	"github.com/sagernet/sing/common/json"

	"github.com/stretchr/testify/require"
)

// The full AmneziaWG 3.1 set has to survive the strict configuration parser:
// ranges, CPS packets, the header protection key and the two booleans.
func TestWireGuardAmneziaCarriesTheCompleteAWG31Set(t *testing.T) {
	content := `{` +
		`"address":["10.0.0.2/32"],` +
		`"private_key":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",` +
		`"amnezia":{` +
		`"jc":120,"jmin":23,"jmax":911,"s1":40,"s2":120,"s3":12,"s4":12,` +
		`"h1":"1","h2":"2","h3":"3","h4":"4",` +
		`"i1":"<b 0xc70000000108>",` +
		`"header_protection_key":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=",` +
		`"content_padding_addition":"50-100","rekey_after_time":"100-140",` +
		`"rekey_timeout":"4-6","reject_after_time":"160-200",` +
		`"keepalive_timeout":"8-12","max_handshake_attempts":"15-20",` +
		`"random_trailers":true,"disable_cookies":false}}`

	var options WireGuardEndpointOptions
	require.NoError(t, json.UnmarshalDisallowUnknownFields([]byte(content), &options))
	require.NotNil(t, options.Amnezia)
	require.Equal(t, 120, options.Amnezia.JC)
	require.Equal(t, "<b 0xc70000000108>", options.Amnezia.I1)
	require.Equal(t, "50-100", options.Amnezia.ContentPaddingAddition.String())
	require.Equal(t, "100-140", options.Amnezia.RekeyAfterTime.String())
	require.Equal(t, "15-20", options.Amnezia.MaxHandshakeAttempts.String())
	require.NotNil(t, options.Amnezia.RandomTrailers)
	require.True(t, *options.Amnezia.RandomTrailers)
	require.NotNil(t, options.Amnezia.DisableCookies)
	require.False(t, *options.Amnezia.DisableCookies)
}

// The two 3.1 booleans are JSON booleans in the contract: a renderer that sends
// them as strings produces a configuration the strict parser refuses.
func TestWireGuardAmneziaRejectsStringifiedBooleans(t *testing.T) {
	var options WireGuardEndpointOptions
	err := json.UnmarshalDisallowUnknownFields([]byte(`{"amnezia":{"random_trailers":"true"}}`), &options)
	require.Error(t, err)
	require.Contains(t, err.Error(), "random_trailers")
}
