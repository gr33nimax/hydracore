package transport

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPSDestinationPreservesEscapedPathAndQuery(t *testing.T) {
	destination := &url.URL{Scheme: "https", Host: "dns.example"}
	require.NoError(t, SetHTTPSDestinationPathAndQuery(destination, "/dns%2Dquery", "token=a%2Bb", false))
	require.Equal(t, "https://dns.example/dns%2Dquery?token=a%2Bb", destination.String())
}

func TestHTTPSDestinationPreservesEmptyQuery(t *testing.T) {
	destination := &url.URL{Scheme: "https", Host: "dns.example"}
	require.NoError(t, SetHTTPSDestinationPathAndQuery(destination, "", "", true))
	require.Equal(t, "https://dns.example/dns-query?", destination.String())
}
