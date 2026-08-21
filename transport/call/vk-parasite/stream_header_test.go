package vkparasite

import (
	"bytes"
	"io"
	"testing"

	M "github.com/sagernet/sing/common/metadata"
	"github.com/stretchr/testify/require"
)

// ai-generated: unit test for QUIC stream header serialization and roundtrip
func TestStreamHeaderRoundtrip(t *testing.T) {
	testCases := []struct {
		name string
		kind byte
		dest M.Socksaddr
	}{
		{
			name: "IPv4_TCP",
			kind: streamKindTCP,
			dest: M.ParseSocksaddr("1.2.3.4:8080"),
		},
		{
			name: "IPv4_UDP",
			kind: streamKindUDP,
			dest: M.ParseSocksaddr("1.2.3.4:53"),
		},
		{
			name: "IPv6_TCP",
			kind: streamKindTCP,
			dest: M.ParseSocksaddr("[2001:db8::1]:443"),
		},
		{
			name: "IPv6_UDP",
			kind: streamKindUDP,
			dest: M.ParseSocksaddr("[2001:db8::1]:53"),
		},
		{
			name: "Domain_TCP",
			kind: streamKindTCP,
			dest: M.ParseSocksaddr("example.com:443"),
		},
		{
			name: "Domain_UDP",
			kind: streamKindUDP,
			dest: M.ParseSocksaddr("dns.google:53"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := writeStreamHeader(&buf, tc.kind, tc.dest)
			require.NoError(t, err)

			readKind, readDest, err := readStreamHeader(&buf)
			require.NoError(t, err)
			require.Equal(t, tc.kind, readKind)
			require.Equal(t, tc.dest.String(), readDest.String())
		})
	}
}

func TestStreamHeaderRejectsUnknownKind(t *testing.T) {
	var buf bytes.Buffer
	dest := M.ParseSocksaddr("1.2.3.4:80")

	err := writeStreamHeader(&buf, 0x00, dest)
	require.Error(t, err)

	err = writeStreamHeader(&buf, 0x03, dest)
	require.Error(t, err)

	// Simulate unknown kind on read
	buf.Reset()
	buf.WriteByte(0x99)
	_ = M.SocksaddrSerializer.WriteAddrPort(&buf, dest)

	_, _, err = readStreamHeader(&buf)
	require.Error(t, err)
}

func TestStreamHeaderRejectsTruncated(t *testing.T) {
	var buf bytes.Buffer
	dest := M.ParseSocksaddr("example.com:443")
	err := writeStreamHeader(&buf, streamKindTCP, dest)
	require.NoError(t, err)

	full := buf.Bytes()
	for i := 0; i < len(full)-1; i++ {
		truncated := bytes.NewReader(full[:i])
		_, _, err := readStreamHeader(truncated)
		require.Error(t, err)
	}

	empty := bytes.NewReader(nil)
	_, _, err = readStreamHeader(empty)
	require.ErrorIs(t, err, io.EOF)
}
