package wireguard

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/sagernet/sing/common/json/badoption"
	"github.com/stretchr/testify/require"
)

type recordingFailingIPCSetter struct {
	config string
	err    error
}

func (s *recordingFailingIPCSetter) IpcSet(config string) error {
	s.config = config
	return s.err
}

func TestConfigureWireGuardDeviceRedactsSecretConfiguration(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("invalid UAPI input")
	setter := &recordingFailingIPCSetter{err: sentinel}
	config := "private_key=private-secret\npreshared_key=pre-shared-secret"

	err := configureWireGuardDevice(setter, config)
	require.ErrorIs(t, err, sentinel)
	require.Contains(t, err.Error(), "setup wireguard")
	require.Equal(t, config, setter.config)
	require.NotContains(t, err.Error(), "private-secret")
	require.NotContains(t, err.Error(), "pre-shared-secret")
}

// The maximal AmneziaWG 3.1 block has to reach the UAPI complete: an omitted line
// is a silently different generation on the wire.
func TestAmneziaOptionsEmitTheFullGenerationLineSet(t *testing.T) {
	t.Parallel()
	randomTrailers := true
	disableCookies := false
	var ipcConf strings.Builder
	err := writeAmneziaOptions(&ipcConf, &AmneziaOptions{
		JC:                     120,
		JMin:                   23,
		JMax:                   911,
		S1:                     40,
		S2:                     120,
		S3:                     12,
		S4:                     12,
		H1:                     &badoption.Range[uint32]{From: 1, To: 1},
		H2:                     &badoption.Range[uint32]{From: 2, To: 2},
		H3:                     &badoption.Range[uint32]{From: 3, To: 3},
		H4:                     &badoption.Range[uint32]{From: 4, To: 4},
		I1:                     "<b 0xc70000000108>",
		HeaderProtectionKey:    base64.StdEncoding.EncodeToString(make([]byte, 32)),
		ContentPaddingAddition: &badoption.Range[uint32]{From: 50, To: 100},
		RekeyAfterTime:         &badoption.Range[uint32]{From: 100, To: 140},
		RekeyTimeout:           &badoption.Range[uint32]{From: 4, To: 6},
		RejectAfterTime:        &badoption.Range[uint32]{From: 160, To: 200},
		KeepaliveTimeout:       &badoption.Range[uint32]{From: 8, To: 12},
		MaxHandshakeAttempts:   &badoption.Range[uint32]{From: 15, To: 20},
		RandomTrailers:         &randomTrailers,
		DisableCookies:         &disableCookies,
	})
	require.NoError(t, err)
	config := ipcConf.String()
	for _, line := range []string{
		"jc=120", "jmin=23", "jmax=911",
		"s1=40", "s2=120", "s3=12", "s4=12",
		"h1=1", "h2=2", "h3=3", "h4=4",
		"i1=<b 0xc70000000108>",
		"header_protection_key=" + strings.Repeat("00", 32),
		"content_padding_addition=50-100",
		"rekey_after_time=100-140",
		"rekey_timeout=4-6",
		"reject_after_time=160-200",
		"keepalive_timeout=8-12",
		"max_handshake_attempts=15-20",
		"random_trailers=true",
		"disable_cookies=false",
	} {
		require.Contains(t, config, line)
	}
}

// A nil block and a legacy configuration must not grow a single 3.x line: an
// absent parameter keeps the standard WireGuard behaviour in the fork.
func TestAmneziaOptionsStayLegacyWhenGenerationFieldsAreAbsent(t *testing.T) {
	t.Parallel()
	var ipcConf strings.Builder
	require.NoError(t, writeAmneziaOptions(&ipcConf, nil))
	require.Empty(t, ipcConf.String())

	ipcConf.Reset()
	require.NoError(t, writeAmneziaOptions(&ipcConf, &AmneziaOptions{JC: 5, JMin: 50, JMax: 150}))
	require.Equal(t, "\njc=5\njmin=50\njmax=150", ipcConf.String())
	require.NotContains(t, ipcConf.String(), "random_trailers")
	require.NotContains(t, ipcConf.String(), "disable_cookies")
}

func TestAmneziaHeaderProtectionKeyRejectsMalformedInput(t *testing.T) {
	t.Parallel()
	var ipcConf strings.Builder
	err := writeAmneziaOptions(&ipcConf, &AmneziaOptions{HeaderProtectionKey: "not base64!"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode header protection key")
}
