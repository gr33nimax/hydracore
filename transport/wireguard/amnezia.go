package wireguard

import (
	"encoding/base64"
	"encoding/hex"
	"strconv"
	"strings"

	E "github.com/sagernet/sing/common/exceptions"
)

// writeAmneziaOptions appends the AmneziaWG generation directives to the UAPI
// configuration. It is separate from Start so the exact line set for a maximal
// 3.1 configuration and for a legacy one can be asserted without a device.
func writeAmneziaOptions(ipcConf *strings.Builder, amnezia *AmneziaOptions) error {
	if amnezia == nil {
		return nil
	}
	if amnezia.JC > 0 {
		ipcConf.WriteString("\njc=" + strconv.Itoa(amnezia.JC))
	}
	if amnezia.JMin > 0 {
		ipcConf.WriteString("\njmin=" + strconv.Itoa(amnezia.JMin))
	}
	if amnezia.JMax > 0 {
		ipcConf.WriteString("\njmax=" + strconv.Itoa(amnezia.JMax))
	}
	if amnezia.S1 > 0 {
		ipcConf.WriteString("\ns1=" + strconv.Itoa(amnezia.S1))
	}
	if amnezia.S2 > 0 {
		ipcConf.WriteString("\ns2=" + strconv.Itoa(amnezia.S2))
	}
	if amnezia.S3 > 0 {
		ipcConf.WriteString("\ns3=" + strconv.Itoa(amnezia.S3))
	}
	if amnezia.S4 > 0 {
		ipcConf.WriteString("\ns4=" + strconv.Itoa(amnezia.S4))
	}
	if amnezia.H1 != nil {
		ipcConf.WriteString("\nh1=" + amnezia.H1.String())
	}
	if amnezia.H2 != nil {
		ipcConf.WriteString("\nh2=" + amnezia.H2.String())
	}
	if amnezia.H3 != nil {
		ipcConf.WriteString("\nh3=" + amnezia.H3.String())
	}
	if amnezia.H4 != nil {
		ipcConf.WriteString("\nh4=" + amnezia.H4.String())
	}
	if amnezia.I1 != "" {
		ipcConf.WriteString("\ni1=" + amnezia.I1)
	}
	if amnezia.I2 != "" {
		ipcConf.WriteString("\ni2=" + amnezia.I2)
	}
	if amnezia.I3 != "" {
		ipcConf.WriteString("\ni3=" + amnezia.I3)
	}
	if amnezia.I4 != "" {
		ipcConf.WriteString("\ni4=" + amnezia.I4)
	}
	if amnezia.I5 != "" {
		ipcConf.WriteString("\ni5=" + amnezia.I5)
	}
	if amnezia.HeaderProtectionKey != "" {
		headerProtectionKeyBytes, err := base64.StdEncoding.DecodeString(amnezia.HeaderProtectionKey)
		if err != nil {
			return E.Cause(err, "decode header protection key")
		}
		ipcConf.WriteString("\nheader_protection_key=" + hex.EncodeToString(headerProtectionKeyBytes))
	}
	if amnezia.ContentPaddingAddition != nil {
		ipcConf.WriteString("\ncontent_padding_addition=" + amnezia.ContentPaddingAddition.String())
	}
	if amnezia.RekeyAfterTime != nil {
		ipcConf.WriteString("\nrekey_after_time=" + amnezia.RekeyAfterTime.String())
	}
	if amnezia.RekeyTimeout != nil {
		ipcConf.WriteString("\nrekey_timeout=" + amnezia.RekeyTimeout.String())
	}
	if amnezia.RejectAfterTime != nil {
		ipcConf.WriteString("\nreject_after_time=" + amnezia.RejectAfterTime.String())
	}
	if amnezia.KeepaliveTimeout != nil {
		ipcConf.WriteString("\nkeepalive_timeout=" + amnezia.KeepaliveTimeout.String())
	}
	if amnezia.MaxHandshakeAttempts != nil {
		ipcConf.WriteString("\nmax_handshake_attempts=" + amnezia.MaxHandshakeAttempts.String())
	}
	if amnezia.RandomTrailers != nil {
		ipcConf.WriteString("\nrandom_trailers=" + strconv.FormatBool(*amnezia.RandomTrailers))
	}
	if amnezia.DisableCookies != nil {
		ipcConf.WriteString("\ndisable_cookies=" + strconv.FormatBool(*amnezia.DisableCookies))
	}
	return nil
}
