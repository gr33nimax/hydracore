//go:build !with_wdtt

package libbox

import E "github.com/sagernet/sing/common/exceptions"

const wdttIncluded = false

func SetHydraWDTTCredential(string, string, string) error {
	return E.New("WDTT is not included in this HydraCore build")
}

func ClearHydraWDTTCredentials() {}

func SetHydraWDTTVKAccountCredentials(string, string, string, string, int64) error {
	return E.New("WDTT is not included in this HydraCore build")
}
