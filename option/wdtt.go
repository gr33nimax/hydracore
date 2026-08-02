// SPDX-License-Identifier: GPL-3.0-only

package option

// WDTTEndpointOptions describes a subscription-provided WDTT server.
//
// Device identity, the local UDP bridge, VK TURN credentials and the dynamic
// WireGuard keys are intentionally runtime-owned and therefore absent here.
type WDTTEndpointOptions struct {
	Server        string   `json:"server"`
	ServerPort    uint16   `json:"server_port"`
	CredentialRef string   `json:"credential_ref,omitempty"`
	Password      string   `json:"password,omitempty"`
	VKHashes      []string `json:"vk_hashes"`
	Workers       int      `json:"workers,omitempty"`
	Obfs          string   `json:"obfs,omitempty"`
	VKAuth        string   `json:"vk_auth,omitempty"`
	VKAnonPath    string   `json:"vk_anon_path,omitempty"`
}
