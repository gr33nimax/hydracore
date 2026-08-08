package libbox

import (
	"encoding/json"
	"runtime"
	"strings"

	C "github.com/sagernet/sing-box/constant"
)

const (
	hydraCoreSourceRepository = "https://github.com/gr33nimax/hydracore"
	hydraCoreUpstreamProject  = "sing-box-extended"
	hydraCoreUpstreamRepo     = "https://github.com/shtorm-7/sing-box-extended.git"
	hydraCoreUpstreamBranch   = "extended"
	hydraCoreUpstreamTag      = "v1.13.16-extended-2.6.1"
	hydraCoreUpstreamCommit   = "da4c532efb1f86a38a324909fc9b8867f811551c"
	hydraCoreGomobileVersion  = "v0.1.12"
	hydraCoreJavaVersion      = "17"
	hydraCoreAndroidNDK       = "r28"
	hydraCoreAndroidAPI       = 23
	hydraCoreReleaseBuildTags = "with_gvisor,with_quic,with_dhcp,with_wireguard,with_utls,with_acme,with_clash_api,with_manager,with_admin_panel,with_profiler,with_v2ray_api,with_masque,with_mtproxy,with_ccm,with_ocm,with_openvpn,with_trusttunnel,with_call,with_sudoku,with_snell,with_naive_outbound,badlinkname,tfogo_checklinkname0,with_tailscale,ts_omit_logtail,ts_omit_ssh,ts_omit_drive,ts_omit_taildrop,ts_omit_webclient,ts_omit_doctor,ts_omit_capture,ts_omit_kube,ts_omit_aws,ts_omit_synology,ts_omit_bird"
)

var hydraCoreSourceCommit = "unknown"

type hydraCoreBuildInformation struct {
	SchemaVersion int `json:"schema_version"`
	Distribution  struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Version string `json:"version"`
	} `json:"distribution"`
	Source struct {
		Repository string `json:"repository"`
		Commit     string `json:"commit"`
	} `json:"source"`
	Upstream struct {
		Project    string `json:"project"`
		Repository string `json:"repository"`
		Branch     string `json:"branch"`
		Tag        string `json:"tag"`
		Commit     string `json:"commit"`
	} `json:"upstream"`
	Toolchain struct {
		Go         string   `json:"go"`
		Gomobile   string   `json:"gomobile"`
		Java       string   `json:"java"`
		AndroidNDK string   `json:"android_ndk"`
		AndroidAPI int      `json:"android_api"`
		BuildTags  []string `json:"build_tags"`
	} `json:"toolchain"`
	Lineage []hydraCoreLineageEntry `json:"lineage"`
}

type hydraCoreLineageEntry struct {
	Project string `json:"project"`
	Role    string `json:"role"`
}

func HydraCoreBuildInfo() string {
	var info hydraCoreBuildInformation
	info.SchemaVersion = 1
	info.Distribution.ID = "io.hydrabox.hydracore"
	info.Distribution.Name = "HydraCore"
	info.Distribution.Version = C.Version
	info.Source.Repository = hydraCoreSourceRepository
	info.Source.Commit = hydraCoreSourceCommit
	info.Upstream.Project = hydraCoreUpstreamProject
	info.Upstream.Repository = hydraCoreUpstreamRepo
	info.Upstream.Branch = hydraCoreUpstreamBranch
	info.Upstream.Tag = hydraCoreUpstreamTag
	info.Upstream.Commit = hydraCoreUpstreamCommit
	info.Toolchain.Go = runtime.Version()
	info.Toolchain.Gomobile = hydraCoreGomobileVersion
	info.Toolchain.Java = hydraCoreJavaVersion
	info.Toolchain.AndroidNDK = hydraCoreAndroidNDK
	info.Toolchain.AndroidAPI = hydraCoreAndroidAPI
	info.Toolchain.BuildTags = strings.Split(hydraCoreReleaseBuildTags, ",")
	info.Lineage = []hydraCoreLineageEntry{
		{Project: "sing-box", Role: "origin"},
		{Project: "sing-box-extended", Role: "active-upstream"},
		{Project: "etonify-core", Role: "historical-mobile-integration"},
	}
	content, err := json.Marshal(info)
	if err != nil {
		return ""
	}
	return string(content)
}
