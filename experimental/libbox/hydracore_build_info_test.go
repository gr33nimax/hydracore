package libbox

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"

	C "github.com/sagernet/sing-box/constant"
	"github.com/stretchr/testify/require"
)

func TestHydraCoreBuildInfo(t *testing.T) {
	t.Parallel()

	content := HydraCoreBuildInfo()
	var info hydraCoreBuildInformation
	require.NoError(t, json.Unmarshal([]byte(content), &info))
	require.Equal(t, 1, info.SchemaVersion)
	require.Equal(t, "io.hydrabox.hydracore", info.Distribution.ID)
	require.Equal(t, C.Version, info.Distribution.Version)
	require.Equal(t, hydraCoreSourceRepository, info.Source.Repository)
	require.Equal(t, hydraCoreUpstreamProject, info.Upstream.Project)
	require.Equal(t, hydraCoreUpstreamCommit, info.Upstream.Commit)
	require.Contains(t, info.Toolchain.BuildTags, "with_call")
	require.Contains(t, info.Lineage, hydraCoreLineageEntry{Project: "sing-box-extended", Role: "active-upstream"})
	require.Contains(t, info.Lineage, hydraCoreLineageEntry{Project: "etonify-core", Role: "historical-mobile-integration"})
}

func TestHydraCoreBuildInfoMatchesReleaseBaseline(t *testing.T) {
	t.Parallel()
	content, err := os.ReadFile("../../release/UPSTREAM_BASELINE")
	require.NoError(t, err)
	baseline := make(map[string]string)
	for _, line := range strings.Split(strings.ReplaceAll(string(content), "\r\n", "\n"), "\n") {
		key, value, found := strings.Cut(line, "=")
		if found {
			baseline[key] = value
		}
	}
	require.Equal(t, baseline["UPSTREAM_REPOSITORY"], hydraCoreUpstreamRepo)
	require.Equal(t, baseline["UPSTREAM_BRANCH"], hydraCoreUpstreamBranch)
	require.Equal(t, baseline["UPSTREAM_TAG"], hydraCoreUpstreamTag)
	require.Equal(t, baseline["UPSTREAM_COMMIT"], hydraCoreUpstreamCommit)
	require.Equal(t, baseline["GOMOBILE_VERSION"], hydraCoreGomobileVersion)
	require.Equal(t, baseline["JAVA_VERSION"], hydraCoreJavaVersion)
	require.Equal(t, baseline["ANDROID_NDK_VERSION"], hydraCoreAndroidNDK)
	androidAPI, err := strconv.Atoi(baseline["LIBBOX_ANDROID_API"])
	require.NoError(t, err)
	require.Equal(t, androidAPI, hydraCoreAndroidAPI)
	require.Equal(t, baseline["LIBBOX_BUILD_TAGS"], hydraCoreReleaseBuildTags)
}
