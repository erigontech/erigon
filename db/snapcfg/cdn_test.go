package snapcfg

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/chain/networkname"
)

func TestChainTomlR2URL(t *testing.T) {
	tests := []struct {
		branch, chain string
		want          string
	}{
		{"release/3.4", "mainnet", "https://erigon-snapshots.erigon.network/release/3.4/mainnet.toml"},
		{"main", "chiado", "https://erigon-snapshots.erigon.network/main/chiado.toml"},
		{"release/3.4", "gnosis", "https://erigon-snapshots.erigon.network/release/3.4/gnosis.toml"},
	}
	for _, tt := range tests {
		if got := ChainTomlR2URL(tt.branch, tt.chain); got != tt.want {
			t.Errorf("ChainTomlR2URL(%q, %q) = %q, want %q", tt.branch, tt.chain, got, tt.want)
		}
	}
}

func TestChainTomlGitHubURL(t *testing.T) {
	tests := []struct {
		branch, chain string
		want          string
	}{
		{"release/3.4", "mainnet", "https://raw.githubusercontent.com/erigontech/erigon-snapshot/release/3.4/mainnet.toml"},
		{"main", "chiado", "https://raw.githubusercontent.com/erigontech/erigon-snapshot/main/chiado.toml"},
		{"release/3.4", "gnosis", "https://raw.githubusercontent.com/erigontech/erigon-snapshot/release/3.4/gnosis.toml"},
	}
	for _, tt := range tests {
		if got := ChainTomlGitHubURL(tt.branch, tt.chain); got != tt.want {
			t.Errorf("ChainTomlGitHubURL(%q, %q) = %q, want %q", tt.branch, tt.chain, got, tt.want)
		}
	}
}

func TestWebseedsFollowTheManifestBranch(t *testing.T) {
	prevBranch, prevFetch := snapshotGitBranch, fetchWebseeds
	t.Cleanup(func() { snapshotGitBranch, fetchWebseeds = prevBranch, prevFetch })
	webseeds := func(branch string, file string, fetchErr error) []string {
		t.Helper()
		snapshotGitBranch = branch
		fetchWebseeds = func(ctx context.Context, branch, chain string) ([]byte, error) {
			_, bounded := ctx.Deadline()
			require.True(t, bounded, "the fetch runs on every startup, so it needs a timeout")
			require.Equal(t, snapshotGitBranch, branch)
			require.Equal(t, networkname.Mainnet, chain)
			return []byte(file), fetchErr
		}
		got, ok := Webseeds(t.Context(), networkname.Mainnet)
		require.True(t, ok)
		return got
	}
	embedded := EmbeddedWebseeds[networkname.Mainnet]
	branchFile := `"e3-v1-pub" = "v1:https://erigon310-v1-snapshots-mainnet.erigon.network"`

	require.Equal(t, []string{"v1:https://erigon310-v1-snapshots-mainnet.erigon.network"}, webseeds("release/3.10", branchFile, nil))
	require.Equal(t, embedded, webseeds(version.DefaultSnapshotGitBranch, branchFile, nil), "the embedded list is the default branch's")
	require.Equal(t, embedded, webseeds("release/3.10", "", errors.New("404")), "a branch without a webseed file falls back to the embedded list")
	require.Equal(t, embedded, webseeds("release/3.10", "", nil), "an empty file falls back to the embedded list")
	require.Equal(t, embedded, webseeds("release/3.10", `"e3-v2-pub" = "v2:https://erigon310-v2-snapshots-mainnet.erigon.network"`, nil), "a webseed this version cannot parse falls back to the embedded list")
	for _, bad := range []string{"v1:", "v1:https://:443", "v1:erigon310-v1-snapshots-mainnet.erigon.network", "v1:ftp://erigon310-v1-snapshots-mainnet.erigon.network"} {
		require.Equal(t, embedded, webseeds("release/3.10", `"e3-v1-pub" = "`+bad+`"`, nil), "%q is no HTTP(S) URL with a host", bad)
	}
}
