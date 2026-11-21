package snapcfg

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
	fetched := map[string]string{}
	fetch := func(_ context.Context, branch, chain string) ([]byte, error) {
		fetched[chain] = branch
		return []byte(`"e3-v1-pub" = "v1:https://erigon310-v1-snapshots-mainnet.erigon.network"`), nil
	}
	got, ok := webseedsFrom(t.Context(), "release/3.10", networkname.Mainnet, fetch)
	require.True(t, ok)
	require.Equal(t, []string{"v1:https://erigon310-v1-snapshots-mainnet.erigon.network"}, got)
	require.Equal(t, "release/3.10", fetched[networkname.Mainnet])

	got, ok = webseedsFrom(t.Context(), version.DefaultSnapshotGitBranch, networkname.Mainnet, fetch)
	require.True(t, ok)
	require.Equal(t, EmbeddedWebseeds[networkname.Mainnet], got, "the embedded list is the default branch's")

	failing := func(context.Context, string, string) ([]byte, error) { return nil, errors.New("404") }
	got, ok = webseedsFrom(t.Context(), "release/3.10", networkname.Mainnet, failing)
	require.True(t, ok)
	require.Equal(t, EmbeddedWebseeds[networkname.Mainnet], got, "a branch without a webseed file falls back to the embedded list")

	unknownFormat := func(context.Context, string, string) ([]byte, error) {
		return []byte(`"e3-v2-pub" = "v2:https://erigon310-v2-snapshots-mainnet.erigon.network"`), nil
	}
	got, ok = webseedsFrom(t.Context(), "release/3.10", networkname.Mainnet, unknownFormat)
	require.True(t, ok)
	require.Equal(t, EmbeddedWebseeds[networkname.Mainnet], got, "a webseed this version cannot parse falls back to the embedded list")

	for _, bad := range []string{"v1:", "v1:erigon310-v1-snapshots-mainnet.erigon.network", "v1:ftp://erigon310-v1-snapshots-mainnet.erigon.network"} {
		badURL := func(context.Context, string, string) ([]byte, error) {
			return []byte(`"e3-v1-pub" = "` + bad + `"`), nil
		}
		got, ok = webseedsFrom(t.Context(), "release/3.10", networkname.Mainnet, badURL)
		require.True(t, ok)
		require.Equal(t, EmbeddedWebseeds[networkname.Mainnet], got, "%q is no HTTP(S) URL with a host", bad)
	}
}

// Webseeds, which the downloader and the verify commands use, reads the branch the
// manifest comes from.
func TestWebseedsUseTheSnapshotBranch(t *testing.T) {
	prevBranch, prevFetch := snapshotGitBranch, fetchWebseeds
	t.Cleanup(func() { snapshotGitBranch, fetchWebseeds = prevBranch, prevFetch })
	var fetchedBranch string
	snapshotGitBranch = "release/3.10"
	fetchWebseeds = func(_ context.Context, branch, _ string) ([]byte, error) {
		fetchedBranch = branch
		return []byte(`"e3-v1-pub" = "v1:https://erigon310-v1-snapshots-mainnet.erigon.network"`), nil
	}
	got, ok := Webseeds(t.Context(), networkname.Mainnet)
	require.True(t, ok)
	require.Equal(t, []string{"v1:https://erigon310-v1-snapshots-mainnet.erigon.network"}, got)
	require.Equal(t, "release/3.10", fetchedBranch)
}

func TestFetchSnapshotFileErrorNamesTheFile(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	url := srv.URL + "/release/3.10/webseed/mainnet.toml"
	_, err := fetchSnapshotFile(t.Context(), url, false)
	require.ErrorContains(t, err, url)
	require.NotContains(t, err.Error(), "overridden")
}
