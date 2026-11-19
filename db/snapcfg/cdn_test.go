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
}
