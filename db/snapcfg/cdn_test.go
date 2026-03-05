package snapcfg

import "testing"

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
