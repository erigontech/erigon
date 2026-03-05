package downloader

import (
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

// NodeSource provides access to discovered peers for chain.toml discovery.
// This interface decouples the discovery logic from the concrete discv5 implementation.
type NodeSource interface {
	// AllNodes returns all currently known nodes in the routing table.
	AllNodes() []*enode.Node
}

// DiscoverChainToml queries all known peers for their "chain-toml" ENR entry
// and returns the one with the highest FrozenTx. This represents the most
// advanced snapshot set available on the network.
//
// Returns nil if no peers have the chain-toml entry.
func DiscoverChainToml(nodes NodeSource) *enr.ChainToml {
	var best *enr.ChainToml

	for _, node := range nodes.AllNodes() {
		var ct enr.ChainToml
		if err := node.Record().Load(&ct); err != nil {
			continue // peer doesn't have chain-toml entry
		}

		if best == nil || ct.FrozenTx > best.FrozenTx {
			found := ct // copy
			best = &found
		}
	}

	return best
}

// DiscoverAllChainToml collects all "chain-toml" ENR entries from known peers.
// Useful for future statistical trust (quorum agreement).
func DiscoverAllChainToml(nodes NodeSource) []enr.ChainToml {
	var results []enr.ChainToml

	for _, node := range nodes.AllNodes() {
		var ct enr.ChainToml
		if err := node.Record().Load(&ct); err != nil {
			continue
		}
		results = append(results, ct)
	}

	return results
}
