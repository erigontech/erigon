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

// CompositeNodeSource combines multiple NodeSource implementations,
// deduplicating nodes by ID.
type CompositeNodeSource struct {
	Sources []NodeSource
}

func (c *CompositeNodeSource) AllNodes() []*enode.Node {
	seen := make(map[enode.ID]struct{})
	var result []*enode.Node
	for _, src := range c.Sources {
		if src == nil {
			continue
		}
		for _, n := range src.AllNodes() {
			if _, ok := seen[n.ID()]; !ok {
				seen[n.ID()] = struct{}{}
				result = append(result, n)
			}
		}
	}
	return result
}

// PeerNodeSource wraps a function that returns connected devp2p peers as enode.Node.
// This allows checking ENR entries on directly connected peers, not just
// the discv5 routing table.
type PeerNodeSource struct {
	PeersFn func() []*enode.Node
}

func (p *PeerNodeSource) AllNodes() []*enode.Node {
	if p.PeersFn == nil {
		return nil
	}
	return p.PeersFn()
}

// NodeResolver resolves an enode.Node to its latest ENR record.
// Typically backed by discv5's Resolve method.
type NodeResolver interface {
	Resolve(n *enode.Node) *enode.Node
}

// ResolvingPeerNodeSource wraps a PeerNodeSource and resolves each peer's
// ENR record via discv5. This is needed because devp2p peer records from the
// handshake may be stale — they don't reflect ENR updates (like chain-toml)
// that occurred after the connection was established.
type ResolvingPeerNodeSource struct {
	PeersFn  func() []*enode.Node
	Resolver NodeResolver
}

func (r *ResolvingPeerNodeSource) AllNodes() []*enode.Node {
	if r.PeersFn == nil {
		return nil
	}
	peers := r.PeersFn()
	if r.Resolver == nil {
		return peers
	}
	resolved := make([]*enode.Node, 0, len(peers))
	for _, p := range peers {
		resolved = append(resolved, r.Resolver.Resolve(p))
	}
	return resolved
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
