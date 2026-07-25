package downloader

import (
	"crypto/ecdsa"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

// mockNodeSource implements NodeSource for testing.
type mockNodeSource struct {
	nodes []*enode.Node
}

func (m *mockNodeSource) AllNodes() []*enode.Node {
	return m.nodes
}

func makeTestNode(t *testing.T, key *ecdsa.PrivateKey, ct *enr.ChainToml) *enode.Node {
	t.Helper()
	var r enr.Record
	if ct != nil {
		r.Set(*ct)
	}
	require.NoError(t, enode.SignV4(&r, key))
	n, err := enode.New(enode.ValidSchemes, &r)
	require.NoError(t, err)
	return n
}

func TestDiscoverChainToml_NoPeers(t *testing.T) {
	src := &mockNodeSource{}
	result := DiscoverChainToml(src)
	assert.Nil(t, result)
}

func TestDiscoverChainToml_NoPeersWithEntry(t *testing.T) {
	key, _ := crypto.GenerateKey()
	src := &mockNodeSource{
		nodes: []*enode.Node{makeTestNode(t, key, nil)},
	}
	result := DiscoverChainToml(src)
	assert.Nil(t, result)
}

func TestDiscoverChainToml_SinglePeer(t *testing.T) {
	key, _ := crypto.GenerateKey()
	ct := &enr.ChainToml{
		KnownBlocks: 100,
		InfoHash:    [20]byte{1, 2, 3},
	}
	src := &mockNodeSource{
		nodes: []*enode.Node{makeTestNode(t, key, ct)},
	}

	result := DiscoverChainToml(src)
	require.NotNil(t, result)
	assert.Equal(t, uint64(100), result.ChainToml.KnownBlocks)
	assert.Equal(t, [20]byte{1, 2, 3}, result.ChainToml.InfoHash)
}

func TestDiscoverChainToml_PicksHighestKnownBlocks(t *testing.T) {
	key1, _ := crypto.GenerateKey()
	key2, _ := crypto.GenerateKey()
	key3, _ := crypto.GenerateKey()

	src := &mockNodeSource{
		nodes: []*enode.Node{
			makeTestNode(t, key1, &enr.ChainToml{KnownBlocks: 50, InfoHash: [20]byte{1}}),
			makeTestNode(t, key2, &enr.ChainToml{KnownBlocks: 200, InfoHash: [20]byte{2}}),
			makeTestNode(t, key3, &enr.ChainToml{KnownBlocks: 100, InfoHash: [20]byte{3}}),
		},
	}

	result := DiscoverChainToml(src)
	require.NotNil(t, result)
	assert.Equal(t, uint64(200), result.ChainToml.KnownBlocks)
	assert.Equal(t, [20]byte{2}, result.ChainToml.InfoHash)
}

func TestDiscoverChainToml_MixedPeers(t *testing.T) {
	key1, _ := crypto.GenerateKey()
	key2, _ := crypto.GenerateKey()
	key3, _ := crypto.GenerateKey()

	src := &mockNodeSource{
		nodes: []*enode.Node{
			makeTestNode(t, key1, nil), // no chain-toml
			makeTestNode(t, key2, &enr.ChainToml{KnownBlocks: 42, InfoHash: [20]byte{0xab}}),
			makeTestNode(t, key3, nil), // no chain-toml
		},
	}

	result := DiscoverChainToml(src)
	require.NotNil(t, result)
	assert.Equal(t, uint64(42), result.ChainToml.KnownBlocks)
}

func TestDiscoverChainToml_ReturnsNode(t *testing.T) {
	key, _ := crypto.GenerateKey()
	ct := &enr.ChainToml{KnownBlocks: 42, InfoHash: [20]byte{0xab}}
	node := makeTestNode(t, key, ct)
	src := &mockNodeSource{nodes: []*enode.Node{node}}

	result := DiscoverChainToml(src)
	require.NotNil(t, result)
	assert.Equal(t, node.ID(), result.Node.ID())
}

func TestDiscoverChainToml_WithBTPort(t *testing.T) {
	key, _ := crypto.GenerateKey()
	var r enr.Record
	r.Set(enr.ChainToml{KnownBlocks: 100, InfoHash: [20]byte{1}})
	r.Set(enr.BT(42069))
	require.NoError(t, enode.SignV4(&r, key))
	node, err := enode.New(enode.ValidSchemes, &r)
	require.NoError(t, err)

	src := &mockNodeSource{nodes: []*enode.Node{node}}
	result := DiscoverChainToml(src)
	require.NotNil(t, result)

	// Verify we can extract the BT port from the returned node
	var bt enr.BT
	require.NoError(t, result.Node.Record().Load(&bt))
	assert.Equal(t, enr.BT(42069), bt)
}

func TestDiscoverAllChainToml(t *testing.T) {
	key1, _ := crypto.GenerateKey()
	key2, _ := crypto.GenerateKey()
	key3, _ := crypto.GenerateKey()

	src := &mockNodeSource{
		nodes: []*enode.Node{
			makeTestNode(t, key1, &enr.ChainToml{KnownBlocks: 10}),
			makeTestNode(t, key2, nil), // no entry
			makeTestNode(t, key3, &enr.ChainToml{KnownBlocks: 20}),
		},
	}

	results := DiscoverAllChainToml(src)
	assert.Len(t, results, 2)
}
