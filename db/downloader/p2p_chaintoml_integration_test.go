package downloader

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

// newTestLocalNode builds a real enode.LocalNode backed by an in-memory
// DB. Mirrors the LocalNode a p2p.Server owns at runtime — Set/Node
// operate through the same signing path, so ENR encoding, sequence
// bumps and DecodeRLP round-trip through this test.
func newTestLocalNode(t *testing.T) *enode.LocalNode {
	t.Helper()
	db, err := enode.OpenDB("")
	require.NoError(t, err)
	t.Cleanup(db.Close)
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	return enode.NewLocalNode(db, key)
}

// backendENRUpdater mirrors the closure wired at
// node/eth/backend.go:1144-1157: on each Publish, stamp both the
// "chain-toml" entry and the "bt" torrent port on the publisher's
// LocalNode. torrentPort==0 skips the BT stamp (matches production's
// validPort check).
func backendENRUpdater(ln *enode.LocalNode, torrentPort uint16) func(enr.ChainToml) {
	return func(ct enr.ChainToml) {
		ln.Set(ct)
		if torrentPort > 0 {
			ln.Set(enr.BT(torrentPort))
		}
	}
}

// staticResolver returns whatever the resolveFn produces for the given
// ID. Substitutes for discv5.Resolve in tests — a consumer looks up a
// peer node ID against the publisher's own LocalNode.Node() to obtain
// the freshest signed record. Absent this resolver step, the consumer
// would keep a snapshot taken at connect time and miss any ENR entry
// the publisher stamped after that.
type staticResolver struct {
	resolveFn func(id enode.ID) *enode.Node
}

func (s *staticResolver) Resolve(n *enode.Node) *enode.Node {
	if s.resolveFn == nil {
		return n
	}
	return s.resolveFn(n.ID())
}

// TestChainTomlENRExchange_TwoLocalNodes_RoundTrip drives the exact seam
// wired at backend.go:1144: the downloader-side updater callback stamps
// chain-toml + BT on the publisher's LocalNode; a consumer holding the
// publisher's node record picks the entry up via DiscoverChainToml.
//
// This is the unit-level shape of the wire we've been unable to verify
// live: it doesn't run discv5 lookups, but it proves the Set → sign →
// Record().Load() pipeline round-trips faithfully for both chain-toml
// and BT entries.
func TestChainTomlENRExchange_TwoLocalNodes_RoundTrip(t *testing.T) {
	publisher := newTestLocalNode(t)
	consumer := newTestLocalNode(t)
	require.NotEqual(t, publisher.ID(), consumer.ID(), "test nodes must be distinct")

	// Before Publish: consumer's node source sees the publisher but
	// no chain-toml entry — DiscoverChainToml returns nil.
	src := &ResolvingPeerNodeSource{
		PeersFn:  func() []*enode.Node { return []*enode.Node{publisher.Node()} },
		Resolver: &staticResolver{resolveFn: func(id enode.ID) *enode.Node { return publisher.Node() }},
	}
	require.Nil(t, DiscoverChainToml(src),
		"consumer must see no chain-toml before publisher stamps its ENR")

	// Publish: apply the backend.go closure.
	ct := enr.ChainToml{
		AuthoritativeBlocks: 3_290_000,
		KnownBlocks:         3_290_000,
		InfoHash:            [20]byte{0xaa, 0xbb, 0xcc},
		DomainSteps:         288,
		MergeDepth:          64,
		V2InfoHash:          [20]byte{0xaa, 0xbb, 0xcd},
		MinStep:             128,
	}
	backendENRUpdater(publisher, 42069)(ct)

	// Consumer now discovers the chain-toml — the resolver returns
	// the publisher's freshly-signed record.
	got := DiscoverChainToml(src)
	require.NotNil(t, got, "consumer must discover publisher's chain-toml")
	assert.Equal(t, publisher.ID(), got.Node.ID())
	assert.Equal(t, ct.AuthoritativeBlocks, got.ChainToml.AuthoritativeBlocks)
	assert.Equal(t, ct.KnownBlocks, got.ChainToml.KnownBlocks)
	assert.Equal(t, ct.InfoHash, got.ChainToml.InfoHash)
	assert.Equal(t, ct.DomainSteps, got.ChainToml.DomainSteps)
	assert.Equal(t, ct.MergeDepth, got.ChainToml.MergeDepth)
	assert.Equal(t, ct.V2InfoHash, got.ChainToml.V2InfoHash)
	assert.Equal(t, ct.MinStep, got.ChainToml.MinStep)

	// The BT entry piggybacks on the same LocalNode.Node() record —
	// consumers use it to open a direct torrent connection.
	var bt enr.BT
	require.NoError(t, got.Node.Record().Load(&bt))
	assert.Equal(t, enr.BT(42069), bt)
}

// TestChainTomlENRExchange_RepublishBumpsSeq guards the property the
// live E2E depends on: a consumer that captured a stale copy of the
// publisher's ENR must see the new chain-toml after the publisher
// republishes, provided it re-resolves via discv5. Without the
// re-resolve step, the stale record wins.
func TestChainTomlENRExchange_RepublishBumpsSeq(t *testing.T) {
	publisher := newTestLocalNode(t)

	first := enr.ChainToml{
		AuthoritativeBlocks: 100,
		KnownBlocks:         100,
		InfoHash:            [20]byte{0x01},
	}
	backendENRUpdater(publisher, 42069)(first)
	staleCopy := publisher.Node()
	staleSeq := staleCopy.Seq()

	// Publisher advances — new chain-toml, later InfoHash.
	second := enr.ChainToml{
		AuthoritativeBlocks: 200,
		KnownBlocks:         200,
		InfoHash:            [20]byte{0x02},
	}
	backendENRUpdater(publisher, 42069)(second)

	freshCopy := publisher.Node()
	require.Greater(t, freshCopy.Seq(), staleSeq,
		"republish must bump the ENR sequence number")

	// Consumer with a plain (non-resolving) PeerNodeSource keeps
	// the stale copy — it observes the OLD InfoHash. This is the
	// production symptom without ResolvingPeerNodeSource.
	staleSrc := &PeerNodeSource{
		PeersFn: func() []*enode.Node { return []*enode.Node{staleCopy} },
	}
	staleGot := DiscoverChainToml(staleSrc)
	require.NotNil(t, staleGot)
	assert.Equal(t, [20]byte{0x01}, staleGot.ChainToml.InfoHash,
		"non-resolving source pins the stale record")

	// The same consumer wrapped in ResolvingPeerNodeSource with a
	// discv5-style resolver sees the fresh InfoHash.
	freshSrc := &ResolvingPeerNodeSource{
		PeersFn:  func() []*enode.Node { return []*enode.Node{staleCopy} },
		Resolver: &staticResolver{resolveFn: func(id enode.ID) *enode.Node { return publisher.Node() }},
	}
	freshGot := DiscoverChainToml(freshSrc)
	require.NotNil(t, freshGot)
	assert.Equal(t, [20]byte{0x02}, freshGot.ChainToml.InfoHash,
		"resolving source must see the republished chain-toml")
}

// TestChainTomlENRExchange_MultiplePublishers picks the highest-KnownBlocks
// peer through the two-LocalNode wire — the same DiscoverChainToml selection
// rule the consumer applies live, but proven against real signed records.
func TestChainTomlENRExchange_MultiplePublishers(t *testing.T) {
	pubA := newTestLocalNode(t)
	pubB := newTestLocalNode(t)
	pubC := newTestLocalNode(t)

	backendENRUpdater(pubA, 42069)(enr.ChainToml{KnownBlocks: 100, InfoHash: [20]byte{0xa1}})
	backendENRUpdater(pubB, 42070)(enr.ChainToml{KnownBlocks: 300, InfoHash: [20]byte{0xb2}})
	backendENRUpdater(pubC, 42071)(enr.ChainToml{KnownBlocks: 200, InfoHash: [20]byte{0xc3}})

	src := &ResolvingPeerNodeSource{
		PeersFn: func() []*enode.Node {
			return []*enode.Node{pubA.Node(), pubB.Node(), pubC.Node()}
		},
		Resolver: &staticResolver{},
	}

	got := DiscoverChainToml(src)
	require.NotNil(t, got)
	assert.Equal(t, uint64(300), got.ChainToml.KnownBlocks)
	assert.Equal(t, [20]byte{0xb2}, got.ChainToml.InfoHash)
	assert.Equal(t, pubB.ID(), got.Node.ID())
}

// TestChainTomlENRExchange_ResolvingSourceIsConcurrent guards
// DiscoverChainToml against a data-race under the ResolvingPeerNodeSource
// used in production — the resolver may be called from more than one
// goroutine in a heartbeat loop, and it must not corrupt the seen[] map
// or the walk. Failure would surface as a race detector hit on -race.
func TestChainTomlENRExchange_ResolvingSourceIsConcurrent(t *testing.T) {
	publisher := newTestLocalNode(t)
	backendENRUpdater(publisher, 42069)(enr.ChainToml{
		KnownBlocks: 1_000, InfoHash: [20]byte{0x77},
	})

	src := &ResolvingPeerNodeSource{
		PeersFn:  func() []*enode.Node { return []*enode.Node{publisher.Node()} },
		Resolver: &staticResolver{resolveFn: func(id enode.ID) *enode.Node { return publisher.Node() }},
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 32 {
				got := DiscoverChainToml(src)
				require.NotNil(t, got)
			}
		})
	}
	wg.Wait()
}
