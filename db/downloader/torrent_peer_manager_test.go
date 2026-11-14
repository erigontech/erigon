package downloader

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
)

func TestTorrentPeerManager_TracksPeersWithBT(t *testing.T) {
	client, err := torrent.NewClient(newTestClientConfig(t, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Create two peers: one with BT port, one without.
	key1, _ := crypto.GenerateKey()
	key2, _ := crypto.GenerateKey()

	peerWithBT := makeNodeWithBT(t, key1, []byte{127, 0, 0, 1},
		&enr.ChainToml{KnownBlocks: 100}, enr.BT(42069))
	peerWithoutBT := makeTestNode(t, key2, &enr.ChainToml{KnownBlocks: 50})

	source := &mockNodeSource{nodes: []*enode.Node{peerWithBT, peerWithoutBT}}

	logger := log.New()

	m := NewTorrentPeerManager(client, func() NodeSource { return source }, logger)

	// First sync: tracks only the peer with a BT port. Nothing listens on it, so it is not added to the torrents.
	m.sync(t.Context())
	assert.Equal(t, 1, m.PeerCount())

	// Second sync with same peers: no change.
	m.sync(t.Context())
	assert.Equal(t, 1, m.PeerCount())

	// Remove the BT peer from the source.
	source.nodes = []*enode.Node{peerWithoutBT}
	m.sync(t.Context())
	assert.Equal(t, 0, m.PeerCount(), "peer should be forgotten when it leaves DevP2P")

	// Add it back.
	source.nodes = []*enode.Node{peerWithBT, peerWithoutBT}
	m.sync(t.Context())
	assert.Equal(t, 1, m.PeerCount(), "peer should be tracked again when it rejoins DevP2P")
}

func TestTorrentPeerManager_NilNodeSource(t *testing.T) {
	client, err := torrent.NewClient(newTestClientConfig(t, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	logger := log.New()
	m := NewTorrentPeerManager(client, func() NodeSource { return nil }, logger)

	// Should not panic when node source returns nil.
	m.sync(t.Context())
	assert.Equal(t, 0, m.PeerCount())
}

// fakeReachability drives a TorrentPeerManager without dialing: it answers reachability per address and
// records probes and injections.
type fakeReachability struct {
	mu        sync.Mutex // the manager checks endpoints concurrently
	reachable map[string]bool
	probes    map[string]int
	injected  []string
	now       time.Time
}

func newFakeReachabilityManager(t *testing.T, source *mockNodeSource) (*TorrentPeerManager, *fakeReachability) {
	t.Helper()
	f := &fakeReachability{reachable: map[string]bool{}, probes: map[string]int{}, now: time.Now()}
	m := NewTorrentPeerManager(nil, func() NodeSource { return source }, log.New())
	m.probe = func(_ context.Context, addr string) bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.probes[addr]++
		return f.reachable[addr]
	}
	m.inject = func(pi torrent.PeerInfo) { f.injected = append(f.injected, pi.Addr.String()) }
	m.now = func() time.Time { return f.now }
	return m, f
}

func btNode(t *testing.T, port enr.BT) *enode.Node {
	t.Helper()
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	return makeNodeWithBT(t, key, []byte{127, 0, 0, 1}, &enr.ChainToml{KnownBlocks: 1}, port)
}

// A peer is added to the torrents only once its endpoint answered: an unreachable one would be dialed
// once per torrent, and every dial would wait for its timeout.
func TestTorrentPeerManagerAddsOnlyReachablePeers(t *testing.T) {
	source := &mockNodeSource{nodes: []*enode.Node{btNode(t, 1001), btNode(t, 1002)}}
	m, f := newFakeReachabilityManager(t, source)
	f.reachable["127.0.0.1:1002"] = true

	m.sync(t.Context())
	require.Equal(t, []string{"127.0.0.1:1002"}, f.injected)
	require.Equal(t, 2, m.PeerCount())

	m.sync(t.Context())
	require.Equal(t, []string{"127.0.0.1:1002"}, f.injected, "a reachable peer is added once")
	require.Equal(t, 1, f.probes["127.0.0.1:1002"], "a reachable peer is not checked again")
}

// An unreachable peer is checked again after a growing wait, and added once it answers.
func TestTorrentPeerManagerRetriesAnUnreachablePeerWithBackoff(t *testing.T) {
	source := &mockNodeSource{nodes: []*enode.Node{btNode(t, 1001)}}
	m, f := newFakeReachabilityManager(t, source)
	const addr = "127.0.0.1:1001"

	wait := btPeerRetryMin
	m.sync(t.Context())
	for range 8 {
		f.now = f.now.Add(wait - time.Second)
		m.sync(t.Context())
		require.Equal(t, 1, f.probes[addr], "checked again before %v", wait)
		f.now = f.now.Add(time.Second)
		m.sync(t.Context())
		require.Equal(t, 2, f.probes[addr], "not checked again after %v", wait)
		f.probes[addr] = 1
		wait = min(2*wait, btPeerRetryMax)
	}
	require.Empty(t, f.injected)

	f.reachable[addr] = true
	f.now = f.now.Add(btPeerRetryMax)
	m.sync(t.Context())
	require.Equal(t, []string{addr}, f.injected)
}

// A peer that moves its endpoint is checked again at the new one.
func TestTorrentPeerManagerChecksAChangedEndpoint(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	source := &mockNodeSource{nodes: []*enode.Node{makeNodeWithBT(t, key, []byte{127, 0, 0, 1}, &enr.ChainToml{KnownBlocks: 1}, 1001)}}
	m, f := newFakeReachabilityManager(t, source)
	f.reachable["127.0.0.1:1001"], f.reachable["127.0.0.1:1002"] = true, true

	m.sync(t.Context())
	source.nodes = []*enode.Node{makeNodeWithBT(t, key, []byte{127, 0, 0, 1}, &enr.ChainToml{KnownBlocks: 1}, 1002)}
	m.sync(t.Context())
	require.Equal(t, []string{"127.0.0.1:1001", "127.0.0.1:1002"}, f.injected)
}

// The default check dials over the client's own sockets: a listening torrent client answers, a closed port
// does not.
func TestTorrentPeerManagerDefaultCheckDialsWithTheClientSockets(t *testing.T) {
	client, err := torrent.NewClient(newTestClientConfig(t, t.TempDir()))
	require.NoError(t, err)
	defer client.Close()
	remote, err := torrent.NewClient(newTestClientConfig(t, t.TempDir()))
	require.NoError(t, err)
	defer remote.Close()
	closed, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	closedAddr := closed.Addr().String()
	require.NoError(t, closed.Close())

	m := NewTorrentPeerManager(client, func() NodeSource { return nil }, log.New())
	require.True(t, m.probe(t.Context(), fmt.Sprintf("127.0.0.1:%d", remote.LocalPort())))
	require.False(t, m.probe(t.Context(), closedAddr))
}

// The wait before a check again starts when the failed check ends: listing the peers resolves them over
// the network, and a check can take up to its timeout.
func TestTorrentPeerManagerCountsTheWaitFromTheFailedCheck(t *testing.T) {
	source := &mockNodeSource{nodes: []*enode.Node{btNode(t, 1001)}}
	m, f := newFakeReachabilityManager(t, source)
	const addr = "127.0.0.1:1001"
	probe := m.probe
	m.probe = func(ctx context.Context, a string) bool {
		f.mu.Lock()
		f.now = f.now.Add(btPeerProbeTimeout) // a check that runs into its timeout
		f.mu.Unlock()
		return probe(ctx, a)
	}

	m.sync(t.Context())
	f.now = f.now.Add(btPeerRetryMin - time.Second)
	m.sync(t.Context())
	require.Equal(t, 1, f.probes[addr], "checked again before the wait after the failed check ended")
}

// A peer whose client accepts only uTP (--torrent.disable-tcp) must be found reachable over uTP.
func TestTorrentPeerManagerDefaultCheckReachesAUTPOnlyPeer(t *testing.T) {
	utpOnly := func() *torrent.Client {
		cfg := newTestClientConfig(t, t.TempDir())
		// One UDP socket on a port the OS picks: no second bind on a chosen port, which Windows can refuse.
		cfg.DisableUTP, cfg.DisableTCP, cfg.DisableIPv6 = false, true, true
		// The uTP socket drops a connection unless some torrent wants one; these clients have no torrents.
		cfg.AlwaysWantConns = true
		client, err := torrent.NewClient(cfg)
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })
		return client
	}
	client, remote := utpOnly(), utpOnly()

	m := NewTorrentPeerManager(client, func() NodeSource { return nil }, log.New())
	require.True(t, m.probe(t.Context(), fmt.Sprintf("127.0.0.1:%d", remote.LocalPort())))
}

// With dialing disabled (a zero download rate), the client never dials a peer, so the peer manager does not run: its
// reachability check would otherwise open outbound connections over the client's sockets.
func TestStartTorrentPeerManagerRespectsDisabledDialing(t *testing.T) {
	for _, dial := range []bool{false, true} {
		t.Run(fmt.Sprintf("dial=%v", dial), func(t *testing.T) {
			cfg := newTestClientConfig(t, t.TempDir())
			cfg.DialForPeerConns = dial
			client, err := torrent.NewClient(cfg)
			require.NoError(t, err)
			defer client.Close()
			d := &Downloader{
				cfg:           &downloadercfg.Cfg{ClientConfig: cfg},
				torrentClient: client,
				logger:        log.New(),
				nodeSourceFn:  func() NodeSource { return &mockNodeSource{} },
			}
			ctx, cancel := context.WithCancel(t.Context())

			d.StartTorrentPeerManager(ctx)
			cancel()
			d.wg.Wait()

			require.Equal(t, dial, d.peerManager != nil)
		})
	}
}
