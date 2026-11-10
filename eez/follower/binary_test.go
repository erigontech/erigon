// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package follower_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/l1test"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/overlay"
	"github.com/erigontech/erigon/eez/seqsig"
	"github.com/erigontech/erigon/p2p/enode"
)

// handedOut holds the ports freePort returned: the OS may hand a port out
// again as soon as freePort closes it.
var handedOut = struct {
	sync.Mutex
	ports map[int]bool
}{ports: map[int]bool{}}

// freePort returns a loopback TCP port that is free and that no earlier call
// returned.
func freePort(t *testing.T) string {
	t.Helper()
	handedOut.Lock()
	defer handedOut.Unlock()
	for {
		l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
		require.NoError(t, err)
		port := l.Addr().(*net.TCPAddr).Port
		require.NoError(t, l.Close())
		if !handedOut.ports[port] {
			handedOut.ports[port] = true
			return strconv.Itoa(port)
		}
	}
}

// call makes a JSON-RPC call to url and decodes its result into out.
func call(ctx context.Context, url, method string, out any, params ...any) error {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": append([]any{}, params...)})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var reply struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return err
	}
	if reply.Error != nil {
		return fmt.Errorf("%s: %s", method, reply.Error.Message)
	}
	return json.Unmarshal(reply.Result, out)
}

func blockHash(ctx context.Context, url, tag string) (common.Hash, error) {
	var b *struct {
		Hash common.Hash `json:"hash"`
	}
	if err := call(ctx, url, "eth_getBlockByNumber", &b, tag, false); err != nil {
		return common.Hash{}, err
	}
	if b == nil {
		return common.Hash{}, fmt.Errorf("no block %s", tag)
	}
	return b.Hash, nil
}

// waitStatus waits until the follower at url reports cursor and safe in
// eez_followerStatus. It publishes them after the fork choice that moves the
// safe block, so the status can lag that block for the rest of a step.
func waitStatus(t *testing.T, url string, cursor, safe uint64) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var status struct {
			Cursor hexutil.Uint64 `json:"cursor"`
			Safe   hexutil.Uint64 `json:"safe"`
		}
		require.NoError(c, call(t.Context(), url, "eez_followerStatus", &status))
		assert.Equal(c, cursor, uint64(status.Cursor), "cursor")
		assert.Equal(c, safe, uint64(status.Safe), "safe")
	}, 10*time.Second, 100*time.Millisecond, "eez_followerStatus")
}

// devChain returns the flags that keep a node of the dev chain off the
// default ports of its consensus layer and MCP server.
func devChain(t *testing.T) []string {
	return []string{
		"--chain=dev", "--beacon.api.port", freePort(t), "--caplin.discovery.port", freePort(t),
		"--caplin.discovery.tcpport", freePort(t), "--caplin.discovery.quicport", freePort(t), "--sentinel.port", freePort(t),
		"--mcp.disable",
	}
}

// overlaySigner is the sequencer address of the binary tests' overlay.
const overlaySigner = "0x5fbdb2315678afecb367f032d93f642f64180aa3"

// refused runs bin with args, which it must refuse at startup naming want.
func refused(t *testing.T, bin, want string, args ...string) {
	t.Helper()
	args = append(args, "--datadir", t.TempDir(), "--http.port", freePort(t), "--authrpc.port", freePort(t),
		"--private.api.addr", "127.0.0.1:"+freePort(t), "--port", freePort(t), "--torrent.port", freePort(t))
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, args...).CombinedOutput()
	var exit *exec.ExitError
	require.ErrorAs(t, err, &exit, "%v: %s", args, out)
	require.Equal(t, 1, exit.ExitCode(), "%v: %s", args, out)
	require.Contains(t, string(out), want, "%v", args)
}

// TestFollowerBinary runs the erigon binary named by EEZ_ERIGON as an EEZ
// node against a composed L1 served over JSON-RPC: it derives the settled
// blocks into <datadir>/eez/chaindata, serves no engine API, joins the
// signed-head overlay, and exits with status 3 when its chain diverges from
// L1. A second node in peers mode, with the first as trusted peer and
// discovery off, completes the eth/68 and eth/69 handshake with it, syncs
// from it the history below the root L1's finalized block F holds, seeds
// derivation at F and keeps no checkpoint. Discovery without --bootnodes,
// peers mode on a rollupX chain, and the overlay beside the sequencer RPC
// and on a chain whose sequencer signs no heads are refused.
func TestFollowerBinary(t *testing.T) {
	bin := os.Getenv("EEZ_ERIGON")
	if bin == "" {
		t.Skip("EEZ_ERIGON names no erigon binary")
	}
	c := derivertest.NewComposer(t)
	first := c.Compose(nil,
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: transfers(c, 2)},
		derivertest.Block{Coinbase: alice, Extra: []byte("eez"), Txs: [][]byte{}},
	)
	c.Post(derivertest.Posting{Batch: first})
	second := c.Compose(nil, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{c.Transfer(alice, 1)}})
	c.Post(derivertest.Posting{Batch: second})

	server, err := l1test.NewServer(c.L1, log.New())
	require.NoError(t, err)
	t.Cleanup(server.Stop)
	l1 := httptest.NewServer(server)
	t.Cleanup(l1.Close)

	overlay := func() []string {
		return []string{"--eez.p2p.listen=/ip4/127.0.0.1/tcp/" + freePort(t), "--eez.sequencer.address=" + overlaySigner}
	}
	refused(t, bin, "discovery finds no L2 peers without --bootnodes", "--eez.enabled", "--chain=eez-devnet", "--eez.sync.l1only=true", "--eez.l1.rpc", l1.URL)
	refused(t, bin, "rollupX chains derive their history from L1 alone", "--eez.enabled", "--chain=eez-chiado", "--eez.sync.l1only=false", "--eez.l1.rpc", l1.URL)
	sequencerRPC := []string{"--eez.enabled", "--chain=eez-devnet", "--eez.sync.l1only=true", "--nodiscover", "--eez.l1.rpc", l1.URL, "--eez.sequencer.rpc=http://127.0.0.1:1"}
	refused(t, bin, "both set the unsafe head", append(sequencerRPC, overlay()...)...)
	unsigned := []string{"--eez.enabled", "--chain=eez-chiado", "--eez.l1.rpc", l1.URL}
	refused(t, bin, "signs no heads", append(unsigned, overlay()...)...)

	dir := t.TempDir()
	httpPort, authPort, p2pPort, overlayPort := freePort(t), freePort(t), freePort(t), freePort(t)
	cmd := exec.CommandContext(t.Context(), bin, "--eez.enabled", "--chain=eez-devnet", "--datadir", dir, "--eez.sync.l1only=true",
		"--eez.l1.rpc", l1.URL, "--eez.l1.poll=200ms", "--http.port", httpPort, "--http.api=eth,eez,admin", "--authrpc.port", authPort,
		"--private.api.addr", "127.0.0.1:"+freePort(t), "--port", p2pPort, "--torrent.port", freePort(t), "--nodiscover",
		"--eez.p2p.listen=/ip4/127.0.0.1/tcp/"+overlayPort, "--eez.sequencer.address="+overlaySigner)
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	require.NoError(t, cmd.Start())
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		if t.Failed() {
			t.Log(output.String())
		}
	})

	url := "http://127.0.0.1:" + httpPort
	waitFor := func(tag string, want common.Hash) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Minute)
		for {
			h, err := blockHash(t.Context(), url, tag)
			if err == nil && h == want {
				return
			}
			select {
			case err := <-exited:
				t.Fatalf("erigon exited: %v", err)
			case <-time.After(200 * time.Millisecond):
			}
			require.True(t, time.Now().Before(deadline), "block %s: got %x (%v), want %x", tag, h, err, want)
		}
	}
	waitFor("0x2", first.Blocks[1].Hash())
	waitFor("latest", second.Blocks[0].Hash())
	waitFor("safe", second.Blocks[0].Hash())
	require.FileExists(t, filepath.Join(dir, "eez", "chaindata", "mdbx.dat"))
	require.DirExists(t, filepath.Join(dir, "snapshots", "eez", "domain"))
	require.FileExists(t, filepath.Join(dir, "eez", "eez-reconcile-checkpoint"))
	waitStatus(t, url, 3, 3)
	_, err = (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", "127.0.0.1:"+authPort)
	require.Error(t, err, "the engine API is down")
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", "127.0.0.1:"+overlayPort)
	require.NoError(t, err, "the overlay listens")
	require.NoError(t, conn.Close())

	var info struct {
		Enode string `json:"enode"`
	}
	require.NoError(t, call(t.Context(), url, "admin_nodeInfo", &info))
	var id string
	_, err = fmt.Sscanf(info.Enode, "enode://%128s", &id)
	require.NoError(t, err, info.Enode)
	f, err := c.L1.Head(t.Context())
	require.NoError(t, err)
	require.NoError(t, c.L1.Finalize(f.Number))
	peerHTTP, peerDir := freePort(t), t.TempDir()
	peerURL := "http://127.0.0.1:" + peerHTTP
	peer := exec.CommandContext(t.Context(), bin, "--eez.enabled", "--chain=eez-devnet", "--datadir", peerDir, "--eez.sync.l1only=false",
		"--eez.l1.rpc", l1.URL, "--eez.l1.poll=200ms", "--http.port", peerHTTP, "--http.api=eth,eez,admin",
		"--authrpc.port", freePort(t), "--private.api.addr", "127.0.0.1:"+freePort(t), "--port", freePort(t), "--torrent.port", freePort(t),
		"--nodiscover", "--trustedpeers", "enode://"+id+"@127.0.0.1:"+p2pPort)
	peerOutput := &syncBuffer{}
	peer.Stdout, peer.Stderr = peerOutput, peerOutput
	require.NoError(t, peer.Start())
	t.Cleanup(func() {
		_ = peer.Process.Kill()
		if t.Failed() {
			t.Log(peerOutput.String())
		}
	})
	// A peer is listed once its eth handshake passes; a trusted peer is
	// dialled as a static one.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var peers []struct {
			Caps    []string `json:"caps"`
			Network struct {
				Trusted bool `json:"trusted"`
				Static  bool `json:"static"`
			} `json:"network"`
		}
		require.NoError(c, call(t.Context(), peerURL, "admin_peers", &peers))
		require.Len(c, peers, 1)
		assert.ElementsMatch(c, []string{"eth/68", "eth/69"}, peers[0].Caps)
		assert.True(c, peers[0].Network.Trusted, "trusted")
		assert.True(c, peers[0].Network.Static, "static")
	}, 2*time.Minute, 250*time.Millisecond, "admin_peers")
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		h, err := blockHash(t.Context(), peerURL, "finalized")
		require.NoError(c, err)
		assert.Equal(c, second.Blocks[0].Hash(), h)
	}, 2*time.Minute, 250*time.Millisecond, "the root at L1 finality")
	// The sync moves finalized to the root before boot seeds derivation.
	require.Eventually(t, func() bool {
		return bytes.Contains([]byte(peerOutput.String()), []byte("event=eez.node.sync.seeded l1Block="+strconv.FormatUint(f.Number, 10)))
	}, 2*time.Minute, 250*time.Millisecond, "seeded at F")
	require.NotContains(t, peerOutput.String(), "deriving from the registry deploy block")
	require.NoFileExists(t, filepath.Join(peerDir, "eez", "eez-reconcile-checkpoint"))

	c.Post(derivertest.Posting{Batch: tampered(c)})
	select {
	case err := <-exited:
		var exit *exec.ExitError
		require.ErrorAs(t, err, &exit)
		require.Equal(t, 3, exit.ExitCode())
	case <-time.After(2 * time.Minute):
		t.Fatal("erigon did not halt")
	}
}

// nodeInfo returns the enode of the node whose admin API is at url.
func nodeInfo(t *testing.T, url string) *enode.Node {
	t.Helper()
	var info struct {
		Enode string `json:"enode"`
	}
	require.NoError(t, call(t.Context(), url, "admin_nodeInfo", &info))
	n, err := enode.ParseV4(info.Enode)
	require.NoError(t, err, info.Enode)
	return n
}

// peerInfo is an entry of admin_peers.
type peerInfo struct {
	Enode   string   `json:"enode"`
	Caps    []string `json:"caps"`
	Network struct {
		Static bool `json:"static"`
	} `json:"network"`
}

// peers returns the peers that completed the eth handshake with the node
// whose admin API is at url.
func peers(t require.TestingT, ctx context.Context, url string) []peerInfo {
	var out []peerInfo
	require.NoError(t, call(ctx, url, "admin_peers", &out))
	return out
}

// TestAllInOneBinary runs the erigon binary named by EEZ_ERIGON as an
// all-in-one node: the dev chain as L1 and beside it an EEZ chain settling
// on it, under <datadir>/eez with its own lock, HTTP-RPC port and sentry,
// whose follower takes the L1 heads in-process. The EEZ chain's sentry
// listens on --eez.port with the node key in <datadir>/eez/nodekey and
// completes the eth/68 and eth/69 handshake with a standalone node of its
// chain named by --eez.staticpeers, which the L1, whose --staticpeers names
// it too, refuses at the eth handshake. SIGTERM stops both chains: the L1's
// consensus layer resets SIGINT to its default action once it starts.
// Restarted in peers mode with the same node key, the EEZ chain joins the
// standalone node's signed-head overlay and relays the heads it is sent.
// RollupX chains refuse peers mode and the overlay beside their L1 too.
func TestAllInOneBinary(t *testing.T) {
	bin := os.Getenv("EEZ_ERIGON")
	if bin == "" {
		t.Skip("EEZ_ERIGON names no erigon binary")
	}
	rollupX := slices.Clip(append(devChain(t), "--eez.enabled", "--eez.chain=eez-chiado"))
	refused(t, bin, "rollupX chains derive their history from L1 alone", append(rollupX, "--eez.sync.l1only=false")...)
	refused(t, bin, "signs no heads", append(rollupX, "--eez.p2p.listen=/ip4/127.0.0.1/tcp/"+freePort(t), "--eez.sequencer.address="+overlaySigner)...)
	refused(t, bin, "set --eez.bootnodes, --eez.staticpeers or --eez.trustedpeers, or --eez.sync.l1only",
		append(devChain(t), "--eez.enabled", "--eez.chain=eez-devnet")...)
	dir, peerDir := t.TempDir(), t.TempDir()
	sequencer, err := crypto.GenerateKey()
	require.NoError(t, err)
	signer := crypto.PubkeyToAddress(sequencer.PublicKey)

	// eez-devnet settling on the dev chain, with the timestamp rewritten
	// as a deployment does, so the stored l2 section stands.
	raw, err := os.ReadFile(filepath.Join("..", "networks", "devnet-genesis.json"))
	require.NoError(t, err)
	var genesis map[string]any
	require.NoError(t, json.Unmarshal(raw, &genesis))
	genesis["timestamp"] = "0x1"
	raw, err = json.Marshal(genesis)
	require.NoError(t, err)
	genesisFile := filepath.Join(t.TempDir(), "genesis.json")
	require.NoError(t, os.WriteFile(genesisFile, raw, 0o644))
	for _, d := range []string{dir, peerDir} {
		out, err := exec.CommandContext(t.Context(), bin, "init", "--eez.enabled", "--datadir", d, "--eez.l1.chainid=1337",
			"--eez.registry=0x5fbdb2315678afecb367f032d93f642f64180aa3", "--eez.registry.deployblock=0", "--eez.rollupid=1",
			"--eez.l1.blocktime=12s", "--eez.l2.blocktime=2s", genesisFile).CombinedOutput()
		require.NoError(t, err, "%s", out)
	}

	// A standalone node of the same chain, on an L1 that holds its genesis
	// block only.
	peerL1, err := recording.NewMemoryChain(1337, &recording.Block{Header: l1.Header{Hash: common.HexToHash("0x01")}})
	require.NoError(t, err)
	server, err := l1test.NewServer(peerL1, log.New())
	require.NoError(t, err)
	t.Cleanup(server.Stop)
	peerL1RPC := httptest.NewServer(server)
	t.Cleanup(peerL1RPC.Close)
	peerHTTP, peerP2P, peerOverlay := freePort(t), freePort(t), freePort(t)
	peerURL := "http://127.0.0.1:" + peerHTTP
	standalone := exec.CommandContext(t.Context(), bin, "--eez.enabled", "--datadir", peerDir, "--eez.sync.l1only=true",
		"--eez.l1.rpc", peerL1RPC.URL, "--http.port", peerHTTP, "--http.api=eth,eez,admin", "--authrpc.port", freePort(t),
		"--private.api.addr", "127.0.0.1:"+freePort(t), "--port", peerP2P, "--torrent.port", freePort(t), "--nodiscover",
		"--eez.p2p.listen=/ip4/127.0.0.1/tcp/"+peerOverlay, "--eez.sequencer.address="+signer.Hex())
	peerOutput := &syncBuffer{}
	standalone.Stdout, standalone.Stderr = peerOutput, peerOutput
	require.NoError(t, standalone.Start())
	t.Cleanup(func() {
		_ = standalone.Process.Kill()
		if t.Failed() {
			t.Log(peerOutput.String())
		}
	})
	var peerNode *enode.Node
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var info struct {
			Enode string `json:"enode"`
		}
		require.NoError(c, call(t.Context(), peerURL, "admin_nodeInfo", &info))
		n, err := enode.ParseV4(info.Enode)
		require.NoError(c, err)
		peerNode = n
	}, 2*time.Minute, 250*time.Millisecond, "the standalone node's enode")
	peerEnode := enode.NewV4(peerNode.Pubkey(), net.IPv4(127, 0, 0, 1), peerNode.TCP(), 0).URLv4()

	l1Port, l2Port, l1P2P, l2P2P := freePort(t), freePort(t), freePort(t), freePort(t)
	l1URL, l2URL := "http://127.0.0.1:"+l1Port, "http://127.0.0.1:"+l2Port
	// A static peer of the L1 alone, which the EEZ chain must not dial.
	l1PeerKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	l1Peer := enode.NewV4(&l1PeerKey.PublicKey, net.IPv4(127, 0, 0, 1), 1, 0)
	node := slices.Clip(append(devChain(t), "--datadir", dir, "--eez.enabled", "--eez.chain=eez-devnet",
		"--eez.http.port", l2Port, "--eez.http.api=eth,eez,admin", "--http.port", l1Port, "--http.api=eth,admin",
		"--authrpc.port", freePort(t), "--private.api.addr", "127.0.0.1:"+freePort(t), "--port", l1P2P, "--torrent.port", freePort(t),
		"--eez.port", l2P2P, "--eez.staticpeers", peerEnode, "--staticpeers", peerEnode+","+l1Peer.URLv4(), "--log.dir.verbosity=trace"))
	// start runs the node with args added, waitFor fails the test if it
	// exits, and stop interrupts it and checks that it released both locks.
	var cmd *exec.Cmd
	var exited chan error
	start := func(args ...string) *syncBuffer {
		t.Helper()
		c := exec.CommandContext(t.Context(), bin, append(slices.Clone(node), args...)...)
		output := &syncBuffer{}
		c.Stdout, c.Stderr = output, output
		require.NoError(t, c.Start())
		done := make(chan error, 1)
		go func() { done <- c.Wait() }()
		t.Cleanup(func() {
			_ = c.Process.Kill()
			if t.Failed() {
				t.Log(output.String())
			}
		})
		cmd, exited = c, done
		return output
	}
	waitFor := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Minute)
		for !ok() {
			select {
			case err := <-exited:
				t.Fatalf("erigon exited: %v", err)
			case <-time.After(500 * time.Millisecond):
			}
			require.True(t, time.Now().Before(deadline), what)
		}
	}
	stop := func() {
		t.Helper()
		require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
		select {
		case err := <-exited:
			require.NoError(t, err)
		case <-time.After(2 * time.Minute):
			t.Fatal("erigon did not stop")
		}
		for _, d := range []string{dir, filepath.Join(dir, "eez")} {
			l, ok, err := datadir.TryFlock(datadir.Open(d))
			require.NoError(t, err)
			require.True(t, ok, "%s unlocked", d)
			require.NoError(t, l.Unlock())
		}
	}

	chainID := func(url string) uint64 {
		var id hexutil.Uint64
		_ = call(t.Context(), url, "eth_chainId", &id)
		return uint64(id)
	}
	start("--eez.sync.l1only=true")
	waitFor("L1 chain id", func() bool { return chainID(l1URL) == 1337 })
	waitFor("EEZ chain id", func() bool { return chainID(l2URL) == networks.Devnet.Config.ChainID.Uint64() })
	var status struct {
		L1Scanned hexutil.Uint64 `json:"l1Scanned"`
		Safe      hexutil.Uint64 `json:"safe"`
	}
	waitFor("the follower scans the L1 blocks", func() bool {
		return call(t.Context(), l2URL, "eez_followerStatus", &status) == nil && status.L1Scanned >= 2
	})
	require.Zero(t, status.Safe, "the dev chain settles nothing")
	require.FileExists(t, filepath.Join(dir, "chaindata", "mdbx.dat"))
	require.FileExists(t, filepath.Join(dir, "eez", "chaindata", "mdbx.dat"))
	require.FileExists(t, filepath.Join(dir, "eez", "LOCK"))
	require.DirExists(t, filepath.Join(dir, "eez", "temp"))
	require.Error(t, call(t.Context(), l1URL, "eez_followerStatus", &status), "the L1 serves no eez API")

	// Each chain has its own sentry: port, key and node database.
	l1Node, l2Node := nodeInfo(t, l1URL), nodeInfo(t, l2URL)
	require.NotEqual(t, l1Node.ID(), l2Node.ID())
	require.Equal(t, l1P2P, strconv.Itoa(l1Node.TCP()))
	require.Equal(t, l2P2P, strconv.Itoa(l2Node.TCP()))
	for d, n := range map[string]*enode.Node{dir: l1Node, filepath.Join(dir, "eez"): l2Node} {
		key, err := crypto.LoadECDSA(filepath.Join(d, "nodekey"))
		require.NoError(t, err)
		require.Equal(t, enode.PubkeyToIDV4(&key.PublicKey), n.ID(), "%s/nodekey", d)
	}
	require.DirExists(t, filepath.Join(dir, "eez", "nodes", "eth"))

	// The EEZ chain peers the standalone node; the L1 refuses it at the
	// eth handshake, a peer of another network, genesis and fork id. The
	// trace log tells the chains apart: the EEZ chain's lines carry chain=.
	traced := func(parts ...string) bool {
		raw, err := os.ReadFile(filepath.Join(dir, "logs", "erigon.log"))
		if err != nil {
			return false
		}
		for line := range strings.SplitSeq(string(raw), "\n") {
			if !slices.ContainsFunc(parts, func(p string) bool {
				if absent, ok := strings.CutPrefix(p, "!"); ok {
					return strings.Contains(line, absent)
				}
				return !strings.Contains(line, p)
			}) {
				return true
			}
		}
		return false
	}
	peerPub := strings.TrimPrefix(strings.Split(peerEnode, "@")[0], "enode://")
	require.Eventually(t, func() bool {
		return traced("Removing p2p peer", "!chain=", peerPub, "incompatible remote status")
	}, 2*time.Minute, 250*time.Millisecond, "the L1 refuses the EEZ node")
	require.True(t, traced("[p2p] Adding static node", "!chain=", "id="+l1Peer.ID().String()), "the L1 dials its own static peer")
	require.False(t, traced("chain=eez-devnet", "id="+l1Peer.ID().String()), "the EEZ chain dials the L1's static peer")
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		got := peers(c, t.Context(), l2URL)
		require.Len(c, got, 1)
		assert.Equal(c, peerNode.ID(), enode.MustParse(got[0].Enode).ID())
		assert.ElementsMatch(c, []string{"eth/68", "eth/69"}, got[0].Caps)
		assert.True(c, got[0].Network.Static, "static")
	}, 2*time.Minute, 250*time.Millisecond, "the EEZ chain's peers")
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		got := peers(c, t.Context(), peerURL)
		require.Len(c, got, 1)
		assert.Equal(c, l2Node.ID(), enode.MustParse(got[0].Enode).ID())
	}, 2*time.Minute, 250*time.Millisecond, "the standalone node's peers")
	require.Empty(t, peers(t, t.Context(), l1URL), "the L1's peers")
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var status struct {
			SyncMode string         `json:"syncMode"`
			Peers    hexutil.Uint64 `json:"peers"`
		}
		require.NoError(c, call(t.Context(), l2URL, "eez_followerStatus", &status))
		assert.Equal(c, "l1_only", status.SyncMode)
		assert.Equal(c, uint64(1), uint64(status.Peers))
	}, time.Minute, 250*time.Millisecond, "eez_followerStatus")
	stop()

	// Restarted in peers mode, the default on an R0 chain, set explicitly
	// as on every test node. The overlay identity is the EEZ chain's node
	// key.
	key, err := crypto.LoadECDSA(filepath.Join(dir, "eez", "nodekey"))
	require.NoError(t, err)
	self, err := overlay.PeerID(&key.PublicKey)
	require.NoError(t, err)
	peerID, err := overlay.PeerID(peerNode.Pubkey())
	require.NoError(t, err)
	addr := func(port string, id peer.ID) peer.AddrInfo {
		return peer.AddrInfo{ID: id, Addrs: []ma.Multiaddr{ma.StringCast("/ip4/127.0.0.1/tcp/" + port)}}
	}
	overlayPort := freePort(t)
	output := start("--eez.sync.l1only=false", "--eez.p2p.listen=/ip4/127.0.0.1/tcp/"+overlayPort, "--eez.sequencer.address="+signer.Hex(),
		"--eez.p2p.peers=/ip4/127.0.0.1/tcp/"+peerOverlay+"/p2p/"+peerID.String())
	waitFor("EEZ chain id", func() bool { return chainID(l2URL) == networks.Devnet.Config.ChainID.Uint64() })
	require.Equal(t, l2Node.ID(), nodeInfo(t, l2URL).ID(), "the node key is kept")
	var restarted hexutil.Uint64
	require.NoError(t, call(t.Context(), l1URL, "eth_blockNumber", &restarted))
	logged := func(parts ...string) bool {
		for line := range strings.SplitSeq(output.String(), "\n") {
			if !slices.ContainsFunc(parts, func(p string) bool { return !strings.Contains(line, p) }) {
				return true
			}
		}
		return false
	}
	waitFor("peers mode", func() bool { return logged("event=eez.node.sync.mode mode=peers") })
	waitFor("the overlay", func() bool { return logged("[eez] overlay listening", "peer="+self.String()) })

	// A head published to the all-in-one node reaches a node that only the
	// standalone node relays to.
	domain := seqsig.Domain{ChainID: networks.Devnet.Config.ChainID.Uint64(), Registry: common.HexToAddress("0x5fbdb2315678afecb367f032d93f642f64180aa3"), RollupID: 1}
	join := func(to peer.AddrInfo) *overlay.Overlay {
		key, err := crypto.GenerateKey()
		require.NoError(t, err)
		listen := ma.StringCast("/ip4/127.0.0.1/tcp/" + freePort(t))
		o, err := overlay.New(overlay.Config{Listen: listen, Peers: []peer.AddrInfo{to}, Key: key, Domain: domain, Signer: signer}, log.New())
		require.NoError(t, err)
		require.NoError(t, o.Start())
		t.Cleanup(func() { _ = o.Stop() })
		return o
	}
	publisher, subscriber := join(addr(overlayPort, self)), join(addr(peerOverlay, peerID))
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		epoch := uint64(time.Now().UnixMilli())
		for counter := uint64(1); ctx.Err() == nil; counter++ {
			h := seqsig.Head{RollupID: 1, Number: counter, BlockHash: common.Hash{byte(counter)}, ParentHash: common.Hash{0xff}, EpochMs: epoch, Counter: counter}
			if sig, err := seqsig.Sign(h, domain, sequencer); err == nil {
				_ = publisher.Publish(ctx, seqsig.Envelope{Head: h, Signature: sig})
			}
			select {
			case <-ctx.Done():
			case <-time.After(500 * time.Millisecond):
			}
		}
	}()
	select {
	case h := <-subscriber.Heads():
		require.Equal(t, signer, h.Signer)
	case err := <-exited:
		t.Fatalf("erigon exited: %v", err)
	case <-time.After(2 * time.Minute):
		t.Fatal("no head relayed through the all-in-one node")
	}
	cancel()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		var status struct {
			SyncMode     string          `json:"syncMode"`
			OverlayPeers *hexutil.Uint64 `json:"overlayPeers"`
			SignedHeads  []struct {
				Signer common.Address `json:"signer"`
			} `json:"signedHeads"`
		}
		require.NoError(c, call(t.Context(), l2URL, "eez_followerStatus", &status))
		assert.Equal(c, "peers", status.SyncMode)
		require.NotNil(c, status.OverlayPeers)
		assert.NotZero(c, uint64(*status.OverlayPeers))
		require.NotEmpty(c, status.SignedHeads)
		assert.Equal(c, signer, status.SignedHeads[0].Signer)
	}, time.Minute, 250*time.Millisecond, "eez_followerStatus")
	// Caplin builds the dev chain's blocks and panics when interrupted while
	// it starts: wait for a block it built after the restart.
	waitFor("an L1 block", func() bool {
		var head hexutil.Uint64
		return call(t.Context(), l1URL, "eth_blockNumber", &head) == nil && head > restarted
	})
	stop()
}
