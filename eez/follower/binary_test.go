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
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/l1/l1test"
	"github.com/erigontech/erigon/eez/networks"
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

// TestFollowerBinary runs the erigon binary named by EEZ_ERIGON as an EEZ
// node against a composed L1 served over JSON-RPC: it derives the settled
// blocks into <datadir>/eez/chaindata, serves no engine API, and exits with
// status 3 when its chain diverges from L1.
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

	dir := t.TempDir()
	httpPort, authPort := freePort(t), freePort(t)
	cmd := exec.CommandContext(t.Context(), bin, "--eez.enabled", "--chain=eez-devnet", "--datadir", dir,
		"--eez.l1.rpc", l1.URL, "--eez.l1.poll=200ms", "--http.port", httpPort, "--http.api=eth,eez", "--authrpc.port", authPort,
		"--private.api.addr", "127.0.0.1:"+freePort(t), "--port", freePort(t), "--torrent.port", freePort(t))
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

// TestAllInOneBinary runs the erigon binary named by EEZ_ERIGON as an
// all-in-one node: the dev chain as L1 and beside it an EEZ chain settling
// on it, under <datadir>/eez with its own lock and HTTP-RPC port, whose
// follower takes the L1 heads in-process. SIGTERM stops both chains: the L1's
// consensus layer resets SIGINT to its default action once it starts.
func TestAllInOneBinary(t *testing.T) {
	bin := os.Getenv("EEZ_ERIGON")
	if bin == "" {
		t.Skip("EEZ_ERIGON names no erigon binary")
	}
	dir := t.TempDir()

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
	out, err := exec.CommandContext(t.Context(), bin, "init", "--eez.enabled", "--datadir", dir, "--eez.l1.chainid=1337",
		"--eez.registry=0x5fbdb2315678afecb367f032d93f642f64180aa3", "--eez.registry.deployblock=0", "--eez.rollupid=1",
		"--eez.l1.blocktime=12s", "--eez.l2.blocktime=2s", genesisFile).CombinedOutput()
	require.NoError(t, err, "%s", out)

	l1Port, l2Port := freePort(t), freePort(t)
	cmd := exec.CommandContext(t.Context(), bin, "--chain=dev", "--datadir", dir, "--eez.enabled", "--eez.chain=eez-devnet",
		"--eez.http.port", l2Port, "--http.port", l1Port, "--http.api=eth", "--authrpc.port", freePort(t),
		"--private.api.addr", "127.0.0.1:"+freePort(t), "--port", freePort(t), "--torrent.port", freePort(t),
		"--beacon.api.port", freePort(t), "--caplin.discovery.port", freePort(t), "--caplin.discovery.tcpport", freePort(t),
		"--sentinel.port", freePort(t), "--mcp.disable")
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

	l1URL, l2URL := "http://127.0.0.1:"+l1Port, "http://127.0.0.1:"+l2Port
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
	chainID := func(url string) uint64 {
		var id hexutil.Uint64
		_ = call(t.Context(), url, "eth_chainId", &id)
		return uint64(id)
	}
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
