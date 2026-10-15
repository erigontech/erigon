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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/l1/l1test"
)

func freePort(t *testing.T) string {
	t.Helper()
	l, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

func blockHash(ctx context.Context, url, tag string) (common.Hash, error) {
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber", "params": []any{tag, false}})
	if err != nil {
		return common.Hash{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return common.Hash{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return common.Hash{}, err
	}
	defer resp.Body.Close()
	var out struct {
		Result *struct {
			Hash common.Hash `json:"hash"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return common.Hash{}, err
	}
	if out.Result == nil {
		return common.Hash{}, fmt.Errorf("no block %s", tag)
	}
	return out.Result.Hash, nil
}

// TestFollowerBinary runs the erigon binary named by EEZ_ERIGON in follower
// mode against a composed L1 served over JSON-RPC: it derives the settled
// blocks under <datadir>/eez, serves no engine API, and exits with status 3
// when its chain diverges from L1.
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
	cmd := exec.CommandContext(t.Context(), bin, "--eez.enabled", "--eez.follower", "--chain=eez-devnet", "--datadir", dir,
		"--eez.l1.rpc", l1.URL, "--eez.l1.poll=200ms", "--http.port", httpPort, "--authrpc.port", authPort,
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
	require.DirExists(t, filepath.Join(dir, "eez", "chaindata"))
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
