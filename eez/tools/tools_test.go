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

package tools_test

import (
	"context"
	"flag"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/recording"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/eez/tools"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

var update = flag.Bool("update", false, "rewrite the golden files")

var (
	alice = common.HexToAddress("0xa11ce")
	bob   = common.HexToAddress("0xb0b")
)

// TestDecode is T-TOOL-3: window-84 decodes to its golden rendering.
func TestDecode(t *testing.T) {
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(t, err)
	got, err := tools.Decode(w.PostBatch)
	require.NoError(t, err)
	golden := filepath.Join("testdata", "decode-window-84.json")
	if *update {
		require.NoError(t, os.WriteFile(golden, got, 0o644))
	}
	want, err := os.ReadFile(golden)
	require.NoError(t, err)
	require.Equal(t, string(want), string(got))
}

func compose(t *testing.T) (*derivertest.Composer, []*derivertest.Batch) {
	t.Helper()
	c := derivertest.NewComposer(t)
	first := c.Compose(nil,
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{c.Transfer(bob, 1)}},
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{}},
	)
	c.Post(derivertest.Posting{Batch: first})
	second := c.Compose([]da.Action{{
		SourceRollupID: 0, TargetRollupID: 1, SourceAddress: bob, TargetAddress: alice,
		Data: []byte{}, Success: true, ReturnData: []byte{},
	}}, derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{c.Transfer(alice, 2)}})
	c.Post(derivertest.Posting{Batch: second})
	return c, []*derivertest.Batch{first, second}
}

// TestCapture is T-CAP-1 on an in-process chain: its capture loads through
// the recording reader and scans to the same batches.
func TestCapture(t *testing.T) {
	c, _ := compose(t)
	head, err := c.L1.Head(t.Context())
	require.NoError(t, err)
	require.NoError(t, c.L1.Finalize(head.Number-1))
	from := head.Number - 3
	captured, err := tools.Capture(t.Context(), c.L1, c.Registry(), from, head.Number)
	require.NoError(t, err)
	dir := t.TempDir()
	require.NoError(t, recording.Write(dir, captured))
	rec, err := recording.Load(dir)
	require.NoError(t, err)

	scan := func(r l1.ChainReader) []l1.ScannedBatch {
		batches, reached, err := l1.NewScanner(r, c.Registry(), 1).Scan(t.Context(), from, head.Number)
		require.NoError(t, err)
		require.Equal(t, head.Number, reached)
		return batches
	}
	want := scan(c.L1)
	require.Len(t, want, 2)
	require.Equal(t, want, scan(rec))
	f, err := rec.Finalized(t.Context())
	require.NoError(t, err)
	require.Equal(t, head.Number-1, f.Number)

	_, err = tools.Capture(t.Context(), c.L1, c.Registry(), head.Number+1, head.Number)
	require.Error(t, err)
}

// serve exposes m's chain over JSON-RPC.
func serve(t *testing.T, m *execmoduletester.ExecModuleTester) *tools.RPCSource {
	t.Helper()
	logger := log.New()
	api := jsonrpc.NewEthAPI(jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}),
		m.DB, nil, nil, nil, &rpccfg.EthApiConfig{
			GasCap:                  5_000_000,
			FeeCap:                  ethconfig.Defaults.RPCTxFeeCap,
			ReturnDataLimit:         100_000,
			RpcTxSyncDefaultTimeout: time.Second,
			RpcTxSyncMaxTimeout:     time.Second,
		}, logger)
	server := rpc.NewServer(1, false, false, true, logger, 0)
	require.NoError(t, server.RegisterName("eth", api))
	t.Cleanup(server.Stop)
	hs := httptest.NewServer(server)
	t.Cleanup(hs.Close)
	s, err := tools.DialSource(context.Background(), hs.URL, logger)
	require.NoError(t, err)
	t.Cleanup(s.Close)
	return s
}

// TestDiff is T-TOOL-2: identical chains show no difference; a block built
// otherwise is the first difference, with its differing fields.
func TestDiff(t *testing.T) {
	a, _ := compose(t)
	b, _ := compose(t)
	other := derivertest.NewComposer(t)
	other.Compose(nil,
		derivertest.Block{Coinbase: alice, Extra: []byte{}, Txs: [][]byte{other.Transfer(bob, 1)}},
		derivertest.Block{Coinbase: bob, Extra: []byte{}, Txs: [][]byte{}},
	)
	sa, sb, so := serve(t, a.L2), serve(t, b.L2), serve(t, other.L2)

	d, err := tools.Diff(t.Context(), sa, sb, 0, 3)
	require.NoError(t, err)
	require.Nil(t, d)

	d, err = tools.Diff(t.Context(), sa, so, 0, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), d.Block)
	require.Contains(t, d.Fields, "miner")
	require.Contains(t, d.Fields, "hash")
	require.NotContains(t, d.Fields, "number")

	_, err = tools.Diff(t.Context(), sa, so, 0, 3)
	require.ErrorIs(t, err, tools.ErrNoBlock)

	_, err = tools.Diff(t.Context(), sa, so, 3, 2)
	require.ErrorContains(t, err, "empty range")
}
