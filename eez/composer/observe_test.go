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

package composer

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/l1"
	"github.com/erigontech/erigon/eez/l1/post"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/types"
)

var (
	eezL1     = common.HexToAddress("0x5fbdb2315678afecb367f032d93f642f64180aa3")
	elsewhere = common.HexToAddress("0xdead")
)

func batchPosted(t *testing.T, at common.Address) *types.Log {
	t.Helper()
	l, err := protocol.BatchPostedLog(at, common.Hash{9}, []uint64{1})
	require.NoError(t, err)
	return l
}

func performed(t *testing.T, at common.Address, rollup uint64, root common.Hash) *types.Log {
	t.Helper()
	l, err := protocol.L2ExecutionPerformedLog(at, rollup, root, new(big.Int))
	require.NoError(t, err)
	return l
}

func skipped(t *testing.T) *types.Log {
	t.Helper()
	l, err := protocol.L2TxSkippedLog(eezL1, new(big.Int), []byte{})
	require.NoError(t, err)
	return l
}

// receipt is a receipt of transaction 2 of block 70 with logs.
func receipt(status uint64, logs ...*types.Log) *l1.Receipt {
	for i, l := range logs {
		l.TxIndex, l.Index, l.BlockNumber = 2, hexutil.Uint(10+i), 70
	}
	return &l1.Receipt{Status: status, BlockNumber: 70, TxIndex: 2, Logs: logs}
}

func anchorBatch(t *testing.T) (*protocol.ProofSystemBatch, common.Hash) {
	t.Helper()
	_, b, _ := window(t, "captured-devnet-anchor-402")
	return b, b.Entries[0].RollupUpdates[0].NewRoot
}

func TestJudge(t *testing.T) {
	b, terminal := anchorBatch(t)
	o := &Observer{RollupID: 1, EEZ: eezL1, Logger: log.New()}
	for _, c := range []struct {
		name string
		r    *l1.Receipt
		want Resolution
	}{
		{"settled", receipt(1, batchPosted(t, eezL1), performed(t, eezL1, 1, terminal)), Settled},
		{"elsewhere", receipt(1, batchPosted(t, eezL1), performed(t, eezL1, 1, common.Hash{7})), SettledPartial},
		{"another rollup", receipt(1, batchPosted(t, eezL1), performed(t, eezL1, 2, terminal)), Failed},
		{"skipped", receipt(1, batchPosted(t, eezL1), skipped(t)), Failed},
		{"reverted", receipt(0, batchPosted(t, eezL1), performed(t, eezL1, 1, terminal)), Failed},
		{"not posted", receipt(1), Failed},
		{"performed without a posting", receipt(1, performed(t, eezL1, 1, terminal)), Failed},
		{"another contract", receipt(1, batchPosted(t, elsewhere), performed(t, elsewhere, 1, terminal)), Failed},
	} {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, o.judge(c.r, b, terminal))
		})
	}
}

// scripted is an L1 whose state script changes on each receipt read.
type scripted struct {
	mu     sync.Mutex
	head   uint64
	got    *l1.Receipt
	failed error
	reads  int
	script func(c *scripted, read int)
	// has reports the posting known to the node; lookups counts the reads.
	has          bool
	lookupFailed error
	lookups      int
}

func (c *scripted) HasTransaction(context.Context, common.Hash) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lookups++
	return c.has, c.lookupFailed
}

func (c *scripted) HeaderByNumber(_ context.Context, n uint64) (*l1.Header, error) {
	return &l1.Header{Number: n}, nil
}

func (c *scripted) Head(context.Context) (*l1.Header, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &l1.Header{Number: c.head}, c.failed
}

func (c *scripted) Receipt(context.Context, common.Hash) (*l1.Receipt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reads++
	if c.script != nil {
		c.script(c, c.reads)
	}
	if c.failed != nil {
		return nil, c.failed
	}
	return c.got, nil
}

// stalling is an L1 node that answers its first receipt read never, the
// others with settles.
type stalling struct {
	reads   atomic.Int32
	settles *l1.Receipt
}

func (*stalling) Head(context.Context) (*l1.Header, error) { return &l1.Header{Number: 100}, nil }

func (*stalling) HeaderByNumber(_ context.Context, n uint64) (*l1.Header, error) {
	return &l1.Header{Number: n}, nil
}

func (s *stalling) Receipt(ctx context.Context, _ common.Hash) (*l1.Receipt, error) {
	if s.reads.Add(1) == 1 {
		<-ctx.Done()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.settles, nil
}

func (*stalling) HasTransaction(context.Context, common.Hash) (bool, error) { return false, nil }

// TestWatchOutlivesAHungRead: a read the node never answers ends within
// Read, and the next poll gets the verdict.
func TestWatchOutlivesAHungRead(t *testing.T) {
	b, terminal := anchorBatch(t)
	node := &stalling{settles: receipt(1, batchPosted(t, eezL1), performed(t, eezL1, 1, terminal))}
	o := &Observer{Node: node, RollupID: 1, EEZ: eezL1, Every: time.Millisecond, Read: 50 * time.Millisecond, Logger: log.New()}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	got, err := o.Watch(ctx, common.Hash{1}, b, terminal, post.Sent{Block: 101})
	require.NoError(t, err)
	require.Equal(t, Settled, got)
}

func TestWatch(t *testing.T) {
	b, terminal := anchorBatch(t)
	settles := receipt(1, batchPosted(t, eezL1), performed(t, eezL1, 1, terminal))
	down := errors.New("node down")
	for _, c := range []struct {
		name        string
		head        uint64
		pooled, has bool
		script      func(c *scripted, read int)
		want        Resolution
	}{
		{name: "settles once its receipt appears", head: 100, script: func(c *scripted, read int) {
			if read == 3 {
				c.got = settles
			}
		}, want: Settled},
		{name: "dropped once L1 passes the target", head: 100, script: func(c *scripted, read int) {
			if read == 3 {
				c.head = 102
			}
		}, want: Failed},
		{name: "the target block itself is no verdict", head: 100, script: func(c *scripted, read int) {
			switch read {
			case 2:
				c.head = 101
			case 5:
				c.got = settles
			}
		}, want: Settled},
		{name: "a receipt behind the head", head: 102, script: func(c *scripted, read int) {
			if read == 2 {
				c.got = settles
			}
		}, want: Settled},
		{name: "a failed recheck is no verdict", head: 102, script: func(c *scripted, read int) {
			switch read {
			case 2:
				c.failed = down
			case 3:
				c.failed, c.got = nil, settles
			}
		}, want: Settled},
		{name: "failed reads are no verdict", head: 100, script: func(c *scripted, read int) {
			switch read {
			case 1:
				c.failed, c.head = down, 102
			case 4:
				c.failed, c.got = nil, settles
			}
		}, want: Settled},
		{name: "a pooled posting the node has outlives its target by its grace", head: 100, pooled: true, has: true, script: func(c *scripted, read int) {
			switch read {
			case 2:
				c.head = 101 + pooledGrace
			case 6:
				c.got = settles
			}
		}, want: Settled},
		{name: "a pooled posting the node has, past its grace", head: 100, pooled: true, has: true, script: func(c *scripted, read int) {
			if read == 3 {
				c.head = 102 + pooledGrace
			}
		}, want: Failed},
		{name: "a pooled posting the node lost", head: 100, pooled: true, has: true, script: func(c *scripted, read int) {
			if read == 3 {
				c.has = false
			}
		}, want: Failed},
		{name: "a lost pooled posting's late receipt", head: 100, pooled: true, has: true, script: func(c *scripted, read int) {
			switch read {
			case 3:
				c.has = false
			case 4:
				c.got = settles
			}
		}, want: Settled},
		{name: "a failed lookup is no verdict", head: 100, pooled: true, has: true, script: func(c *scripted, read int) {
			switch read {
			case 2:
				c.lookupFailed, c.has, c.head = down, false, 105
			case 5:
				c.lookupFailed, c.got = nil, settles
			}
		}, want: Settled},
		{name: "a pooled posting the node never showed falls back on its target", head: 100, pooled: true, script: func(c *scripted, read int) {
			if read == 3 {
				c.head = 102
			}
		}, want: Failed},
	} {
		t.Run(c.name, func(t *testing.T) {
			node := &scripted{head: c.head, script: c.script, has: c.has}
			o := &Observer{Node: node, RollupID: 1, EEZ: eezL1, Every: time.Millisecond, Logger: log.New()}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			got, err := o.Watch(ctx, common.Hash{1}, b, terminal, post.Sent{Block: 101, Pooled: c.pooled})
			require.NoError(t, err)
			require.Equal(t, c.want, got)
			if !c.pooled {
				require.Zero(t, node.lookups, "a bundle is not looked up")
			}
		})
	}
	t.Run("ends with its context", func(t *testing.T) {
		o := &Observer{Node: &scripted{head: 100}, RollupID: 1, EEZ: eezL1, Every: time.Millisecond, Logger: log.New()}
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		got, err := o.Watch(ctx, common.Hash{1}, b, terminal, post.Sent{Block: 101})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, Pending, got)
	})
}

// TestWatchScansTheBlock: with a scanner, a posting's settlement is read
// from its whole block, where the triggers that consume its entries log.
func TestWatchScansTheBlock(t *testing.T) {
	b, terminal := anchorBatch(t)
	posting := common.Hash{1}
	landed := receipt(1, batchPosted(t, eezL1))
	for _, c := range []struct {
		name    string
		scanned []l1.ScannedBatch
		failed  error
		want    Resolution
	}{
		{"settled by its triggers", []l1.ScannedBatch{{TxHash: posting, Settlement: protocol.Settlement{Applied: []protocol.AppliedEntry{{}}, FinalState: terminal}}}, nil, Settled},
		{"part of it", []l1.ScannedBatch{{TxHash: posting, Settlement: protocol.Settlement{Applied: []protocol.AppliedEntry{{}}, FinalState: common.Hash{7}}}}, nil, SettledPartial},
		{"another posting's", []l1.ScannedBatch{{TxHash: common.Hash{2}, Settlement: protocol.Settlement{Applied: []protocol.AppliedEntry{{}}, FinalState: terminal}}}, nil, Failed},
		{"nothing applied", nil, nil, Failed},
	} {
		t.Run(c.name, func(t *testing.T) {
			var asked []uint64
			o := &Observer{
				Node: &scripted{head: 100, got: landed}, RollupID: 1, EEZ: eezL1, Every: time.Millisecond, Logger: log.New(),
				Scan: func(_ context.Context, block uint64) ([]l1.ScannedBatch, error) {
					asked = append(asked, block)
					return c.scanned, nil
				},
			}
			got, err := o.Watch(t.Context(), posting, b, terminal, post.Sent{Block: 101})
			require.NoError(t, err)
			require.Equal(t, c.want, got)
			require.Equal(t, []uint64{landed.BlockNumber}, asked)
		})
	}
	t.Run("a failed scan is no verdict", func(t *testing.T) {
		scans := 0
		o := &Observer{
			Node: &scripted{head: 100, got: landed}, RollupID: 1, EEZ: eezL1, Every: time.Millisecond, Logger: log.New(),
			Scan: func(context.Context, uint64) ([]l1.ScannedBatch, error) {
				if scans++; scans < 3 {
					return nil, errors.New("node down")
				}
				return []l1.ScannedBatch{{TxHash: posting, Settlement: protocol.Settlement{Applied: []protocol.AppliedEntry{{}}, FinalState: terminal}}}, nil
			},
		}
		got, err := o.Watch(t.Context(), posting, b, terminal, post.Sent{Block: 101})
		require.NoError(t, err)
		require.Equal(t, Settled, got)
		require.Equal(t, 3, scans)
	})
}
