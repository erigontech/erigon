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

package follower

import (
	"context"
	"errors"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

var errWrite = errors.New("write failed")

// stubChain is a chain whose writes set the head as told: next is the head a
// write leaves, nil for none, and fail the error it returns.
type stubChain struct {
	deriver.Chain
	head *types.Header
	next *types.Header
	fail error
}

func (c *stubChain) Head(context.Context) (*types.Header, error) { return c.head, nil }

func (c *stubChain) write() error {
	if c.next != nil {
		c.head = c.next
	}
	return c.fail
}

func (c *stubChain) ForkChoice(context.Context, common.Hash, common.Hash, common.Hash) error {
	return c.write()
}

func (c *stubChain) Extend(context.Context, *types.Header, deriver.HeaderInputs, []types.Transaction, common.Hash, common.Hash) (*types.BlockWithReceipts, error) {
	return c.built()
}

func (c *stubChain) Build(context.Context, deriver.HeaderInputs, txnprovider.TxnProvider, common.Hash, common.Hash) (*types.BlockWithReceipts, error) {
	return c.built()
}

func (c *stubChain) built() (*types.BlockWithReceipts, error) {
	if err := c.write(); err != nil {
		return nil, err
	}
	return &types.BlockWithReceipts{Block: types.NewBlockWithHeader(c.head, nil)}, nil
}

func header(n uint64) *types.Header { return &types.Header{Number: *uint256.NewInt(n)} }

// TestHeadFunnel: once open, the funnel hands the head each write leaves when
// it differs from the last one handed, whatever the write reports: a fork
// choice that succeeds without moving the head hands nothing, an Extend that
// fails after moving the head to its parent hands the parent.
func TestHeadFunnel(t *testing.T) {
	ctx := t.Context()
	h0, h1, h2, h3 := header(0), header(1), header(2), header(3)
	chain := &stubChain{head: h0}
	var handed []common.Hash
	f := &headFunnel{Chain: chain, sink: func(h common.Hash) { handed = append(handed, h) }, logger: log.New()}
	hash := common.Hash{0xff}

	chain.next = h1
	require.NoError(t, f.ForkChoice(ctx, hash, hash, hash))
	require.Empty(t, handed, "a write before the funnel opens")
	f.start(ctx)
	require.Equal(t, []common.Hash{h1.Hash()}, handed, "the head boot ended at")

	for _, step := range []struct {
		name  string
		write func() error
		next  *types.Header
		fail  error
		want  *types.Header
	}{
		{"fork choice that moves", func() error { return f.ForkChoice(ctx, hash, hash, hash) }, h2, nil, h2},
		{"fork choice that keeps the head", func() error { return f.ForkChoice(ctx, h3.Hash(), hash, hash) }, nil, nil, nil},
		{"refused fork choice", func() error { return f.ForkChoice(ctx, h3.Hash(), hash, hash) }, nil, errWrite, nil},
		{"failed fork choice that moved", func() error { return f.ForkChoice(ctx, hash, hash, hash) }, h1, errWrite, h1},
		{"extend", func() error { _, err := f.Extend(ctx, h1, deriver.HeaderInputs{}, nil, hash, hash); return err }, h2, nil, h2},
		{"extend failing after its rewind", func() error {
			_, err := f.Extend(ctx, h1, deriver.HeaderInputs{}, nil, hash, hash)
			return err
		}, h1, errWrite, h1},
		{"build", func() error { _, err := f.Build(ctx, deriver.HeaderInputs{}, nil, hash, hash); return err }, h3, nil, h3},
		{"build failing", func() error { _, err := f.Build(ctx, deriver.HeaderInputs{}, nil, hash, hash); return err }, nil, errWrite, nil},
	} {
		chain.next, chain.fail = step.next, step.fail
		before := len(handed)
		err := step.write()
		require.ErrorIs(t, err, step.fail, step.name)
		if step.want == nil {
			require.Len(t, handed, before, step.name)
			continue
		}
		require.Len(t, handed, before+1, step.name)
		require.Equal(t, step.want.Hash(), handed[before], step.name)
	}
}
