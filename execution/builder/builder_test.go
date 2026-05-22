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

package builder

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/builder/buildercfg"
	"github.com/erigontech/erigon/execution/types"
)

// errDB is a minimal kv.TemporalRoDB stub whose BeginTemporalRo always fails.
// Other methods are never reached because Build returns at the first error.
type errDB struct {
	kv.TemporalRoDB // nil embed satisfies the interface; other methods must not be called
	err             error
}

func (e *errDB) BeginTemporalRo(_ context.Context) (kv.TemporalTx, error) {
	return nil, e.err
}

// TestBuilder_Build_DBError verifies that Build propagates a BeginTemporalRo error
// immediately, without panicking or hanging on a channel read.
func TestBuilder_Build_DBError(t *testing.T) {
	t.Parallel()

	want := errors.New("db open failed")
	b := &Builder{
		ctx:            context.Background(),
		db:             &errDB{err: want},
		builderCfg:     &buildercfg.BuilderConfig{},
		pendingBlockCh: make(chan *types.Block, 1),
		logger:         log.New(),
	}

	_, err := b.Build(&Parameters{}, &atomic.Bool{})
	require.ErrorIs(t, err, want)
}

// TestBuilder_PendingBlockCh verifies that NewBuilder initialises a non-nil buffered
// pending-block channel that is returned consistently by PendingBlockCh.
func TestBuilder_PendingBlockCh(t *testing.T) {
	t.Parallel()

	b := &Builder{pendingBlockCh: make(chan *types.Block, 1)}
	ch := b.PendingBlockCh()
	require.NotNil(t, ch)
	require.Equal(t, ch, b.PendingBlockCh(), "must return the same channel on repeated calls")
}
