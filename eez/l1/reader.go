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
// Package l1 reads the L1 facts an EEZ follower derives from, and turns them
// into settled batches and chain events.
package l1

import (
	"context"
	"errors"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

var (
	// ErrNotFound reports a block, transaction or tag the source does not serve.
	ErrNotFound = errors.New("not served by the L1 source")
	// ErrRangeTooWide reports a log query the source refuses as too large; a narrower range can succeed.
	ErrRangeTooWide = errors.New("log range too wide for the L1 source")
	// ErrInconsistent reports a response that contradicts itself or an earlier one.
	ErrInconsistent = errors.New("inconsistent L1 response")
)

// Header is the part of an L1 header the follower reads.
type Header struct {
	Number     uint64
	Hash       common.Hash
	ParentHash common.Hash
	Time       uint64
}

// ChainReader is every L1 fact the follower reads: headers, the head and finality tags, logs and transactions.
type ChainReader interface {
	ChainID(ctx context.Context) (uint64, error)
	// HeaderByNumber returns the canonical header at number.
	HeaderByNumber(ctx context.Context, number uint64) (*Header, error)
	// HeaderByHash returns the header with hash, canonical or not; a source
	// may answer ErrNotFound for a block it no longer holds.
	HeaderByHash(ctx context.Context, hash common.Hash) (*Header, error)
	// Head returns the latest canonical header.
	Head(ctx context.Context) (*Header, error)
	// Finalized returns the finalized header, or ErrNotFound before the
	// chain finalizes anything.
	Finalized(ctx context.Context) (*Header, error)
	// Logs returns the canonical logs of [from, to] emitted by address and
	// matching topics, where topics[i] lists the accepted values at position
	// i and an empty list accepts any. Logs are ordered by block,
	// transaction and log index and carry their position.
	Logs(ctx context.Context, from, to uint64, address common.Address, topics [][]common.Hash) ([]*types.Log, error)
	// TransactionByBlockHashAndIndex returns the transaction at index in the
	// block with blockHash.
	TransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index uint64) (types.Transaction, error)
}
