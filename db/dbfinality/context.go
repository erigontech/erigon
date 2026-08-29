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

package dbfinality

import (
	"context"

	"github.com/erigontech/erigon/db/kv"
)

// Context defines immutable block boundaries for database retention work.
type Context interface {
	PruneToBlockNum() uint64
	RetireToBlockNum() uint64
	MaxReorgDepth() uint64
	ReadyForCollation(ctx context.Context, db kv.RoDB, stepLastTxNum uint64) (finalisedBlockNum, lastBlockInStep, lastBlockInDB, lastTxInDB uint64, ok bool, err error)
}
