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

package announce

import (
	"context"

	"github.com/c2h5oh/datasize"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// OpenSized opens a store in dir/announcements with a map of size and a
// window of window blocks.
func OpenSized(ctx context.Context, dir string, size datasize.ByteSize, window uint64, logger log.Logger) (*Store, error) {
	return open(ctx, dir, size, window, logger)
}

// Len returns the number of announcements s keeps.
func (s *Store) Len(ctx context.Context) (n uint64, err error) {
	err = s.db.View(ctx, func(tx kv.Tx) error {
		n, err = tx.Count(table)
		return err
	})
	return n, err
}

// WrapDB makes s use wrap(db) in place of its database db.
func (s *Store) WrapDB(wrap func(kv.RwDB) kv.RwDB) { s.db = wrap(s.db) }

// Keeper is the store a Signer keeps its announcements in.
type Keeper = keeper

// SetKeeper makes s keep its announcements in k.
func (s *Signer) SetKeeper(k Keeper) { s.store = k }

// SetBackfillLimit sets the blocks below its own one Record of s checks.
func (s *Signer) SetBackfillLimit(limit uint64) { s.limit = limit }

// Gap is the lowest block from which the unsettled blocks may lack their
// announcement for s, zero for none.
func (s *Signer) Gap() uint64 { return s.gap }
