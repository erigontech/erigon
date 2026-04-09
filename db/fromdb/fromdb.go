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

package fromdb

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
)

func ChainConfig(db kv.RoDB) (cc *chain.Config) {
	err := db.View(context.Background(), func(tx kv.Tx) error {
		genesisBlockHash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		cc, err = rawdb.ReadChainConfig(tx, genesisBlockHash)
		return err
	})

	if err != nil {
		panic(fmt.Errorf("failed to read chain config: %w", err))
	}

	if cc == nil {
		panic("database is not initialized")
	}
	return cc
}

func PruneMode(db kv.RoDB) (pm prune.Mode) {
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		var err error
		pm, err = prune.Get(tx)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return
}
