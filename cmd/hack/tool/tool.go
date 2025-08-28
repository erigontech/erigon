// Copyright 2024 The Erigon Authors
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

package tool

import (
	"context"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
)

func Check(e error) {
	if e != nil {
		panic(e)
	}
}

func ChainConfig(tx kv.Tx) *chain.Config {
	genesisBlockHash, err := rawdb.ReadCanonicalHash(tx, 0)
	Check(err)
	chainConfig, err := rawdb.ReadChainConfig(tx, genesisBlockHash)
	Check(err)
	return chainConfig
}

func ChainConfigFromDB(db kv.RoDB) (cc *chain.Config) {
	err := db.View(context.Background(), func(tx kv.Tx) error {
		cc = ChainConfig(tx)
		return nil
	})
	Check(err)
	return cc
}
