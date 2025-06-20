// Copyright 2018 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package rawdb

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/params"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/types"
)

// GenesisWithoutStateToBlock creates the genesis block, assuming an empty state.
func GenesisWithoutStateToBlock(g *types.Genesis) (head *types.Header, withdrawals []*types.Withdrawal) {
	head = &types.Header{
		Number:        new(big.Int).SetUint64(g.Number),
		Nonce:         types.EncodeNonce(g.Nonce),
		Time:          g.Timestamp,
		ParentHash:    g.ParentHash,
		Extra:         g.ExtraData,
		GasLimit:      g.GasLimit,
		GasUsed:       g.GasUsed,
		Difficulty:    g.Difficulty,
		MixDigest:     g.Mixhash,
		Coinbase:      g.Coinbase,
		BaseFee:       g.BaseFee,
		BlobGasUsed:   g.BlobGasUsed,
		ExcessBlobGas: g.ExcessBlobGas,
		RequestsHash:  g.RequestsHash,
		Root:          empty.RootHash,
	}
	if g.AuRaSeal != nil && len(g.AuRaSeal.AuthorityRound.Signature) > 0 {
		head.AuRaSeal = g.AuRaSeal.AuthorityRound.Signature
		head.AuRaStep = uint64(g.AuRaSeal.AuthorityRound.Step)
	}
	if g.GasLimit == 0 {
		head.GasLimit = params.GenesisGasLimit
	}
	if g.Difficulty == nil {
		head.Difficulty = params.GenesisDifficulty
	}
	if g.Config != nil && g.Config.IsLondon(0) {
		if g.BaseFee != nil {
			head.BaseFee = g.BaseFee
		} else {
			head.BaseFee = new(big.Int).SetUint64(params.InitialBaseFee)
		}
	}

	withdrawals = nil
	if g.Config != nil && g.Config.IsShanghai(g.Timestamp) {
		withdrawals = []*types.Withdrawal{}
	}

	if g.Config != nil && g.Config.IsCancun(g.Timestamp) {
		if g.BlobGasUsed != nil {
			head.BlobGasUsed = g.BlobGasUsed
		} else {
			head.BlobGasUsed = new(uint64)
		}
		if g.ExcessBlobGas != nil {
			head.ExcessBlobGas = g.ExcessBlobGas
		} else {
			head.ExcessBlobGas = new(uint64)
		}
		if g.ParentBeaconBlockRoot != nil {
			head.ParentBeaconBlockRoot = g.ParentBeaconBlockRoot
		} else {
			head.ParentBeaconBlockRoot = &common.Hash{}
		}
	}

	if g.Config != nil && g.Config.IsPrague(g.Timestamp) {
		if g.RequestsHash != nil {
			head.RequestsHash = g.RequestsHash
		} else {
			head.RequestsHash = &empty.RequestsHash
		}
	}

	if g.Config != nil && g.Config.Bor != nil {
		if g.Config.IsAgra(0) {
			withdrawals = []*types.Withdrawal{}
		}
		head.BlobGasUsed = new(uint64)
		head.ExcessBlobGas = new(uint64)
		head.ParentBeaconBlockRoot = &common.Hash{}
	}

	return
}

// WriteChainConfig writes the chain config settings to the database.
func WriteChainConfig(db kv.Putter, hash common.Hash, cfg *chain.Config) error {
	if cfg == nil {
		return nil
	}

	if cfg.Bor != nil {
		borJSON, err := json.Marshal(cfg.Bor)
		if err != nil {
			return fmt.Errorf("failed to JSON encode chain config 'bor': %w", err)
		}
		cfg.BorJSON = borJSON
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to JSON encode chain config: %w", err)
	}

	if err := db.Put(kv.ConfigTable, hash[:], data); err != nil {
		return fmt.Errorf("failed to store chain config: %w", err)
	}
	return nil
}

// Write writes the block a genesis specification to the database.
// The block is committed as the canonical head block.
func WriteGenesisBesideState(block *types.Block, tx kv.RwTx, g *types.Genesis) error {
	config := g.Config
	if config == nil {
		config = chain.AllProtocolChanges
	}
	if err := config.CheckConfigForkOrder(); err != nil {
		return err
	}

	if err := WriteBlock(tx, block); err != nil {
		return err
	}
	if err := WriteTd(tx, block.Hash(), block.NumberU64(), g.Difficulty); err != nil {
		return err
	}
	if err := rawdbv3.TxNums.Append(tx, 0, uint64(block.Transactions().Len()+1)); err != nil {
		return err
	}

	if err := WriteCanonicalHash(tx, block.Hash(), block.NumberU64()); err != nil {
		return err
	}

	WriteHeadBlockHash(tx, block.Hash())
	if err := WriteHeadHeaderHash(tx, block.Hash()); err != nil {
		return err
	}
	return WriteChainConfig(tx, block.Hash(), config)
}

func MustCommitGenesisWithoutState(g *types.Genesis, db kv.RwDB) *types.Block {
	tx, err := db.BeginRw(context.Background())
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	header, withdrawals := GenesisWithoutStateToBlock(g)
	block := types.NewBlock(header, nil, nil, nil, withdrawals)
	err = WriteGenesisBesideState(block, tx, g)
	if err != nil {
		panic(err)
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	return block
}
