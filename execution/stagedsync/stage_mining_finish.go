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

package stagedsync

import (
	"fmt"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/turbo/services"
)

type MiningFinishCfg struct {
	db                    kv.RwDB
	chainConfig           *chain.Config
	engine                consensus.Engine
	sealCancel            chan struct{}
	miningState           MiningState
	blockReader           services.FullBlockReader
	latestBlockBuiltStore *builder.LatestBlockBuiltStore
}

func StageMiningFinishCfg(
	db kv.RwDB,
	chainConfig *chain.Config,
	engine consensus.Engine,
	miningState MiningState,
	sealCancel chan struct{},
	blockReader services.FullBlockReader,
	latestBlockBuiltStore *builder.LatestBlockBuiltStore,
) MiningFinishCfg {
	return MiningFinishCfg{
		db:                    db,
		chainConfig:           chainConfig,
		engine:                engine,
		miningState:           miningState,
		sealCancel:            sealCancel,
		blockReader:           blockReader,
		latestBlockBuiltStore: latestBlockBuiltStore,
	}
}

func SpawnMiningFinishStage(s *StageState, tx kv.RwTx, cfg MiningFinishCfg, quit <-chan struct{}, logger log.Logger) error {
	logPrefix := s.LogPrefix()
	current := cfg.miningState.MiningBlock

	// Short circuit when receiving duplicate result caused by resubmitting.
	//if w.chain.HasBlock(block.Hash(), block.NumberU64()) {
	//	continue
	//}

	block := types.NewBlockForAsembling(current.Header, current.Txns, current.Uncles, current.Receipts, current.Withdrawals)
	blockWithReceipts := &types.BlockWithReceipts{Block: block, Receipts: current.Receipts, Requests: current.Requests}
	*current = MiningBlock{} // hack to clean global data

	//sealHash := engine.SealHash(block.Header())
	// Reject duplicate sealing work due to resubmitting.
	//if sealHash == prev {
	//	s.Done()
	//	return nil
	//}
	//prev = sealHash
	cfg.latestBlockBuiltStore.AddBlockBuilt(block)

	// Tests may set pre-calculated nonce
	if block.NonceU64() != 0 {
		// Note: To propose a new signer for Clique consensus, the block nonce should be set to 0xFFFFFFFFFFFFFFFF.
		if cfg.engine.Type() != chain.CliqueConsensus {
			cfg.miningState.MiningResultCh <- blockWithReceipts
			return nil
		}
	}

	cfg.miningState.PendingResultCh <- block

	if block.Transactions().Len() > 0 {
		logger.Info(fmt.Sprintf("[%s] block ready for seal", logPrefix),
			"blockNum", block.NumberU64(),
			"nonce", block.Nonce(),
			"hash", block.Hash(),
			"gasLimit", block.GasLimit(),
			"gasUsed", block.GasUsed(),
			"blobGasUsed", block.Header().BlobGasUsed,
			"transactionsCount", block.Transactions().Len(),
			"coinbase", block.Coinbase(),
			"stateRoot", block.Root(),
			"withdrawalsHash", block.WithdrawalsHash(),
			"requestsHash", block.RequestsHash(),
		)
	}
	// interrupt aborts the in-flight sealing task.
	select {
	case cfg.sealCancel <- struct{}{}:
	default:
		logger.Trace("No in-flight sealing task.")
	}
	chain := ChainReader{Cfg: cfg.chainConfig, Db: tx, BlockReader: cfg.blockReader, Logger: logger}
	if err := cfg.engine.Seal(chain, blockWithReceipts, cfg.miningState.MiningResultCh, cfg.sealCancel); err != nil {
		logger.Warn("Block sealing failed", "err", err)
	}

	return nil
}
