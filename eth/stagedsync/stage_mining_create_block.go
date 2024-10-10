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
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/erigontech/erigon-lib/wrap"

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/common/debug"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/consensus/misc"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethutils"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/turbo/services"
)

type MiningBlock struct {
	ParentHeaderTime uint64
	Header           *types.Header
	Uncles           []*types.Header
	Txs              types.Transactions
	Receipts         types.Receipts
	Withdrawals      []*types.Withdrawal
	PreparedTxs      types.TransactionsStream
	Requests         types.Requests
}

type MiningState struct {
	MiningConfig    *params.MiningConfig
	PendingResultCh chan *types.Block
	MiningResultCh  chan *types.BlockWithReceipts
	MiningBlock     *MiningBlock
}

func NewMiningState(cfg *params.MiningConfig) MiningState {
	return MiningState{
		MiningConfig:    cfg,
		PendingResultCh: make(chan *types.Block, 1),
		MiningResultCh:  make(chan *types.BlockWithReceipts, 1),
		MiningBlock:     &MiningBlock{},
	}
}

type MiningCreateBlockCfg struct {
	db                     kv.RwDB
	miner                  MiningState
	chainConfig            chain.Config
	engine                 consensus.Engine
	txPoolDB               kv.RoDB
	tmpdir                 string
	blockBuilderParameters *core.BlockBuilderParameters
	blockReader            services.FullBlockReader
}

func StageMiningCreateBlockCfg(db kv.RwDB, miner MiningState, chainConfig chain.Config, engine consensus.Engine, txPoolDB kv.RoDB, blockBuilderParameters *core.BlockBuilderParameters, tmpdir string, blockReader services.FullBlockReader) MiningCreateBlockCfg {
	return MiningCreateBlockCfg{
		db:                     db,
		miner:                  miner,
		chainConfig:            chainConfig,
		engine:                 engine,
		txPoolDB:               txPoolDB,
		tmpdir:                 tmpdir,
		blockBuilderParameters: blockBuilderParameters,
		blockReader:            blockReader,
	}
}

var maxTransactions uint16 = 1000

// SpawnMiningCreateBlockStage
// TODO:
// - resubmitAdjustCh - variable is not implemented
func SpawnMiningCreateBlockStage(s *StageState, txc wrap.TxContainer, cfg MiningCreateBlockCfg, quit <-chan struct{}, logger log.Logger) (err error) {
	current := cfg.miner.MiningBlock
	txPoolLocals := []libcommon.Address{} //txPoolV2 has no concept of local addresses (yet?)
	coinbase := cfg.miner.MiningConfig.Etherbase

	const (
		// staleThreshold is the maximum depth of the acceptable stale block.
		staleThreshold = 7
	)

	logPrefix := s.LogPrefix()
	executionAt, err := s.ExecutionAt(txc.Tx)
	if err != nil {
		return fmt.Errorf("getting last executed block: %w", err)
	}
	parent := rawdb.ReadHeaderByNumber(txc.Tx, executionAt)
	if parent == nil { // todo: how to return error and don't stop Erigon?
		return fmt.Errorf("empty block %d", executionAt)
	}

	if cfg.blockBuilderParameters != nil && cfg.blockBuilderParameters.ParentHash != parent.Hash() {
		return fmt.Errorf("wrong head block: %x (current) vs %x (requested)", parent.Hash(), cfg.blockBuilderParameters.ParentHash)
	}

	if cfg.miner.MiningConfig.Etherbase == (libcommon.Address{}) {
		if cfg.blockBuilderParameters == nil {
			return errors.New("refusing to mine without etherbase")
		}
		// If we do not have an etherbase, let's use the suggested one
		coinbase = cfg.blockBuilderParameters.SuggestedFeeRecipient
	}

	blockNum := executionAt + 1

	localUncles, remoteUncles, err := readNonCanonicalHeaders(txc.Tx, blockNum, cfg.engine, coinbase, txPoolLocals)
	if err != nil {
		return err
	}
	chain := ChainReader{Cfg: cfg.chainConfig, Db: txc.Tx, BlockReader: cfg.blockReader, Logger: logger}
	var GetBlocksFromHash = func(hash libcommon.Hash, n int) (blocks []*types.Block) {
		number, _ := cfg.blockReader.HeaderNumber(context.Background(), txc.Tx, hash)
		if number == nil {
			return nil
		}
		for i := 0; i < n; i++ {
			block, _, _ := cfg.blockReader.BlockWithSenders(context.Background(), txc.Tx, hash, *number)
			if block == nil {
				break
			}
			blocks = append(blocks, block)
			hash = block.ParentHash()
			*number--
		}
		return
	}

	// re-written miner/worker.go:commitNewWork
	var timestamp uint64
	if cfg.blockBuilderParameters == nil {
		timestamp = uint64(time.Now().Unix())
		if parent.Time >= timestamp {
			timestamp = parent.Time + 1
		}
	} else {
		// If we are on proof-of-stake timestamp should be already set for us
		timestamp = cfg.blockBuilderParameters.Timestamp
	}

	type envT struct {
		signer    *types.Signer
		ancestors mapset.Set[libcommon.Hash] // ancestor set (used for checking uncle parent validity)
		family    mapset.Set[libcommon.Hash] // family set (used for checking uncle invalidity)
		uncles    mapset.Set[libcommon.Hash] // uncle set
	}
	env := &envT{
		signer:    types.MakeSigner(&cfg.chainConfig, blockNum, timestamp),
		ancestors: mapset.NewSet[libcommon.Hash](),
		family:    mapset.NewSet[libcommon.Hash](),
		uncles:    mapset.NewSet[libcommon.Hash](),
	}
	var header *types.Header
	useEip7783 := cfg.miner.MiningConfig.GasLimit == 0
	if useEip7783 {
		gasLimit := misc.CalcGasLimitEIP7783(
			parent.Number.Uint64()+1,
			cfg.miner.MiningConfig.EIP7783BlockNumStart,
			cfg.miner.MiningConfig.EIP7783InitialGas,
			cfg.miner.MiningConfig.Eip7783IncreaseRate,
			cfg.miner.MiningConfig.EIP7783GasLimitCap,
		)
		header = core.MakeEmptyHeader(parent, &cfg.chainConfig, timestamp, &gasLimit)
	} else {
		header = core.MakeEmptyHeader(parent, &cfg.chainConfig, timestamp, &cfg.miner.MiningConfig.GasLimit)
	}

	if err := misc.VerifyGaslimit(parent.GasLimit, header.GasLimit); err != nil {
		logger.Warn("Failed to verify gas limit given by the validator, defaulting to parent gas limit", "err", err)
		header.GasLimit = parent.GasLimit
	}

	header.Coinbase = coinbase
	header.Extra = cfg.miner.MiningConfig.ExtraData

	logger.Info(fmt.Sprintf("[%s] Start mine", logPrefix), "block", executionAt+1, "baseFee", header.BaseFee, "gasLimit", header.GasLimit)
	ibs := state.New(state.NewReaderV3(txc.Doms))

	if err = cfg.engine.Prepare(chain, header, ibs); err != nil {
		logger.Error("Failed to prepare header for mining",
			"err", err,
			"headerNumber", header.Number.Uint64(),
			"headerRoot", header.Root.String(),
			"headerParentHash", header.ParentHash.String(),
			"parentNumber", parent.Number.Uint64(),
			"parentHash", parent.Hash().String(),
			"callers", debug.Callers(10))
		return err
	}

	if cfg.blockBuilderParameters != nil {
		header.MixDigest = cfg.blockBuilderParameters.PrevRandao
		header.ParentBeaconBlockRoot = cfg.blockBuilderParameters.ParentBeaconBlockRoot

		current.ParentHeaderTime = parent.Time
		current.Header = header
		current.Uncles = nil
		current.Withdrawals = cfg.blockBuilderParameters.Withdrawals
		return nil
	}

	// If we are care about TheDAO hard-fork check whether to override the extra-data or not
	if daoBlock := cfg.chainConfig.DAOForkBlock; daoBlock != nil {
		// Check whether the block is among the fork extra-override range
		limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
		if header.Number.Cmp(daoBlock) >= 0 && header.Number.Cmp(limit) < 0 {
			header.Extra = libcommon.Copy(params.DAOForkBlockExtra)
		}
	}

	// analog of miner.Worker.updateSnapshot
	var makeUncles = func(proposedUncles mapset.Set[libcommon.Hash]) []*types.Header {
		var uncles []*types.Header
		proposedUncles.Each(func(hash libcommon.Hash) bool {
			uncle, exist := localUncles[hash]
			if !exist {
				uncle, exist = remoteUncles[hash]
			}
			if !exist {
				return false
			}
			uncles = append(uncles, uncle)
			return false
		})
		return uncles
	}

	// when 08 is processed ancestors contain 07 (quick block)
	for _, ancestor := range GetBlocksFromHash(parent.Hash(), 7) {
		for _, uncle := range ancestor.Uncles() {
			env.family.Add(uncle.Hash())
		}
		env.family.Add(ancestor.Hash())
		env.ancestors.Add(ancestor.Hash())
	}
	commitUncle := func(env *envT, uncle *types.Header) error {
		hash := uncle.Hash()
		if env.uncles.Contains(hash) {
			return errors.New("uncle not unique")
		}
		if parent.Hash() == uncle.ParentHash {
			return errors.New("uncle is sibling")
		}
		if !env.ancestors.Contains(uncle.ParentHash) {
			return errors.New("uncle's parent unknown")
		}
		if env.family.Contains(hash) {
			return errors.New("uncle already included")
		}
		env.uncles.Add(uncle.Hash())
		return nil
	}
	// Accumulate the miningUncles for the env block
	// Prefer to locally generated uncle
	uncles := make([]*types.Header, 0, 2)
	for _, blocks := range []map[libcommon.Hash]*types.Header{localUncles, remoteUncles} {
		// Clean up stale uncle blocks first
		for hash, uncle := range blocks {
			if uncle.Number.Uint64()+staleThreshold <= header.Number.Uint64() {
				delete(blocks, hash)
			}
		}
		for hash, uncle := range blocks {
			if len(uncles) == 2 {
				break
			}
			if err = commitUncle(env, uncle); err != nil {
				logger.Trace("Possible uncle rejected", "hash", hash, "reason", err)
			} else {
				logger.Trace("Committing new uncle to block", "hash", hash)
				uncles = append(uncles, uncle)
			}
		}
	}

	current.Header = header
	current.Uncles = makeUncles(env.uncles)
	current.Withdrawals = nil
	return nil
}

func readNonCanonicalHeaders(tx kv.Tx, blockNum uint64, engine consensus.Engine, coinbase libcommon.Address, txPoolLocals []libcommon.Address) (localUncles, remoteUncles map[libcommon.Hash]*types.Header, err error) {
	localUncles, remoteUncles = map[libcommon.Hash]*types.Header{}, map[libcommon.Hash]*types.Header{}
	nonCanonicalBlocks, err := rawdb.ReadHeadersByNumber(tx, blockNum)
	if err != nil {
		return
	}
	for _, u := range nonCanonicalBlocks {
		if ethutils.IsLocalBlock(engine, coinbase, txPoolLocals, u) {
			localUncles[u.Hash()] = u
		} else {
			remoteUncles[u.Hash()] = u
		}

	}
	return
}
