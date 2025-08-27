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

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/eth/ethutils"
	"github.com/erigontech/erigon/execution/builder/buildercfg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/consensus/misc"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/turbo/services"
)

type MiningBlock struct {
	ParentHeaderTime uint64
	Header           *types.Header
	Uncles           []*types.Header
	Txns             types.Transactions
	Receipts         types.Receipts
	Withdrawals      []*types.Withdrawal
	PreparedTxns     types.Transactions
	Requests         types.FlatRequests

	headerRlpSize         *int
	withdrawalsRlpSize    *int
	unclesRlpSize         *int
	txnsRlpSize           int
	txnsRlpSizeCalculated int
}

func (mb *MiningBlock) AddTxn(txn types.Transaction) {
	mb.Txns = append(mb.Txns, txn)
	s := txn.EncodingSize()
	s += rlp.ListPrefixLen(s)
	mb.txnsRlpSize += s
	mb.txnsRlpSizeCalculated++
}

func (mb *MiningBlock) AvailableRlpSpace(chainConfig *chain.Config, withAdditional ...types.Transaction) int {
	if mb.headerRlpSize == nil {
		s := mb.Header.EncodingSize()
		s += rlp.ListPrefixLen(s)
		mb.headerRlpSize = &s
	}
	if mb.withdrawalsRlpSize == nil {
		var s int
		if mb.Withdrawals != nil {
			s = types.EncodingSizeGenericList(mb.Withdrawals)
			s += rlp.ListPrefixLen(s)
		}
		mb.withdrawalsRlpSize = &s
	}
	if mb.unclesRlpSize == nil {
		s := types.EncodingSizeGenericList(mb.Uncles)
		s += rlp.ListPrefixLen(s)
		mb.unclesRlpSize = &s
	}

	blockSize := *mb.headerRlpSize
	blockSize += *mb.unclesRlpSize
	blockSize += *mb.withdrawalsRlpSize
	blockSize += mb.TxnsRlpSize(withAdditional...)
	blockSize += rlp.ListPrefixLen(blockSize)
	maxSize := chainConfig.GetMaxRlpBlockSize(mb.Header.Number.Uint64())
	return maxSize - blockSize
}

func (mb *MiningBlock) TxnsRlpSize(withAdditional ...types.Transaction) int {
	if len(mb.PreparedTxns) > 0 {
		s := types.EncodingSizeGenericList(mb.PreparedTxns)
		s += rlp.ListPrefixLen(s)
		return s
	}
	if len(mb.Txns) != mb.txnsRlpSizeCalculated {
		panic("mismatch between mb.Txns and mb.txnsRlpSizeCalculated - did you forget to use mb.AddTxn()?")
	}
	s := mb.txnsRlpSize
	s += types.EncodingSizeGenericList(withAdditional) // what size would be if we add additional txns
	s += rlp.ListPrefixLen(s)
	return s
}

type MiningState struct {
	MiningConfig    *buildercfg.MiningConfig
	PendingResultCh chan *types.Block
	MiningResultCh  chan *types.BlockWithReceipts
	MiningBlock     *MiningBlock
}

func NewMiningState(cfg *buildercfg.MiningConfig) MiningState {
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
	chainConfig            *chain.Config
	engine                 consensus.Engine
	tmpdir                 string
	blockBuilderParameters *core.BlockBuilderParameters
	blockReader            services.FullBlockReader
}

func StageMiningCreateBlockCfg(
	db kv.RwDB,
	miner MiningState,
	chainConfig *chain.Config,
	engine consensus.Engine,
	blockBuilderParameters *core.BlockBuilderParameters,
	tmpdir string,
	blockReader services.FullBlockReader,
) MiningCreateBlockCfg {
	return MiningCreateBlockCfg{
		db:                     db,
		miner:                  miner,
		chainConfig:            chainConfig,
		engine:                 engine,
		tmpdir:                 tmpdir,
		blockBuilderParameters: blockBuilderParameters,
		blockReader:            blockReader,
	}
}

// SpawnMiningCreateBlockStage
// TODO:
// - resubmitAdjustCh - variable is not implemented
func SpawnMiningCreateBlockStage(s *StageState, txc wrap.TxContainer, cfg MiningCreateBlockCfg, quit <-chan struct{}, logger log.Logger) (err error) {
	current := cfg.miner.MiningBlock
	*current = MiningBlock{}          // always start with a clean state
	var txPoolLocals []common.Address //txPoolV2 has no concept of local addresses (yet?)
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

	if cfg.miner.MiningConfig.Etherbase == (common.Address{}) {
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
	var GetBlocksFromHash = func(hash common.Hash, n int) (blocks []*types.Block) {
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
		ancestors mapset.Set[common.Hash] // ancestor set (used for checking uncle parent validity)
		family    mapset.Set[common.Hash] // family set (used for checking uncle invalidity)
		uncles    mapset.Set[common.Hash] // uncle set
	}
	env := &envT{
		signer:    types.MakeSigner(cfg.chainConfig, blockNum, timestamp),
		ancestors: mapset.NewSet[common.Hash](),
		family:    mapset.NewSet[common.Hash](),
		uncles:    mapset.NewSet[common.Hash](),
	}

	header := core.MakeEmptyHeader(parent, cfg.chainConfig, timestamp, cfg.miner.MiningConfig.GasLimit)
	if err := misc.VerifyGaslimit(parent.GasLimit, header.GasLimit); err != nil {
		logger.Warn("Failed to verify gas limit given by the validator, defaulting to parent gas limit", "err", err)
		header.GasLimit = parent.GasLimit
	}

	header.Coinbase = coinbase
	header.Extra = cfg.miner.MiningConfig.ExtraData

	logger.Info(fmt.Sprintf("[%s] Start mine", logPrefix), "block", executionAt+1, "baseFee", header.BaseFee, "gasLimit", header.GasLimit)
	ibs := state.New(state.NewReaderV3(txc.Doms.AsGetter(txc.Tx)))

	if err = cfg.engine.Prepare(chain, header, ibs); err != nil {
		logger.Error("Failed to prepare header for mining",
			"err", err,
			"headerNumber", header.Number.Uint64(),
			"headerRoot", header.Root.String(),
			"headerParentHash", header.ParentHash.String(),
			"parentNumber", parent.Number.Uint64(),
			"parentHash", parent.Hash().String(),
			"stack", dbg.Stack())
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
		limit := new(big.Int).Add(daoBlock, misc.DAOForkExtraRange)
		if header.Number.Cmp(daoBlock) >= 0 && header.Number.Cmp(limit) < 0 {
			header.Extra = common.Copy(misc.DAOForkBlockExtra)
		}
	}

	// analog of miner.Worker.updateSnapshot
	var makeUncles = func(proposedUncles mapset.Set[common.Hash]) []*types.Header {
		var uncles []*types.Header
		proposedUncles.Each(func(hash common.Hash) bool {
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
	for _, blocks := range []map[common.Hash]*types.Header{localUncles, remoteUncles} {
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

func readNonCanonicalHeaders(tx kv.Tx, blockNum uint64, engine consensus.Engine, coinbase common.Address, txPoolLocals []common.Address) (localUncles, remoteUncles map[common.Hash]*types.Header, err error) {
	localUncles, remoteUncles = map[common.Hash]*types.Header{}, map[common.Hash]*types.Header{}
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
