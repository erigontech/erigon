package stagedsync

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/misc"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

type miningBlock struct {
	header   *types.Header
	uncles   []*types.Header
	txs      []*types.Transaction
	receipts types.Receipts
}

// SpawnMiningCreateBlockStage
//TODO:
// - from uncles we nned only their hashes and numbers, but whole types.Block object received
// - interrupt - variable is not implemented, see miner/worker.go:798
// - resubmitAdjustCh - variable is not implemented
func SpawnMiningCreateBlockStage(s *StageState, tx ethdb.Database, current *miningBlock, chainConfig *params.ChainConfig, engine consensus.Engine, extra hexutil.Bytes, gasFloor, gasCeil uint64, coinbase common.Address, txPoolLocals []common.Address, quit <-chan struct{}) error {
	const (
		// staleThreshold is the maximum depth of the acceptable stale block.
		staleThreshold = 7
	)

	if coinbase == (common.Address{}) {
		return fmt.Errorf("refusing to mine without etherbase")
	}

	logPrefix := s.state.LogPrefix()
	executionAt, err := s.ExecutionAt(tx)
	if err != nil {
		return fmt.Errorf("%s: getting last executed block: %w", logPrefix, err)
	}
	parent := rawdb.ReadHeaderByNumber(tx, executionAt)
	if parent == nil { // todo: how to return error and don't stop TG?
		return fmt.Errorf(fmt.Sprintf("[%s] Empty block", logPrefix), "blocknum", executionAt)
	}

	blockNum := executionAt + 1

	localUncles := map[common.Hash]*types.Block{}
	remoteUncles := map[common.Hash]*types.Block{}
	nonCanonicalBlocks, err := readNonCanonicalBlocks(tx, blockNum)
	if err != nil {
		return err
	}
	for _, u := range nonCanonicalBlocks {
		if eth.IsLocalBlock(engine, coinbase, txPoolLocals, u.Header()) {
			localUncles[u.Hash()] = u
		} else {
			remoteUncles[u.Hash()] = u
		}

	}
	chain := ChainReader{chainConfig, tx}
	var GetBlocksFromHash = func(hash common.Hash, n int) (blocks []*types.Block) {
		number := rawdb.ReadHeaderNumber(tx, hash)
		if number == nil {
			return nil
		}
		for i := 0; i < n; i++ {
			block := chain.GetBlock(hash, *number)
			if block == nil {
				break
			}
			blocks = append(blocks, block)
			hash = block.ParentHash()
			*number--
		}
		return
	}

	batch := tx.NewBatch()
	defer batch.Rollback()

	type envT struct {
		signer    types.Signer
		ancestors mapset.Set // ancestor set (used for checking uncle parent validity)
		family    mapset.Set // family set (used for checking uncle invalidity)
		uncles    mapset.Set // uncle set

		ibs         *state.IntraBlockState // apply state changes here
		stateWriter state.WriterWithChangeSets
	}
	env := &envT{
		signer:      types.NewEIP155Signer(chainConfig.ChainID),
		ancestors:   mapset.NewSet(),
		family:      mapset.NewSet(),
		uncles:      mapset.NewSet(),
		ibs:         state.New(state.NewPlainStateReader(batch)),
		stateWriter: state.NewPlainStateWriter(batch, batch, blockNum),
	}

	// re-written miner/worker.go:commitNewWork
	timestamp := time.Now().Unix()
	if parent.Time >= uint64(timestamp) {
		timestamp = int64(parent.Time + 1)
	}
	num := parent.Number
	header := &types.Header{
		ParentHash: parent.Hash(),
		Number:     num.Add(num, common.Big1),
		GasLimit:   core.CalcGasLimit(parent.GasUsed, parent.GasLimit, gasFloor, gasCeil),
		Extra:      extra,
		Time:       uint64(timestamp),
	}

	// Only set the coinbase if our consensus engine is running (avoid spurious block rewards)
	//if w.isRunning() {
	header.Coinbase = coinbase
	//}

	if err = engine.Prepare(chain, header); err != nil {
		log.Error("Failed to prepare header for mining",
			"err", err,
			"headerNumber", header.Number.Uint64(),
			"headerRoot", header.Root.String(),
			"headerParentHash", header.ParentHash.String(),
			"parentNumber", parent.Number.Uint64(),
			"parentHash", parent.Hash().String(),
			"callers", debug.Callers(10))
		return fmt.Errorf("mining failed")
	}

	// If we are care about TheDAO hard-fork check whether to override the extra-data or not
	if daoBlock := chainConfig.DAOForkBlock; daoBlock != nil {
		// Check whether the block is among the fork extra-override range
		limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
		if header.Number.Cmp(daoBlock) >= 0 && header.Number.Cmp(limit) < 0 {
			// Depending whether we support or oppose the fork, override differently
			if chainConfig.DAOForkSupport {
				header.Extra = common.CopyBytes(params.DAOForkBlockExtra)
			} else if bytes.Equal(header.Extra, params.DAOForkBlockExtra) {
				header.Extra = []byte{} // If miner opposes, don't let it use the reserved extra-data
			}
		}
	}
	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(header.Number) == 0 {
		misc.ApplyDAOHardFork(env.ibs)
	}

	// analog of miner.Worker.updateSnapshot
	var makeUncles = func(proposedUncles mapset.Set) []*types.Header {
		var uncles []*types.Header
		proposedUncles.Each(func(item interface{}) bool {
			hash, ok := item.(common.Hash)
			if !ok {
				return false
			}

			uncle, exist := localUncles[hash]
			if !exist {
				uncle, exist = remoteUncles[hash]
			}
			if !exist {
				return false
			}
			uncles = append(uncles, uncle.Header())
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
	for _, blocks := range []map[common.Hash]*types.Block{localUncles, remoteUncles} {
		// Clean up stale uncle blocks first
		for hash, uncle := range blocks {
			if uncle.NumberU64()+staleThreshold <= header.Number.Uint64() {
				delete(blocks, hash)
			}
		}
		for hash, uncle := range blocks {
			if len(uncles) == 2 {
				break
			}
			if err = commitUncle(env, uncle.Header()); err != nil {
				log.Trace("Possible uncle rejected", "hash", hash, "reason", err)
			} else {
				log.Debug("Committing new uncle to block", "hash", hash)
				uncles = append(uncles, uncle.Header())
			}
		}
	}

	current.header = header
	current.uncles = makeUncles(env.uncles)
	return nil
}

func readNonCanonicalBlocks(tx ethdb.Database, blockNum uint64) ([]*types.Block, error) {
	//todo: implement me
	return nil, nil
}
