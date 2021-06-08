package stagedsync

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethutils"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/params"
)

type miningBlock struct {
	Header   *types.Header
	Uncles   []*types.Header
	Txs      []types.Transaction
	Receipts types.Receipts

	LocalTxs  types.TransactionsStream
	RemoteTxs types.TransactionsStream
}

type MiningCreateBlockCfg struct {
	db          ethdb.RwKV
	mining      params.MiningConfig
	chainConfig params.ChainConfig
	engine      consensus.Engine
	txPool      *core.TxPool
	tmpdir      string
}

func StageMiningCreateBlockCfg(
	db ethdb.RwKV,
	mining params.MiningConfig,
	chainConfig params.ChainConfig,
	engine consensus.Engine,
	txPool *core.TxPool,
	tmpdir string,
) MiningCreateBlockCfg {
	return MiningCreateBlockCfg{
		db:          db,
		mining:      mining,
		chainConfig: chainConfig,
		engine:      engine,
		txPool:      txPool,
		tmpdir:      tmpdir,
	}
}

// SpawnMiningCreateBlockStage
//TODO:
// - resubmitAdjustCh - variable is not implemented
func SpawnMiningCreateBlockStage(s *StageState, tx ethdb.RwTx, cfg MiningCreateBlockCfg, current *miningBlock, quit <-chan struct{}) error {
	txPoolLocals := cfg.txPool.Locals()
	pendingTxs, err := cfg.txPool.Pending()
	if err != nil {
		return err
	}

	const (
		// staleThreshold is the maximum depth of the acceptable stale block.
		staleThreshold = 7
	)

	if cfg.mining.Etherbase == (common.Address{}) {
		return fmt.Errorf("refusing to mine without etherbase")
	}

	logPrefix := s.state.LogPrefix()
	executionAt, err := s.ExecutionAt(tx)
	if err != nil {
		return fmt.Errorf("%s: getting last executed block: %w", logPrefix, err)
	}
	parent := rawdb.ReadHeaderByNumber(tx, executionAt)
	if parent == nil { // todo: how to return error and don't stop Erigon?
		return fmt.Errorf(fmt.Sprintf("[%s] Empty block", logPrefix), "blocknum", executionAt)
	}

	blockNum := executionAt + 1
	signer := types.MakeSigner(&cfg.chainConfig, blockNum)

	localUncles, remoteUncles, err := readNonCanonicalHeaders(tx, blockNum, cfg.engine, cfg.mining.Etherbase, txPoolLocals)
	if err != nil {
		return err
	}
	chain := ChainReader{Cfg: cfg.chainConfig, Db: ethdb.WrapIntoTxDB(tx)}
	var GetBlocksFromHash = func(hash common.Hash, n int) (blocks []*types.Block) {
		number := rawdb.ReadHeaderNumber(tx, hash)
		if number == nil {
			return nil
		}
		for i := 0; i < n; i++ {
			block := rawdb.ReadBlock(tx, hash, *number)
			if block == nil {
				break
			}
			blocks = append(blocks, block)
			hash = block.ParentHash()
			*number--
		}
		return
	}

	type envT struct {
		signer    *types.Signer
		ancestors mapset.Set // ancestor set (used for checking uncle parent validity)
		family    mapset.Set // family set (used for checking uncle invalidity)
		uncles    mapset.Set // uncle set
	}
	env := &envT{
		signer:    types.MakeSigner(&cfg.chainConfig, blockNum),
		ancestors: mapset.NewSet(),
		family:    mapset.NewSet(),
		uncles:    mapset.NewSet(),
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
		GasLimit:   core.CalcGasLimit(parent.GasUsed, parent.GasLimit, cfg.mining.GasFloor, cfg.mining.GasCeil),
		Extra:      cfg.mining.ExtraData,
		Time:       uint64(timestamp),
	}

	// Only set the coinbase if our consensus engine is running (avoid spurious block rewards)
	//if w.isRunning() {
	header.Coinbase = cfg.mining.Etherbase
	//}

	if err = cfg.engine.Prepare(chain, header); err != nil {
		log.Error("Failed to prepare header for mining",
			"err", err,
			"headerNumber", header.Number.Uint64(),
			"headerRoot", header.Root.String(),
			"headerParentHash", header.ParentHash.String(),
			"parentNumber", parent.Number.Uint64(),
			"parentHash", parent.Hash().String(),
			"callers", debug.Callers(10))
		return fmt.Errorf("[%s] mining failed", logPrefix)
	}

	// If we are care about TheDAO hard-fork check whether to override the extra-data or not
	if daoBlock := cfg.chainConfig.DAOForkBlock; daoBlock != nil {
		// Check whether the block is among the fork extra-override range
		limit := new(big.Int).Add(daoBlock, params.DAOForkExtraRange)
		if header.Number.Cmp(daoBlock) >= 0 && header.Number.Cmp(limit) < 0 {
			// Depending whether we support or oppose the fork, override differently
			if cfg.chainConfig.DAOForkSupport {
				header.Extra = common.CopyBytes(params.DAOForkBlockExtra)
			} else if bytes.Equal(header.Extra, params.DAOForkBlockExtra) {
				header.Extra = []byte{} // If miner opposes, don't let it use the reserved extra-data
			}
		}
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
				log.Trace("Possible uncle rejected", "hash", hash, "reason", err)
			} else {
				log.Debug("Committing new uncle to block", "hash", hash)
				uncles = append(uncles, uncle)
			}
		}
	}

	current.Header = header
	current.Uncles = makeUncles(env.uncles)

	// Split the pending transactions into locals and remotes
	localTxs, remoteTxs := types.TransactionsGroupedBySender{}, types.TransactionsGroupedBySender{}
	for _, txs := range pendingTxs {
		if len(txs) == 0 {
			continue
		}
		from, _ := txs[0].Sender(*signer)
		isLocal := false
		for _, local := range txPoolLocals {
			if local == from {
				isLocal = true
				break
			}
		}

		if isLocal {
			localTxs = append(localTxs, txs)
		} else {
			remoteTxs = append(remoteTxs, txs)
		}
	}

	current.LocalTxs = types.NewTransactionsByPriceAndNonce(*signer, localTxs)
	current.RemoteTxs = types.NewTransactionsByPriceAndNonce(*signer, remoteTxs)
	s.Done()
	return nil
}

func readNonCanonicalHeaders(tx ethdb.Tx, blockNum uint64, engine consensus.Engine, coinbase common.Address, txPoolLocals []common.Address) (localUncles, remoteUncles map[common.Hash]*types.Header, err error) {
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
