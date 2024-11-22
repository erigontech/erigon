package stagedsync

import (
	"bytes"
	"context"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/membatchwithdb"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/wrap"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/ethdb/prune"
	"github.com/erigontech/erigon/turbo/rpchelper"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/trie"
)

type WitnessCfg struct {
	db                      kv.RwDB
	enableWitnessGeneration bool
	maxWitnessLimit         uint64
	chainConfig             *chain.Config
	engine                  consensus.Engine
	blockReader             services.FullBlockReader
	dirs                    datadir.Dirs
}

type WitnessStore struct {
	Tds             *state.TrieDbState
	TrieStateWriter *state.TrieStateWriter
	Statedb         *state.IntraBlockState
	ChainReader     *ChainReaderImpl
	GetHashFn       func(n uint64) libcommon.Hash
}

func StageWitnessCfg(enableWitnessGeneration bool, maxWitnessLimit uint64, chainConfig *chain.Config, engine consensus.Engine, blockReader services.FullBlockReader, dirs datadir.Dirs) WitnessCfg {
	return WitnessCfg{
		enableWitnessGeneration: enableWitnessGeneration,
		maxWitnessLimit:         maxWitnessLimit,
		chainConfig:             chainConfig,
		engine:                  engine,
		blockReader:             blockReader,
		dirs:                    dirs,
	}
}

// PrepareForWitness abstracts the process of initialising bunch of necessary things required for witness
// generation and puts them in a WitnessStore.
func PrepareForWitness(tx kv.Tx, block *types.Block, prevRoot libcommon.Hash, cfg *WitnessCfg, ctx context.Context, logger log.Logger) (*WitnessStore, error) {
	blockNr := block.NumberU64()
	txNumsReader := rawdbv3.TxNums
	reader, err := rpchelper.CreateHistoryStateReader(tx, txNumsReader, blockNr, 0, cfg.chainConfig.ChainName)
	// stateReader, err := rpchelper.CreateHistoryStateReader(roTx, txNumsReader, blockNr, 0, "")
	if err != nil {
		return nil, err
	}

	tds := state.NewTrieDbState(prevRoot, blockNr-1, reader)
	tds.SetResolveReads(true)

	tds.StartNewBuffer()
	trieStateWriter := tds.TrieStateWriter()

	statedb := state.New(tds)
	statedb.SetDisableBalanceInc(true)

	chainReader := NewChainReaderImpl(cfg.chainConfig, tx, cfg.blockReader, logger)

	getHeader := func(hash libcommon.Hash, number uint64) *types.Header {
		h, e := cfg.blockReader.Header(ctx, tx, hash, number)
		if e != nil {
			log.Error("getHeader error", "number", number, "hash", hash, "err", e)
		}
		return h
	}
	getHashFn := core.GetHashFn(block.Header(), getHeader)

	return &WitnessStore{
		Tds:             tds,
		TrieStateWriter: trieStateWriter,
		Statedb:         statedb,
		ChainReader:     chainReader,
		GetHashFn:       getHashFn,
	}, nil
}

// RewindStagesForWitness rewinds the Execution stage to previous block.
func RewindStagesForWitness(batch *membatchwithdb.MemoryMutation, blockNr, latestBlockNr uint64, cfg *WitnessCfg, regenerateHash bool, ctx context.Context, logger log.Logger) error {
	// Rewind the Execution stage to previous block
	unwindState := &UnwindState{ID: stages.Execution, UnwindPoint: blockNr - 1, CurrentBlockNumber: latestBlockNr}
	stageState := &StageState{ID: stages.Execution, BlockNumber: blockNr}

	txc := wrap.TxContainer{Tx: batch}
	batchSizeStr := "512M"
	var batchSize datasize.ByteSize
	err := batchSize.UnmarshalText([]byte(batchSizeStr))
	if err != nil {
		return err
	}

	pruneMode := prune.Mode{
		Initialised: false,
	}
	vmConfig := &vm.Config{}
	dirs := cfg.dirs
	blockReader := cfg.blockReader
	syncCfg := ethconfig.Defaults.Sync
	execCfg := StageExecuteBlocksCfg(batch.MemDB(), pruneMode, batchSize, cfg.chainConfig, cfg.engine, vmConfig, nil,
		/*stateStream=*/ false,
		/*badBlockHalt=*/ true, dirs, blockReader, nil, nil, syncCfg, nil)

	if err := UnwindExecutionStage(unwindState, stageState, txc, ctx, execCfg, logger); err != nil {
		return err
	}

	return nil
}

func ExecuteBlockStatelessly(block *types.Block, prevHeader *types.Header, chainReader consensus.ChainReader, tds *state.TrieDbState, cfg *WitnessCfg, buf *bytes.Buffer, getHashFn func(n uint64) libcommon.Hash, logger log.Logger) (libcommon.Hash, error) {
	blockNr := block.NumberU64()
	nw, err := trie.NewWitnessFromReader(bytes.NewReader(buf.Bytes()), false)
	if err != nil {
		return libcommon.Hash{}, err
	}

	statelessIbs, err := state.NewStateless(prevHeader.Root, nw, blockNr-1, true /* trace */, false /* is binary */)
	if err != nil {
		return libcommon.Hash{}, err
	}
	execResult, err := core.ExecuteBlockEphemerally(cfg.chainConfig, &vm.Config{}, getHashFn, cfg.engine, block, statelessIbs, statelessIbs, chainReader, nil, logger)
	if err != nil {
		return libcommon.Hash{}, err
	}
	_ = execResult
	return statelessIbs.Finalize(), nil
}

// TODO: Implement
func UnwindWitnessStage() error {
	return nil
}

// TODO: Implement
func PruneWitnessStage() error {
	return nil
}

func UnwindIntermediateHashes(logPrefix string, rl *trie.RetainList, u *UnwindState, s *StageState, db kv.RwTx, cfg TrieCfg, quit <-chan struct{}, logger log.Logger) error {
	// p := NewHashPromoter(db, cfg.tmpDir, quit, logPrefix, logger)
	// collect := func(k, v []byte) {
	// 	rl.AddKeyWithMarker(k, len(v) == 0)
	// }
	// if err := p.UnwindOnHistoryV3(logPrefix, s.BlockNumber, u.UnwindPoint, false, collect); err != nil {
	// 	return err
	// }
	// if err := p.UnwindOnHistoryV3(logPrefix, s.BlockNumber, u.UnwindPoint, true, collect); err != nil {
	// 	return err
	// }
	return nil
}
