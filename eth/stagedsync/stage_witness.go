package stagedsync

import (
	"bytes"
	"context"
	"fmt"

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
	"github.com/holiman/uint256"
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

func StageWitnessCfg(db kv.RwDB, enableWitnessGeneration bool, maxWitnessLimit uint64, chainConfig *chain.Config, engine consensus.Engine, blockReader services.FullBlockReader, dirs datadir.Dirs) WitnessCfg {
	return WitnessCfg{
		db:                      db,
		enableWitnessGeneration: enableWitnessGeneration,
		maxWitnessLimit:         maxWitnessLimit,
		chainConfig:             chainConfig,
		engine:                  engine,
		blockReader:             blockReader,
		dirs:                    dirs,
	}
}

func SpawnWitnessStage(s *StageState, rootTx kv.RwTx, cfg WitnessCfg, ctx context.Context, logger log.Logger) error {
	if !cfg.enableWitnessGeneration {
		logger.Debug(fmt.Sprintf("[%s] Skipping Witness Generation", s.LogPrefix()))
		return nil
	}

	useExternalTx := rootTx != nil
	if !useExternalTx {
		var err error
		rootTx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer rootTx.Rollback()
	}

	// We'll need to use `rootTx` to write witness. As during rewind
	// the tx is updated to an in-memory batch, we'll operate on the copy
	// to keep the `rootTx` as it is.
	tx := rootTx

	logPrefix := s.LogPrefix()
	execStageBlock, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	lastWitnessBlock := s.BlockNumber
	if lastWitnessBlock >= execStageBlock {
		// We already did witness generation for this block
		return nil
	}

	var from, to uint64

	// Skip witness generation for past blocks. This can happen when we're upgrading
	// the node to this version having witness stage or when witness stage is disabled
	// and then enabled again.
	if lastWitnessBlock == 0 {
		s.Update(tx, execStageBlock-1)
		return nil
	}

	// We'll generate witness for all blocks from `lastWitnessBlock+1` until `execStageBlock - 1`
	to = execStageBlock - 1
	from = lastWitnessBlock + 1
	if to <= 0 {
		return nil
	}

	// We only need to store last `maxWitnessLimit` witnesses. As during sync, we
	// can do batch imports, trim down the blocks until this limit.
	if to-from+1 > cfg.maxWitnessLimit {
		from = to - cfg.maxWitnessLimit + 1
	}

	batch := membatchwithdb.NewMemoryBatch(tx, "", logger)
	defer batch.Rollback()

	logger.Info(fmt.Sprintf("[%s] Witness Generation", logPrefix), "from", from, "to", to)

	for blockNr := from; blockNr <= to; blockNr++ {
		tx := rootTx

		block, err := cfg.blockReader.BlockByNumber(ctx, tx, blockNr)
		if err != nil {
			return err
		}
		if block == nil {
			return fmt.Errorf("block %d not found while generating witness", blockNr)
		}

		prevHeader, err := cfg.blockReader.HeaderByNumber(ctx, tx, blockNr-1)
		if err != nil {
			return err
		}

		err = RewindStagesForWitness(batch, blockNr, blockNr, &cfg, false, ctx, logger)
		if err != nil {
			return err
		}

		// Update the tx to operate on the in-memory batch
		tx = batch.MemTx()

		store, err := PrepareForWitness(tx, block, prevHeader.Root, &cfg, ctx, logger)
		if err != nil {
			return err
		}

		w, txTds, err := GenerateWitness(tx, block, prevHeader, true, 0, store.Tds, store.TrieStateWriter, store.Statedb, store.GetHashFn, &cfg, false, ctx, logger)
		if err != nil {
			return err
		}
		if w == nil {
			return fmt.Errorf("unable to generate witness for block %d", blockNr)
		}

		var buf bytes.Buffer
		_, err = w.WriteInto(&buf)
		if err != nil {
			return err
		}

		_, err = VerifyWitness(tx, block, prevHeader, true, 0, store.ChainReader, store.Tds, txTds, store.GetHashFn, &cfg, &buf, logger)
		if err != nil {
			return fmt.Errorf("error verifying witness for block %d: %v", blockNr, err)
		}

		// Check if we already have a witness for the same block (can happen during a reorg)
		exist, _ := HasWitness(rootTx, kv.Witnesses, Uint64ToBytes(blockNr))
		if exist {
			logger.Debug("Deleting witness chunks for existing block", "block", blockNr)
			err = DeleteChunks(rootTx, kv.Witnesses, Uint64ToBytes(blockNr))
			if err != nil {
				return fmt.Errorf("error deletig witness for block %d: %v", blockNr, err)
			}
		}

		// Write the witness buffer against the corresponding block number
		logger.Debug("Writing witness to db", "block", blockNr)
		err = WriteChunks(rootTx, kv.Witnesses, Uint64ToBytes(blockNr), buf.Bytes())
		if err != nil {
			return fmt.Errorf("error writing witness for block %d: %v", blockNr, err)
		}

		// Update the stage with the latest block number
		s.Update(rootTx, blockNr)

		// If we're overlimit, delete oldest witness
		oldestWitnessBlock, _ := FindOldestWitness(rootTx, kv.Witnesses)
		if blockNr-oldestWitnessBlock+1 > cfg.maxWitnessLimit {
			// If the user reduces `witness.limit`, we'll need to delete witnesses more than just oldest
			deleteFrom := oldestWitnessBlock
			deleteTo := blockNr - cfg.maxWitnessLimit
			logger.Debug("Reached max witness limit, deleting oldest witness", "from", deleteFrom, "to", deleteTo)
			for i := deleteFrom; i <= deleteTo; i++ {
				err = DeleteChunks(rootTx, kv.Witnesses, Uint64ToBytes(i))
				if err != nil {
					return fmt.Errorf("error deleting witness for block %d: %v", i, err)
				}
			}
		}

		logger.Info(fmt.Sprintf("[%s] Generated witness", logPrefix), "block", blockNr, "len", len(buf.Bytes()))
	}

	logger.Info(fmt.Sprintf("[%s] Done Witness Generation", logPrefix), "until", to)

	return nil
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

	tds := state.NewTrieDbState(prevRoot, tx, blockNr-1, reader)
	tds.SetResolveReads(true)

	tds.StartNewBuffer()
	trieStateWriter := tds.TrieStateWriter()

	statedb := state.New(tds)
	statedb.SetDisableBalanceInc(true)

	chainReader := NewChainReaderImpl(cfg.chainConfig, tx, cfg.blockReader, logger)
	// if err := core.InitializeBlockExecution(cfg.engine, chainReader, block.Header(), cfg.chainConfig, statedb, trieStateWriter, logger, nil); err != nil {
	// 	return nil, err
	// }

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
	execCfg := StageExecuteBlocksCfg(cfg.db, pruneMode, batchSize, cfg.chainConfig, cfg.engine, vmConfig, nil,
		/*stateStream=*/ false,
		/*badBlockHalt=*/ true, dirs, blockReader, nil, nil, syncCfg, nil)

	if err := UnwindExecutionStage(unwindState, stageState, txc, ctx, execCfg, logger); err != nil {
		return err
	}

	return nil
}

// GenerateWitness does the core witness generation part by re-executing transactions in the block. It
// assumes that the 'HashState' and 'IntermediateHashes' stages are already rewinded to the previous block.
func GenerateWitness(tx kv.Tx, block *types.Block, prevHeader *types.Header, fullBlock bool, txIndex uint64, tds *state.TrieDbState, trieStateWriter *state.TrieStateWriter, statedb *state.IntraBlockState, getHashFn func(n uint64) libcommon.Hash, cfg *WitnessCfg, regenerateHash bool, ctx context.Context, logger log.Logger) (*trie.Witness, *state.TrieDbState, error) {
	blockNr := block.NumberU64()
	usedGas := new(uint64)
	usedBlobGas := new(uint64)
	gp := new(core.GasPool).AddGas(block.GasLimit()).AddBlobGas(cfg.chainConfig.GetMaxBlobGasPerBlock())
	var receipts types.Receipts

	if len(block.Transactions()) == 0 {
		statedb.GetBalance(libcommon.HexToAddress("0x1234"))
	}

	vmConfig := vm.Config{}

	loadFunc := func(loader *trie.SubTrieLoader, rl *trie.RetainList, dbPrefixes [][]byte, fixedbits []int, accountNibbles [][]byte) (trie.SubTries, error) {
		rl.Rewind()
		receiver := trie.NewSubTrieAggregator(nil, nil, false)
		receiver.SetRetainList(rl)
		pr := trie.NewMultiAccountProofRetainer(rl)
		pr.AccHexKeys = accountNibbles
		receiver.SetProofRetainer(pr)

		loaderRl := rl
		if regenerateHash {
			loaderRl = trie.NewRetainList(0)
		}
		subTrieloader := trie.NewFlatDBTrieLoader[trie.SubTries]("eth_getWitness", loaderRl, nil, nil, false, receiver)
		subTries, err := subTrieloader.Result(tx, nil)

		rl.Rewind()

		if err != nil {
			return receiver.EmptyResult(), err
		}

		err = trie.AttachRequestedCode(tx, loader.CodeRequests())

		if err != nil {
			return receiver.EmptyResult(), err
		}

		// Reverse the subTries.Hashes and subTries.roots
		for i, j := 0, len(subTries.Hashes)-1; i < j; i, j = i+1, j-1 {
			subTries.Hashes[i], subTries.Hashes[j] = subTries.Hashes[j], subTries.Hashes[i]
			subTries.Roots()[i], subTries.Roots()[j] = subTries.Roots()[j], subTries.Roots()[i]
		}

		return subTries, nil
	}

	var txTds *state.TrieDbState

	for i, txn := range block.Transactions() {
		statedb.SetTxContext(txn.Hash(), i)

		// Ensure that the access list is loaded into witness
		for _, a := range txn.GetAccessList() {
			statedb.GetBalance(a.Address)

			for _, k := range a.StorageKeys {
				v := uint256.NewInt(0)
				statedb.GetState(a.Address, &k, v)
			}
		}

		receipt, _, err := core.ApplyTransaction(cfg.chainConfig, getHashFn, cfg.engine, nil, gp, statedb, trieStateWriter, block.Header(), txn, usedGas, usedBlobGas, vmConfig)
		if err != nil {
			return nil, nil, err
		}

		if !fullBlock && i == int(txIndex) {
			txTds = tds.WithLastBuffer()
			break
		}

		if !cfg.chainConfig.IsByzantium(block.NumberU64()) || (!fullBlock && i+1 == int(txIndex)) {
			tds.StartNewBuffer()
		}

		receipts = append(receipts, receipt)
	}

	if fullBlock {
		if _, _, _, err := cfg.engine.FinalizeAndAssemble(cfg.chainConfig, block.Header(), statedb, block.Transactions(), block.Uncles(), receipts, block.Withdrawals(), nil, nil, nil, nil, logger); err != nil {
			fmt.Printf("Finalize of block %d failed: %v\n", blockNr, err)
			return nil, nil, err
		}

		statedb.FinalizeTx(cfg.chainConfig.Rules(block.NumberU64(), block.Header().Time), trieStateWriter)
	}

	triePreroot := tds.LastRoot()

	if fullBlock && !bytes.Equal(prevHeader.Root[:], triePreroot[:]) {
		return nil, nil, fmt.Errorf("mismatch in expected state root computed %v vs %v indicates bug in witness implementation", prevHeader.Root, triePreroot)
	}

	if err := tds.ResolveStateTrieWithFunc(loadFunc); err != nil {
		return nil, nil, err
	}

	w, err := tds.ExtractWitness(false, false)
	if err != nil {
		return nil, nil, err
	}

	return w, txTds, nil
}

func ExecuteBlockStatelessly(block *types.Block, prevHeader *types.Header, chainReader consensus.ChainReader, tds *state.TrieDbState, cfg *WitnessCfg, buf *bytes.Buffer, getHashFn func(n uint64) libcommon.Hash, logger log.Logger) (libcommon.Hash, error) {
	blockNr := block.NumberU64()
	nw, err := trie.NewWitnessFromReader(bytes.NewReader(buf.Bytes()), false)
	if err != nil {
		return libcommon.Hash{}, err
	}

	statelessWriter, err := state.NewStateless(prevHeader.Root, nw, blockNr-1, false, false /* is binary */)
	execResult, err := core.ExecuteBlockEphemerally(cfg.chainConfig, &vm.Config{}, getHashFn, cfg.engine, block, tds, statelessWriter, chainReader, nil, logger)
	if err != nil {
		return libcommon.Hash{}, err
	}
	_ = execResult
	return statelessWriter.Finalize(), nil
}

// VerifyWitness verifies if the correct state trie can be re-generated by the witness (prepared earlier).
func VerifyWitness(tx kv.Tx, block *types.Block, prevHeader *types.Header, fullBlock bool, txIndex uint64, chainReader *ChainReaderImpl, tds *state.TrieDbState, txTds *state.TrieDbState, getHashFn func(n uint64) libcommon.Hash, cfg *WitnessCfg, buf *bytes.Buffer, logger log.Logger) (*bytes.Buffer, error) {
	blockNr := block.NumberU64()
	nw, err := trie.NewWitnessFromReader(bytes.NewReader(buf.Bytes()), false)
	if err != nil {
		return nil, err
	}

	s, err := state.NewStateless(prevHeader.Root, nw, blockNr-1, false, false /* is binary */)
	if err != nil {
		return nil, err
	}
	ibs := state.New(s)
	s.SetBlockNr(blockNr)

	gp := new(core.GasPool).AddGas(block.GasLimit()).AddBlobGas(cfg.chainConfig.GetMaxBlobGasPerBlock())
	usedGas := new(uint64)
	usedBlobGas := new(uint64)
	receipts := types.Receipts{}
	vmConfig := vm.Config{}

	if err := core.InitializeBlockExecution(cfg.engine, chainReader, block.Header(), cfg.chainConfig, ibs, s, logger, nil); err != nil {
		return nil, err
	}
	header := block.Header()

	for i, txn := range block.Transactions() {
		if !fullBlock && i == int(txIndex) {
			s.Finalize()
			break
		}

		ibs.SetTxContext(txn.Hash(), i)
		receipt, _, err := core.ApplyTransaction(cfg.chainConfig, getHashFn, cfg.engine, nil, gp, ibs, s, header, txn, usedGas, usedBlobGas, vmConfig)
		if err != nil {
			return nil, fmt.Errorf("tx %x failed: %v", txn.Hash(), err)
		}
		receipts = append(receipts, receipt)
	}

	if !fullBlock {
		err = txTds.ResolveStateTrieWithFunc(
			func(loader *trie.SubTrieLoader, rl *trie.RetainList, dbPrefixes [][]byte, fixedbits []int, accountNibbles [][]byte) (trie.SubTries, error) {
				return trie.SubTries{}, nil
			},
		)

		if err != nil {
			return nil, err
		}

		rl := txTds.GetRetainList()

		w, err := s.GetTrie().ExtractWitness(false, rl)

		if err != nil {
			return nil, err
		}

		var buf bytes.Buffer
		_, err = w.WriteInto(&buf)
		if err != nil {
			return nil, err
		}

		return &buf, nil
	}

	receiptSha := types.DeriveSha(receipts)
	if !vmConfig.StatelessExec && cfg.chainConfig.IsByzantium(block.NumberU64()) && !vmConfig.NoReceipts && receiptSha != block.ReceiptHash() {
		return nil, fmt.Errorf("mismatched receipt headers for block %d (%s != %s)", block.NumberU64(), receiptSha.Hex(), block.ReceiptHash().Hex())
	}

	if !vmConfig.StatelessExec && *usedGas != header.GasUsed {
		return nil, fmt.Errorf("gas used by execution: %d, in header: %d", *usedGas, header.GasUsed)
	}

	if header.BlobGasUsed != nil && *usedBlobGas != *header.BlobGasUsed {
		return nil, fmt.Errorf("blob gas used by execution: %d, in header: %d", *usedBlobGas, *header.BlobGasUsed)
	}

	var bloom types.Bloom
	if !vmConfig.NoReceipts {
		bloom = types.CreateBloom(receipts)
		if !vmConfig.StatelessExec && bloom != header.Bloom {
			return nil, fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
		}
	}

	if !vmConfig.ReadOnly {
		newBlock, _, _, err := cfg.engine.FinalizeAndAssemble(cfg.chainConfig, block.Header(), ibs, block.Transactions(), block.Uncles(), receipts, block.Withdrawals(), nil, nil, nil, nil, logger)
		if err != nil {
			return nil, err
		}

		newRoot := newBlock.Root().Bytes()
		if !bytes.Equal(newRoot, block.Root().Bytes()) {
			fmt.Printf("newRoot(%x) != expectedRoot(%x\n)", newRoot, block.Root().Bytes())
		}

		rules := cfg.chainConfig.Rules(block.NumberU64(), header.Time)

		ibs.FinalizeTx(rules, s)

		if err := ibs.CommitBlock(rules, s); err != nil {
			return nil, fmt.Errorf("committing block %d failed: %v", block.NumberU64(), err)
		}
	}

	if err = s.CheckRoot(header.Root); err != nil {
		return nil, err
	}

	roots, err := tds.UpdateStateTrie()
	if err != nil {
		return nil, err
	}

	if roots[len(roots)-1] != block.Root() {
		return nil, fmt.Errorf("mismatch in expected state root computed %v vs %v indicates bug in witness implementation", roots[len(roots)-1], block.Root())
	}

	return buf, nil
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
