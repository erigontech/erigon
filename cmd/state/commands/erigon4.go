package commands

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon-lib/commitment"

	chain2 "github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"

	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon/cmd/state/exec3"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconsensusconfig"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

var (
	blockTo    int
	traceBlock int
)

func init() {
	withBlock(erigon4Cmd)
	withDataDir(erigon4Cmd)
	withChain(erigon4Cmd)

	erigon4Cmd.Flags().IntVar(&commitmentFrequency, "commfreq", 125000, "how many blocks to skip between calculating commitment")
	erigon4Cmd.Flags().BoolVar(&commitments, "commitments", false, "set to true to calculate commitments")
	erigon4Cmd.Flags().StringVar(&commitmentMode, "commitments.mode", "direct", "defines the way to calculate commitments: 'direct' mode reads from state directly, 'update' accumulate updates before commitment")
	erigon4Cmd.Flags().Uint64Var(&startTxNumFrom, "tx", 0, "tx number to start from")
	erigon4Cmd.Flags().StringVar(&commitmentTrie, "commitments.trie", "hex", "hex - use Hex Patricia Hashed Trie for commitments, bin - use of binary patricia trie")
	erigon4Cmd.Flags().IntVar(&height, "height", 32, "amount of steps in biggest file")
	erigon4Cmd.Flags().Uint64Var(&stepSize, "step-size", ethconfig.HistoryV3AggregationStep, "amount of tx in one step")

	rootCmd.AddCommand(erigon4Cmd)
}

var (
	startTxNumFrom      uint64                           // flag --tx
	commitmentMode      string                           // flag --commitments.mode [direct|update]
	logInterval         = 30 * time.Second               // time period to print aggregation stat to log
	dirtySpaceThreshold = uint64(2 * 1024 * 1024 * 1024) /* threshold of dirty space in MDBX transaction that triggers a commit */
	commitmentFrequency int                              // How many blocks to skip between calculating commitment
	commitments         bool
	commitmentTrie      string

	height   int
	stepSize uint64

	blockExecutionTimer       = metrics.GetOrCreateSummary("chain_execution_seconds")
	blockRootMismatchExpected bool // if trie variant is not hex, we could not have another rootHash with to verify it
)

var erigon4Cmd = &cobra.Command{
	Use:   "erigon4",
	Short: "Experimental command to re-execute blocks from beginning using erigon2 state representation and history/domain",
	RunE: func(cmd *cobra.Command, args []string) error {
		var logger log.Logger
		var err error
		if logger, err = debug.SetupCobra(cmd, "erigon4"); err != nil {
			logger.Error("Setting up", "error", err)
			return err
		}
		return Erigon4(genesis, chainConfig, logger)
	},
}

func Erigon4(genesis *types.Genesis, chainConfig *chain2.Config, logger log.Logger) error {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	historyDb, err := kv2.NewMDBX(logger).Path(path.Join(datadirCli, "chaindata")).Open()
	if err != nil {
		return fmt.Errorf("opening chaindata as read only: %v", err)
	}
	defer historyDb.Close()

	ctx := context.Background()
	historyTx, err1 := historyDb.BeginRo(ctx)
	if err1 != nil {
		return err1
	}
	defer historyTx.Rollback()

	stateDbPath := path.Join(datadirCli, "db4")
	if _, err = os.Stat(stateDbPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	db, err2 := kv2.NewMDBX(logger).Path(stateDbPath).WriteMap().Open()
	if err2 != nil {
		return err2
	}
	defer db.Close()

	dirs := datadir.New(datadirCli)
	aggPath := filepath.Join(datadirCli, "erigon4")

	var rwTx kv.RwTx
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}

	trieVariant := commitment.ParseTrieVariant(commitmentTrie)
	if trieVariant != commitment.VariantHexPatriciaTrie {
		blockRootMismatchExpected = true
	}
	mode := libstate.ParseCommitmentMode(commitmentMode)
	logger.Info("aggregator commitment trie", "variant", trieVariant, "mode", mode.String())

	agg, err3 := libstate.NewAggregator(aggPath, dirs.Tmp, stepSize, mode, trieVariant, logger)
	if err3 != nil {
		return fmt.Errorf("create aggregator: %w", err3)
	}
	if err := agg.ReopenFolder(); err != nil {
		return err
	}

	defer agg.Close()

	startTxNum := agg.EndTxNumMinimax()
	fmt.Printf("Max txNum in files: %d\n", startTxNum)

	agg.SetTx(rwTx)
	agg.StartWrites()
	defer agg.FinishWrites()

	latestBlock, latestTx, err := agg.SeekCommitment()
	if err != nil && startTxNum != 0 {
		return fmt.Errorf("failed to seek commitment to tx %d: %w", startTxNum, err)
	}
	if latestTx > startTxNum {
		fmt.Printf("Max txNum in DB: %d\n", latestTx)
		startTxNum = latestTx
	}
	if startTxNumFrom != 0 {
		startTxNum = startTxNumFrom
	}

	interrupt := false
	if startTxNum == 0 {
		genBlock, genesisIbs, err := core.GenesisToBlock(genesis, "")
		if err != nil {
			return err
		}
		agg.SetTxNum(0)
		if err = genesisIbs.CommitBlock(&chain2.Rules{}, &StateWriterV4{w: agg}); err != nil {
			return fmt.Errorf("cannot write state: %w", err)
		}

		blockRootHash, err := agg.ComputeCommitment(true, false)
		if err != nil {
			return err
		}
		if err = agg.FinishTx(); err != nil {
			return err
		}

		genesisRootHash := genBlock.Root()
		if !bytes.Equal(blockRootHash, genesisRootHash[:]) {
			return fmt.Errorf("genesis root hash mismatch: expected %x got %x", genesisRootHash, blockRootHash)
		}
	}

	logger.Info("Initialised chain configuration", "startTxNum", startTxNum, "block", latestBlock, "config", chainConfig)

	var (
		blockNum uint64
		trace    bool
		vmConfig vm.Config
		txNum    uint64 = 2 // Consider that each block contains at least first system tx and enclosing transactions, except for Clique consensus engine
		started         = time.Now()
	)

	if startTxNum != 0 {
		txNum = startTxNum
		blockNum = latestBlock
	}

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	statx := &stat23{
		prevBlock: blockNum,
		prevTime:  time.Now(),
	}

	go func() {
		for range logEvery.C {
			aStats := agg.Stats()
			statx.delta(aStats, blockNum, txNum).print(aStats, logger)
		}
	}()

	var blockReader services.FullBlockReader
	var allSnapshots = snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), path.Join(datadirCli, "snapshots"), logger)
	defer allSnapshots.Close()
	if err := allSnapshots.ReopenFolder(); err != nil {
		return fmt.Errorf("reopen snapshot segments: %w", err)
	}
	blockReader = snapshotsync.NewBlockReader(allSnapshots)
	engine := initConsensusEngine(chainConfig, allSnapshots, logger)

	getHeader := func(hash libcommon.Hash, number uint64) *types.Header {
		h, err := blockReader.Header(ctx, historyTx, hash, number)
		if err != nil {
			panic(err)
		}
		return h
	}
	readWrapper := &StateReaderV4{ac: agg.MakeContext(), roTx: rwTx}
	writeWrapper := &StateWriterV4{w: agg}

	commitFn := func(txn uint64) error {
		if db == nil || rwTx == nil {
			return fmt.Errorf("commit failed due to invalid db/rwTx")
		}
		var spaceDirty uint64
		if spaceDirty, _, err = rwTx.(*kv2.MdbxTx).SpaceDirty(); err != nil {
			return fmt.Errorf("retrieving spaceDirty: %w", err)
		}
		if spaceDirty >= dirtySpaceThreshold {
			logger.Info("Initiated tx commit", "block", blockNum, "space dirty", libcommon.ByteCount(spaceDirty))
		}
		logger.Info("database commitment", "block", blockNum, "txNum", txn, "uptime", time.Since(started))
		if err := agg.Flush(ctx); err != nil {
			return err
		}
		if err = rwTx.Commit(); err != nil {
			return err
		}
		if interrupt {
			return nil
		}

		if rwTx, err = db.BeginRw(ctx); err != nil {
			return err
		}

		readWrapper.ac.Close()
		agg.SetTx(rwTx)
		readWrapper.roTx = rwTx
		readWrapper.ac = agg.MakeContext()
		return nil
	}

	mergedRoots := agg.AggregatedRoots()
	for !interrupt {
		blockNum++
		trace = traceBlock > 0 && blockNum == uint64(traceBlock)
		blockHash, err := blockReader.CanonicalHash(ctx, historyTx, blockNum)
		if err != nil {
			return err
		}

		b, _, err := blockReader.BlockWithSenders(ctx, historyTx, blockHash, blockNum)
		if err != nil {
			return err
		}
		if b == nil {
			logger.Info("history: block is nil", "block", blockNum)
			break
		}
		agg.SetTx(rwTx)
		agg.SetTxNum(txNum)
		agg.SetBlockNum(blockNum)

		if txNum, _, err = processBlock23(startTxNum, trace, txNum, readWrapper, writeWrapper, chainConfig, engine, getHeader, b, vmConfig); err != nil {
			logger.Error("processing error", "block", blockNum, "err", err)
			return fmt.Errorf("processing block %d: %w", blockNum, err)
		}

		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			// Commit transaction only when interrupted or just before computing commitment (so it can be re-done)
			if err := agg.Flush(ctx); err != nil {
				logger.Error("aggregator flush", "err", err)
			}

			logger.Info(fmt.Sprintf("interrupted, please wait for cleanup, next time start with --tx %d", agg.Stats().TxCount))
			if err := commitFn(txNum); err != nil {
				logger.Error("db commit", "err", err)
			}
		case <-mergedRoots:
			if err := commitFn(txNum); err != nil {
				logger.Error("db commit on merge", "err", err)
			}
		default:
		}
	}

	return nil
}

type stat23 struct {
	blockNum     uint64
	hits         uint64
	misses       uint64
	prevBlock    uint64
	hitMissRatio float64
	blockSpeed   float64
	txSpeed      float64
	txNum        uint64
	prevTxNum    uint64
	prevTime     time.Time
	mem          runtime.MemStats
}

func (s *stat23) print(aStats libstate.FilesStats, logger log.Logger) {
	totalFiles := aStats.FilesCount
	totalDatSize := aStats.DataSize
	totalIdxSize := aStats.IdxSize

	logger.Info("Progress", "block", s.blockNum, "blk/s", s.blockSpeed, "tx", s.txNum, "txn/s", s.txSpeed, "state files", totalFiles,
		"total dat", libcommon.ByteCount(totalDatSize), "total idx", libcommon.ByteCount(totalIdxSize),
		"hit ratio", s.hitMissRatio, "hits+misses", s.hits+s.misses,
		"alloc", libcommon.ByteCount(s.mem.Alloc), "sys", libcommon.ByteCount(s.mem.Sys),
	)
}

func (s *stat23) delta(aStats libstate.FilesStats, blockNum, txNum uint64) *stat23 {
	currentTime := time.Now()
	dbg.ReadMemStats(&s.mem)

	interval := currentTime.Sub(s.prevTime).Seconds()
	s.blockNum = blockNum
	s.blockSpeed = float64(s.blockNum-s.prevBlock) / interval
	s.txNum = txNum
	s.txSpeed = float64(s.txNum-s.prevTxNum) / interval
	s.prevBlock = blockNum
	s.prevTxNum = txNum
	s.prevTime = currentTime

	total := s.hits + s.misses
	if total > 0 {
		s.hitMissRatio = float64(s.hits) / float64(total)
	}
	return s
}

func processBlock23(startTxNum uint64, trace bool, txNumStart uint64, rw *StateReaderV4, ww *StateWriterV4, chainConfig *chain2.Config,
	engine consensus.Engine, getHeader func(hash libcommon.Hash, number uint64) *types.Header, block *types.Block, vmConfig vm.Config,
) (uint64, types.Receipts, error) {
	defer blockExecutionTimer.UpdateDuration(time.Now())

	header := block.Header()
	vmConfig.Debug = true
	gp := new(core.GasPool).AddGas(block.GasLimit()).AddDataGas(params.MaxDataGasPerBlock)
	usedGas := new(uint64)
	var receipts types.Receipts
	rules := chainConfig.Rules(block.NumberU64(), block.Time())
	txNum := txNumStart
	ww.w.SetTxNum(txNum)

	rw.blockNum = block.NumberU64()

	daoFork := txNum >= startTxNum && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0
	if daoFork {
		ibs := state.New(rw)
		// TODO Actually add tracing to the DAO related accounts
		misc.ApplyDAOHardFork(ibs)
		if err := ibs.FinalizeTx(rules, ww); err != nil {
			return 0, nil, err
		}
		if err := ww.w.FinishTx(); err != nil {
			return 0, nil, fmt.Errorf("finish daoFork failed: %w", err)
		}
	}

	txNum++ // Pre-block transaction
	ww.w.SetTxNum(txNum)
	if err := ww.w.FinishTx(); err != nil {
		return 0, nil, fmt.Errorf("finish pre-block tx %d (block %d) has failed: %w", txNum, block.NumberU64(), err)
	}

	getHashFn := core.GetHashFn(header, getHeader)
	for i, tx := range block.Transactions() {
		if txNum >= startTxNum {
			ibs := state.New(rw)
			ibs.SetTxContext(tx.Hash(), block.Hash(), i)
			ct := exec3.NewCallTracer()
			vmConfig.Tracer = ct
			receipt, _, err := core.ApplyTransaction(chainConfig, getHashFn, engine, nil, gp, ibs, ww, header, tx, usedGas, vmConfig)
			if err != nil {
				return 0, nil, fmt.Errorf("could not apply tx %d [%x] failed: %w", i, tx.Hash(), err)
			}
			for from := range ct.Froms() {
				if err := ww.w.AddTraceFrom(from[:]); err != nil {
					return 0, nil, err
				}
			}
			for to := range ct.Tos() {
				if err := ww.w.AddTraceTo(to[:]); err != nil {
					return 0, nil, err
				}
			}
			receipts = append(receipts, receipt)
			for _, log := range receipt.Logs {
				if err = ww.w.AddLogAddr(log.Address[:]); err != nil {
					return 0, nil, fmt.Errorf("adding event log for addr %x: %w", log.Address, err)
				}
				for _, topic := range log.Topics {
					if err = ww.w.AddLogTopic(topic[:]); err != nil {
						return 0, nil, fmt.Errorf("adding event log for topic %x: %w", topic, err)
					}
				}
			}
			if err = ww.w.FinishTx(); err != nil {
				return 0, nil, fmt.Errorf("finish tx %d [%x] failed: %w", i, tx.Hash(), err)
			}
			if trace {
				fmt.Printf("FinishTx called for blockNum=%d, txIndex=%d, txNum=%d txHash=[%x]\n", block.NumberU64(), i, txNum, tx.Hash())
			}
		}
		txNum++
		ww.w.SetTxNum(txNum)
	}

	if txNum >= startTxNum {
		if chainConfig.IsByzantium(block.NumberU64()) {
			receiptSha := types.DeriveSha(receipts)
			if receiptSha != block.ReceiptHash() {
				fmt.Printf("mismatched receipt headers for block %d\n", block.NumberU64())
				for j, receipt := range receipts {
					fmt.Printf("tx %d, used gas: %d\n", j, receipt.GasUsed)
				}
			}
		}
		ibs := state.New(rw)
		if err := ww.w.AddTraceTo(block.Coinbase().Bytes()); err != nil {
			return 0, nil, fmt.Errorf("adding coinbase trace: %w", err)
		}
		for _, uncle := range block.Uncles() {
			if err := ww.w.AddTraceTo(uncle.Coinbase.Bytes()); err != nil {
				return 0, nil, fmt.Errorf("adding uncle trace: %w", err)
			}
		}

		// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
		if _, _, err := engine.Finalize(chainConfig, header, ibs, block.Transactions(), block.Uncles(), receipts, block.Withdrawals(), nil, nil); err != nil {
			return 0, nil, fmt.Errorf("finalize of block %d failed: %w", block.NumberU64(), err)
		}

		if err := ibs.CommitBlock(rules, ww); err != nil {
			return 0, nil, fmt.Errorf("committing block %d failed: %w", block.NumberU64(), err)
		}

		if err := ww.w.FinishTx(); err != nil {
			return 0, nil, fmt.Errorf("failed to finish tx: %w", err)
		}
		if trace {
			fmt.Printf("FinishTx called for %d block %d\n", txNum, block.NumberU64())
		}
	}

	txNum++ // Post-block transaction
	ww.w.SetTxNum(txNum)
	if txNum >= startTxNum {
		if commitments && commitmentFrequency > 0 && block.Number().Uint64()%uint64(commitmentFrequency) == 0 {
			rootHash, err := ww.w.ComputeCommitment(true, trace)
			if err != nil {
				return 0, nil, err
			}
			if !bytes.Equal(rootHash, header.Root[:]) {
				return 0, nil, fmt.Errorf("invalid root hash for block %d: expected %x got %x", block.NumberU64(), header.Root, rootHash)
			}
		}

		if err := ww.w.FinishTx(); err != nil {
			return 0, nil, fmt.Errorf("finish after-block tx %d (block %d) has failed: %w", txNum, block.NumberU64(), err)
		}
	}

	return txNum, receipts, nil
}

// Implements StateReader and StateWriter
type StateReaderV4 struct {
	roTx     kv.Tx
	ac       *libstate.AggregatorContext
	blockNum uint64
}

type StateWriterV4 struct {
	w *libstate.Aggregator
}

func (rw *StateReaderV4) ReadAccountData(address libcommon.Address) (*accounts.Account, error) {
	enc, err := rw.ac.ReadAccountData(address.Bytes(), rw.roTx)
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}
	var a accounts.Account
	if err := accounts.DeserialiseV3(&a, enc); err != nil {
		return nil, err
	}
	return &a, nil
}

func (rw *StateReaderV4) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
	enc, err := rw.ac.ReadAccountStorage(address.Bytes(), key.Bytes(), rw.roTx)
	if err != nil {
		return nil, err
	}
	if enc == nil {
		return nil, nil
	}
	if len(enc) == 1 && enc[0] == 0 {
		return nil, nil
	}
	return enc, nil
}

func (rw *StateReaderV4) ReadAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) ([]byte, error) {
	return rw.ac.ReadAccountCode(address.Bytes(), rw.roTx)
}

func (rw *StateReaderV4) ReadAccountCodeSize(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (int, error) {
	return rw.ac.ReadAccountCodeSize(address.Bytes(), rw.roTx)
}

func (rw *StateReaderV4) ReadAccountIncarnation(address libcommon.Address) (uint64, error) {
	return 0, nil
}

func (ww *StateWriterV4) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	value := accounts.SerialiseV3(account)
	if err := ww.w.UpdateAccountData(address.Bytes(), value); err != nil {
		return err
	}
	return nil
}

func (ww *StateWriterV4) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	if err := ww.w.UpdateAccountCode(address.Bytes(), code); err != nil {
		return err
	}
	return nil
}

func (ww *StateWriterV4) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	if err := ww.w.DeleteAccount(address.Bytes()); err != nil {
		return err
	}
	return nil
}

func (ww *StateWriterV4) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	if err := ww.w.WriteAccountStorage(address.Bytes(), key.Bytes(), value.Bytes()); err != nil {
		return err
	}
	return nil
}

func (ww *StateWriterV4) CreateContract(address libcommon.Address) error {
	return nil
}

func initConsensusEngine(cc *chain2.Config, snapshots *snapshotsync.RoSnapshots, logger log.Logger) (engine consensus.Engine) {
	config := ethconfig.Defaults

	var consensusConfig interface{}

	if cc.Clique != nil {
		consensusConfig = params.CliqueSnapshot
	} else if cc.Aura != nil {
		consensusConfig = &config.Aura
	} else if cc.Bor != nil {
		consensusConfig = &config.Bor
	} else {
		consensusConfig = &config.Ethash
	}
	return ethconsensusconfig.CreateConsensusEngine(cc, consensusConfig, config.Miner.Notify, config.Miner.Noverify, config.HeimdallgRPCAddress,
		config.HeimdallURL, config.WithoutHeimdall, datadirCli, true /* readonly */, logger)
}
