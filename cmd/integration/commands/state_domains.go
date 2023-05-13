package commands

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/VictoriaMetrics/metrics"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"

	chain2 "github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/commitment"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/common/length"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon/cmd/hack/tool/fromdb"
	"github.com/ledgerwatch/erigon/cmd/state/exec3"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/erigon/params"
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/services"
)

func init() {
	withConfig(stateDomains)
	withDataDir(stateDomains)
	withUnwind(stateDomains)
	withUnwindEvery(stateDomains)
	withBlock(stateDomains)
	withIntegrityChecks(stateDomains)
	withChain(stateDomains)
	withHeimdall(stateDomains)
	withWorkers(stateDomains)
	withStartTx(stateDomains)
	withCommitment(stateDomains)
	withTraceFromTx(stateDomains)

	stateDomains.Flags().Uint64Var(&stepSize, "step-size", ethconfig.HistoryV3AggregationStep, "size of aggregation step, tx")
	stateDomains.Flags().Uint64Var(&lastStep, "last-step", 0, "step of last aggregation, step=txnum/step-size, unsigned integers only")

	rootCmd.AddCommand(stateDomains)

	withDataDir(readDomains)
	withChain(readDomains)
	withHeimdall(readDomains)
	withWorkers(readDomains)
	withStartTx(readDomains)

	rootCmd.AddCommand(readDomains)
}

// if trie variant is not hex, we could not have another rootHash with to verify it
var (
	stepSize                  uint64
	lastStep                  uint64
	dirtySpaceThreshold       = uint64(2 * 1024 * 1024 * 1024) /* threshold of dirty space in MDBX transaction that triggers a commit */
	blockRootMismatchExpected bool

	mxBlockExecutionTimer = metrics.GetOrCreateSummary("chain_execution_seconds")
	mxCommitTook          = metrics.GetOrCreateHistogram("domain_commit_took")
)

// write command to just seek and query state by addr and domain from state db and files (if any)
var readDomains = &cobra.Command{
	Use:       "read_domains",
	Short:     `Run block execution and commitment with Domains.`,
	Example:   "go run ./cmd/integration read_domains --datadir=... --verbosity=3",
	ValidArgs: []string{"account", "storage", "code", "commitment"},
	Args:      cobra.ArbitraryArgs,
	Run: func(cmd *cobra.Command, args []string) {
		var logger log.Logger
		var err error
		if logger, err = debug.SetupCobra(cmd, "integration"); err != nil {
			logger.Error("Setting up", "error", err)
			return
		}
		ctx, _ := libcommon.RootContext()
		cfg := &nodecfg.DefaultConfig
		utils.SetNodeConfigCobra(cmd, cfg)
		ethConfig := &ethconfig.Defaults
		ethConfig.Genesis = core.GenesisBlockByChainName(chain)
		erigoncli.ApplyFlagsForEthConfigCobra(cmd.Flags(), ethConfig)

		var readFromDomain string
		var addrs [][]byte
		for i := 0; i < len(args); i++ {
			if i == 0 {
				switch s := strings.ToLower(args[i]); s {
				case "account", "storage", "code", "commitment":
					readFromDomain = s
				default:
					logger.Error("invalid domain to read from", "arg", args[i])
					return
				}
				continue
			}
			addr, err := hex.DecodeString(strings.TrimPrefix(args[i], "0x"))
			if err != nil {
				logger.Warn("invalid address passed", "str", args[i], "at position", i, "err", err)
				continue
			}
			addrs = append(addrs, addr)
		}

		dirs := datadir.New(datadirCli)
		chainDb, err := openDB(dbCfg(kv.ChainDB, dirs.Chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer chainDb.Close()

		stateDb, err := kv2.NewMDBX(log.New()).Path(filepath.Join(dirs.DataDir, "statedb")).WriteMap().Open()
		if err != nil {
			return
		}
		defer stateDb.Close()

		if err := requestDomains(chainDb, stateDb, ctx, readFromDomain, addrs, logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

func requestDomains(chainDb, stateDb kv.RwDB, ctx context.Context, readDomain string, addrs [][]byte, logger log.Logger) error {
	trieVariant := commitment.ParseTrieVariant(commitmentTrie)
	if trieVariant != commitment.VariantHexPatriciaTrie {
		blockRootMismatchExpected = true
	}
	mode := libstate.ParseCommitmentMode(commitmentMode)
	libstate.COMPARE_INDEXES = true

	_, _, _, agg := newDomains(ctx, chainDb, stepSize, mode, trieVariant, logger)
	defer agg.Close()

	histTx, err := chainDb.BeginRo(ctx)
	must(err)
	defer histTx.Rollback()

	stateTx, err := stateDb.BeginRw(ctx)
	must(err)
	defer stateTx.Rollback()

	agg.SetTx(stateTx)
	defer agg.StartWrites().FinishWrites()

	latestBlock, latestTx, err := agg.SeekCommitment()
	if err != nil && startTxNum != 0 {
		return fmt.Errorf("failed to seek commitment to tx %d: %w", startTxNum, err)
	}
	if latestTx < startTxNum {
		return fmt.Errorf("latest available tx to start is  %d and its less than start tx %d", latestTx, startTxNum)
	}
	if latestTx > 0 {
		logger.Info("aggregator files opened", "txn", latestTx, "block", latestBlock)
	}
	agg.SetTxNum(latestTx)

	r := ReaderWrapper4{
		roTx: histTx,
		ac:   agg.MakeContext(),
	}

	switch readDomain {
	case "account":
		for _, addr := range addrs {
			acc, err := r.ReadAccountData(libcommon.BytesToAddress(addr))
			if err != nil {
				logger.Error("failed to read account", "addr", addr, "err", err)
				continue
			}
			fmt.Printf("%x: nonce=%d balance=%d code=%x root=%x\n", addr, acc.Nonce, acc.Balance.Uint64(), acc.CodeHash, acc.Root)
		}
	case "storage":
		for _, addr := range addrs {
			a, s := libcommon.BytesToAddress(addr[:length.Addr]), libcommon.BytesToHash(addr[length.Addr:])
			st, err := r.ReadAccountStorage(a, 0, &s)
			if err != nil {
				logger.Error("failed to read storage", "addr", a.String(), "key", s.String(), "err", err)
				continue
			}
			fmt.Printf("%s %s -> %x\n", a.String(), s.String(), st)
		}
	case "code":
		for _, addr := range addrs {
			code, err := r.ReadAccountCode(libcommon.BytesToAddress(addr), 0, libcommon.Hash{})
			if err != nil {
				logger.Error("failed to read code", "addr", addr, "err", err)
				continue
			}
			fmt.Printf("%s: %x\n", addr, code)
		}
	}
	return nil
}

// write command to just seek and query state by addr and domain from state db and files (if any)
var stateDomains = &cobra.Command{
	Use:     "state_domains",
	Short:   `Run block execution and commitment with Domains.`,
	Example: "go run ./cmd/integration state_domains --datadir=... --verbosity=3 --unwind=100 --unwind.every=100000 --block=2000000",
	Run: func(cmd *cobra.Command, args []string) {
		var logger log.Logger
		var err error
		if logger, err = debug.SetupCobra(cmd, "integration"); err != nil {
			logger.Error("Setting up", "error", err)
			return
		}
		ctx, _ := libcommon.RootContext()
		cfg := &nodecfg.DefaultConfig
		utils.SetNodeConfigCobra(cmd, cfg)
		ethConfig := &ethconfig.Defaults
		ethConfig.Genesis = core.GenesisBlockByChainName(chain)
		erigoncli.ApplyFlagsForEthConfigCobra(cmd.Flags(), ethConfig)

		dirs := datadir.New(datadirCli)
		chainDb, err := openDB(dbCfg(kv.ChainDB, dirs.Chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer chainDb.Close()

		//stateDB := kv.Label(6)
		//stateOpts := dbCfg(stateDB, filepath.Join(dirs.DataDir, "statedb")).WriteMap()
		//stateOpts.MapSize(1 * datasize.TB).WriteMap().DirtySpace(dirtySpaceThreshold)
		//stateDb := openDB(stateOpts, true)
		//defer stateDb.Close()

		stateDb, err := kv2.NewMDBX(log.New()).Path(filepath.Join(dirs.DataDir, "statedb")).WriteMap().Open()
		if err != nil {
			return
		}
		defer stateDb.Close()

		if err := loopProcessDomains(chainDb, stateDb, ctx, logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

func loopProcessDomains(chainDb, stateDb kv.RwDB, ctx context.Context, logger log.Logger) error {
	trieVariant := commitment.ParseTrieVariant(commitmentTrie)
	if trieVariant != commitment.VariantHexPatriciaTrie {
		blockRootMismatchExpected = true
	}
	mode := libstate.ParseCommitmentMode(commitmentMode)

	engine, _, _, agg := newDomains(ctx, chainDb, stepSize, mode, trieVariant, logger)
	defer agg.Close()

	histTx, err := chainDb.BeginRo(ctx)
	must(err)
	defer histTx.Rollback()

	stateTx, err := stateDb.BeginRw(ctx)
	must(err)
	defer stateTx.Rollback()

	agg.SetTx(stateTx)
	defer agg.StartWrites().FinishWrites()

	latestBlock, latestTx, err := agg.SeekCommitment()
	if err != nil && startTxNum != 0 {
		return fmt.Errorf("failed to seek commitment to tx %d: %w", startTxNum, err)
	}
	if latestTx < startTxNum {
		return fmt.Errorf("latest available tx to start is  %d and its less than start tx %d", latestTx, startTxNum)
	}
	if latestTx > 0 {
		logger.Info("aggregator files opened", "txn", latestTx, "block", latestBlock)
	}

	aggWriter, aggReader := WrapAggregator(agg, stateTx)
	proc := blockProcessor{
		chainConfig: fromdb.ChainConfig(chainDb),
		vmConfig:    vm.Config{},
		engine:      engine,
		reader:      aggReader,
		writer:      aggWriter,
		blockReader: getBlockReader(chainDb),
		stateTx:     stateTx,
		stateDb:     stateDb,
		blockNum:    latestBlock,
		txNum:       latestTx,
		startTxNum:  latestTx,
		histTx:      histTx,
		agg:         agg,
		logger:      log.New(),
		stat:        stat4{startedAt: time.Now()},
	}
	if proc.txNum > 0 {
		proc.txNum--
	}
	if proc.blockNum == 0 {
		proc.txNum = 2
	}

	mergedRoots := agg.AggregatedRoots()
	go proc.PrintStatsLoop(ctx, 30*time.Second)

	if proc.startTxNum == 0 {
		genesis := core.GenesisBlockByChainName(chain)
		if err := proc.ApplyGenesis(genesis); err != nil {
			return err
		}
	}

	for {
		// Check for interrupts
		select {
		case <-ctx.Done():
			logger.Info(fmt.Sprintf("interrupted, please wait for commitment and cleanup, next time start with --tx %d", proc.txNum))
			rh, err := proc.agg.ComputeCommitment(true, false)
			if err != nil {
				logger.Error("failed to compute commitment", "err", err)
			}
			logger.Info("commitment: state root computed", "root", hex.EncodeToString(rh))
			if err := agg.Flush(ctx); err != nil {
				logger.Error("failed to flush aggregator", "err", err)
			}
			os.Exit(0)
		case <-mergedRoots: // notified with rootHash of latest aggregation
			if err := proc.commit(ctx); err != nil {
				logger.Error("chainDb commit on merge", "err", err)
			}
		default:
		}

		if lastStep > 0 && proc.txNum/stepSize >= lastStep {
			logger.Info("last step reached")
			// Commit transaction only when interrupted or just before computing commitment (so it can be re-done)
			break
		}

		err := proc.ProcessNext(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

type blockProcessor struct {
	engine      consensus.Engine
	agg         *libstate.Aggregator
	blockReader services.FullBlockReader
	writer      *WriterWrapper4
	reader      *ReaderWrapper4
	stateDb     kv.RwDB
	stateTx     kv.RwTx
	histTx      kv.Tx
	blockNum    uint64
	startTxNum  uint64
	txNum       uint64
	stat        stat4
	trace       bool
	logger      log.Logger
	vmConfig    vm.Config
	chainConfig *chain2.Config
}

func (b *blockProcessor) getHeader(hash libcommon.Hash, number uint64) *types.Header {
	h, err := b.blockReader.Header(context.Background(), b.histTx, hash, number)
	if err != nil {
		panic(err)
	}
	return h
}

func (b *blockProcessor) commit(ctx context.Context) error {
	if b.stateDb == nil || b.stateTx == nil {
		return fmt.Errorf("commit failed due to invalid chainDb/rwTx")
	}

	s := time.Now()
	defer mxCommitTook.UpdateDuration(s)

	var spaceDirty uint64
	var err error
	if spaceDirty, _, err = b.stateTx.(*kv2.MdbxTx).SpaceDirty(); err != nil {
		return fmt.Errorf("retrieving spaceDirty: %w", err)
	}
	if spaceDirty >= dirtySpaceThreshold {
		b.logger.Info("Initiated tx commit", "block", b.blockNum, "space dirty", libcommon.ByteCount(spaceDirty))
	}

	b.logger.Info("database commitment", "block", b.blockNum, "txNum", b.txNum, "uptime", time.Since(b.stat.startedAt))
	if err := b.agg.Flush(ctx); err != nil {
		return err
	}
	if err = b.stateTx.Commit(); err != nil {
		return err
	}

	if b.stateTx, err = b.stateDb.BeginRw(ctx); err != nil {
		return err
	}

	b.agg.SetTx(b.stateTx)
	b.reader.SetTx(b.stateTx, b.agg.MakeContext())

	return nil
}

func (b *blockProcessor) PrintStatsLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.stat.delta(b.blockNum, b.txNum).print(b.agg.Stats(), b.logger)
		}
	}
}

func (b *blockProcessor) ApplyGenesis(genesis *types.Genesis) error {
	b.logger.Info("apply genesis", "chain_id", genesis.Config.ChainID)
	genBlock, genesisIbs, err := core.GenesisToBlock(genesis, "")
	if err != nil {
		return err
	}
	b.agg.SetTxNum(0)
	if err = genesisIbs.CommitBlock(&chain2.Rules{}, b.writer); err != nil {
		return fmt.Errorf("cannot write state: %w", err)
	}

	blockRootHash, err := b.agg.ComputeCommitment(true, false)
	if err != nil {
		return err
	}
	if err = b.agg.FinishTx(); err != nil {
		return err
	}

	genesisRootHash := genBlock.Root()
	if !blockRootMismatchExpected && !bytes.Equal(blockRootHash, genesisRootHash[:]) {
		return fmt.Errorf("genesis root hash mismatch: expected %x got %x", genesisRootHash, blockRootHash)
	}
	return nil
}

func (b *blockProcessor) ProcessNext(ctx context.Context) error {
	b.blockNum++
	b.trace = traceFromTx > 0 && b.txNum == traceFromTx

	blockHash, err := b.blockReader.CanonicalHash(ctx, b.histTx, b.blockNum)
	if err != nil {
		return err
	}

	block, _, err := b.blockReader.BlockWithSenders(ctx, b.histTx, blockHash, b.blockNum)
	if err != nil {
		return err
	}
	if block == nil {
		b.logger.Info("history: block is nil", "block", b.blockNum)
		return fmt.Errorf("block %d is nil", b.blockNum)
	}

	b.agg.SetTx(b.stateTx)
	b.agg.SetTxNum(b.txNum)
	b.agg.SetBlockNum(b.blockNum)

	if _, err = b.applyBlock(ctx, block); err != nil {
		b.logger.Error("processing error", "block", b.blockNum, "err", err)
		return fmt.Errorf("processing block %d: %w", b.blockNum, err)
	}
	return err
}

func (b *blockProcessor) applyBlock(
	ctx context.Context,
	block *types.Block,
) (types.Receipts, error) {
	defer mxBlockExecutionTimer.UpdateDuration(time.Now())

	header := block.Header()
	b.vmConfig.Debug = true
	gp := new(core.GasPool).AddGas(block.GasLimit()).AddDataGas(params.MaxDataGasPerBlock)
	usedGas := new(uint64)
	var receipts types.Receipts
	rules := b.chainConfig.Rules(block.NumberU64(), block.Time())

	b.blockNum = block.NumberU64()
	b.writer.w.SetTxNum(b.txNum)

	daoFork := b.txNum >= b.startTxNum && b.chainConfig.DAOForkBlock != nil && b.chainConfig.DAOForkBlock.Cmp(block.Number()) == 0
	if daoFork {
		ibs := state.New(b.reader)
		// TODO Actually add tracing to the DAO related accounts
		misc.ApplyDAOHardFork(ibs)
		if err := ibs.FinalizeTx(rules, b.writer); err != nil {
			return nil, err
		}
		if err := b.writer.w.FinishTx(); err != nil {
			return nil, fmt.Errorf("finish daoFork failed: %w", err)
		}
	}

	b.txNum++ // Pre-block transaction
	b.writer.w.SetTxNum(b.txNum)
	if err := b.writer.w.FinishTx(); err != nil {
		return nil, fmt.Errorf("finish pre-block tx %d (block %d) has failed: %w", b.txNum, block.NumberU64(), err)
	}

	getHashFn := core.GetHashFn(header, b.getHeader)
	parentHeader := b.getHeader(block.ParentHash(), b.blockNum-1)
	for i, tx := range block.Transactions() {
		if b.txNum >= b.startTxNum {
			ibs := state.New(b.reader)
			ibs.SetTxContext(tx.Hash(), block.Hash(), i)
			ct := exec3.NewCallTracer()
			b.vmConfig.Tracer = ct
			receipt, _, err := core.ApplyTransaction(b.chainConfig, getHashFn, b.engine, nil, gp, ibs, b.writer, header, tx, usedGas, b.vmConfig, parentHeader.ExcessDataGas)
			if err != nil {
				return nil, fmt.Errorf("could not apply tx %d [%x] failed: %w", i, tx.Hash(), err)
			}
			for from := range ct.Froms() {
				if err := b.writer.w.AddTraceFrom(from[:]); err != nil {
					return nil, err
				}
			}
			for to := range ct.Tos() {
				if err := b.writer.w.AddTraceTo(to[:]); err != nil {
					return nil, err
				}
			}
			receipts = append(receipts, receipt)
			for _, log := range receipt.Logs {
				if err = b.writer.w.AddLogAddr(log.Address[:]); err != nil {
					return nil, fmt.Errorf("adding event log for addr %x: %w", log.Address, err)
				}
				for _, topic := range log.Topics {
					if err = b.writer.w.AddLogTopic(topic[:]); err != nil {
						return nil, fmt.Errorf("adding event log for topic %x: %w", topic, err)
					}
				}
			}
			if err = b.writer.w.FinishTx(); err != nil {
				return nil, fmt.Errorf("finish tx %d [%x] failed: %w", i, tx.Hash(), err)
			}
			if b.trace {
				fmt.Printf("FinishTx called for blockNum=%d, txIndex=%d, txNum=%d txHash=[%x]\n", b.blockNum, i, b.txNum, tx.Hash())
			}
		}
		b.txNum++
		b.writer.w.SetTxNum(b.txNum)
	}

	if b.txNum >= b.startTxNum {
		if b.chainConfig.IsByzantium(block.NumberU64()) {
			receiptSha := types.DeriveSha(receipts)
			if receiptSha != block.ReceiptHash() {
				fmt.Printf("mismatched receipt headers for block %d\n", block.NumberU64())
				for j, receipt := range receipts {
					fmt.Printf("tx %d, used gas: %d\n", j, receipt.GasUsed)
				}
			}
		}
		ibs := state.New(b.reader)
		if err := b.writer.w.AddTraceTo(block.Coinbase().Bytes()); err != nil {
			return nil, fmt.Errorf("adding coinbase trace: %w", err)
		}
		for _, uncle := range block.Uncles() {
			if err := b.writer.w.AddTraceTo(uncle.Coinbase.Bytes()); err != nil {
				return nil, fmt.Errorf("adding uncle trace: %w", err)
			}
		}

		// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
		if _, _, err := b.engine.Finalize(b.chainConfig, header, ibs, block.Transactions(), block.Uncles(), receipts, block.Withdrawals(), nil, nil); err != nil {
			return nil, fmt.Errorf("finalize of block %d failed: %w", block.NumberU64(), err)
		}

		if err := ibs.CommitBlock(rules, b.writer); err != nil {
			return nil, fmt.Errorf("committing block %d failed: %w", block.NumberU64(), err)
		}

		if err := b.writer.w.FinishTx(); err != nil {
			return nil, fmt.Errorf("failed to finish tx: %w", err)
		}
		if b.trace {
			fmt.Printf("FinishTx called for %d block %d\n", b.txNum, block.NumberU64())
		}
	}

	b.txNum++ // Post-block transaction
	b.writer.w.SetTxNum(b.txNum)
	if b.txNum >= b.startTxNum {
		if block.Number().Uint64()%uint64(commitmentFreq) == 0 {
			rootHash, err := b.writer.w.ComputeCommitment(true, b.trace)
			if err != nil {
				return nil, err
			}
			if !blockRootMismatchExpected && !bytes.Equal(rootHash, header.Root[:]) {
				return nil, fmt.Errorf("invalid root hash for block %d: expected %x got %x", block.NumberU64(), header.Root, rootHash)
			}
		}

		if err := b.writer.w.FinishTx(); err != nil {
			return nil, fmt.Errorf("finish after-block tx %d (block %d) has failed: %w", b.txNum, block.NumberU64(), err)
		}
	}

	return receipts, nil
}

// Implements StateReader and StateWriter
type ReaderWrapper4 struct {
	roTx kv.Tx
	ac   *libstate.AggregatorContext
}

type WriterWrapper4 struct {
	w *libstate.Aggregator
}

func WrapAggregator(agg *libstate.Aggregator, roTx kv.Tx) (*WriterWrapper4, *ReaderWrapper4) {
	return &WriterWrapper4{w: agg}, &ReaderWrapper4{ac: agg.MakeContext(), roTx: roTx}
}

func (rw *ReaderWrapper4) SetTx(roTx kv.Tx, ctx *libstate.AggregatorContext) {
	rw.roTx = roTx
	rw.ac.Close()
	rw.ac = ctx
}

func (rw *ReaderWrapper4) ReadAccountData(address libcommon.Address) (*accounts.Account, error) {
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

func (rw *ReaderWrapper4) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
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

func (rw *ReaderWrapper4) ReadAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) ([]byte, error) {
	return rw.ac.ReadAccountCode(address.Bytes(), rw.roTx)
}

func (rw *ReaderWrapper4) ReadAccountCodeSize(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (int, error) {
	return rw.ac.ReadAccountCodeSize(address.Bytes(), rw.roTx)
}

func (rw *ReaderWrapper4) ReadAccountIncarnation(address libcommon.Address) (uint64, error) {
	return 0, nil
}

func (ww *WriterWrapper4) UpdateAccountData(address libcommon.Address, original, account *accounts.Account) error {
	value := accounts.SerialiseV3(account)
	if err := ww.w.UpdateAccountData(address.Bytes(), value); err != nil {
		return err
	}
	return nil
}

func (ww *WriterWrapper4) UpdateAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash, code []byte) error {
	if err := ww.w.UpdateAccountCode(address.Bytes(), code); err != nil {
		return err
	}
	return nil
}

func (ww *WriterWrapper4) DeleteAccount(address libcommon.Address, original *accounts.Account) error {
	if err := ww.w.DeleteAccount(address.Bytes()); err != nil {
		return err
	}
	return nil
}

func (ww *WriterWrapper4) WriteAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash, original, value *uint256.Int) error {
	if err := ww.w.WriteAccountStorage(address.Bytes(), key.Bytes(), value.Bytes()); err != nil {
		return err
	}
	return nil
}

func (ww *WriterWrapper4) CreateContract(address libcommon.Address) error {
	return nil
}

type stat4 struct {
	prevBlock    uint64
	blockNum     uint64
	hits         uint64
	misses       uint64
	hitMissRatio float64
	blockSpeed   float64
	txSpeed      float64
	prevTxNum    uint64
	txNum        uint64
	prevTime     time.Time
	mem          runtime.MemStats
	startedAt    time.Time
}

func (s *stat4) print(aStats libstate.FilesStats, logger log.Logger) {
	totalFiles := aStats.FilesCount
	totalDatSize := aStats.DataSize
	totalIdxSize := aStats.IdxSize

	logger.Info("Progress", "block", s.blockNum, "blk/s", s.blockSpeed, "tx", s.txNum, "txn/s", s.txSpeed, "state files", totalFiles,
		"total dat", libcommon.ByteCount(totalDatSize), "total idx", libcommon.ByteCount(totalIdxSize),
		"hit ratio", s.hitMissRatio, "hits+misses", s.hits+s.misses,
		"alloc", libcommon.ByteCount(s.mem.Alloc), "sys", libcommon.ByteCount(s.mem.Sys),
	)
}

func (s *stat4) delta(blockNum, txNum uint64) *stat4 {
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
	if s.startedAt.IsZero() {
		s.startedAt = currentTime
	}

	total := s.hits + s.misses
	if total > 0 {
		s.hitMissRatio = float64(s.hits) / float64(total)
	}
	return s
}
