package commands

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	metrics2 "github.com/VictoriaMetrics/metrics"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/aggregator"
	"github.com/ledgerwatch/erigon-lib/commitment"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/eth/ethconsensusconfig"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

const (
	aggregationStep     = 15625                  /* this is 500'000 / 32 */
	unwindLimit         = 90000                  /* how it is in geth */
	logInterval         = 30 * time.Second       // time period to print aggregation stat to log
	dirtySpaceThreshold = 2 * 1024 * 1024 * 1024 /* threshold of dirty space in MDBX transaction that triggers a commit */
)

var (
	commitmentFrequency int // How many blocks to skip between calculating commitment
	changesets          bool
	commitments         bool
	commitmentTrie      string
)

var blockExecutionTimer = metrics2.GetOrCreateSummary("chain_execution_seconds")

func init() {
	withBlock(erigon2Cmd)
	withDataDir(erigon2Cmd)
	withSnapshotBlocks(erigon2Cmd)
	withChain(erigon2Cmd)

	erigon2Cmd.Flags().BoolVar(&changesets, "changesets", false, "set to true to generate changesets")
	erigon2Cmd.Flags().IntVar(&commitmentFrequency, "commfreq", 625, "how many blocks to skip between calculating commitment")
	erigon2Cmd.Flags().BoolVar(&commitments, "commitments", false, "set to true to calculate commitments")
	erigon2Cmd.Flags().StringVar(&commitmentTrie, "commitments.trie", "hex", "hex - use Hex Patricia Hashed Trie for commitments, bin - use of binary patricia trie")
	erigon2Cmd.Flags().IntVar(&traceBlock, "traceblock", 0, "block number at which to turn on tracing")
	rootCmd.AddCommand(erigon2Cmd)
}

var erigon2Cmd = &cobra.Command{
	Use:   "erigon2",
	Short: "Exerimental command to re-execute blocks from beginning using erigon2 state representation",
	RunE: func(cmd *cobra.Command, args []string) error {
		if commitmentFrequency < 1 || commitmentFrequency > aggregationStep {
			return fmt.Errorf("commitmentFrequency cannot be less than 1 or more than %d: %d", commitmentFrequency, aggregationStep)
		}
		logger := log.New()
		return Erigon2(genesis, chainConfig, logger)
	},
}

func Erigon2(genesis *core.Genesis, chainConfig *params.ChainConfig, logger log.Logger) error {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()

	historyDb, err := kv2.NewMDBX(logger).Path(path.Join(datadir, "chaindata")).Open()
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
	stateDbPath := path.Join(datadir, "statedb")
	if block == 0 {
		if _, err = os.Stat(stateDbPath); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		} else if err = os.RemoveAll(stateDbPath); err != nil {
			return err
		}
	}
	db, err2 := kv2.NewMDBX(logger).Path(stateDbPath).WriteMap().Open()
	if err2 != nil {
		return err2
	}
	defer db.Close()

	aggPath := filepath.Join(datadir, "aggregator")
	if block == 0 {
		if _, err = os.Stat(aggPath); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		} else if err = os.RemoveAll(aggPath); err != nil {
			return err
		}
		if err = os.Mkdir(aggPath, os.ModePerm); err != nil {
			return err
		}
	}

	var rwTx kv.RwTx
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	var blockRootMismatchExpected bool
	var trieVariant commitment.TrieVariant
	switch commitmentTrie {
	case "bin":
		trieVariant = commitment.VariantBinPatriciaTrie
		blockRootMismatchExpected = true
	case "hex":
		fallthrough
	default:
		trieVariant = commitment.VariantHexPatriciaTrie
	}

	trie := commitment.InitializeTrie(trieVariant)
	logger.Info("commitment trie initialized", "variant", trie.Variant())

	agg, err3 := aggregator.NewAggregator(aggPath, unwindLimit, aggregationStep, changesets, commitments, 100_000_000, trie, rwTx)
	if err3 != nil {
		return fmt.Errorf("create aggregator: %w", err3)
	}
	defer agg.Close()

	interrupt := false
	w := agg.MakeStateWriter(changesets /* beforeOn */)
	var rootHash []byte
	if block == 0 {
		genBlock, genesisIbs, err4 := genesis.ToBlock()
		if err4 != nil {
			return err4
		}
		if err = w.Reset(0, rwTx); err != nil {
			return err
		}
		if err = genesisIbs.CommitBlock(&params.Rules{}, &WriterWrapper{w: w}); err != nil {
			return fmt.Errorf("cannot write state: %w", err)
		}
		if err = w.FinishTx(0, false); err != nil {
			return err
		}
		if commitments {
			if rootHash, err = w.ComputeCommitment(false); err != nil {
				return err
			}
		}
		if err = w.Aggregate(false); err != nil {
			return err
		}
		if commitments && !blockRootMismatchExpected {
			if !bytes.Equal(rootHash, genBlock.Header().Root[:]) {
				return fmt.Errorf("root hash mismatch for genesis block, expected [%x], was [%x]", genBlock.Header().Root[:], rootHash)
			}
		}
	}

	logger.Info("Initialised chain configuration", "config", chainConfig)

	var (
		blockNum uint64
		trace    bool
		vmConfig vm.Config

		txNum uint64 = 2 // Consider that each block contains at least first system tx and enclosing transactions, except for Clique consensus engine
	)

	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	statx := &stat{
		prevBlock: blockNum,
		prevTime:  time.Now(),
	}

	go func() {
		for range logEvery.C {
			aStats := agg.Stats()
			statx.delta(aStats, blockNum).print(aStats, logger)
		}
	}()

	var blockReader services.FullBlockReader
	var allSnapshots *snapshotsync.RoSnapshots
	useSnapshots := ethconfig.UseSnapshotsByChainName(chainConfig.ChainName) && snapshotsCli
	if useSnapshots {
		var snapshotsPath string
		if snapdir != "" {
			snapshotsPath = snapdir
		} else {
			snapshotsPath = path.Join(datadir, "snapshots")
		}
		allSnapshots = snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), snapshotsPath)
		defer allSnapshots.Close()
		if err := allSnapshots.ReopenWithDB(db); err != nil {
			return fmt.Errorf("reopen snapshot segments: %w", err)
		}
		blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)
	} else {
		blockReader = snapshotsync.NewBlockReader()
	}
	engine := initConsensusEngine(chainConfig, logger, allSnapshots)

	for !interrupt {
		blockNum++
		trace = traceBlock > 0 && blockNum == uint64(traceBlock)
		blockHash, err := blockReader.CanonicalHash(ctx, historyTx, blockNum)
		if err != nil {
			return err
		}

		if blockNum <= block {
			_, _, txAmount := rawdb.ReadBody(historyTx, blockHash, blockNum)

			// Skip that block, but increase txNum
			txNum += uint64(txAmount) + 2 // Pre and Post block transaction
			continue
		}

		block, _, err := blockReader.BlockWithSenders(ctx, historyTx, blockHash, blockNum)
		if err != nil {
			return err
		}
		if block == nil {
			log.Info("history: block is nil", "block", blockNum)
			break
		}
		if err = w.Reset(blockNum, rwTx); err != nil {
			return err
		}

		r := agg.MakeStateReader(blockNum, rwTx)
		readWrapper := &ReaderWrapper{r: r, blockNum: blockNum}
		writeWrapper := &WriterWrapper{w: w, blockNum: blockNum}
		getHeader := func(hash common.Hash, number uint64) *types.Header {
			h, err := blockReader.Header(ctx, historyTx, hash, number)
			if err != nil {
				panic(err)
			}
			return h
		}

		txNum++ // Pre-block transaction

		if txNum, _, err = processBlock(trace, txNum, readWrapper, writeWrapper, chainConfig, engine, getHeader, block, vmConfig); err != nil {
			return fmt.Errorf("processing block %d: %w", blockNum, err)
		}
		if err := w.FinishTx(txNum, trace); err != nil {
			return fmt.Errorf("failed to finish tx: %w", err)
		}
		if trace {
			fmt.Printf("FinishTx called for %d block %d\n", txNum, blockNum)
		}

		txNum++ // Post-block transaction

		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			log.Info(fmt.Sprintf("interrupted, please wait for cleanup, next time start with --block %d", blockNum))
		default:
		}

		if commitments && (interrupt || blockNum%uint64(commitmentFrequency) == 0) {
			if rootHash, err = w.ComputeCommitment(trace /* trace */); err != nil {
				return err
			}
			if !blockRootMismatchExpected {
				if !bytes.Equal(rootHash, block.Header().Root[:]) {
					return fmt.Errorf("root hash mismatch for block %d, expected [%x], was [%x]", blockNum, block.Header().Root[:], rootHash)
				}
			}
		}
		if err = w.Aggregate(trace); err != nil {
			return err
		}
		// Commit transaction only when interrupted or just before computing commitment (so it can be re-done)
		commit := interrupt
		if !commit && (blockNum+1)%uint64(commitmentFrequency) == 0 {
			var spaceDirty uint64
			if spaceDirty, _, err = rwTx.(*mdbx.MdbxTx).SpaceDirty(); err != nil {
				return fmt.Errorf("retrieving spaceDirty: %w", err)
			}
			if spaceDirty >= dirtySpaceThreshold {
				log.Info("Initiated tx commit", "block", blockNum, "space dirty", libcommon.ByteCount(spaceDirty))
				commit = true
			}
		}
		if commit {
			if err = rwTx.Commit(); err != nil {
				return err
			}
			if !interrupt {
				if rwTx, err = db.BeginRw(ctx); err != nil {
					return err
				}
			}
		}
	}

	aStats := agg.Stats()
	statx.delta(aStats, blockNum).print(aStats, logger)
	if w != nil {
		w.Close()
	}
	return nil
}

type stat struct {
	blockNum     uint64
	hits         uint64
	misses       uint64
	prevBlock    uint64
	prevMisses   uint64
	prevHits     uint64
	hitMissRatio float64
	speed        float64
	prevTime     time.Time
	mem          runtime.MemStats
}

func (s *stat) print(aStats aggregator.FilesStats, logger log.Logger) {
	totalFiles := aStats.AccountsCount + aStats.CodeCount + aStats.StorageCount + aStats.CommitmentCount
	totalDatSize := aStats.AccountsDatSize + aStats.CodeDatSize + aStats.StorageDatSize + aStats.CommitmentDatSize
	totalIdxSize := aStats.AccountsIdxSize + aStats.CodeIdxSize + aStats.StorageIdxSize + aStats.CommitmentIdxSize

	logger.Info("Progress", "block", s.blockNum, "blk/s", s.speed, "state files", totalFiles,
		"accounts", libcommon.ByteCount(uint64(aStats.AccountsDatSize+aStats.AccountsIdxSize)),
		"code", libcommon.ByteCount(uint64(aStats.CodeDatSize+aStats.CodeIdxSize)),
		"storage", libcommon.ByteCount(uint64(aStats.StorageDatSize+aStats.StorageIdxSize)),
		"commitment", libcommon.ByteCount(uint64(aStats.CommitmentDatSize+aStats.CommitmentIdxSize)),
		"total dat", libcommon.ByteCount(uint64(totalDatSize)), "total idx", libcommon.ByteCount(uint64(totalIdxSize)),
		"hit ratio", s.hitMissRatio, "hits+misses", s.hits+s.misses,
		"alloc", libcommon.ByteCount(s.mem.Alloc), "sys", libcommon.ByteCount(s.mem.Sys),
	)
}

func (s *stat) delta(aStats aggregator.FilesStats, blockNum uint64) *stat {
	currentTime := time.Now()
	libcommon.ReadMemStats(&s.mem)

	interval := currentTime.Sub(s.prevTime).Seconds()
	s.blockNum = blockNum
	s.speed = float64(s.blockNum-s.prevBlock) / interval
	s.prevBlock = blockNum
	s.prevTime = currentTime

	s.hits = aStats.Hits - s.prevHits
	s.misses = aStats.Misses - s.prevMisses
	s.prevHits = aStats.Hits
	s.prevMisses = aStats.Misses

	total := s.hits + s.misses
	if total > 0 {
		s.hitMissRatio = float64(s.hits) / float64(total)
	}
	return s
}

func processBlock(trace bool, txNumStart uint64, rw *ReaderWrapper, ww *WriterWrapper, chainConfig *params.ChainConfig, engine consensus.Engine, getHeader func(hash common.Hash, number uint64) *types.Header, block *types.Block, vmConfig vm.Config) (uint64, types.Receipts, error) {
	defer blockExecutionTimer.UpdateDuration(time.Now())

	header := block.Header()
	vmConfig.TraceJumpDest = true
	gp := new(core.GasPool).AddGas(block.GasLimit())
	usedGas := new(uint64)
	var receipts types.Receipts
	daoBlock := chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0
	rules := chainConfig.Rules(block.NumberU64())
	txNum := txNumStart

	for i, tx := range block.Transactions() {
		ibs := state.New(rw)
		if daoBlock {
			misc.ApplyDAOHardFork(ibs)
			daoBlock = false
		}
		ibs.Prepare(tx.Hash(), block.Hash(), i)
		receipt, _, err := core.ApplyTransaction(chainConfig, core.GetHashFn(header, getHeader), engine, nil, gp, ibs, ww, header, tx, usedGas, vmConfig, nil)
		if err != nil {
			return 0, nil, fmt.Errorf("could not apply tx %d [%x] failed: %w", i, tx.Hash(), err)
		}
		receipts = append(receipts, receipt)
		if err = ww.w.FinishTx(txNum, trace); err != nil {
			return 0, nil, fmt.Errorf("finish tx %d [%x] failed: %w", i, tx.Hash(), err)
		}
		if trace {
			fmt.Printf("FinishTx called for %d [%x]\n", txNum, tx.Hash())
		}
		txNum++
	}

	ibs := state.New(rw)

	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	if _, _, _, err := engine.FinalizeAndAssemble(chainConfig, header, ibs, block.Transactions(), block.Uncles(), receipts, nil, nil, nil, nil); err != nil {
		return 0, nil, fmt.Errorf("finalize of block %d failed: %w", block.NumberU64(), err)
	}

	if err := ibs.CommitBlock(rules, ww); err != nil {
		return 0, nil, fmt.Errorf("committing block %d failed: %w", block.NumberU64(), err)
	}

	return txNum, receipts, nil
}

// Implements StateReader and StateWriter
type ReaderWrapper struct {
	blockNum uint64
	r        *aggregator.Reader
}

type WriterWrapper struct {
	blockNum uint64
	w        *aggregator.Writer
}

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
}

func (rw *ReaderWrapper) ReadAccountData(address common.Address) (*accounts.Account, error) {
	enc, err := rw.r.ReadAccountData(address.Bytes(), false /* trace */)
	if err != nil {
		return nil, err
	}
	if len(enc) == 0 {
		return nil, nil
	}

	var a accounts.Account
	a.Reset()
	pos := 0
	nonceBytes := int(enc[pos])
	pos++
	if nonceBytes > 0 {
		a.Nonce = bytesToUint64(enc[pos : pos+nonceBytes])
		pos += nonceBytes
	}
	balanceBytes := int(enc[pos])
	pos++
	if balanceBytes > 0 {
		a.Balance.SetBytes(enc[pos : pos+balanceBytes])
		pos += balanceBytes
	}
	codeHashBytes := int(enc[pos])
	pos++
	if codeHashBytes > 0 {
		copy(a.CodeHash[:], enc[pos:pos+codeHashBytes])
		pos += codeHashBytes
	}
	incBytes := int(enc[pos])
	pos++
	if incBytes > 0 {
		a.Incarnation = bytesToUint64(enc[pos : pos+incBytes])
	}
	return &a, nil
}

func (rw *ReaderWrapper) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	trace := false
	enc, err := rw.r.ReadAccountStorage(address.Bytes(), key.Bytes(), trace)
	if err != nil {
		return nil, err
	}
	if enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (rw *ReaderWrapper) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	return rw.r.ReadAccountCode(address.Bytes(), false /* trace */)
}

func (rw *ReaderWrapper) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	return rw.r.ReadAccountCodeSize(address.Bytes(), false /* trace */)
}

func (rw *ReaderWrapper) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}

func (ww *WriterWrapper) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
	var l int
	l++
	if account.Nonce > 0 {
		l += (bits.Len64(account.Nonce) + 7) / 8
	}
	l++
	if !account.Balance.IsZero() {
		l += account.Balance.ByteLen()
	}
	l++
	if !account.IsEmptyCodeHash() {
		l += 32
	}
	l++
	if account.Incarnation > 0 {
		l += (bits.Len64(account.Incarnation) + 7) / 8
	}
	value := make([]byte, l)
	pos := 0
	if account.Nonce == 0 {
		value[pos] = 0
		pos++
	} else {
		nonceBytes := (bits.Len64(account.Nonce) + 7) / 8
		value[pos] = byte(nonceBytes)
		var nonce = account.Nonce
		for i := nonceBytes; i > 0; i-- {
			value[pos+i] = byte(nonce)
			nonce >>= 8
		}
		pos += nonceBytes + 1
	}
	if account.Balance.IsZero() {
		value[pos] = 0
		pos++
	} else {
		balanceBytes := account.Balance.ByteLen()
		value[pos] = byte(balanceBytes)
		pos++
		account.Balance.WriteToSlice(value[pos : pos+balanceBytes])
		pos += balanceBytes
	}
	if account.IsEmptyCodeHash() {
		value[pos] = 0
		pos++
	} else {
		value[pos] = 32
		pos++
		copy(value[pos:pos+32], account.CodeHash[:])
		pos += 32
	}
	if account.Incarnation == 0 {
		value[pos] = 0
	} else {
		incBytes := (bits.Len64(account.Incarnation) + 7) / 8
		value[pos] = byte(incBytes)
		var inc = account.Incarnation
		for i := incBytes; i > 0; i-- {
			value[pos+i] = byte(inc)
			inc >>= 8
		}
	}
	if err := ww.w.UpdateAccountData(address.Bytes(), value, false /* trace */); err != nil {
		return err
	}
	return nil
}

func (ww *WriterWrapper) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if err := ww.w.UpdateAccountCode(address.Bytes(), code, false /* trace */); err != nil {
		return err
	}
	return nil
}

func (ww *WriterWrapper) DeleteAccount(address common.Address, original *accounts.Account) error {
	if err := ww.w.DeleteAccount(address.Bytes(), false /* trace */); err != nil {
		return err
	}
	return nil
}

func (ww *WriterWrapper) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	trace := false
	if trace {
		fmt.Printf("block %d, WriteAccountStorage %x %x, original %s, value %s\n", ww.blockNum, address, *key, original, value)
	}
	if err := ww.w.WriteAccountStorage(address.Bytes(), key.Bytes(), value.Bytes(), trace); err != nil {
		return err
	}
	return nil
}

func (ww *WriterWrapper) CreateContract(address common.Address) error {
	return nil
}

func initConsensusEngine(chainConfig *params.ChainConfig, logger log.Logger, snapshots *snapshotsync.RoSnapshots) (engine consensus.Engine) {
	config := ethconfig.Defaults

	switch {
	case chainConfig.Clique != nil:
		c := params.CliqueSnapshot
		c.DBPath = filepath.Join(datadir, "clique", "db")
		engine = ethconsensusconfig.CreateConsensusEngine(chainConfig, logger, c, config.Miner.Notify, config.Miner.Noverify, "", true, datadir, snapshots, true /* readonly */)
	case chainConfig.Aura != nil:
		consensusConfig := &params.AuRaConfig{DBPath: filepath.Join(datadir, "aura")}
		engine = ethconsensusconfig.CreateConsensusEngine(chainConfig, logger, consensusConfig, config.Miner.Notify, config.Miner.Noverify, "", true, datadir, snapshots, true /* readonly */)
	case chainConfig.Parlia != nil:
		consensusConfig := &params.ParliaConfig{DBPath: filepath.Join(datadir, "parlia")}
		engine = ethconsensusconfig.CreateConsensusEngine(chainConfig, logger, consensusConfig, config.Miner.Notify, config.Miner.Noverify, "", true, datadir, snapshots, true /* readonly */)
	case chainConfig.Bor != nil:
		consensusConfig := &config.Bor
		engine = ethconsensusconfig.CreateConsensusEngine(chainConfig, logger, consensusConfig, config.Miner.Notify, config.Miner.Noverify, "http://localhost:1317", false, datadir, snapshots, true /* readonly */)
	default: //ethash
		engine = ethash.NewFaker()
	}
	return
}
