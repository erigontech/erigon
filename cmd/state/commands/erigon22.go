package commands

import (
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

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

const (
	AggregationStep = 3_125_000 /* number of transactions in smallest static file */
)

func init() {
	withBlock(erigon22Cmd)
	withDataDir(erigon22Cmd)
	withChain(erigon22Cmd)

	rootCmd.AddCommand(erigon22Cmd)
}

var erigon22Cmd = &cobra.Command{
	Use:   "erigon22",
	Short: "Exerimental command to re-execute blocks from beginning using erigon2 state representation and histoty (ugrade 2)",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return Erigon22(genesis, chainConfig, logger)
	},
}

func Erigon22(genesis *core.Genesis, chainConfig *params.ChainConfig, logger log.Logger) error {
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

	aggPath := filepath.Join(datadir, "erigon22")
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

	agg, err3 := libstate.NewAggregator(aggPath, AggregationStep)
	if err3 != nil {
		return fmt.Errorf("create aggregator: %w", err3)
	}
	defer agg.Close()

	interrupt := false
	if block == 0 {
		_, genesisIbs, err := genesis.ToBlock()
		if err != nil {
			return err
		}
		agg.SetTx(rwTx)
		agg.SetTxNum(0)
		if err = genesisIbs.CommitBlock(&params.Rules{}, &WriterWrapper22{w: agg}); err != nil {
			return fmt.Errorf("cannot write state: %w", err)
		}
		if err = agg.FinishTx(); err != nil {
			return err
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

	statx := &stat22{
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
	allSnapshots = snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), path.Join(datadir, "snapshots"))
	defer allSnapshots.Close()
	if err := allSnapshots.Reopen(); err != nil {
		return fmt.Errorf("reopen snapshot segments: %w", err)
	}
	blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)
	engine := initConsensusEngine(chainConfig, logger, allSnapshots)

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
			log.Info("history: block is nil", "block", blockNum)
			break
		}
		if blockNum <= block {
			// Skip that block, but increase txNum
			txNum += uint64(len(b.Transactions())) + 2 // Pre and Post block transaction
			continue
		}
		agg.SetTx(rwTx)
		agg.SetTxNum(txNum)

		readWrapper := &ReaderWrapper22{r: agg, roTx: rwTx, blockNum: blockNum}
		writeWrapper := &WriterWrapper22{w: agg, blockNum: blockNum}
		getHeader := func(hash common.Hash, number uint64) *types.Header {
			h, err := blockReader.Header(ctx, historyTx, hash, number)
			if err != nil {
				panic(err)
			}
			return h
		}

		txNum++ // Pre-block transaction
		agg.SetTxNum(txNum)

		if txNum, _, err = processBlock22(trace, txNum, readWrapper, writeWrapper, chainConfig, engine, getHeader, b, vmConfig); err != nil {
			return fmt.Errorf("processing block %d: %w", blockNum, err)
		}
		agg.SetTxNum(txNum)
		if err := agg.FinishTx(); err != nil {
			return fmt.Errorf("failed to finish tx: %w", err)
		}
		if trace {
			fmt.Printf("FinishTx called for %d block %d\n", txNum, blockNum)
		}

		txNum++ // Post-block transaction
		agg.SetTxNum(txNum)

		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			log.Info(fmt.Sprintf("interrupted, please wait for cleanup, next time start with --block %d", blockNum))
		default:
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

	return nil
}

type stat22 struct {
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

func (s *stat22) print(aStats libstate.FilesStats, logger log.Logger) {
	totalFiles := 0
	totalDatSize := 0
	totalIdxSize := 0

	logger.Info("Progress", "block", s.blockNum, "blk/s", s.speed, "state files", totalFiles,
		"total dat", libcommon.ByteCount(uint64(totalDatSize)), "total idx", libcommon.ByteCount(uint64(totalIdxSize)),
		"hit ratio", s.hitMissRatio, "hits+misses", s.hits+s.misses,
		"alloc", libcommon.ByteCount(s.mem.Alloc), "sys", libcommon.ByteCount(s.mem.Sys),
	)
}

func (s *stat22) delta(aStats libstate.FilesStats, blockNum uint64) *stat22 {
	currentTime := time.Now()
	libcommon.ReadMemStats(&s.mem)

	interval := currentTime.Sub(s.prevTime).Seconds()
	s.blockNum = blockNum
	s.speed = float64(s.blockNum-s.prevBlock) / interval
	s.prevBlock = blockNum
	s.prevTime = currentTime

	total := s.hits + s.misses
	if total > 0 {
		s.hitMissRatio = float64(s.hits) / float64(total)
	}
	return s
}

func processBlock22(trace bool, txNumStart uint64, rw *ReaderWrapper22, ww *WriterWrapper22, chainConfig *params.ChainConfig, engine consensus.Engine, getHeader func(hash common.Hash, number uint64) *types.Header, block *types.Block, vmConfig vm.Config) (uint64, types.Receipts, error) {
	defer blockExecutionTimer.UpdateDuration(time.Now())

	header := block.Header()
	vmConfig.Debug = true
	gp := new(core.GasPool).AddGas(block.GasLimit())
	usedGas := new(uint64)
	var receipts types.Receipts
	daoBlock := chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0
	rules := chainConfig.Rules(block.NumberU64())
	txNum := txNumStart
	ww.w.SetTxNum(txNum)
	trace = block.NumberU64() == 1700059

	for i, tx := range block.Transactions() {
		ibs := state.New(rw)
		if daoBlock {
			misc.ApplyDAOHardFork(ibs)
			daoBlock = false
		}
		ibs.Prepare(tx.Hash(), block.Hash(), i)
		ct := NewCallTracer()
		vmConfig.Tracer = ct
		receipt, _, err := core.ApplyTransaction(chainConfig, getHeader, engine, nil, gp, ibs, ww, header, tx, usedGas, vmConfig, nil)
		if err != nil {
			return 0, nil, fmt.Errorf("could not apply tx %d [%x] failed: %w", i, tx.Hash(), err)
		}
		for from := range ct.froms {
			if err := ww.w.AddTraceFrom(from[:]); err != nil {
				return 0, nil, err
			}
		}
		for to := range ct.tos {
			if trace {
				fmt.Printf("TraceTo [%x]\n", to[:])
			}
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
			fmt.Printf("FinishTx called for %d [%x]\n", txNum, tx.Hash())
		}
		txNum++
		ww.w.SetTxNum(txNum)
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
	if _, _, _, err := engine.FinalizeAndAssemble(chainConfig, header, ibs, block.Transactions(), block.Uncles(), receipts, nil, nil, nil, nil); err != nil {
		return 0, nil, fmt.Errorf("finalize of block %d failed: %w", block.NumberU64(), err)
	}

	if err := ibs.CommitBlock(rules, ww); err != nil {
		return 0, nil, fmt.Errorf("committing block %d failed: %w", block.NumberU64(), err)
	}

	return txNum, receipts, nil
}

// Implements StateReader and StateWriter
type ReaderWrapper22 struct {
	blockNum uint64
	roTx     kv.Tx
	r        *libstate.Aggregator
}

type WriterWrapper22 struct {
	blockNum uint64
	w        *libstate.Aggregator
}

func (rw *ReaderWrapper22) ReadAccountData(address common.Address) (*accounts.Account, error) {
	enc, err := rw.r.ReadAccountData(address.Bytes(), rw.roTx)
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

func (rw *ReaderWrapper22) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	enc, err := rw.r.ReadAccountStorage(address.Bytes(), key.Bytes(), rw.roTx)
	if err != nil {
		return nil, err
	}
	if enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (rw *ReaderWrapper22) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	return rw.r.ReadAccountCode(address.Bytes(), rw.roTx)
}

func (rw *ReaderWrapper22) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	return rw.r.ReadAccountCodeSize(address.Bytes(), rw.roTx)
}

func (rw *ReaderWrapper22) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}

func (ww *WriterWrapper22) UpdateAccountData(address common.Address, original, account *accounts.Account) error {
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
	if err := ww.w.UpdateAccountData(address.Bytes(), value); err != nil {
		return err
	}
	return nil
}

func (ww *WriterWrapper22) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	if err := ww.w.UpdateAccountCode(address.Bytes(), code); err != nil {
		return err
	}
	return nil
}

func (ww *WriterWrapper22) DeleteAccount(address common.Address, original *accounts.Account) error {
	if err := ww.w.DeleteAccount(address.Bytes()); err != nil {
		return err
	}
	return nil
}

func (ww *WriterWrapper22) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	if err := ww.w.WriteAccountStorage(address.Bytes(), key.Bytes(), value.Bytes()); err != nil {
		return err
	}
	return nil
}

func (ww *WriterWrapper22) CreateContract(address common.Address) error {
	return nil
}
