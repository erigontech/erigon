package commands

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/bits"
	"path/filepath"
	"runtime"
	"time"

	"github.com/holiman/uint256"
	chain2 "github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/commitment"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"

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
	erigoncli "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/ledgerwatch/erigon/turbo/services"
)

func init() {
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

	rootCmd.AddCommand(stateDomains)
}

// if trie variant is not hex, we could not have another rootHash with to verify it
var (
	dirtySpaceThreshold       = uint64(2 * 1024 * 1024 * 1024) /* threshold of dirty space in MDBX transaction that triggers a commit */
	blockRootMismatchExpected bool
)

var stateDomains = &cobra.Command{
	Use:     "state_domains",
	Short:   `Run block execution and commitment with Domains.`,
	Example: "go run ./cmd/integration state_domains --datadir=... --verbosity=3 --unwind=100 --unwind.every=100000 --block=2000000",
	Run: func(cmd *cobra.Command, args []string) {
		ctx, _ := libcommon.RootContext()
		cfg := &nodecfg.DefaultConfig
		utils.SetNodeConfigCobra(cmd, cfg)
		ethConfig := &ethconfig.Defaults
		ethConfig.Genesis = core.DefaultGenesisBlockByChainName(chain)
		erigoncli.ApplyFlagsForEthConfigCobra(cmd.Flags(), ethConfig)

		dirs := datadir.New(datadirCli)
		chainDb := openDB(dbCfg(kv.ChainDB, dirs.Chaindata), true)
		defer chainDb.Close()

		stateDB := kv.Label(6)

		stateDb := openDB(dbCfg(stateDB, filepath.Join(dirs.DataDir, "state")), true)
		defer stateDb.Close()

		if err := loopProcessDomains(chainDb, stateDb, ctx); err != nil {
			if !errors.Is(err, context.Canceled) {
				log.Error(err.Error())
			}
			return
		}
	},
}

func ParseTrieVariant(s string) commitment.TrieVariant {
	var trieVariant commitment.TrieVariant
	switch s {
	case "bin":
		trieVariant = commitment.VariantBinPatriciaTrie
	case "hex":
		fallthrough
	default:
		trieVariant = commitment.VariantHexPatriciaTrie
	}
	return trieVariant
}

func ParseCommitmentMode(s string) libstate.CommitmentMode {
	var mode libstate.CommitmentMode
	switch s {
	case "off":
		mode = libstate.CommitmentModeDisabled
	case "update":
		mode = libstate.CommitmentModeUpdate
	default:
		mode = libstate.CommitmentModeDirect
	}
	return mode
}

func loopProcessDomains(chainDb, stateDb kv.RwDB, ctx context.Context) error {
	trieVariant := ParseTrieVariant(commitmentTrie)
	if trieVariant != commitment.VariantHexPatriciaTrie {
		blockRootMismatchExpected = true
	}
	mode := ParseCommitmentMode(commitmentMode)

	engine, _, _, agg := newDomains(ctx, chainDb, mode, trieVariant)
	defer agg.Close()

	histTx, err := chainDb.BeginRo(ctx)
	must(err)
	defer histTx.Rollback()

	stateTx, err := stateDb.BeginRw(ctx)
	must(err)
	defer stateTx.Rollback()

	agg.SetTx(stateTx)
	defer agg.StartWrites().FinishWrites()

	latestTx, err := agg.SeekCommitment()
	if err != nil && startTxNum != 0 {
		return fmt.Errorf("failed to seek commitment to tx %d: %w", startTxNum, err)
	}
	if latestTx < startTxNum {
		return fmt.Errorf("latest available tx to start is  %d and its less than start tx %d", latestTx, startTxNum)
	}
	fmt.Printf("Max txNum in files: %d\n", latestTx)

	var (
		interrupt bool
		blockNum  uint64
		txNum     uint64

		readWrapper  = &ReaderWrapper4{ac: agg.MakeContext(), roTx: histTx}
		writeWrapper = &WriterWrapper4{w: agg}
		started      = time.Now()
	)

	commitFn := func(txn uint64) error {
		if stateDb == nil || stateTx == nil {
			return fmt.Errorf("commit failed due to invalid chainDb/rwTx")
		}
		var spaceDirty uint64
		if spaceDirty, _, err = stateTx.(*kv2.MdbxTx).SpaceDirty(); err != nil {
			return fmt.Errorf("retrieving spaceDirty: %w", err)
		}
		if spaceDirty >= dirtySpaceThreshold {
			log.Info("Initiated tx commit", "block", blockNum, "space dirty", libcommon.ByteCount(spaceDirty))
		}
		log.Info("database commitment", "block", blockNum, "txNum", txn, "uptime", time.Since(started))
		if err := agg.Flush(ctx); err != nil {
			return err
		}
		if err = stateTx.Commit(); err != nil {
			return err
		}
		if interrupt {
			return nil
		}

		if stateTx, err = stateDb.BeginRw(ctx); err != nil {
			return err
		}

		readWrapper.ac.Close()
		agg.SetTx(stateTx)
		readWrapper.roTx = stateTx
		readWrapper.ac = agg.MakeContext()
		return nil
	}

	blockReader := getBlockReader(chainDb)
	mergedRoots := agg.AggregatedRoots()

	proc := blockProcessor{
		chainConfig: fromdb.ChainConfig(chainDb),
		vmConfig:    vm.Config{},
		engine:      engine,
		reader:      readWrapper,
		writer:      writeWrapper,
		blockReader: blockReader,
		agg:         agg,
		getHeader: func(hash libcommon.Hash, number uint64) *types.Header {
			h, err := blockReader.Header(ctx, histTx /*, historyTx*/, hash, number)
			if err != nil {
				panic(err)
			}
			return h
		},
	}

	go proc.PrintStatsLoop(ctx, 30*time.Second, log.New())

	if startTxNum == 0 {
		genesis := core.DefaultGenesisBlockByChainName(chain)
		if err := proc.ApplyGenesis(genesis); err != nil {
			return err
		}
	}

	for {
		err := proc.ProcessNext(ctx, stateTx)
		if err != nil {
			return err
		}

		// Check for interrupts
		select {
		case <-ctx.Done():
			interrupt = true
			// Commit transaction only when interrupted or just before computing commitment (so it can be re-done)
			if err := proc.agg.Flush(ctx); err != nil {
				log.Error("aggregator flush", "err", err)
			}

			log.Info(fmt.Sprintf("interrupted, please wait for cleanup, next time start with --tx %d", txNum))
			if err := commitFn(txNum); err != nil {
				log.Error("chainDb commit", "err", err)
			}
			return nil
		case <-mergedRoots: // notified with rootHash of latest aggregation
			if err := commitFn(txNum); err != nil {
				log.Error("chainDb commit on merge", "err", err)
			}
		default:
		}
	}
	return nil
}

type blockProcessor struct {
	vmConfig    vm.Config
	chainConfig *chain2.Config
	engine      consensus.Engine
	agg         *libstate.Aggregator
	stat        stat4
	trace       bool

	blockReader services.FullBlockReader
	writer      *WriterWrapper4
	reader      *ReaderWrapper4

	blockNum uint64
	txNum    uint64

	getHeader func(hash libcommon.Hash, number uint64) *types.Header
}

func (b *blockProcessor) PrintStatsLoop(ctx context.Context, interval time.Duration, logger log.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.stat.delta(b.blockNum, b.txNum).print(b.agg.Stats(), logger)
		}
	}
}

func (b *blockProcessor) ApplyGenesis(genesis *core.Genesis) error {
	genBlock, genesisIbs, err := genesis.ToBlock("")
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
	if blockRootMismatchExpected && !bytes.Equal(blockRootHash, genesisRootHash[:]) {
		return fmt.Errorf("genesis root hash mismatch: expected %x got %x", genesisRootHash, blockRootHash)
	}
	return nil
}

func (b *blockProcessor) blockHeader(hash libcommon.Hash, number uint64) *types.Header {
	return b.getHeader(hash, number)
}

func (b *blockProcessor) ProcessNext(ctx context.Context, tx kv.RwTx) error {
	b.blockNum++
	b.trace = traceFromTx > 0 && b.txNum == traceFromTx
	blockHash, err := b.blockReader.CanonicalHash(ctx, tx, b.blockNum)
	if err != nil {
		return err
	}

	block, _, err := b.blockReader.BlockWithSenders(ctx, tx, blockHash, b.blockNum)
	if err != nil {
		return err
	}
	if block == nil {
		log.Info("history: block is nil", "block", b.blockNum)
		return fmt.Errorf("block %d is nil", b.blockNum)
	}

	b.agg.SetTx(tx)
	b.agg.SetTxNum(b.txNum)

	if _, err = b.applyBlock(startTxNum, b.reader, b.writer, block); err != nil {
		log.Error("processing error", "block", b.blockNum, "err", err)
		return fmt.Errorf("processing block %d: %w", b.blockNum, err)
	}
	return err
}

func (b *blockProcessor) applyBlock(
	 startTxNum uint64,
	 rw *ReaderWrapper4,
	 ww *WriterWrapper4,
	 block *types.Block,
) (types.Receipts, error) {
	//defer blockExecutionTimer.UpdateDuration(time.Now())

	header := block.Header()
	b.vmConfig.Debug = true
	gp := new(core.GasPool).AddGas(block.GasLimit())
	usedGas := new(uint64)
	var receipts types.Receipts
	rules := b.chainConfig.Rules(block.NumberU64(), block.Time())

	rw.blockNum = block.NumberU64()
	b.blockNum = rw.blockNum
	ww.w.SetTxNum(b.txNum)

	daoFork := b.txNum >= startTxNum && b.chainConfig.DAOForkSupport && b.chainConfig.DAOForkBlock != nil && b.chainConfig.DAOForkBlock.Cmp(block.Number()) == 0
	if daoFork {
		ibs := state.New(rw)
		// TODO Actually add tracing to the DAO related accounts
		misc.ApplyDAOHardFork(ibs)
		if err := ibs.FinalizeTx(rules, ww); err != nil {
			return nil, err
		}
		if err := ww.w.FinishTx(); err != nil {
			return nil, fmt.Errorf("finish daoFork failed: %w", err)
		}
	}

	b.txNum++ // Pre-block transaction
	ww.w.SetTxNum(b.txNum)
	if err := ww.w.FinishTx(); err != nil {
		return nil, fmt.Errorf("finish pre-block tx %d (block %d) has failed: %w", b.txNum, block.NumberU64(), err)
	}

	getHashFn := core.GetHashFn(header, b.getHeader)

	for i, tx := range block.Transactions() {
		if b.txNum >= startTxNum {
			ibs := state.New(rw)
			ibs.Prepare(tx.Hash(), block.Hash(), i)
			ct := exec3.NewCallTracer()
			b.vmConfig.Tracer = ct
			receipt, _, err := core.ApplyTransaction(b.chainConfig, getHashFn, b.engine, nil, gp, ibs, ww, header, tx, usedGas, b.vmConfig)
			if err != nil {
				return nil, fmt.Errorf("could not apply tx %d [%x] failed: %w", i, tx.Hash(), err)
			}
			for from := range ct.Froms() {
				if err := ww.w.AddTraceFrom(from[:]); err != nil {
					return nil, err
				}
			}
			for to := range ct.Tos() {
				if err := ww.w.AddTraceTo(to[:]); err != nil {
					return nil, err
				}
			}
			receipts = append(receipts, receipt)
			for _, log := range receipt.Logs {
				if err = ww.w.AddLogAddr(log.Address[:]); err != nil {
					return nil, fmt.Errorf("adding event log for addr %x: %w", log.Address, err)
				}
				for _, topic := range log.Topics {
					if err = ww.w.AddLogTopic(topic[:]); err != nil {
						return nil, fmt.Errorf("adding event log for topic %x: %w", topic, err)
					}
				}
			}
			if err = ww.w.FinishTx(); err != nil {
				return nil, fmt.Errorf("finish tx %d [%x] failed: %w", i, tx.Hash(), err)
			}
			if b.trace {
				fmt.Printf("FinishTx called for blockNum=%d, txIndex=%d, txNum=%d txHash=[%x]\n", b.blockNum, i, b.txNum, tx.Hash())
			}
		}
		b.txNum++
		ww.w.SetTxNum(b.txNum)
	}

	if b.txNum >= startTxNum {
		if b.chainConfig.IsByzantium(block.NumberU64()) {
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
			return nil, fmt.Errorf("adding coinbase trace: %w", err)
		}
		for _, uncle := range block.Uncles() {
			if err := ww.w.AddTraceTo(uncle.Coinbase.Bytes()); err != nil {
				return nil, fmt.Errorf("adding uncle trace: %w", err)
			}
		}

		// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
		if _, _, err := b.engine.Finalize(b.chainConfig, header, ibs, block.Transactions(), block.Uncles(), receipts, block.Withdrawals(), nil, nil, nil); err != nil {
			return nil, fmt.Errorf("finalize of block %d failed: %w", block.NumberU64(), err)
		}

		if err := ibs.CommitBlock(rules, ww); err != nil {
			return nil, fmt.Errorf("committing block %d failed: %w", block.NumberU64(), err)
		}

		if err := ww.w.FinishTx(); err != nil {
			return nil, fmt.Errorf("failed to finish tx: %w", err)
		}
		if b.trace {
			fmt.Printf("FinishTx called for %d block %d\n", b.txNum, block.NumberU64())
		}
	}

	b.txNum++ // Post-block transaction
	ww.w.SetTxNum(b.txNum)
	if b.txNum >= startTxNum {
		if block.Number().Uint64()%uint64(commitmentFreq) == 0 {
			rootHash, err := ww.w.ComputeCommitment(true, b.trace)
			if err != nil {
				return nil, err
			}
			if !blockRootMismatchExpected && !bytes.Equal(rootHash, header.Root[:]) {
				return nil, fmt.Errorf("invalid root hash for block %d: expected %x got %x", block.NumberU64(), header.Root, rootHash)
			}
		}

		if err := ww.w.FinishTx(); err != nil {
			return nil, fmt.Errorf("finish after-block tx %d (block %d) has failed: %w", b.txNum, block.NumberU64(), err)
		}
	}

	return receipts, nil
}

// Implements StateReader and StateWriter
type ReaderWrapper4 struct {
	roTx     kv.Tx
	ac       *libstate.AggregatorContext
	blockNum uint64
}

type WriterWrapper4 struct {
	w *libstate.Aggregator
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

func bytesToUint64(buf []byte) (x uint64) {
	for i, b := range buf {
		x = x<<8 + uint64(b)
		if i == 7 {
			return
		}
	}
	return
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

	total := s.hits + s.misses
	if total > 0 {
		s.hitMissRatio = float64(s.hits) / float64(total)
	}
	return s
}

