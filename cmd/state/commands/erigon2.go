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
	"syscall"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/aggregator"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

const (
	aggregationStep = 15625 /* this is 500'000 / 32 */
	unwindLimit     = 90000 /* how it is in geth */
)

var (
	commitmentFrequency int // How many blocks to skip between calculating commitment
	changesets          bool
)

func init() {
	withBlock(erigon2Cmd)
	withDatadir(erigon2Cmd)
	erigon2Cmd.Flags().BoolVar(&changesets, "changesets", false, "set to true to generate changesets")
	erigon2Cmd.Flags().IntVar(&commitmentFrequency, "commfreq", 625, "how many blocks to skip between calculating commitment")
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
		return Erigon2(genesis, logger)
	},
}

const (
	logInterval = 30 * time.Second
)

func Erigon2(genesis *core.Genesis, logger log.Logger) error {
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
	aggPath := path.Join(datadir, "aggregator")
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
	agg, err3 := aggregator.NewAggregator(aggPath, unwindLimit, aggregationStep)
	if err3 != nil {
		return fmt.Errorf("create aggregator: %w", err3)
	}
	defer agg.Close()
	agg.GenerateChangesets(changesets)
	chainConfig := genesis.Config
	vmConfig := vm.Config{}

	interrupt := false
	blockNum := block
	w := agg.MakeStateWriter(false /* beforeOn */)
	var rootHash []byte
	if block == 0 {
		genBlock, genesisIbs, err4 := genesis.ToBlock()
		if err4 != nil {
			return err4
		}
		if err = w.Reset(0); err != nil {
			return err
		}
		if err = genesisIbs.CommitBlock(params.Rules{}, &WriterWrapper{w: w}); err != nil {
			return fmt.Errorf("cannot write state: %w", err)
		}
		if err = w.FinishTx(0, false); err != nil {
			return err
		}
		if rootHash, err = w.ComputeCommitment(false); err != nil {
			return err
		}
		if err = w.Aggregate(false); err != nil {
			return err
		}
		if !bytes.Equal(rootHash, genBlock.Header().Root[:]) {
			return fmt.Errorf("root hash mismatch for genesis block, expected [%x], was [%x]", genBlock.Header().Root[:], rootHash)
		}
	}
	var txNum uint64 = 1
	trace := false
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	prevBlock := blockNum
	prevTime := time.Now()
	for !interrupt {
		select {
		default:
		case <-logEvery.C:
			aStats := agg.Stats()
			totalFiles := aStats.AccountsCount + aStats.CodeCount + aStats.StorageCount + aStats.CommitmentCount
			totalDatSize := aStats.AccountsDatSize + aStats.CodeDatSize + aStats.StorageDatSize + aStats.CommitmentDatSize
			totalIdxSize := aStats.AccountsIdxSize + aStats.CodeIdxSize + aStats.StorageIdxSize + aStats.CommitmentIdxSize
			currentTime := time.Now()
			interval := currentTime.Sub(prevTime)
			speed := float64(blockNum-prevBlock) / (float64(interval) / float64(time.Second))
			prevBlock = blockNum
			prevTime = currentTime
			log.Info("Progress", "block", blockNum, "blk/s", speed, "state files", totalFiles,
				"accounts", libcommon.ByteCount(uint64(aStats.AccountsDatSize+aStats.AccountsIdxSize)),
				"code", libcommon.ByteCount(uint64(aStats.CodeDatSize+aStats.CodeIdxSize)),
				"storage", libcommon.ByteCount(uint64(aStats.StorageDatSize+aStats.StorageIdxSize)),
				"commitment", libcommon.ByteCount(uint64(aStats.CommitmentDatSize+aStats.CommitmentIdxSize)),
				"total dat", libcommon.ByteCount(uint64(totalDatSize)), "total idx", libcommon.ByteCount(uint64(totalIdxSize)),
			)
		}
		blockNum++
		blockHash, err := rawdb.ReadCanonicalHash(historyTx, blockNum)
		if err != nil {
			return err
		}
		var b *types.Block
		b, _, err = rawdb.ReadBlockWithSenders(historyTx, blockHash, blockNum)
		if err != nil {
			return err
		}
		if b == nil {
			break
		}
		r := agg.MakeStateReader(blockNum)
		if err = w.Reset(blockNum); err != nil {
			return err
		}
		readWrapper := &ReaderWrapper{r: r, blockNum: blockNum}
		writeWrapper := &WriterWrapper{w: w, blockNum: blockNum}
		getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(historyTx, hash, number) }
		if txNum, _, err = runBlock2(trace, txNum, readWrapper, writeWrapper, chainConfig, getHeader, b, vmConfig); err != nil {
			return fmt.Errorf("block %d: %w", blockNum, err)
		}
		if err := w.FinishTx(txNum, trace); err != nil {
			return fmt.Errorf("final finish failed: %w", err)
		}
		if trace {
			fmt.Printf("FinishTx called for %d block %d\n", txNum, blockNum)
		}
		txNum++
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			log.Info(fmt.Sprintf("interrupted, please wait for cleanup, next time start with --block %d", blockNum))
		default:
		}
		if interrupt || blockNum%uint64(commitmentFrequency) == 0 {
			if rootHash, err = w.ComputeCommitment(trace /* trace */); err != nil {
				return err
			}
			if !bytes.Equal(rootHash, b.Header().Root[:]) {
				return fmt.Errorf("root hash mismatch for block %d, expected [%x], was [%x]", blockNum, b.Header().Root[:], rootHash)
			}
		}
		if err = w.Aggregate(trace); err != nil {
			return err
		}
	}
	if w != nil {
		w.Close()
	}
	return nil
}

func runBlock2(trace bool, txNumStart uint64, rw *ReaderWrapper, ww *WriterWrapper, chainConfig *params.ChainConfig, getHeader func(hash common.Hash, number uint64) *types.Header, block *types.Block, vmConfig vm.Config) (uint64, types.Receipts, error) {
	header := block.Header()
	vmConfig.TraceJumpDest = true
	engine := ethash.NewFullFaker()
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
		receipt, _, err := core.ApplyTransaction(chainConfig, getHeader, engine, nil, gp, ibs, ww, header, tx, usedGas, vmConfig, nil)
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
	enc := rw.r.ReadAccountData(address.Bytes(), false /* trace */)
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
	enc := rw.r.ReadAccountStorage(address.Bytes(), key.Bytes(), trace)
	if enc == nil {
		return nil, nil
	}
	return enc.Bytes(), nil
}

func (rw *ReaderWrapper) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	return rw.r.ReadAccountCode(address.Bytes(), false /* trace */), nil
}

func (rw *ReaderWrapper) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	return rw.r.ReadAccountCodeSize(address.Bytes(), false /* trace */), nil
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
	ww.w.UpdateAccountData(address.Bytes(), value, false /* trace */)
	return nil
}

func (ww *WriterWrapper) UpdateAccountCode(address common.Address, incarnation uint64, codeHash common.Hash, code []byte) error {
	ww.w.UpdateAccountCode(address.Bytes(), code, false /* trace */)
	return nil
}

func (ww *WriterWrapper) DeleteAccount(address common.Address, original *accounts.Account) error {
	ww.w.DeleteAccount(address.Bytes(), false /* trace */)
	return nil
}

func (ww *WriterWrapper) WriteAccountStorage(address common.Address, incarnation uint64, key *common.Hash, original, value *uint256.Int) error {
	trace := false
	if trace {
		fmt.Printf("block %d, WriteAccountStorage %x %x, original %s, value %s\n", ww.blockNum, address, *key, original, value)
	}
	ww.w.WriteAccountStorage(address.Bytes(), key.Bytes(), value, trace)
	return nil
}

func (ww *WriterWrapper) CreateContract(address common.Address) error {
	return nil
}
