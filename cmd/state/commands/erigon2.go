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

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/aggregator"
	"github.com/ledgerwatch/erigon-lib/kv"
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

var (
	check      bool
	changesets bool
)

func init() {
	withBlock(erigon2Cmd)
	withDatadir(erigon2Cmd)
	erigon2Cmd.Flags().BoolVar(&check, "check", false, "set to true to compare state reads with with historical state (for debugging)")
	erigon2Cmd.Flags().BoolVar(&changesets, "changesets", false, "set to true to generate changesets")
	rootCmd.AddCommand(erigon2Cmd)
}

var erigon2Cmd = &cobra.Command{
	Use:   "erigon2",
	Short: "Exerimental command to re-execute blocks from beginning using erigon2 state representation",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return Erigon2(genesis, logger, block, datadir)
	},
}

func Erigon2(genesis *core.Genesis, logger log.Logger, blockNum uint64, datadir string) error {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	historyDb, err := kv2.NewMDBX(logger).Path(path.Join(datadir, "chaindata")).Readonly().Open()
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
	if _, err = os.Stat(stateDbPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	} else if err = os.RemoveAll(stateDbPath); err != nil {
		return err
	}
	db, err2 := kv2.NewMDBX(logger).Path(stateDbPath).Open()
	if err2 != nil {
		return err2
	}
	defer db.Close()
	aggPath := path.Join(datadir, "aggregator")
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
	agg, err3 := aggregator.NewAggregator(aggPath, 90000, 4096)
	if err3 != nil {
		return fmt.Errorf("create aggregator: %w", err3)
	}
	agg.GenerateChangesets(changesets)
	chainConfig := genesis.Config
	vmConfig := vm.Config{}

	interrupt := false
	block := uint64(0)
	var rwTx kv.RwTx
	defer func() {
		rwTx.Rollback()
	}()
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	genBlock, genesisIbs, err4 := genesis.ToBlock()
	if err4 != nil {
		return err4
	}
	w := agg.MakeStateWriter()
	if err = w.Reset(rwTx, 0); err != nil {
		return err
	}
	if err = genesisIbs.CommitBlock(params.Rules{}, &WriterWrapper{w: w}); err != nil {
		return fmt.Errorf("cannot write state: %w", err)
	}
	if err = w.FinishTx(0, false); err != nil {
		return err
	}
	var rootHash []byte
	if rootHash, err = w.FinishBlock(false); err != nil {
		return err
	}
	if !bytes.Equal(rootHash, genBlock.Header().Root[:]) {
		return fmt.Errorf("root hash mismatch for genesis block, expected [%x], was [%x]", genBlock.Header().Root[:], rootHash)
	}
	if err = rwTx.Commit(); err != nil {
		return err
	}
	var tx kv.Tx
	defer func() {
		if tx != nil {
			tx.Rollback()
		}
	}()
	var txNum uint64 = 1
	trace := false
	for !interrupt {
		block++
		if block >= blockNum {
			break
		}
		blockHash, err := rawdb.ReadCanonicalHash(historyTx, block)
		if err != nil {
			return err
		}
		var b *types.Block
		b, _, err = rawdb.ReadBlockWithSenders(historyTx, blockHash, block)
		if err != nil {
			return err
		}
		if b == nil {
			break
		}
		if tx, err = db.BeginRo(ctx); err != nil {
			return err
		}
		if rwTx, err = db.BeginRw(ctx); err != nil {
			return err
		}
		r := agg.MakeStateReader(tx, block)
		var checkR state.StateReader
		if check {
			checkR = state.NewPlainState(historyTx, block-1)
		}
		if err = w.Reset(rwTx, block); err != nil {
			return err
		}
		intraBlockState := state.New(&ReaderWrapper{r: r, checkR: checkR, blockNum: block})
		getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(historyTx, hash, number) }
		if txNum, _, err = runBlock2(trace, txNum, intraBlockState, &WriterWrapper{w: w, blockNum: block}, chainConfig, getHeader, nil, b, vmConfig); err != nil {
			return fmt.Errorf("block %d: %w", block, err)
		}
		if block%1000 == 0 {
			log.Info("Processed", "blocks", block)
		}
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			fmt.Printf("interrupted on block %d, please wait for cleanup...\n", block)
		default:
		}
		tx.Rollback()
		if err := w.FinishTx(txNum, trace); err != nil {
			return fmt.Errorf("final finish failed: %w", err)
		}
		if trace {
			fmt.Printf("FinishTx called for %d block %d\n", txNum, block)
		}
		txNum++
		if rootHash, err = w.FinishBlock(trace /* trace */); err != nil {
			return err
		}
		if bytes.Equal(rootHash, b.Header().Root[:]) {
			if err = rwTx.Commit(); err != nil {
				return err
			}
		} else {
			if trace {
				return fmt.Errorf("root hash mismatch for block %d, expected [%x], was [%x]", block, b.Header().Root[:], rootHash)
			} else {
				block--
				trace = true
			}
			rwTx.Rollback()
		}
	}
	return nil
}

func runBlock2(trace bool, txNumStart uint64, ibs *state.IntraBlockState, ww *WriterWrapper,
	chainConfig *params.ChainConfig, getHeader func(hash common.Hash, number uint64) *types.Header, contractHasTEVM func(common.Hash) (bool, error), block *types.Block, vmConfig vm.Config) (uint64, types.Receipts, error) {
	header := block.Header()
	vmConfig.TraceJumpDest = true
	engine := ethash.NewFullFaker()
	gp := new(core.GasPool).AddGas(block.GasLimit())
	usedGas := new(uint64)
	var receipts types.Receipts
	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	rules := chainConfig.Rules(block.NumberU64())
	txNum := txNumStart
	for i, tx := range block.Transactions() {
		ibs.Prepare(tx.Hash(), block.Hash(), i)
		receipt, _, err := core.ApplyTransaction(chainConfig, getHeader, engine, nil, gp, ibs, ww, header, tx, usedGas, vmConfig, contractHasTEVM)
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

	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	if _, _, err := engine.FinalizeAndAssemble(chainConfig, header, ibs, block.Transactions(), block.Uncles(), receipts, nil, nil, nil, nil); err != nil {
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
	checkR   state.StateReader
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
	var checkA *accounts.Account
	var checkErr error
	if rw.checkR != nil {
		if checkA, checkErr = rw.checkR.ReadAccountData(address); checkErr != nil {
			fmt.Printf("readAccountData %x checkR: %v\n", address, checkErr)
			return nil, fmt.Errorf("readAccountData %x checkR: %w", address, checkErr)
		}
	}
	if len(enc) == 0 {
		if checkA != nil {
			fmt.Printf("readAccountData %x enc [%x], checkEnc [%+v]\n", address, enc, checkA)
			return nil, fmt.Errorf("readAccountData %x enc [%x], checkEnc [%+v]", address, enc, checkA)
		}
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
	if rw.checkR != nil {
		if !a.Equals(checkA) {
			fmt.Printf("readAccountData %x enc [%+v], checkEnc [%+v]\n", address, a, checkA)
			return nil, fmt.Errorf("readAccountData %x enc [%+v], checkEnc [%+v]", address, a, checkA)
		}
	}
	return &a, nil
}

func (rw *ReaderWrapper) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	trace := false
	enc, err := rw.r.ReadAccountStorage(address.Bytes(), key.Bytes(), trace)
	if err != nil {
		return nil, err
	}
	var checkEnc []byte
	var checkErr error
	if rw.checkR != nil {
		if checkEnc, checkErr = rw.checkR.ReadAccountStorage(address, incarnation, key); checkErr != nil {
			fmt.Printf("block %d ReadAccountStorage %x %x checkR: %v\n", rw.blockNum, address, *key, checkErr)
			return nil, fmt.Errorf("readAccountStorage %x %x checkR: %w", address, *key, checkErr)
		}
	}
	if enc == nil {
		if len(checkEnc) != 0 {
			fmt.Printf("block %d ReadAccountStorage %x %x enc [%x], checkEnc [%x]\n", rw.blockNum, address, *key, enc, checkEnc)
			return nil, fmt.Errorf("readAccountStorage %x %x enc [%x], checkEnc [%x]", address, *key, enc, checkEnc)
		}
		return nil, nil
	}
	if rw.checkR != nil {
		if !bytes.Equal(enc.Bytes(), checkEnc) {
			fmt.Printf("block %d ReadAccountStorage %x %x enc [%x], checkEnc [%x]\n", rw.blockNum, address, *key, enc, checkEnc)
			return nil, fmt.Errorf("readAccountStorage %x %x enc [%x], checkEnc [%x]", address, *key, enc, checkEnc)
		}
	}
	return enc.Bytes(), nil
}

func (rw *ReaderWrapper) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	enc, err := rw.r.ReadAccountCode(address.Bytes(), false /* trace */)
	if err != nil {
		return nil, err
	}
	if rw.checkR != nil {
		checkEnc, checkErr := rw.checkR.ReadAccountCode(address, incarnation, codeHash)
		if checkErr != nil {
			fmt.Printf("readAccountCode %x checkR: %v\n", address, checkErr)
			return nil, fmt.Errorf("readAccountCode %x checkR: %w", address, checkErr)
		}
		if !bytes.Equal(enc, checkEnc) {
			fmt.Printf("readAccountCode %x enc [%x], checkEnc [%x]\n", address, enc, checkEnc)
			return nil, fmt.Errorf("readAccountCode %x enc [%x], checkEnc [%x]", address, enc, checkEnc)
		}
	}
	return enc, nil
}

func (rw *ReaderWrapper) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	enc, err := rw.r.ReadAccountCode(address.Bytes(), false /* trace */)
	if err != nil {
		return 0, err
	}
	/*
		checkEnc, checkErr := rw.checkR.ReadAccountCode(address, incarnation, codeHash)
		if checkErr != nil {
			fmt.Printf("readAccountCode %x checkR: %v\n", address, checkErr)
			return 0, fmt.Errorf("readAccountCode %x checkR: %w", address, checkErr)
		}
		if !bytes.Equal(enc, checkEnc) {
			fmt.Printf("readAccountCode %x enc [%x], checkEnc [%x]\n", address, enc, checkEnc)
			return 0, fmt.Errorf("readAccountCode %x enc [%x], checkEnc [%x]", address, enc, checkEnc)
		}
	*/
	return len(enc), nil
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
	if err := ww.w.WriteAccountStorage(address.Bytes(), key.Bytes(), value, trace); err != nil {
		return err
	}
	return nil
}

func (ww *WriterWrapper) CreateContract(address common.Address) error {
	return nil
}
