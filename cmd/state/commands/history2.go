package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/ledgerwatch/erigon-lib/aggregator"
	chain2 "github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
)

var (
	blockTo    int
	traceBlock int
)

func init() {
	withBlock(history2Cmd)
	withDataDir(history2Cmd)
	history2Cmd.Flags().IntVar(&traceBlock, "traceblock", 0, "block number at which to turn on tracing")
	history2Cmd.Flags().IntVar(&blockTo, "blockto", 0, "block number to stop replay of history at")
	rootCmd.AddCommand(history2Cmd)
}

var history2Cmd = &cobra.Command{
	Use:   "history2",
	Short: "Exerimental command to re-execute historical transactions in erigon2 format",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return History2(genesis, logger)
	},
}

func History2(genesis *core.Genesis, logger log.Logger) error {
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
	aggPath := filepath.Join(datadirCli, "aggregator")
	h, err3 := aggregator.NewHistory(aggPath, uint64(blockTo), aggregationStep)
	if err3 != nil {
		return fmt.Errorf("create history: %w", err3)
	}
	defer h.Close()
	chainConfig := genesis.Config
	vmConfig := vm.Config{}

	interrupt := false
	blockNum := uint64(0)
	var txNum uint64 = 2
	trace := false
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	prevBlock := blockNum
	prevTime := time.Now()
	for !interrupt {
		select {
		default:
		case <-logEvery.C:
			currentTime := time.Now()
			interval := currentTime.Sub(prevTime)
			speed := float64(blockNum-prevBlock) / (float64(interval) / float64(time.Second))
			prevBlock = blockNum
			prevTime = currentTime
			log.Info("Progress", "block", blockNum, "blk/s", speed)
		}
		blockNum++
		if blockNum > uint64(blockTo) {
			break
		}
		blockHash, err := rawdb.ReadCanonicalHash(historyTx, blockNum)
		if err != nil {
			return err
		}
		if blockNum <= block {
			_, _, txAmount := rawdb.ReadBody(historyTx, blockHash, blockNum)
			// Skip that block, but increase txNum
			txNum += uint64(txAmount) + 2
			continue
		}
		var b *types.Block
		b, _, err = rawdb.ReadBlockWithSenders(historyTx, blockHash, blockNum)
		if err != nil {
			return err
		}
		if b == nil {
			break
		}
		r := h.MakeHistoryReader()
		readWrapper := &HistoryWrapper{r: r}
		if traceBlock != 0 {
			readWrapper.trace = blockNum == uint64(traceBlock)
		}
		writeWrapper := state.NewNoopWriter()
		txNum++ // Pre block transaction
		getHeader := func(hash libcommon.Hash, number uint64) *types.Header {
			return rawdb.ReadHeader(historyTx, hash, number)
		}
		if txNum, _, err = runHistory2(trace, blockNum, txNum, readWrapper, writeWrapper, chainConfig, getHeader, b, vmConfig); err != nil {
			return fmt.Errorf("block %d: %w", blockNum, err)
		}
		txNum++ // Post block transaction
		// Check for interrupts
		select {
		case interrupt = <-interruptCh:
			log.Info(fmt.Sprintf("interrupted, please wait for cleanup, next time start with --block %d", blockNum))
		default:
		}
	}
	return nil
}

func runHistory2(trace bool, blockNum, txNumStart uint64, hw *HistoryWrapper, ww state.StateWriter, chainConfig *chain2.Config, getHeader func(hash libcommon.Hash, number uint64) *types.Header, block *types.Block, vmConfig vm.Config) (uint64, types.Receipts, error) {
	header := block.Header()
	vmConfig.TraceJumpDest = true
	engine := ethash.NewFullFaker()
	gp := new(core.GasPool).AddGas(block.GasLimit())
	usedGas := new(uint64)
	var receipts types.Receipts
	daoBlock := chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0
	txNum := txNumStart
	for i, tx := range block.Transactions() {
		hw.r.SetNums(blockNum, txNum, false)
		ibs := state.New(hw)
		if daoBlock {
			misc.ApplyDAOHardFork(ibs)
			daoBlock = false
		}
		ibs.Prepare(tx.Hash(), block.Hash(), i)
		receipt, _, err := core.ApplyTransaction(chainConfig, core.GetHashFn(header, getHeader), engine, nil, gp, ibs, ww, header, tx, usedGas, vmConfig)
		if err != nil {
			return 0, nil, fmt.Errorf("could not apply tx %d [%x] failed: %w", i, tx.Hash(), err)
		}
		if traceBlock != 0 && blockNum == uint64(traceBlock) {
			fmt.Printf("tx idx %d, num %d, gas used %d\n", i, txNum, receipt.GasUsed)
		}
		receipts = append(receipts, receipt)
		txNum++
	}

	return txNum, receipts, nil
}

// Implements StateReader and StateWriter
type HistoryWrapper struct {
	r     *aggregator.HistoryReader
	trace bool
}

func (hw *HistoryWrapper) ReadAccountData(address libcommon.Address) (*accounts.Account, error) {
	enc, err := hw.r.ReadAccountData(address.Bytes(), hw.trace)
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
	if pos >= len(enc) {
		fmt.Printf("panic ReadAccountData(%x)=>[%x]\n", address, enc)
	}
	incBytes := int(enc[pos])
	pos++
	if incBytes > 0 {
		a.Incarnation = bytesToUint64(enc[pos : pos+incBytes])
	}
	return &a, nil
}

func (hw *HistoryWrapper) ReadAccountStorage(address libcommon.Address, incarnation uint64, key *libcommon.Hash) ([]byte, error) {
	enc, err := hw.r.ReadAccountStorage(address.Bytes(), key.Bytes(), hw.trace)
	if hw.trace {
		if enc == nil {
			fmt.Printf("ReadAccountStorage [%x] [%x] => []\n", address, key.Bytes())
		} else {
			fmt.Printf("ReadAccountStorage [%x] [%x] => [%x]\n", address, key.Bytes(), enc.Bytes())
		}
	}
	if err != nil {
		fmt.Printf("%v\n", err)
		return nil, err
	}
	if enc == nil {
		return nil, nil
	}
	return enc.Bytes(), nil
}

func (hw *HistoryWrapper) ReadAccountCode(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) ([]byte, error) {
	return hw.r.ReadAccountCode(address.Bytes(), false /* trace */)
}

func (hw *HistoryWrapper) ReadAccountCodeSize(address libcommon.Address, incarnation uint64, codeHash libcommon.Hash) (int, error) {
	return hw.r.ReadAccountCodeSize(address.Bytes(), false /* trace */)
}

func (hw *HistoryWrapper) ReadAccountIncarnation(address libcommon.Address) (uint64, error) {
	return 0, nil
}
