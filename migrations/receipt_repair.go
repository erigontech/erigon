package migrations

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
)

func availableReceiptFrom(tx kv.Tx) (uint64, error) {
	c, err := tx.Cursor(kv.Receipts)
	if err != nil {
		return 0, err
	}
	defer c.Close()
	k, _, err := c.First()
	if err != nil {
		return 0, err
	}
	if len(k) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(k), nil
}

var ReceiptRepair = Migration{
	Name: "receipt_repair",
	Up: func(db kv.RwDB, tmpdir string, progress []byte, BeforeCommit Callback) (err error) {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		blockNum, err := changeset.AvailableFrom(tx)
		if err != nil {
			return err
		}
		receiptsFrom, err := availableReceiptFrom(tx)
		if err != nil {
			return err
		}
		if receiptsFrom > blockNum {
			blockNum = receiptsFrom
		}

		genesisBlock, err := rawdb.ReadBlockByNumber(tx, 0)
		if err != nil {
			return err
		}
		chainConfig, cerr := rawdb.ReadChainConfig(tx, genesisBlock.Hash())
		if cerr != nil {
			return cerr
		}
		vmConfig := vm.Config{}
		noOpWriter := state.NewNoopWriter()
		var buf bytes.Buffer
		fixedCount := 0
		logInterval := 30 * time.Second
		logEvery := time.NewTicker(logInterval)
		var key [8]byte
		var v []byte
		for ; true; blockNum++ {
			select {
			default:
			case <-logEvery.C:
				log.Info("Progress", "block", blockNum, "fixed", fixedCount)
			}
			var hash common.Hash
			if hash, err = rawdb.ReadCanonicalHash(tx, blockNum); err != nil {
				return err
			}
			if hash == (common.Hash{}) {
				break
			}
			binary.BigEndian.PutUint64(key[:], blockNum)
			if v, err = tx.GetOne(kv.Receipts, key[:]); err != nil {
				return err
			}
			var receipts types.Receipts
			if err = cbor.Unmarshal(&receipts, bytes.NewReader(v)); err == nil {
				broken := false
				for _, receipt := range receipts {
					if receipt.CumulativeGasUsed < 10000 {
						broken = true
						break
					}
				}
				if !broken {
					continue
				}
			}
			var block *types.Block
			if block, _, err = rawdb.ReadBlockWithSenders(tx, hash, blockNum); err != nil {
				return err
			}

			dbstate := state.NewPlainState(tx, block.NumberU64()-1)
			intraBlockState := state.New(dbstate)

			getHeader := func(hash common.Hash, number uint64) *types.Header { return rawdb.ReadHeader(tx, hash, number) }
			contractHasTEVM := ethdb.GetHasTEVM(tx)
			receipts1, err1 := runBlock(intraBlockState, noOpWriter, noOpWriter, chainConfig, getHeader, contractHasTEVM, block, vmConfig)
			if err1 != nil {
				return err1
			}
			fix := true
			if chainConfig.IsByzantium(block.Number().Uint64()) {
				receiptSha := types.DeriveSha(receipts1)
				if receiptSha != block.Header().ReceiptHash {
					fmt.Printf("(retrace) mismatched receipt headers for block %d: %x, %x\n", block.NumberU64(), receiptSha, block.Header().ReceiptHash)
					fix = false
				}
			}
			if fix {
				// All good, we can fix receipt record
				buf.Reset()
				err := cbor.Marshal(&buf, receipts1)
				if err != nil {
					return fmt.Errorf("encode block receipts for block %d: %w", blockNum, err)
				}
				if err = tx.Put(kv.Receipts, key[:], buf.Bytes()); err != nil {
					return fmt.Errorf("writing receipts for block %d: %w", blockNum, err)
				}
				fixedCount++
			}
		}
		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}

func runBlock(ibs *state.IntraBlockState, txnWriter state.StateWriter, blockWriter state.StateWriter,
	chainConfig *params.ChainConfig, getHeader func(hash common.Hash, number uint64) *types.Header, contractHasTEVM func(common.Hash) (bool, error), block *types.Block, vmConfig vm.Config) (types.Receipts, error) {
	header := block.Header()
	vmConfig.TraceJumpDest = true
	engine := ethash.NewFullFaker()
	gp := new(core.GasPool).AddGas(block.GasLimit())
	usedGas := new(uint64)
	var receipts types.Receipts
	if chainConfig.DAOForkSupport && chainConfig.DAOForkBlock != nil && chainConfig.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(ibs)
	}
	for i, tx := range block.Transactions() {
		ibs.Prepare(tx.Hash(), block.Hash(), i)
		receipt, _, err := core.ApplyTransaction(chainConfig, getHeader, engine, nil, gp, ibs, txnWriter, header, tx, usedGas, vmConfig, contractHasTEVM)
		if err != nil {
			return nil, fmt.Errorf("could not apply tx %d [%x] failed: %w", i, tx.Hash(), err)
		}
		receipts = append(receipts, receipt)
	}

	if !vmConfig.ReadOnly {
		// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
		userTxs := make([]*types.Transaction, 0, len(block.Transactions()))
		for _, tx := range block.Transactions() {
			userTxs = append(userTxs, &tx)
		}

		if _, _, err := engine.FinalizeAndAssemble(chainConfig, header, ibs, userTxs, block.Uncles(), receipts, nil, nil, nil, nil); err != nil {
			return nil, fmt.Errorf("finalize of block %d failed: %v", block.NumberU64(), err)
		}

		if err := ibs.CommitBlock(chainConfig.Rules(header.Number.Uint64()), blockWriter); err != nil {
			return nil, fmt.Errorf("committing block %d failed: %w", block.NumberU64(), err)
		}
	}

	return receipts, nil
}
