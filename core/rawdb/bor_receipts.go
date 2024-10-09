// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package rawdb

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/dbutils"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/consensus"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/ethdb/cbor"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
)

var (
	// bor receipt key
	borReceiptKey = bortypes.BorReceiptKey
)

// HasBorReceipts verifies the existence of all block receipt belonging to a block.
func HasBorReceipts(db kv.Has, number uint64) bool {
	if has, err := db.Has(kv.BorReceipts, borReceiptKey(number)); !has || err != nil {
		return false
	}
	return true
}

func ReadRawBorReceipt(db kv.Tx, number uint64) (*types.Receipt, bool, error) {
	data, err := db.GetOne(kv.BorReceipts, borReceiptKey(number))
	if err != nil {
		return nil, false, fmt.Errorf("ReadBorReceipt failed getting bor receipt with blockNumber=%d, err=%s", number, err)
	}
	if len(data) == 0 {
		return nil, false, nil
	}

	var borReceipt *types.Receipt
	err = cbor.Unmarshal(&borReceipt, bytes.NewReader(data))
	if err == nil {
		return borReceipt, false, nil
	}

	// Convert the receipts from their storage form to their internal representation
	var borStorageReceipt types.ReceiptForStorage
	if err := rlp.DecodeBytes(data, &borStorageReceipt); err != nil {
		log.Error("Invalid receipt array RLP", "err", err)
		return nil, true, err
	}

	return (*types.Receipt)(&borStorageReceipt), true, nil
}

func ReadBorReceipt(db kv.Tx, blockHash libcommon.Hash, blockNumber uint64, receipts types.Receipts) (*types.Receipt, error) {
	borReceipt, hasEmbeddedLogs, err := ReadRawBorReceipt(db, blockNumber)
	if err != nil {
		return nil, err
	}

	if borReceipt == nil {
		return nil, nil
	}

	if !hasEmbeddedLogs {
		logsData, err := db.GetOne(kv.Log, dbutils.LogKey(blockNumber, uint32(len(receipts))))
		if err != nil {
			return nil, fmt.Errorf("ReadBorReceipt failed getting bor logs with blockNumber=%d, err=%s", blockNumber, err)
		}
		if logsData != nil {
			var logs types.Logs
			if err = cbor.Unmarshal(&logs, bytes.NewReader(logsData)); err != nil {
				return nil, fmt.Errorf("logs unmarshal failed:  %w", err)
			}
			borReceipt.Logs = logs
		}
	}

	bortypes.DeriveFieldsForBorReceipt(borReceipt, blockHash, blockNumber, receipts)

	return borReceipt, nil
}

func GenerateBorReceipt(ctx context.Context, tx kv.Tx, block *types.Block, msg *types.Message, engine consensus.EngineReader, chainConfig *chain.Config, txNumsReader rawdbv3.TxNumsReader, headerReader services.HeaderReader) (*types.Receipt, error) {
	stateReader := state.NewHistoryReaderV3()
	stateReader.SetTx(tx)
	//r.SetTrace(true)
	minTxNum, err := txNumsReader.Min(tx, block.NumberU64())
	if err != nil {
		return nil, err
	}
	stateReader.SetTxNum(uint64(int(minTxNum) + /* 1 system txNum in beginning of block */ 1))
	stateCache := shards.NewStateCache(
		32, 0 /* no limit */) // this cache living only during current RPC call, but required to store state writes
	cachedReader := state.NewCachedReader(stateReader, stateCache)
	ibs := state.New(cachedReader)

	getHeader := func(hash libcommon.Hash, n uint64) *types.Header {
		h, _ := headerReader.HeaderByNumber(ctx, tx, n)
		return h
	}

	gp := new(core.GasPool).AddGas(msg.Gas()).AddBlobGas(msg.BlobGas())
	blockContext := core.NewEVMBlockContext(block.Header(), core.GetHashFn(block.Header(), getHeader), engine, nil, chainConfig)
	evm := vm.NewEVM(blockContext, evmtypes.TxContext{}, ibs, chainConfig, vm.Config{})
	return applyBorTransaction(msg, evm, gp, ibs)
}

func applyBorTransaction(msg *types.Message, evm *vm.EVM, gp *core.GasPool, ibs *state.IntraBlockState) (*types.Receipt, error) {
	txContext := core.NewEVMTxContext(msg)
	evm.Reset(txContext, ibs)
	start := len(ibs.Logs()) - 1

	_, err := core.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */)
	if err != nil {
		return nil, err
	}

	receiptLogs := ibs.Logs()[start:]
	receipt := &types.Receipt{
		Logs: receiptLogs,
	}

	return receipt, nil
}

// WriteBorReceipt stores all the bor receipt belonging to a block (storing the state sync receipt and log).
func WriteBorReceipt(tx kv.RwTx, number uint64, borReceipt *types.Receipt) error {
	// Convert the bor receipt into their storage form and serialize them
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	if err := cbor.Marshal(buf, borReceipt.Logs); err != nil {
		return err
	}
	if err := tx.Append(kv.Log, dbutils.LogKey(number, uint32(borReceipt.TransactionIndex)), buf.Bytes()); err != nil {
		return err
	}

	buf.Reset()
	err := cbor.Marshal(buf, borReceipt)
	if err != nil {
		return err
	}
	// Store the flattened receipt slice
	if err := tx.Append(kv.BorReceipts, borReceiptKey(number), buf.Bytes()); err != nil {
		return err
	}

	return nil
}

func ReadBorTxLookupEntry(db kv.Getter, borTxHash libcommon.Hash) (*uint64, error) {
	blockNumBytes, err := db.GetOne(kv.BorTxLookup, borTxHash.Bytes())
	if err != nil {
		return nil, err
	}
	if blockNumBytes == nil {
		return nil, nil
	}

	blockNum := (new(big.Int).SetBytes(blockNumBytes)).Uint64()
	return &blockNum, nil
}

// ReadBorTransactionForBlock retrieves a specific bor (fake) transaction associated with a block, along with
// its added positional metadata.
func ReadBorTransactionForBlock(db kv.Tx, blockNum uint64) types.Transaction {
	if !HasBorReceipts(db, blockNum) {
		return nil
	}
	return bortypes.NewBorTransaction()
}

// TruncateBorReceipts removes all bor receipt for given block number or newer
func TruncateBorReceipts(db kv.RwTx, number uint64) error {
	if err := db.ForEach(kv.BorReceipts, hexutility.EncodeTs(number), func(k, _ []byte) error {
		return db.Delete(kv.BorReceipts, k)
	}); err != nil {
		return err
	}
	return nil
}
