// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package types

import (
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/common"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/crypto"
)

func (_this *Receipt) Clone() *Receipt {
	postState := make([]byte, len(_this.PostState))
	copy(postState, _this.PostState)

	bloom := Bloom{}
	copy(bloom[:], _this.Bloom[:])

	logs := make(Logs, len(_this.Logs))
	for i, l := range _this.Logs {
		logs[i] = l.Clone()
	}

	txHash := libcommon.Hash{}
	copy(txHash[:], _this.TxHash[:])

	contractAddress := libcommon.Address{}
	copy(contractAddress[:], _this.ContractAddress[:])

	blockHash := libcommon.Hash{}
	copy(blockHash[:], _this.BlockHash[:])

	BlockNumber := big.NewInt(0).Set(_this.BlockNumber)

	return &Receipt{
		Type:              _this.Type,
		PostState:         postState,
		Status:            _this.Status,
		CumulativeGasUsed: _this.CumulativeGasUsed,
		Bloom:             bloom,
		Logs:              logs,
		TxHash:            txHash,
		ContractAddress:   contractAddress,
		BlockHash:         blockHash,
		BlockNumber:       BlockNumber,
		TransactionIndex:  _this.TransactionIndex,
	}
}

// DeriveFields fills the receipts with their computed fields based on consensus
// data and contextual infos like containing block and transactions.
func (r Receipts) DeriveFields_zkEvm(forkId8BlockNum uint64, hash common.Hash, number uint64, txs Transactions, senders []common.Address) error {
	if len(txs) != len(r) {
		return fmt.Errorf("transaction and receipt count mismatch, tx count = %d, receipts count = %d", len(txs), len(r))
	}
	if len(senders) != len(txs) {
		return fmt.Errorf("transaction and senders count mismatch, tx count = %d, senders count = %d", len(txs), len(senders))
	}
	// log index should increment across the whole block, not just the tx
	logIndex := uint(0)
	for i := 0; i < len(r); i++ {
		// The transaction type and hash can be retrieved from the transaction itself
		r[i].Type = txs[i].Type()
		r[i].TxHash = txs[i].Hash()

		// block location fields
		r[i].BlockHash = hash
		r[i].BlockNumber = new(big.Int).SetUint64(number)
		r[i].TransactionIndex = uint(i)

		// The contract address can be derived from the transaction itself
		if txs[i].GetTo() == nil {
			// If one wants to deploy a contract, one needs to send a transaction that does not have `To` field
			// and then the address of the contract one is creating this way will depend on the `tx.From`
			// and the nonce of the creating account (which is `tx.From`).
			r[i].ContractAddress = crypto.CreateAddress(senders[i], txs[i].GetNonce())
		}
		// The used gas can be calculated based on previous r
		// [hack] there was a cumulativeGasUsed bug priod to forkid8, so we need to check for it
		// if the block is before forkId8 comuluative is equal to gas used
		if i == 0 || number < forkId8BlockNum {
			r[i].GasUsed = r[i].CumulativeGasUsed
		} else {
			r[i].GasUsed = r[i].CumulativeGasUsed - r[i-1].CumulativeGasUsed
		}

		// The derived log fields can simply be set from the block and transaction
		for j := 0; j < len(r[i].Logs); j++ {
			r[i].Logs[j].BlockNumber = number
			r[i].Logs[j].BlockHash = hash
			r[i].Logs[j].TxHash = r[i].TxHash
			r[i].Logs[j].TxIndex = uint(i)
			r[i].Logs[j].Index = logIndex
			logIndex++
		}
	}
	return nil
}

func (rs Receipts) ReceiptForTx(txHash common.Hash) *Receipt {
	var receipt *Receipt
	for i := range rs {
		if rs[i].TxHash == txHash {
			receipt = rs[i]
			break
		}
	}

	return receipt
}
