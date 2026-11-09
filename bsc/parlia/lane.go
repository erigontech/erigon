// Copyright 2026 The Erigon Authors
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

package parlia

import (
	"fmt"

	"github.com/erigontech/erigon/bsc/paymentlane"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const laneMetaCacheSize = 4096

var _ rules.LaneEngine = (*Parlia)(nil)

// loadLaneMeta reads the payment lane metadata from the parent post-state, so it must run
// before this block's system contract upgrades. The lane starts on the block after Jenner;
// the Jenner block itself is stored with nil metadata.
func (p *Parlia) loadLaneMeta(chain rules.ChainHeaderReader, header *types.Header, ibs *state.IntraBlockState, syscall rules.SysCallCustom) error {
	number := header.Number.Uint64()
	if !p.chainConfig.IsJenner(number, header.Time) {
		return nil
	}
	parent := chain.GetHeader(header.ParentHash, number-1)
	if parent == nil {
		return fmt.Errorf("parlia: missing parent header for block %d, cannot load the payment lane", number)
	}
	if !p.chainConfig.IsJenner(parent.Number.Uint64(), parent.Time) {
		p.laneMeta.Add(header.Hash(), nil)
		return nil
	}
	contract := accounts.InternAddress(paymentlane.ContractAddress)
	meta, err := paymentlane.LoadMeta(func(input []byte) ([]byte, error) {
		return syscall(contract, input, ibs, header, true)
	})
	if err != nil {
		return fmt.Errorf("parlia: block %d: %w", number, err)
	}
	p.laneMeta.Add(header.Hash(), meta)
	return nil
}

// VerifyLanes checks the payment lane rule (BEP-703) on an executed block.
func (p *Parlia) VerifyLanes(header *types.Header, txs types.Transactions, receipts types.Receipts, recipientHasCode []bool) error {
	number := header.Number.Uint64()
	if !p.chainConfig.IsJenner(number, header.Time) {
		return nil
	}
	meta, ok := p.laneMeta.Get(header.Hash())
	if !ok {
		return fmt.Errorf("parlia: payment lane metadata for block %d was not loaded", number)
	}
	if meta == nil {
		return nil
	}
	if len(receipts) != len(txs) || len(recipientHasCode) != len(txs) {
		return fmt.Errorf("parlia: block %d: payment lane inputs for %d txs: %d receipts, %d code flags", number, len(txs), len(receipts), len(recipientHasCode))
	}
	var paymentGasUsed uint64
	for i, tx := range txs {
		isSystemTx, err := p.IsSystemTransaction(tx, header)
		if err != nil {
			return err
		}
		if paymentlane.Classify(tx, isSystemTx, meta.Listed(), recipientHasCode[i]) == paymentlane.PaymentLane {
			paymentGasUsed += receipts[i].GasUsed
		}
	}
	return paymentlane.CheckInequality(header.GasLimit, header.GasUsed, paymentGasUsed, meta.Quota(header.GasLimit))
}
