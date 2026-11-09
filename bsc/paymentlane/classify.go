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

package paymentlane

import (
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types"
)

type LaneType uint8

const (
	GeneralLane LaneType = iota
	PaymentLane
)

func (c LaneType) String() string {
	if c == PaymentLane {
		return "paymentLane"
	}
	return "generalLane"
}

// Classify runs BEP-703 section 3.2's gates in order. listed comes from the parent post-state.
// recipientHasCode is read on the state the transaction starts from; only an unlisted transfer
// with value reaches it.
func Classify(tx types.Transaction, isSystemTx bool, listed map[common.Address]struct{}, recipientHasCode bool) LaneType {
	if isSystemTx {
		return GeneralLane
	}
	to := tx.GetTo()
	if to == nil {
		return GeneralLane
	}
	switch tx.Type() {
	case types.LegacyTxType, types.AccessListTxType, types.DynamicFeeTxType:
	default:
		return GeneralLane
	}
	if _, ok := listed[*to]; ok {
		return PaymentLane
	}
	if tx.GetValue().IsZero() || recipientHasCode {
		return GeneralLane
	}
	return PaymentLane
}
