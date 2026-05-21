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

package statechange

import (
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/cltypes"
)

func ProcessBuilderPendingPayments(s abstract.BeaconState) {
	quorum := s.GetBuilderPaymentQuorumThreshold()
	slotsPerEpoch := int(s.BeaconConfig().SlotsPerEpoch)

	pendingPayments := s.GetBuilderPendingPayments()
	pendingWithdrawals := s.GetBuilderPendingWithdrawals()

	// Process the first SLOTS_PER_EPOCH payments: if weight >= quorum, promote to withdrawal
	for i := 0; i < slotsPerEpoch && i < pendingPayments.Length(); i++ {
		payment := pendingPayments.Get(i)
		if payment != nil && payment.Weight >= quorum && payment.Withdrawal != nil {
			pendingWithdrawals.Append(payment.Withdrawal)
		}
	}
	s.SetBuilderPendingWithdrawals(pendingWithdrawals)

	// Shift: discard first SLOTS_PER_EPOCH entries, pad end with empty entries
	totalLen := pendingPayments.Length()
	for i := 0; i < totalLen-slotsPerEpoch; i++ {
		pendingPayments.Set(i, pendingPayments.Get(i+slotsPerEpoch))
	}
	for i := totalLen - slotsPerEpoch; i < totalLen; i++ {
		pendingPayments.Set(i, &cltypes.BuilderPendingPayment{
			Withdrawal: &cltypes.BuilderPendingWithdrawal{},
		})
	}
	s.SetBuilderPendingPayments(pendingPayments)
}
