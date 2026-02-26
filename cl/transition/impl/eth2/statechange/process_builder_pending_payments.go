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
		if payment != nil && payment.Weight >= quorum {
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
