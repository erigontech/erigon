package statechange

import (
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

// ProcessBuilderPendingPayments processes builder pending payments from the previous epoch.
// [New in Gloas:EIP7732]
func ProcessBuilderPendingPayments(s abstract.BeaconState) {
	beaconConfig := s.BeaconConfig()
	slotsPerEpoch := beaconConfig.SlotsPerEpoch

	// Calculate quorum threshold
	totalActiveBalance := s.GetTotalActiveBalance()
	perSlotBalance := totalActiveBalance / slotsPerEpoch
	quorum := perSlotBalance * beaconConfig.BuilderPaymentThresholdNumerator / beaconConfig.BuilderPaymentThresholdDenominator

	bpp := s.BuilderPendingPayments()

	// Process previous epoch payments (first SLOTS_PER_EPOCH entries)
	for i := uint64(0); i < slotsPerEpoch; i++ {
		payment := bpp.Get(int(i))
		if payment.Weight > quorum {
			// Compute withdrawable epoch
			exitQueueEpoch := s.ComputeExitEpochAndUpdateChurn(payment.Withdrawal.Amount)
			payment.Withdrawal.WithdrawableEpoch = exitQueueEpoch + beaconConfig.MinValidatorWithdrawabilityDelay

			// Append withdrawal to builder_pending_withdrawals
			bpw := s.BuilderPendingWithdrawals()
			bpw.Append(payment.Withdrawal)
			s.SetBuilderPendingWithdrawals(bpw)
		}
	}

	// Rotate: current epoch payments become previous, new current epoch is zeroed
	newBpp := solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](int(2*slotsPerEpoch), cltypes.NewBuilderPendingPayment().EncodingSizeSSZ())
	for i := uint64(0); i < slotsPerEpoch; i++ {
		// Copy current epoch (second half) to previous epoch (first half)
		newBpp.ListSSZ.Append(bpp.Get(int(slotsPerEpoch + i)))
	}
	for i := uint64(0); i < slotsPerEpoch; i++ {
		// Fill new current epoch with empty payments
		newBpp.ListSSZ.Append(cltypes.NewBuilderPendingPayment())
	}
	s.SetBuilderPendingPayments(newBpp)
}
