package clparams

import "math"

const (
	// non-configurable constants
	// For Gloas
	BuilderIndexFlag                   = uint64(1 << 40) // 2^40
	BuilderIndexSelfBuild              = math.MaxUint64
	BuilderPaymentThresholdNumerator   = uint64(6)
	BuilderPaymentThresholdDenominator = uint64(10)
	PtcSize                            = uint64(512)
	PayloadTimelyThreshold             = PtcSize / 2 // 256
	DataAvailabilityTimelyThreshold    = PtcSize / 2 // 256

	AttestationTimelinessIndex  = 0
	PtcTimelinessIndex          = 1
	NumBlockTimelinessDeadlines = 2

	// [New in Gloas:EIP7732] BPS (basis points) timing constants.
	// These define slot-relative deadlines as basis points (1/10000 of a slot).
	BpsFactor                = uint64(10000) // Denominator for BPS calculations
	AttestationDueBpsGloas   = uint64(2500)  // 25% of slot — attestation deadline
	AggregateDueBpsGloas     = uint64(5000)  // 50% of slot — aggregate deadline
	PayloadAttestationDueBps = uint64(7500)  // 75% of slot — PTC payload attestation deadline

	// Proposer boost reorg constants.
	// REORG_HEAD_WEIGHT_THRESHOLD is the percentage of committee weight below which
	// the head is considered "weak" and eligible for reorging.
	ReorgHeadWeightThreshold = uint64(20)
)
