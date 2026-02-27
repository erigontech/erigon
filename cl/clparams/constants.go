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
)
