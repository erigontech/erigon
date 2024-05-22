package finality

import "sync/atomic"

// BorMilestoneRewind is used as a flag/variable
// Flag: if equals 0, no rewind according to bor whitelisting service
// Variable: if not equals 0, rewind chain back to BorMilestoneRewind
var BorMilestoneRewind atomic.Pointer[uint64]

func IsMilestoneRewindPending() bool {
	return BorMilestoneRewind.Load() != nil && *BorMilestoneRewind.Load() != 0
}
