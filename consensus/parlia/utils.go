package parlia

import (
	"math/rand"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
)

func backOffTime(snap *Snapshot, val libcommon.Address) uint64 {
	if snap.inturn(val) {
		return 0
	} else {
		idx := snap.indexOfVal(val)
		if idx < 0 {
			// The backOffTime does not matter when a validator is not authorized.
			return 0
		}
		s := rand.NewSource(int64(snap.Number))
		r := rand.New(s) // nolint: gosec
		n := len(snap.Validators)
		backOffSteps := make([]uint64, 0, n)
		for idx := uint64(0); idx < uint64(n); idx++ {
			backOffSteps = append(backOffSteps, idx)
		}
		r.Shuffle(n, func(i, j int) {
			backOffSteps[i], backOffSteps[j] = backOffSteps[j], backOffSteps[i]
		})
		delay := initialBackOffTime + backOffSteps[idx]*wiggleTime
		return delay
	}
}
