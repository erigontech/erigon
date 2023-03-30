package parlia

import (
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
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

func (p *Parlia) backOffTime(snap *Snapshot, header *types.Header, val libcommon.Address) uint64 {
	if snap.inturn(val) {
		return 0
	} else {
		delay := initialBackOffTime
		validators := snap.validators()
		if p.chainConfig.IsPlanck(header.Number.Uint64()) {
			// reverse the key/value of snap.Recents to get recentsMap
			recentsMap := make(map[libcommon.Address]uint64, len(snap.Recents))
			bound := uint64(0)
			if n, limit := header.Number.Uint64(), uint64(len(validators)/2+1); n > limit {
				bound = n - limit
			}
			for seen, recent := range snap.Recents {
				if seen <= bound {
					continue
				}
				recentsMap[recent] = seen
			}

			// if the validator has recently signed, it is unexpected, stop here.
			if seen, ok := recentsMap[val]; ok {
				log.Error("unreachable code, validator signed recently",
					"block", header.Number, "address", val,
					"seen", seen, "len(snap.Recents)", len(snap.Recents))
				return 0
			}

			inTurnAddr := validators[(snap.Number+1)%uint64(len(validators))]
			if _, ok := recentsMap[inTurnAddr]; ok {
				log.Info("in turn validator has recently signed, skip initialBackOffTime",
					"inTurnAddr", inTurnAddr)
				delay = 0
			}

			// Exclude the recently signed validators
			temp := make([]libcommon.Address, 0, len(validators))
			for _, addr := range validators {
				if _, ok := recentsMap[addr]; ok {
					continue
				}
				temp = append(temp, addr)
			}
			validators = temp
		}

		// get the index of current validator and its shuffled backoff time.
		idx := -1
		for index, itemAddr := range validators {
			if val == itemAddr {
				idx = index
			}
		}
		if idx < 0 {
			log.Info("The validator is not authorized", "addr", val)
			return 0
		}

		s := rand.NewSource(int64(snap.Number))
		r := rand.New(s) // nolint: gosec
		n := len(validators)
		backOffSteps := make([]uint64, 0, n)

		for i := uint64(0); i < uint64(n); i++ {
			backOffSteps = append(backOffSteps, i)
		}

		r.Shuffle(n, func(i, j int) {
			backOffSteps[i], backOffSteps[j] = backOffSteps[j], backOffSteps[i]
		})

		delay += backOffSteps[idx] * wiggleTime
		return delay
	}
}
