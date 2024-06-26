package vm

import (
	"math"

	zk_consts "github.com/ledgerwatch/erigon/zk/constants"
)

var (
	defaultTotalSteps    = 1 << 23
	defaultCounterLimits = counterLimits{
		totalSteps: defaultTotalSteps,
		arith:      defaultTotalSteps >> 5,
		binary:     defaultTotalSteps >> 4,
		memAlign:   defaultTotalSteps >> 5,
		keccaks:    int(math.Floor(float64(defaultTotalSteps)/155286) * 44),
		padding:    int(math.Floor(float64(defaultTotalSteps) / 56)),
		poseidon:   int(math.Floor(float64(defaultTotalSteps) / 30)),
	}

	unlimitedCounters = counterLimits{
		totalSteps: math.MaxInt32,
		arith:      math.MaxInt32,
		binary:     math.MaxInt32,
		memAlign:   math.MaxInt32,
		keccaks:    math.MaxInt32,
		padding:    math.MaxInt32,
		poseidon:   math.MaxInt32,
	}
)

type counterLimits struct {
	totalSteps, arith, binary, memAlign, keccaks, padding, poseidon, sha256 int
}

func createCountrsByLimits(c counterLimits) *Counters {
	return &Counters{
		S: {
			remaining:     c.totalSteps,
			name:          "defaultTotalSteps",
			initialAmount: c.totalSteps,
		},
		A: {
			remaining:     c.arith,
			name:          "arith",
			initialAmount: c.arith,
		},
		B: {
			remaining:     c.binary,
			name:          "binary",
			initialAmount: c.binary,
		},
		M: {
			remaining:     c.memAlign,
			name:          "memAlign",
			initialAmount: c.memAlign,
		},
		K: {
			remaining:     c.keccaks,
			name:          "keccaks",
			initialAmount: c.keccaks,
		},
		D: {
			remaining:     c.padding,
			name:          "padding",
			initialAmount: c.padding,
		},
		P: {
			remaining:     c.poseidon,
			name:          "poseidon",
			initialAmount: c.poseidon,
		},
		SHA: {
			remaining:     c.sha256,
			name:          "sha256",
			initialAmount: c.sha256,
		},
	}
}

// tp ne used on next forkid counters
func getCounterLimits(forkId uint16) *Counters {
	if forkId <= uint16(zk_consts.ForkID9Elderberry2) {
		return createCountrsByLimits(defaultCounterLimits)
	}

	return createCountrsByLimits(defaultCounterLimits)
}
