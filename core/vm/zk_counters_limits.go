package vm

import (
	"math"

	zk_consts "github.com/ledgerwatch/erigon/zk/constants"
)

var (
	defaultTotalSteps  = 1 << 23
	forkId10TotalSteps = 1 << 24
	forkId11TotalSteps = 1 << 25

	unlimitedCounters = counterLimits{
		totalSteps: math.MaxInt32,
		arith:      math.MaxInt32,
		binary:     math.MaxInt32,
		memAlign:   math.MaxInt32,
		keccaks:    math.MaxInt32,
		padding:    math.MaxInt32,
		poseidon:   math.MaxInt32,
		sha256:     math.MaxInt32,
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
	totalSteps := getTotalSteps(forkId)

	counterLimits := counterLimits{
		totalSteps: totalSteps,
		arith:      totalSteps >> 5,
		binary:     totalSteps >> 4,
		memAlign:   totalSteps >> 5,
		keccaks:    int(math.Floor(float64(totalSteps)/155286) * 44),
		padding:    int(math.Floor(float64(totalSteps) / 56)),
		poseidon:   int(math.Floor(float64(totalSteps) / 30)),
		sha256:     int(math.Floor(float64(totalSteps-1)/31488)) * 7,
	}

	return createCountrsByLimits(counterLimits)
}

func getTotalSteps(forkId uint16) int {
	var totalSteps int

	switch forkId {
	case uint16(zk_consts.ForkID10):
		totalSteps = forkId10TotalSteps
	case uint16(zk_consts.ForkID11):
		totalSteps = forkId11TotalSteps
	default:
		totalSteps = defaultTotalSteps
	}

	return totalSteps
}
