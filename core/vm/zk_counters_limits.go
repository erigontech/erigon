package vm

import (
	"math"

	zk_consts "github.com/ledgerwatch/erigon-lib/chain"
)

const stepDeduction = 200
const baseSafetyPercentage float64 = 0.05

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

	safetyPercentages = map[uint16]float64{
		uint16(zk_consts.ForkID10): 0.025,
		uint16(zk_consts.ForkID11): 0.0125,
	}
)

type counterLimits struct {
	totalSteps, arith, binary, memAlign, keccaks, padding, poseidon, sha256 int
}

func createCountrsByLimits(c counterLimits) *Counters {
	counters := NewCounters()

	counters[S] = &Counter{
		remaining:     c.totalSteps,
		name:          "defaultTotalSteps",
		initialAmount: c.totalSteps,
	}
	counters[A] = &Counter{
		remaining:     c.arith,
		name:          "arith",
		initialAmount: c.arith,
	}
	counters[B] = &Counter{
		remaining:     c.binary,
		name:          "binary",
		initialAmount: c.binary,
	}
	counters[M] = &Counter{
		remaining:     c.memAlign,
		name:          "memAlign",
		initialAmount: c.memAlign,
	}
	counters[K] = &Counter{
		remaining:     c.keccaks,
		name:          "keccaks",
		initialAmount: c.keccaks,
	}
	counters[D] = &Counter{
		remaining:     c.padding,
		name:          "padding",
		initialAmount: c.padding,
	}
	counters[P] = &Counter{
		remaining:     c.poseidon,
		name:          "poseidon",
		initialAmount: c.poseidon,
	}
	counters[SHA] = &Counter{
		remaining:     c.sha256,
		name:          "sha256",
		initialAmount: c.sha256,
	}
	return &counters
}

// tp ne used on next forkid counters
func getCounterLimits(forkId uint16) *Counters {
	totalSteps := getTotalSteps(forkId)

	counterLimits := counterLimits{
		totalSteps: applyDeduction(forkId, totalSteps),
		arith:      applyDeduction(forkId, totalSteps>>5),
		binary:     applyDeduction(forkId, totalSteps>>4),
		memAlign:   applyDeduction(forkId, totalSteps>>5),
		keccaks:    applyDeduction(forkId, int(math.Floor(float64(totalSteps)/155286)*44)),
		padding:    applyDeduction(forkId, int(math.Floor(float64(totalSteps)/56))),
		poseidon:   applyDeduction(forkId, int(math.Floor(float64(totalSteps)/30))),
		sha256:     applyDeduction(forkId, int(math.Floor(float64(totalSteps-1)/31488))*7),
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

	// we need to remove some steps as these will always be used during batch execution
	totalSteps -= stepDeduction

	return totalSteps
}

func applyDeduction(fork uint16, input int) int {
	deduction, found := safetyPercentages[fork]
	if !found {
		deduction = baseSafetyPercentage
	}
	asFloat := float64(input)
	reduction := asFloat * deduction
	newValue := asFloat - reduction
	return int(math.Ceil(newValue))
}
