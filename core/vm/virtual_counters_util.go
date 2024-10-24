package vm

import (
	"math"
	"math/big"
)

func expectedModExpCounters(lenB, lenE, lenM int, B *big.Int, E *big.Int, M *big.Int) (*big.Int, *big.Int, *big.Int) {
	q_b_m := big.NewInt(0).Div(B, M)
	r_b_m := big.NewInt(0).Mod(B, M)

	lenQE2 := newInt(0).SetUint64(uint64(math.Floor(float64(lenE) / 2)))

	base := big.NewInt(0)
	nTimesOdd := big.NewInt(0)
	for E.Cmp(base) == 1 {
		nTimesOdd.Add(nTimesOdd, big.NewInt(0).And(E, big.NewInt(1)))
		E.Rsh(E, 1)
	}
	nTimesEven := big.NewInt(0).Mul(big.NewInt(0).SetUint64(uint64(lenE)), big.NewInt(256))

	stepFirstDiv, binaryFirstDiv, arithFirstDiv := setupAndFirstDivCounters(lenB, lenM, q_b_m, r_b_m)
	stepHalfLoop, binaryHalfLoop, arithHalfLoop := halfLoopCounters(lenB, lenM, lenE, lenQE2)
	stepFullLoop, binaryFullLoop, arithFullLoop := fullLoopCounters(lenB, lenM, lenE, lenQE2)

	steps := combineResults(nTimesOdd, nTimesEven, stepFirstDiv, stepHalfLoop, stepFullLoop)
	binary := combineResults(nTimesOdd, nTimesEven, binaryFirstDiv, binaryHalfLoop, binaryFullLoop)
	arith := combineResults(nTimesOdd, nTimesEven, arithFirstDiv, arithHalfLoop, arithFullLoop)

	return steps, binary, arith
}

func combineResults(nTimesOdd, nTimesEven, firstDiv, halfLoop, fullLoop *big.Int) *big.Int {
	stepsOdd := newInt(0).Set(nTimesOdd)
	stepsEven := newInt(0).Set(nTimesEven)

	stepsEven.Mul(stepsEven, halfLoop)
	stepsOdd.Mul(stepsOdd, fullLoop)

	result := big.NewInt(0).Set(firstDiv)
	result.Add(result, stepsEven)
	result.Add(result, stepsOdd)

	return result
}

func computeLenThisBase(x *big.Int) int {
	if x.Cmp(big.NewInt(0)) == 0 {
		return 1
	}

	length := 0
	for x.Cmp(big.NewInt(0)) == 1 {
		x.Rsh(x, 256)
		length += 1
	}

	return length
}

func setupAndFirstDivCounters(lenB, lenM int, q_b_m, r_b_m *big.Int) (*big.Int, *big.Int, *big.Int) {
	steps := newInt(84)
	steps.Add(steps, newInt(2))
	steps.Add(steps, newMul(newInt(10), newInt(lenB)))
	steps.Add(steps, newMul(newInt(3), newInt(lenM)))
	steps.Add(steps, newMul(newInt(8+19*lenM), newInt(computeLenThisBase(q_b_m))))
	steps.Add(steps, newMul(newInt(12), newInt(computeLenThisBase(r_b_m))))

	binaries := newInt(4)
	binaries.Sub(binaries, newInt(lenM))
	binaries.Add(binaries, newInt(computeLenThisBase(r_b_m)))
	binaries.Add(binaries, newMul(newInt(2), newInt(computeLenThisBase(q_b_m)), newInt(lenM)))

	ariths := newInt(lenM * computeLenThisBase(q_b_m))

	return steps, binaries, ariths
}

func halfLoopCounters(lenB, lenM, lenE int, lenQE2 *big.Int) (*big.Int, *big.Int, *big.Int) {
	steps := newInt(153)
	steps.Add(steps, newMul(newInt(82), newInt(lenM)))
	steps.Add(steps, newMul(newInt(6), newInt(lenE)))
	steps.Add(steps, newInt((80*lenM*(lenM-1))/2))
	steps.Add(steps, newMul(newInt(19), newInt(lenM*lenM)))
	steps.Add(steps, newMul(newInt(25), lenQE2))

	binaries := newInt(9)
	binaries.Add(binaries, newMul(newInt(6), newInt(lenM)))
	binaries.Add(binaries, newInt((23*lenM*(lenM-1))/2))
	binaries.Add(binaries, newMul(newInt(2), newInt(lenM*lenM)))
	binaries.Add(binaries, newMul(newInt(3), lenQE2))

	ariths := newInt(-1)
	ariths.Add(ariths, newMul(newInt(2), newInt(lenM)))
	ariths.Add(ariths, newInt((2*lenM*(lenM-1))/2))
	ariths.Add(ariths, newInt(lenM*lenM))

	return steps, binaries, ariths
}

func fullLoopCounters(lenB, lenM, lenE int, lenQE2 *big.Int) (*big.Int, *big.Int, *big.Int) {
	steps := newInt(263)
	steps.Add(steps, newMul(newInt(114), newInt(lenM)))
	steps.Add(steps, newMul(newInt(6), newInt(lenE)))
	steps.Add(steps, newInt((80*lenM*(lenM-1))/2))
	steps.Add(steps, newMul(newInt(57), newInt(lenM*lenM)))
	steps.Add(steps, newMul(newInt(25), lenQE2))

	binaries := newInt(17)
	binaries.Add(binaries, newMul(newInt(3), newInt(lenM)))
	binaries.Add(binaries, newInt((23*lenM*(lenM-1))/2))
	binaries.Add(binaries, newMul(newInt(6), newInt(lenM*lenM)))
	binaries.Add(binaries, newMul(newInt(3), lenQE2))

	ariths := newInt(-1)
	ariths.Add(ariths, newMul(newInt(2), newInt(lenM)))
	ariths.Add(ariths, newInt((2*lenM*(lenM-1))/2))
	ariths.Add(ariths, newMul(newInt(3), newInt(lenM*lenM)))

	return steps, binaries, ariths
}

func newInt(in int) *big.Int {
	return big.NewInt(0).SetUint64(uint64(in))
}

func newMul(x *big.Int, vals ...*big.Int) *big.Int {
	res := newInt(0)
	for _, val := range vals {
		res.Mul(res, val)
	}
	return res
}
