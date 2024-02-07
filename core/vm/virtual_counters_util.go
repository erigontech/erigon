package vm

import (
	"math"
	"math/big"
)

var BASE = new(big.Int).Lsh(big.NewInt(1), 256)

func expectedModExpCounters(lenB, lenE, lenM int, B *big.Int, E *big.Int, M *big.Int) (*big.Int, *big.Int, *big.Int) {
	q_b_m := big.NewInt(0).Div(B, M)
	r_b_m := big.NewInt(0).Mod(B, M)
	bsq := big.NewInt(0).Mul(B, B)
	nz_bsq := newInt(0).SetUint64(uint64(2*lenB - computeLenThisBase(bsq)))
	q_bsq_m := big.NewInt(0).Div(bsq, M)
	r_bsq_m := big.NewInt(0).Mod(bsq, M)
	bm := big.NewInt(0).Mul(B, M)

	e2 := newInt(0).SetUint64(uint64(math.Floor(float64(lenE) / 2)))

	base := big.NewInt(0)
	nTimesOdd := big.NewInt(0)
	for E.Cmp(base) == 1 {
		nTimesOdd.Add(nTimesOdd, big.NewInt(0).And(E, big.NewInt(1)))
		E.Rsh(E, 1)
	}
	nTimesEven := big.NewInt(0).Mul(big.NewInt(0).SetUint64(uint64(lenE)), big.NewInt(256))

	stepFirstDiv, binaryFirstDiv, arithFirstDiv := setupAndFirstDivCounters(lenB, lenM, q_b_m, r_b_m, B, M)
	stepHalfLoop, binaryHalfLoop, arithHalfLoop := halfLoopCounters(lenB, lenM, lenE, e2, q_bsq_m, r_bsq_m, bsq, M, nz_bsq)
	stepFullLoop, binaryFullLoop, arithFullLoop := fullLoopCounters(lenB, lenM, lenE, e2, q_bsq_m, r_bsq_m, bm, M, bsq, nz_bsq)

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

func setupAndFirstDivCounters(lenB, lenM int, q_b_m, r_b_m, B, M *big.Int) (*big.Int, *big.Int, *big.Int) {
	steps := newInt(218)
	steps.Add(steps, newMul(newInt(39), newInt(lenB)))
	steps.Add(steps, newMul(newInt(45), newInt(lenM)))
	steps.Add(steps, newMul(newInt(computeLenThisBase(q_b_m)), newInt(30*30*lenM)))
	steps.Add(steps, newMul(newInt(17), newInt(computeLenThisBase(r_b_m))))
	steps.Sub(steps, newMul(newInt(14), newInt(firstDiffChunk(B, M))))
	steps.Sub(steps, newMul(newInt(7), newInt(firstDiffChunk(M, r_b_m))))

	binaries := newInt(12)
	binaries.Add(binaries, newMul(newInt(6), newInt(lenB)))
	binaries.Add(binaries, newMul(newInt(3), newInt(lenM)))
	binaries.Add(binaries, newMul(newInt(computeLenThisBase(q_b_m)), newInt(1+4*lenM)))
	binaries.Add(binaries, newInt(computeLenThisBase(r_b_m)))
	binaries.Sub(binaries, newMul(newInt(4), newInt(firstDiffChunk(B, M))))
	binaries.Sub(binaries, newMul(newInt(2), newInt(firstDiffChunk(M, r_b_m))))

	ariths := newInt(1)
	ariths.Add(ariths, newMul(newInt(computeLenThisBase(q_b_m)), newInt(lenM)))

	return steps, binaries, ariths
}

func halfLoopCounters(lenB, lenM, lenE int, E2, q_bsq_m, r_bsq_m, bsq, M, nz_bsq *big.Int) (*big.Int, *big.Int, *big.Int) {
	steps := newInt(399)
	steps.Add(steps, newMul(newInt(100), newInt(lenB)))
	steps.Add(steps, newMul(newInt(61), newInt((lenB*(lenB+1))/2)))
	steps.Add(steps, newMul(newInt(48), newInt(lenM)))
	steps.Add(steps, newMul(newInt(19), newInt(lenE)))
	steps.Add(steps, newMul(newInt(44), E2))
	steps.Add(steps, newMul(newInt(computeLenThisBase(q_bsq_m)), newInt(30*30*lenM)))
	steps.Add(steps, newMul(newInt(14), newInt(computeLenThisBase(r_bsq_m))))
	steps.Add(steps, newMul(newInt(14), newInt(firstDiffChunk(bsq, M))))
	steps.Sub(steps, newMul(newInt(14), newInt(firstDiffChunk(bsq, M))))
	steps.Sub(steps, newMul(newInt(7), newInt(firstDiffChunk(M, r_bsq_m))))
	steps.Sub(steps, newMul(newInt(5), nz_bsq))

	binaries := newInt(23)
	binaries.Add(binaries, newMul(newInt(14), newInt(lenB)))
	binaries.Add(binaries, newMul(newInt(9), newInt((lenB*(lenB+1))/2)))
	binaries.Add(binaries, newMul(newInt(3), newInt(lenM)))
	binaries.Add(binaries, newMul(newInt(2), newInt(lenE)))
	binaries.Add(binaries, newMul(newInt(3), E2))
	binaries.Add(binaries, newMul(newInt(computeLenThisBase(q_bsq_m)), newInt(1+4+lenM)))
	binaries.Add(binaries, newInt(computeLenThisBase(r_bsq_m)))
	binaries.Sub(binaries, newMul(newInt(4), newInt(firstDiffChunk(bsq, M))))
	binaries.Sub(binaries, newMul(newInt(2), newInt(firstDiffChunk(M, r_bsq_m))))
	binaries.Sub(binaries, nz_bsq)

	ariths := newInt(2)
	ariths.Add(ariths, newInt(lenB))
	ariths.Add(ariths, newInt((lenB*(lenB+1))/2))
	ariths.Add(ariths, E2)
	ariths.Add(ariths, newMul(newInt(computeLenThisBase(q_bsq_m)), newInt(lenM)))

	return steps, binaries, ariths
}

func fullLoopCounters(lenB, lenM, lenE int, E2, q_bsq_m, r_bsq_m, BM, M, bsq, nz_bsq *big.Int) (*big.Int, *big.Int, *big.Int) {
	steps := newInt(674)
	steps.Add(steps, newMul(newInt(180), newInt(lenB)))
	steps.Add(steps, newMul(newInt(61), newInt((lenB*(lenB+1))/2)))
	steps.Add(steps, newMul(newInt(149), newInt(lenM)))
	steps.Add(steps, newMul(newInt(19), newInt(lenE)))
	steps.Add(steps, newMul(newInt(44), E2))
	steps.Add(steps, newMul(newInt(66), newInt(lenB), newInt(lenM)))
	steps.Add(steps, newMul(newInt(computeLenThisBase(q_bsq_m)), newInt(30*30*lenM)))
	steps.Add(steps, newMul(newInt(14), newInt(computeLenThisBase(r_bsq_m))))
	steps.Sub(steps, newMul(newInt(14), newInt(firstDiffChunk(BM, M))))
	steps.Sub(steps, newMul(newInt(14), newInt(firstDiffChunk(bsq, M))))
	steps.Sub(steps, newMul(newInt(7), newInt(firstDiffChunk(M, newInt(0)))))
	steps.Sub(steps, newMul(newInt(7), newInt(firstDiffChunk(M, r_bsq_m))))
	steps.Sub(steps, newMul(newInt(5), nz_bsq))

	binaries := newInt(36)
	binaries.Add(binaries, newMul(newInt(21), newInt(lenB)))
	binaries.Add(binaries, newMul(newInt(9), newInt((lenB*(lenB+1))/2)))
	binaries.Add(binaries, newMul(newInt(12), newInt(lenM)))
	binaries.Add(binaries, newMul(newInt(2), newInt(lenE)))
	binaries.Add(binaries, newMul(newInt(3), E2))
	binaries.Add(binaries, newMul(newInt(8), newInt(lenB), newInt(lenM)))
	binaries.Add(binaries, newMul(newInt(computeLenThisBase(q_bsq_m)), newInt(1+4*lenM)))
	binaries.Add(binaries, newInt(computeLenThisBase(r_bsq_m)))
	binaries.Sub(binaries, newMul(newInt(4), newInt(firstDiffChunk(BM, M))))
	binaries.Sub(binaries, newMul(newInt(4), newInt(firstDiffChunk(bsq, M))))
	binaries.Sub(binaries, newMul(newInt(2), newInt(firstDiffChunk(M, newInt(0)))))
	binaries.Sub(binaries, newMul(newInt(2), newInt(firstDiffChunk(M, r_bsq_m))))
	binaries.Sub(binaries, nz_bsq)

	ariths := newInt(4)
	ariths.Add(ariths, newInt(lenB))
	ariths.Add(ariths, newInt((lenB*(lenB+1))/2))
	ariths.Add(ariths, E2)
	ariths.Add(ariths, newMul(newInt(2), newInt(lenB), newInt(lenM)))
	ariths.Add(ariths, newMul(newInt(computeLenThisBase(q_bsq_m)), newInt(lenM)))

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

func firstDiffChunk(x, y *big.Int) int {
	xLen := computeLenThisBase(x)
	yLen := computeLenThisBase(y)

	if xLen != yLen {
		return xLen
	}

	i := xLen - 1
	mask := new(big.Int).SetBytes([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff})
	for ; i >= 0; i-- {
		xShifted := new(big.Int).Rsh(x, uint(i)*256)
		yShifted := new(big.Int).Rsh(y, uint(i)*256)
		if new(big.Int).And(xShifted, mask).Cmp(new(big.Int).And(yShifted, mask)) != 0 {
			break
		}
	}

	return i + 1
}
