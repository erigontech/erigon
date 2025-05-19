// Copyright 2025 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package crypto

import (
	"crypto/rand"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blst "github.com/supranational/blst/bindings/go"
)

func makeTestG1(n int64) *blst.P1Affine {
	return generateP1(big.NewInt(n))
}

func makeTestG2(n int64) *blst.P2Affine {
	return generateP2(big.NewInt(n))
}

func TestEonSecretKeyShare(t *testing.T) {
	zeroKey := ComputeEonSecretKeyShare([]*big.Int{})
	assert.Equal(t, (*big.Int)(zeroKey), big.NewInt(0))

	key1 := ComputeEonSecretKeyShare([]*big.Int{
		big.NewInt(10),
		big.NewInt(20),
		big.NewInt(30),
	})
	assert.Equal(t, (*big.Int)(key1), big.NewInt(60))

	key2 := ComputeEonSecretKeyShare([]*big.Int{
		order,
		big.NewInt(10),
		order,
		big.NewInt(20),
		order,
	})
	assert.Equal(t, (*big.Int)(key2), big.NewInt(30))
}

func TestEonPublicKeyShare(t *testing.T) {
	gammas0 := Gammas{
		makeTestG2(1),
		makeTestG2(2),
	}
	gammas1 := Gammas{
		makeTestG2(3),
		makeTestG2(4),
	}
	gammas2 := Gammas{
		makeTestG2(5),
		makeTestG2(6),
	}
	gammasAff := []*Gammas{
		&gammas0,
		&gammas1,
		&gammas2,
	}
	gammas := [][]*blst.P2{}
	for i := 0; i < len(gammasAff); i++ {
		gammas = append(gammas, []*blst.P2{})
		for j := 0; j < len(*gammasAff[i]); j++ {
			gammas[i] = append(gammas[i], new(blst.P2))
			gammas[i][j].FromAffine((*gammasAff[i])[j])
		}
	}

	x0 := bigToScalar(KeyperX(0))
	x1 := bigToScalar(KeyperX(1))
	x2 := bigToScalar(KeyperX(2))

	mu00 := gammas[0][0].Add(gammas[0][1].Mult(x0))
	mu01 := gammas[1][0].Add(gammas[1][1].Mult(x0))
	mu02 := gammas[2][0].Add(gammas[2][1].Mult(x0))
	mu10 := gammas[0][0].Add(gammas[0][1].Mult(x1))
	mu11 := gammas[1][0].Add(gammas[1][1].Mult(x1))
	mu12 := gammas[2][0].Add(gammas[2][1].Mult(x1))
	mu20 := gammas[0][0].Add(gammas[0][1].Mult(x2))
	mu21 := gammas[1][0].Add(gammas[1][1].Mult(x2))
	mu22 := gammas[2][0].Add(gammas[2][1].Mult(x2))

	pks0 := mu00.Add(mu01).Add(mu02).ToAffine()
	pks1 := mu10.Add(mu11).Add(mu12).ToAffine()
	pks2 := mu20.Add(mu21).Add(mu22).ToAffine()

	assert.True(t, pks0.Equals((*blst.P2Affine)(ComputeEonPublicKeyShare(0, gammasAff))))
	assert.True(t, pks1.Equals((*blst.P2Affine)(ComputeEonPublicKeyShare(1, gammasAff))))
	assert.True(t, pks2.Equals((*blst.P2Affine)(ComputeEonPublicKeyShare(2, gammasAff))))
}

func TestEonSharesMatch(t *testing.T) {
	threshold := uint64(2)
	p1, err := RandomPolynomial(rand.Reader, threshold)
	require.NoError(t, err)
	p2, err := RandomPolynomial(rand.Reader, threshold)
	require.NoError(t, err)
	p3, err := RandomPolynomial(rand.Reader, threshold)
	require.NoError(t, err)

	x1 := KeyperX(0)
	x2 := KeyperX(1)
	x3 := KeyperX(2)

	gammas := []*Gammas{p1.Gammas(), p2.Gammas(), p3.Gammas()}

	v11 := p1.Eval(x1)
	v21 := p1.Eval(x2)
	v31 := p1.Eval(x3)
	v12 := p2.Eval(x1)
	v22 := p2.Eval(x2)
	v32 := p2.Eval(x3)
	v13 := p3.Eval(x1)
	v23 := p3.Eval(x2)
	v33 := p3.Eval(x3)

	esk1 := (*big.Int)(ComputeEonSecretKeyShare([]*big.Int{v11, v12, v13}))
	esk2 := (*big.Int)(ComputeEonSecretKeyShare([]*big.Int{v21, v22, v23}))
	esk3 := (*big.Int)(ComputeEonSecretKeyShare([]*big.Int{v31, v32, v33}))

	epk1 := ComputeEonPublicKeyShare(0, gammas)
	epk2 := ComputeEonPublicKeyShare(1, gammas)
	epk3 := ComputeEonPublicKeyShare(2, gammas)

	epk1Exp := generateP2(esk1)
	epk2Exp := generateP2(esk2)
	epk3Exp := generateP2(esk3)

	assert.True(t, (*blst.P2Affine)(epk1).Equals(epk1Exp))
	assert.True(t, (*blst.P2Affine)(epk2).Equals(epk2Exp))
	assert.True(t, (*blst.P2Affine)(epk3).Equals(epk3Exp))
}

func TestEonPublicKey(t *testing.T) {
	zeroEPK := ComputeEonPublicKey([]*Gammas{})
	assert.True(t, (*blst.P2Affine)(zeroEPK).Equals(new(blst.P2Affine)))

	threshold := uint64(2)
	p1, err := RandomPolynomial(rand.Reader, threshold)
	require.NoError(t, err)
	p2, err := RandomPolynomial(rand.Reader, threshold)
	require.NoError(t, err)
	p3, err := RandomPolynomial(rand.Reader, threshold)
	require.NoError(t, err)

	k1 := ComputeEonPublicKey([]*Gammas{p1.Gammas()})
	assert.True(t, (*blst.P2Affine)(k1).Equals([]*blst.P2Affine(*p1.Gammas())[0]))
	k2 := ComputeEonPublicKey([]*Gammas{p2.Gammas()})
	assert.True(t, (*blst.P2Affine)(k2).Equals([]*blst.P2Affine(*p2.Gammas())[0]))
	k3 := ComputeEonPublicKey([]*Gammas{p3.Gammas()})
	assert.True(t, (*blst.P2Affine)(k3).Equals([]*blst.P2Affine(*p3.Gammas())[0]))
}

func TestEonPublicKeyMatchesSecretKey(t *testing.T) {
	threshold := uint64(2)
	p1, err := RandomPolynomial(rand.Reader, threshold)
	require.NoError(t, err)
	p2, err := RandomPolynomial(rand.Reader, threshold)
	require.NoError(t, err)
	p3, err := RandomPolynomial(rand.Reader, threshold)
	require.NoError(t, err)

	esk := big.NewInt(0)
	for _, p := range []*Polynomial{p1, p2, p3} {
		esk = esk.Add(esk, p.Eval(big.NewInt(0)))
		esk = esk.Mod(esk, order)
	}

	epkExp := generateP2(esk)

	gammas := []*Gammas{p1.Gammas(), p2.Gammas(), p3.Gammas()}
	epk := ComputeEonPublicKey(gammas)
	assert.True(t, (*blst.P2Affine)(epk).Equals(epkExp))
}

var modOrderComparer = func(x, y *big.Int) bool {
	d := new(big.Int).Sub(x, y)
	return d.Mod(d, order).Sign() == 0
}

func TestInverse(t *testing.T) {
	testCases := []*big.Int{
		big.NewInt(1),
		big.NewInt(2),
		big.NewInt(3),
		new(big.Int).Sub(order, big.NewInt(2)),
		new(big.Int).Sub(order, big.NewInt(1)),
	}
	for i := 0; i < 100; i++ {
		x, err := rand.Int(rand.Reader, order)
		require.NoError(t, err)
		if x.Sign() == 0 {
			continue
		}
		testCases = append(testCases, x)
	}

	for _, test := range testCases {
		inv := invert(test)
		one := new(big.Int).Mul(test, inv)
		assert.True(t, modOrderComparer(big.NewInt(1), one))
	}
}

func TestLagrangeCoefficientFactors(t *testing.T) {
	l01 := lagrangeCoefficientFactor(0, 1)
	l02 := lagrangeCoefficientFactor(0, 2)
	l10 := lagrangeCoefficientFactor(1, 0)
	l12 := lagrangeCoefficientFactor(1, 2)
	l20 := lagrangeCoefficientFactor(2, 0)
	l21 := lagrangeCoefficientFactor(2, 1)

	qMinus1 := new(big.Int).Sub(order, big.NewInt(1))
	qMinus2 := new(big.Int).Sub(order, big.NewInt(2))

	l01.Mul(l01, qMinus1)
	assert.True(t, modOrderComparer(big.NewInt(1), l01))
	l02.Mul(l02, qMinus2)
	assert.True(t, modOrderComparer(big.NewInt(1), l02))

	assert.True(t, modOrderComparer(big.NewInt(2), l10))
	l12.Mul(l12, qMinus1)
	assert.True(t, modOrderComparer(big.NewInt(2), l12))

	l20.Mul(l20, big.NewInt(2))
	assert.True(t, modOrderComparer(big.NewInt(3), l20))
	assert.True(t, modOrderComparer(big.NewInt(3), l21))
}

func TestLagrangeCoefficients(t *testing.T) {
	assert.Equal(t, big.NewInt(1), lagrangeCoefficient(0, []int{0}))
	assert.Equal(t, big.NewInt(1), lagrangeCoefficient(1, []int{1}))
	assert.Equal(t, big.NewInt(1), lagrangeCoefficient(2, []int{2}))

	assert.Equal(t, lagrangeCoefficientFactor(1, 0), lagrangeCoefficient(0, []int{0, 1}))
	assert.Equal(t, lagrangeCoefficientFactor(0, 1), lagrangeCoefficient(1, []int{0, 1}))

	l0 := lagrangeCoefficient(0, []int{0, 1, 2})
	l0Exp := lagrangeCoefficientFactor(1, 0)
	l0Exp.Mul(l0Exp, lagrangeCoefficientFactor(2, 0))
	assert.True(t, modOrderComparer(l0Exp, l0))

	l1 := lagrangeCoefficient(1, []int{0, 1, 2})
	l1Exp := lagrangeCoefficientFactor(0, 1)
	l1Exp.Mul(l1Exp, lagrangeCoefficientFactor(2, 1))
	assert.True(t, modOrderComparer(l1Exp, l1))

	l2 := lagrangeCoefficient(2, []int{0, 1, 2})
	l2Exp := lagrangeCoefficientFactor(0, 2)
	l2Exp.Mul(l2Exp, lagrangeCoefficientFactor(1, 2))
	assert.True(t, modOrderComparer(l2Exp, l2))
}

func TestLagrangeReconstruct(t *testing.T) {
	p, err := RandomPolynomial(rand.Reader, uint64(2))
	require.NoError(t, err)

	l1 := lagrangeCoefficient(0, []int{0, 1, 2})
	l2 := lagrangeCoefficient(1, []int{0, 1, 2})
	l3 := lagrangeCoefficient(2, []int{0, 1, 2})
	v1 := p.EvalForKeyper(0)
	v2 := p.EvalForKeyper(1)
	v3 := p.EvalForKeyper(2)

	y1 := new(big.Int).Mul(l1, v1)
	y2 := new(big.Int).Mul(l2, v2)
	y3 := new(big.Int).Mul(l3, v3)
	y1.Mod(y1, order)
	y2.Mod(y2, order)
	y3.Mod(y3, order)

	y := new(big.Int).Add(y1, y2)
	y.Add(y, y3)
	y.Mod(y, order)

	assert.Equal(t, p.Eval(big.NewInt(0)), y)
}

func TestComputeEpochSecretKeyShare(t *testing.T) {
	eonSecretKeyShare := (*EonSecretKeyShare)(big.NewInt(123))
	eonSecretKeyShareScalar := bigToScalar((*big.Int)(eonSecretKeyShare))
	epochID := ComputeEpochID([]byte("epoch1"))
	epochIDP1 := new(blst.P1)
	epochIDP1.FromAffine((*blst.P1Affine)(epochID))
	epochSecretKeyShare := ComputeEpochSecretKeyShare(eonSecretKeyShare, epochID)
	expectedEpochSecretKeyShare := epochIDP1.Mult(eonSecretKeyShareScalar).ToAffine()
	assert.True(t, expectedEpochSecretKeyShare.Equals((*blst.P1Affine)(epochSecretKeyShare)))
}

func TestVerifyEpochSecretKeyShare(t *testing.T) {
	threshold := uint64(2)
	epochID := ComputeEpochID([]byte("epoch1"))
	p1, err := RandomPolynomial(rand.Reader, threshold-1)
	require.NoError(t, err)
	p2, err := RandomPolynomial(rand.Reader, threshold-1)
	require.NoError(t, err)
	p3, err := RandomPolynomial(rand.Reader, threshold-1)
	require.NoError(t, err)

	gammas := []*Gammas{
		p1.Gammas(),
		p2.Gammas(),
		p3.Gammas(),
	}

	epk1 := ComputeEonPublicKeyShare(0, gammas)
	epk2 := ComputeEonPublicKeyShare(1, gammas)
	epk3 := ComputeEonPublicKeyShare(2, gammas)
	esk1 := ComputeEonSecretKeyShare([]*big.Int{p1.EvalForKeyper(0), p2.EvalForKeyper(0), p3.EvalForKeyper(0)})
	esk2 := ComputeEonSecretKeyShare([]*big.Int{p1.EvalForKeyper(1), p2.EvalForKeyper(1), p3.EvalForKeyper(1)})
	esk3 := ComputeEonSecretKeyShare([]*big.Int{p1.EvalForKeyper(2), p2.EvalForKeyper(2), p3.EvalForKeyper(2)})
	epsk1 := ComputeEpochSecretKeyShare(esk1, epochID)
	epsk2 := ComputeEpochSecretKeyShare(esk2, epochID)
	epsk3 := ComputeEpochSecretKeyShare(esk3, epochID)

	assert.True(t, VerifyEpochSecretKeyShare(epsk1, epk1, epochID))
	assert.True(t, VerifyEpochSecretKeyShare(epsk2, epk2, epochID))
	assert.True(t, VerifyEpochSecretKeyShare(epsk3, epk3, epochID))

	assert.False(t, VerifyEpochSecretKeyShare(epsk1, epk2, epochID))
	assert.False(t, VerifyEpochSecretKeyShare(epsk2, epk1, epochID))
	assert.False(t, VerifyEpochSecretKeyShare(epsk1, epk1, ComputeEpochID([]byte("epoch2"))))
}

func TestVerifyEpochSecretKey(t *testing.T) {
	p, err := RandomPolynomial(rand.Reader, 0)
	require.NoError(t, err)
	eonPublicKey := ComputeEonPublicKey([]*Gammas{p.Gammas()})

	epochIDBytes := []byte("epoch1")
	epochID := ComputeEpochID(epochIDBytes)

	v := p.EvalForKeyper(0)
	eonSecretKeyShare := ComputeEonSecretKeyShare([]*big.Int{v})
	epochSecretKeyShare := ComputeEpochSecretKeyShare(eonSecretKeyShare, epochID)
	epochSecretKey, err := ComputeEpochSecretKey(
		[]int{0},
		[]*EpochSecretKeyShare{epochSecretKeyShare},
		1,
	)
	require.NoError(t, err)

	ok, err := VerifyEpochSecretKey(epochSecretKey, eonPublicKey, epochIDBytes)
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = VerifyEpochSecretKey(epochSecretKey, eonPublicKey, append(epochIDBytes, 0xab))
	require.NoError(t, err)
	assert.False(t, ok)

	var sigma Block
	message := []byte("msg")
	ok, err = VerifyEpochSecretKeyDeterministic(epochSecretKey, eonPublicKey, epochIDBytes, sigma, message)
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = VerifyEpochSecretKeyDeterministic(epochSecretKey, eonPublicKey, append(epochIDBytes, 0xab), sigma, message)
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestComputeEpochSecretKey(t *testing.T) {
	n := 3
	threshold := uint64(2)
	epochID := ComputeEpochID([]byte("epoch1"))

	ps := []*Polynomial{}
	for i := 0; i < n; i++ {
		p, err := RandomPolynomial(rand.Reader, threshold-1)
		require.NoError(t, err)
		ps = append(ps, p)
	}

	epochSecretKeyShares := []*EpochSecretKeyShare{}
	for i := 0; i < n; i++ {
		vs := []*big.Int{}
		for _, p := range ps {
			v := p.EvalForKeyper(i)
			vs = append(vs, v)
		}
		eonSecretKeyShare := ComputeEonSecretKeyShare(vs)
		epochSecretKeyShare := ComputeEpochSecretKeyShare(eonSecretKeyShare, epochID)

		epochSecretKeyShares = append(epochSecretKeyShares, epochSecretKeyShare)
	}

	var err error
	_, err = ComputeEpochSecretKey([]int{0}, epochSecretKeyShares[:1], threshold)
	assert.NotEqual(t, err, nil)
	_, err = ComputeEpochSecretKey([]int{0, 1, 2}, epochSecretKeyShares[:2], threshold)
	assert.NotEqual(t, err, nil)
	_, err = ComputeEpochSecretKey([]int{0, 1}, epochSecretKeyShares[:1], threshold)
	assert.NotEqual(t, err, nil)
	_, err = ComputeEpochSecretKey([]int{0}, epochSecretKeyShares[:2], threshold)
	assert.NotEqual(t, err, nil)

	epochSecretKey12, err := ComputeEpochSecretKey(
		[]int{0, 1},
		[]*EpochSecretKeyShare{epochSecretKeyShares[0], epochSecretKeyShares[1]},
		threshold)
	require.NoError(t, err)
	epochSecretKey13, err := ComputeEpochSecretKey(
		[]int{0, 2},
		[]*EpochSecretKeyShare{epochSecretKeyShares[0], epochSecretKeyShares[2]},
		threshold)
	require.NoError(t, err)
	epochSecretKey23, err := ComputeEpochSecretKey(
		[]int{1, 2},
		[]*EpochSecretKeyShare{epochSecretKeyShares[1], epochSecretKeyShares[2]},
		threshold)
	require.NoError(t, err)

	assert.True(t, epochSecretKey12.Equal(epochSecretKey13))
	assert.True(t, epochSecretKey12.Equal(epochSecretKey23))
}

func TestFull(t *testing.T) {
	n := 3
	threshold := uint64(2)
	epochID := ComputeEpochID([]byte("epoch1"))

	ps := []*Polynomial{}
	gammas := []*Gammas{}
	for i := 0; i < n; i++ {
		p, err := RandomPolynomial(rand.Reader, threshold-1)
		require.NoError(t, err)
		ps = append(ps, p)
		gammas = append(gammas, p.Gammas())
	}

	eonSecretKeyShares := []*EonSecretKeyShare{}
	for i := 0; i < n; i++ {
		vs := []*big.Int{}
		for j := 0; j < n; j++ {
			v := ps[j].EvalForKeyper(i)
			vs = append(vs, v)
		}
		eonSecretKeyShare := ComputeEonSecretKeyShare(vs)
		eonSecretKeyShares = append(eonSecretKeyShares, eonSecretKeyShare)
	}

	eonPublicKeyShares := []*EonPublicKeyShare{}
	for i := 0; i < n; i++ {
		eonPublicKeyShare := ComputeEonPublicKeyShare(i, gammas)
		eonPublicKeyShares = append(eonPublicKeyShares, eonPublicKeyShare)
	}

	epochSecretKeyShares := []*EpochSecretKeyShare{}
	for i := 0; i < n; i++ {
		epochSecretKeyShare := ComputeEpochSecretKeyShare(eonSecretKeyShares[i], epochID)
		epochSecretKeyShares = append(epochSecretKeyShares, epochSecretKeyShare)
	}

	// verify (published) epoch sk shares
	for i := 0; i < n; i++ {
		assert.True(t, VerifyEpochSecretKeyShare(epochSecretKeyShares[i], eonPublicKeyShares[i], epochID))
	}

	epochSecretKey, err := ComputeEpochSecretKey(
		[]int{0, 1},
		[]*EpochSecretKeyShare{epochSecretKeyShares[0], epochSecretKeyShares[1]},
		threshold)
	require.NoError(t, err)

	epochSecretKey13, err := ComputeEpochSecretKey(
		[]int{0, 2},
		[]*EpochSecretKeyShare{epochSecretKeyShares[0], epochSecretKeyShares[2]},
		threshold)
	require.NoError(t, err)
	epochSecretKey23, err := ComputeEpochSecretKey(
		[]int{1, 2},
		[]*EpochSecretKeyShare{epochSecretKeyShares[1], epochSecretKeyShares[2]},
		threshold)
	require.NoError(t, err)
	assert.True(t, epochSecretKey.Equal(epochSecretKey13))
	assert.True(t, epochSecretKey.Equal(epochSecretKey23))
}
