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
	mathrand "math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	blst "github.com/supranational/blst/bindings/go"
)

func TestNewPolynomial(t *testing.T) {
	validCoefficients := [][]*big.Int{
		{
			big.NewInt(10),
		},
		{
			big.NewInt(0),
			big.NewInt(10),
			big.NewInt(20),
		},
		{
			new(big.Int).Sub(order, big.NewInt(1)),
		},
	}

	for _, cs := range validCoefficients {
		p, err := NewPolynomial(cs)
		require.NoError(t, err)
		for i, c := range cs {
			assert.Equal(t, c, (*p)[i])
		}
		assert.Equal(t, uint64(len(cs)-1), p.Degree())
	}

	invalidCoefficients := [][]*big.Int{
		{},
		{
			big.NewInt(-1),
		},
		{
			order,
		},
	}

	for _, cs := range invalidCoefficients {
		_, err := NewPolynomial(cs)
		assert.NotEqual(t, err, nil)
	}
}

func TestEval(t *testing.T) {
	p1, err := NewPolynomial([]*big.Int{big.NewInt(10), big.NewInt(20), big.NewInt(30)})
	require.NoError(t, err)
	assert.Equal(t, 0, big.NewInt(10).Cmp(p1.Eval(big.NewInt(0))))
	assert.Equal(t, 0, big.NewInt(10+20*10+30*100).Cmp(p1.Eval(big.NewInt(10))))

	p2, err := NewPolynomial([]*big.Int{big.NewInt(0), new(big.Int).Sub(order, big.NewInt(1))})
	require.NoError(t, err)
	assert.Equal(t, 0, big.NewInt(0).Cmp(p2.Eval(big.NewInt(0))))
	assert.Equal(t, 0, new(big.Int).Sub(order, big.NewInt(1)).Cmp(p2.Eval(big.NewInt(1))))
	assert.Equal(t, 0, new(big.Int).Sub(order, big.NewInt(2)).Cmp(p2.Eval(big.NewInt(2))))

	p3, err := NewPolynomial([]*big.Int{big.NewInt(0), big.NewInt(1)})
	require.NoError(t, err)
	assert.Equal(t, 0, big.NewInt(0).Cmp(p3.Eval(big.NewInt(0))))
	assert.Equal(t, 0, big.NewInt(0).Cmp(p3.Eval(order)))
	assert.Equal(t, 0, big.NewInt(0).Cmp(p3.Eval(new(big.Int).Mul(order, big.NewInt(5)))))
}

func TestEvalForKeyper(t *testing.T) {
	p, err := NewPolynomial([]*big.Int{big.NewInt(10), big.NewInt(20), big.NewInt(30)})
	require.NoError(t, err)
	v0 := p.EvalForKeyper(0)
	v1 := p.EvalForKeyper(1)
	assert.Equal(t, v0, p.Eval(KeyperX(0)))
	assert.Equal(t, v1, p.Eval(KeyperX(1)))
	assert.NotEqual(t, v0.Cmp(v1), 0)
}

func TestValidEval(t *testing.T) {
	valid := []*big.Int{
		big.NewInt(0),
		big.NewInt(1),
		new(big.Int).Sub(order, big.NewInt(2)),
		new(big.Int).Sub(order, big.NewInt(1)),
	}

	invalid := []*big.Int{
		big.NewInt(-2),
		big.NewInt(-1),
		order,
		new(big.Int).Add(order, big.NewInt(1)),
	}
	for _, v := range valid {
		assert.True(t, ValidEval(v))
	}
	for _, v := range invalid {
		assert.False(t, ValidEval(v))
	}
}

func TestRandomPolynomial(t *testing.T) {
	p, err := RandomPolynomial(rand.Reader, uint64(5))
	require.NoError(t, err)
	assert.Equal(t, uint64(5), p.Degree())
}

func TestGammas(t *testing.T) {
	p, err := NewPolynomial([]*big.Int{
		big.NewInt(0),
		big.NewInt(10),
		big.NewInt(20),
	})
	require.NoError(t, err)
	gammas := p.Gammas()
	assert.Equal(t, p.Degree(), uint64(len(*gammas))-1)
	assert.Equal(t, p.Degree(), gammas.Degree())

	expected := Gammas([]*blst.P2Affine{
		makeTestG2(0),
		makeTestG2(10),
		makeTestG2(20),
	})
	assert.Equal(t, len(expected), len(*gammas))
	for i := range *gammas {
		assert.True(t, ([]*blst.P2Affine)(*gammas)[i].Equals(expected[i]))
	}
}

func TestZeroGammas(t *testing.T) {
	g := ZeroGammas(uint64(3))
	assert.Len(t, *g, 4)
	for _, p := range *g {
		assert.True(t, p.Equals(new(blst.P2Affine)))
	}
}

func TestVerifyPolyEval(t *testing.T) {
	randReader := mathrand.New(mathrand.NewSource(0))

	threshold := uint64(2)

	p1, err := RandomPolynomial(randReader, threshold-1)
	require.NoError(t, err)

	p2, err := RandomPolynomial(randReader, threshold-1)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		xi := KeyperX(i)
		vi1 := p1.Eval(xi)
		vi2 := p2.Eval(xi)
		assert.True(t, VerifyPolyEval(i, vi1, p1.Gammas(), threshold))
		assert.True(t, VerifyPolyEval(i, vi2, p2.Gammas(), threshold))
		assert.False(t, VerifyPolyEval(i, vi1, p2.Gammas(), threshold))
		assert.False(t, VerifyPolyEval(i, vi2, p1.Gammas(), threshold))
		assert.False(t, VerifyPolyEval(i+1, vi1, p1.Gammas(), threshold))
		assert.False(t, VerifyPolyEval(i+1, vi2, p2.Gammas(), threshold))
	}
}

func TestPi(t *testing.T) {
	gammasNonAff := []*blst.P2{}
	gammasAff := []*blst.P2Affine{}
	for i := 0; i < 3; i++ {
		gammaAff := makeTestG2(int64(i + 2))
		gamma := new(blst.P2)
		gamma.FromAffine(gammaAff)
		gammasAff = append(gammasAff, gammaAff)
		gammasNonAff = append(gammasNonAff, gamma)
	}
	gammas := Gammas(gammasAff)

	pi1 := gammas.Pi(big.NewInt(1))
	pi2 := gammas.Pi(big.NewInt(2))

	pi1Exp := gammasNonAff[0].Add(gammasNonAff[1]).Add(gammasNonAff[2]).ToAffine()
	bytes2 := make([]byte, 32)
	big.NewInt(2).FillBytes(bytes2)
	pi2Exp := gammasNonAff[0].
		Add(gammasNonAff[1].Mult(bigToScalar(big.NewInt(2)))).
		Add(gammasNonAff[2].Mult(bigToScalar(big.NewInt(4)))).ToAffine()

	assert.True(t, pi1.Equals(pi1Exp))
	assert.True(t, pi2.Equals(pi2Exp))
}

func TestGammasGobable(t *testing.T) {
	p, err := NewPolynomial([]*big.Int{
		big.NewInt(0),
		big.NewInt(10),
		big.NewInt(20),
	})
	require.NoError(t, err)

	gammas := p.Gammas()
	deserialized := new(Gammas)
	EnsureGobable(t, gammas, deserialized)
}
