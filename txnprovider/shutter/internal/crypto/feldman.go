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
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"math/big"

	blst "github.com/supranational/blst/bindings/go"
)

// Polynomial represents a polynomial over Z_q.
type Polynomial []*big.Int

// Gammas is a sequence of G2 points based on a polynomial.
type Gammas []*blst.P2Affine

var order *big.Int

func init() {
	var ok bool
	order, ok = new(big.Int).SetString("73eda753299d7d483339d80809a1d80553bda402fffe5bfeffffffff00000001", 16)
	if !ok {
		panic("invalid order")
	}
}

func generateP1(i *big.Int) *blst.P1Affine {
	s := bigToScalar(i)
	return blst.P1Generator().Mult(s).ToAffine()
}

func generateP2(i *big.Int) *blst.P2Affine {
	s := bigToScalar(i)
	return blst.P2Generator().Mult(s).ToAffine()
}

func bigToScalar(i *big.Int) *blst.Scalar {
	b := make([]byte, 32)
	i.FillBytes(b)
	s := new(blst.Scalar)
	s.FromBEndian(b)
	return s
}

// NewPolynomial creates a new polynomial from the given coefficients. It verifies the number and
// range of them.
func NewPolynomial(coefficients []*big.Int) (*Polynomial, error) {
	if len(coefficients) == 0 {
		return nil, errors.New("no coefficients given")
	}
	for i, v := range coefficients {
		if v.Sign() < 0 {
			return nil, fmt.Errorf("coefficient %d is negative (%d)", i, v)
		}
		if v.Cmp(order) >= 0 {
			return nil, fmt.Errorf("coefficient %d is too big (%d)", i, v)
		}
	}
	p := Polynomial(coefficients)
	return &p, nil
}

// Degree returns the degree of the polynomial.
func (p *Polynomial) Degree() uint64 {
	return uint64(len(*p)) - 1
}

// Degree returns the degree of the underlying polynomial.
func (g *Gammas) Degree() uint64 {
	return uint64(len(*g)) - 1
}

func (g Gammas) Equal(otherG Gammas) bool {
	gs := []*blst.P2Affine(g)
	otherGs := []*blst.P2Affine(otherG)

	if len(gs) != len(otherGs) {
		return false
	}
	for i := range gs {
		if !gs[i].Equals(otherGs[i]) {
			return false
		}
	}
	return true
}

// ZeroGammas returns the zero value for gammas.
func ZeroGammas(degree uint64) *Gammas {
	points := []*blst.P2Affine{}
	for i := uint64(0); i < degree+1; i++ {
		points = append(points, new(blst.P2Affine))
	}
	gammas := Gammas(points)
	return &gammas
}

// DegreeFromThreshold returns the degree polynomials should have for the given threshold.
func DegreeFromThreshold(threshold uint64) uint64 {
	return threshold - 1
}

// Eval evaluates the polynomial at the given coordinate.
func (p *Polynomial) Eval(x *big.Int) *big.Int {
	// uses Horner's method
	res := new(big.Int).Set((*p)[p.Degree()])
	for i := int(p.Degree()) - 1; i >= 0; i-- {
		res.Mul(res, x)
		res.Add(res, (*p)[i])
		res.Mod(res, order)
	}
	return res
}

// EvalForKeyper evaluates the polynomial at the position designated for the given keyper.
func (p *Polynomial) EvalForKeyper(keyperIndex int) *big.Int {
	x := KeyperX(keyperIndex)
	return p.Eval(x)
}

// ValidEval checks if the given value is a valid polynomial evaluation, i.e., if it is in Z_q.
func ValidEval(v *big.Int) bool {
	if v.Sign() < 0 {
		return false
	}
	if v.Cmp(order) >= 0 {
		return false
	}
	return true
}

// Gammas computes the gamma values for a given polynomial.
func (p *Polynomial) Gammas() *Gammas {
	gammas := Gammas{}
	for _, c := range *p {
		gamma := generateP2(c)
		gammas = append(gammas, gamma)
	}
	return &gammas
}

// Pi computes the pi value at the given x coordinate.
func (g *Gammas) Pi(xi *big.Int) *blst.P2Affine {
	xiToJ := big.NewInt(1)
	res := new(blst.P2)
	for _, gamma := range *g {
		p := new(blst.P2)
		p.FromAffine(gamma)
		p.MultAssign(bigToScalar(xiToJ))
		res.AddAssign(p)

		xiToJ.Mul(xiToJ, xi)
		xiToJ.Mod(xiToJ, order)
	}
	return res.ToAffine()
}

// GobEncode encodes a Gammas value. See https://golang.org/pkg/encoding/gob/#GobEncoder
func (g *Gammas) GobEncode() ([]byte, error) {
	buff := bytes.Buffer{}
	if g != nil {
		for _, p := range *g {
			buff.Write(p.Compress())
		}
	}
	return buff.Bytes(), nil
}

// GobDecode decodes a Gammas value. See https://golang.org/pkg/encoding/gob/#GobDecoder
func (g *Gammas) GobDecode(data []byte) error {
	for i := 0; i < len(data); i += blst.BLST_P2_COMPRESS_BYTES {
		p := new(blst.P2Affine)
		p.Uncompress(data[i : i+blst.BLST_P2_COMPRESS_BYTES])
		if !p.InG2() {
			return errors.New("gamma is not on curve")
		}
		*g = append(*g, p)
	}
	return nil
}

// KeyperX computes the x value assigned to the keyper identified by its index.
func KeyperX(keyperIndex int) *big.Int {
	keyperIndexBig := big.NewInt(int64(keyperIndex))
	return new(big.Int).Add(big.NewInt(1), keyperIndexBig)
}

// VerifyPolyEval checks that the evaluation of a polynomial is consistent with the public gammas.
func VerifyPolyEval(keyperIndex int, polyEval *big.Int, gammas *Gammas, threshold uint64) bool {
	if gammas.Degree() != threshold-1 {
		return false
	}
	rhs := generateP2(polyEval)
	lhs := gammas.Pi(KeyperX(keyperIndex))
	return rhs.Equals(lhs)
}

// RandomPolynomial generates a random polynomial of given degree.
func RandomPolynomial(r io.Reader, degree uint64) (*Polynomial, error) {
	coefficients := []*big.Int{}
	for i := uint64(0); i < degree+1; i++ {
		c, err := rand.Int(r, order)
		if err != nil {
			return nil, err
		}
		coefficients = append(coefficients, c)
	}
	return NewPolynomial(coefficients)
}
