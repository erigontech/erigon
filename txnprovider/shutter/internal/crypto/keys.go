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
	"fmt"
	"math/big"

	blst "github.com/supranational/blst/bindings/go"
)

// EonSecretKeyShare represents a share of the eon secret key.
type EonSecretKeyShare big.Int

// EonPublicKeyShare represents a share of the eon public key.
type EonPublicKeyShare blst.P2Affine

// EonPublicKey represents the combined eon public key.
type EonPublicKey blst.P2Affine

// EpochID is the identifier of an epoch.
type EpochID blst.P1Affine

// EpochSecretKeyShare represents a keyper's share of the epoch sk key.
type EpochSecretKeyShare blst.P1Affine

// EpochSecretKey represents an epoch secret key.
type EpochSecretKey blst.P1Affine

func (eonPublicKey *EonPublicKey) GobEncode() ([]byte, error) {
	return eonPublicKey.Marshal(), nil
}

func (eonPublicKey *EonPublicKey) GobDecode(data []byte) error {
	return eonPublicKey.Unmarshal(data)
}

func (eonPublicKey *EonPublicKey) Equal(pk2 *EonPublicKey) bool {
	return (*blst.P2Affine)(eonPublicKey).Equals((*blst.P2Affine)(pk2))
}

func (eonPublicKeyShare *EonPublicKeyShare) GobEncode() ([]byte, error) {
	return eonPublicKeyShare.Marshal(), nil
}

func (eonPublicKeyShare *EonPublicKeyShare) GobDecode(data []byte) error {
	return eonPublicKeyShare.Unmarshal(data)
}

func (eonPublicKeyShare *EonPublicKeyShare) Equal(pk2 *EonPublicKeyShare) bool {
	return (*blst.P2Affine)(eonPublicKeyShare).Equals((*blst.P2Affine)(pk2))
}

func (epochID *EpochID) GobEncode() ([]byte, error) {
	return epochID.Marshal(), nil
}

func (epochID *EpochID) GobDecode(data []byte) error {
	return epochID.Unmarshal(data)
}

func (epochID *EpochID) Equal(g2 *EpochID) bool {
	return (*blst.P1Affine)(epochID).Equals((*blst.P1Affine)(g2))
}

func (epochSecretKeyShare *EpochSecretKeyShare) GobEncode() ([]byte, error) {
	return epochSecretKeyShare.Marshal(), nil
}

func (epochSecretKeyShare *EpochSecretKeyShare) GobDecode(data []byte) error {
	return epochSecretKeyShare.Unmarshal(data)
}

func (epochSecretKeyShare *EpochSecretKeyShare) Equal(g2 *EpochSecretKeyShare) bool {
	return (*blst.P1Affine)(epochSecretKeyShare).Equals((*blst.P1Affine)(g2))
}

func (epochSecretKey *EpochSecretKey) GobEncode() ([]byte, error) {
	return epochSecretKey.Marshal(), nil
}

func (epochSecretKey *EpochSecretKey) GobDecode(data []byte) error {
	return epochSecretKey.Unmarshal(data)
}

func (epochSecretKey *EpochSecretKey) Equal(g2 *EpochSecretKey) bool {
	return (*blst.P1Affine)(epochSecretKey).Equals((*blst.P1Affine)(g2))
}

func (eonSecretKeyShare *EonSecretKeyShare) GobEncode() ([]byte, error) {
	return (*big.Int)(eonSecretKeyShare).GobEncode()
}

func (eonSecretKeyShare *EonSecretKeyShare) GobDecode(data []byte) error {
	return (*big.Int)(eonSecretKeyShare).GobDecode(data)
}

func (eonSecretKeyShare *EonSecretKeyShare) Equal(e2 *EonSecretKeyShare) bool {
	return (*big.Int)(eonSecretKeyShare).Cmp((*big.Int)(e2)) == 0
}

// ComputeEonSecretKeyShare computes the keyper's secret key share from the set of poly evals
// received from the other keypers.
func ComputeEonSecretKeyShare(polyEvals []*big.Int) *EonSecretKeyShare {
	res := big.NewInt(0)
	for _, si := range polyEvals {
		res.Add(res, si)
		res.Mod(res, order)
	}
	share := EonSecretKeyShare(*res)
	return &share
}

// ComputeEonPublicKeyShare computes the eon public key share of the given keyper.
func ComputeEonPublicKeyShare(keyperIndex int, gammas []*Gammas) *EonPublicKeyShare {
	p := new(blst.P2)
	keyperX := KeyperX(keyperIndex)
	for _, gs := range gammas {
		pi := gs.Pi(keyperX)
		p.AddAssign(pi)
	}
	pAffine := p.ToAffine()
	epk := EonPublicKeyShare(*pAffine)
	return &epk
}

// ComputeEonPublicKey computes the combined eon public key from the set of eon public key shares.
func ComputeEonPublicKey(gammas []*Gammas) *EonPublicKey {
	p := new(blst.P2)
	for _, gs := range gammas {
		pi := gs.Pi(big.NewInt(0))
		p.AddAssign(pi)
	}
	pAffine := p.ToAffine()
	epk := EonPublicKey(*pAffine)
	return &epk
}

// ComputeEpochSecretKeyShare computes a keyper's epoch sk share.
func ComputeEpochSecretKeyShare(eonSecretKeyShare *EonSecretKeyShare, epochID *EpochID) *EpochSecretKeyShare {
	p := new(blst.P1)
	p.FromAffine((*blst.P1Affine)(epochID))
	p.MultAssign(bigToScalar((*big.Int)(eonSecretKeyShare)))

	pAffine := p.ToAffine()
	epochSecretKeyShare := EpochSecretKeyShare(*pAffine)
	return &epochSecretKeyShare
}

// ComputeEpochID computes the id of the given epoch.
func ComputeEpochID(epochIDBytes []byte) *EpochID {
	return (*EpochID)(Hash1(epochIDBytes))
}

// LagrangeCoeffs stores the lagrange coefficients that are needed to compute an epoch secret key
// for a certain array of keypers. We use this to speedup epoch secret key generation.
type LagrangeCoeffs struct {
	lambdas []*big.Int
}

// NewLagrangeCoeffs computes the lagrange coefficients for the given array of keypers.
func NewLagrangeCoeffs(keyperIndices []int) *LagrangeCoeffs {
	lambdas := make([]*big.Int, len(keyperIndices))
	for i, keyperIndex := range keyperIndices {
		lambdas[i] = lagrangeCoefficient(keyperIndex, keyperIndices)
	}
	return &LagrangeCoeffs{
		lambdas: lambdas,
	}
}

// ComputeEpochSecretKey computes the epoch secret key given the secret key shares of the keypers.
// The caller has to ensure that the secret shares match the keyperIndices used during
// initialisation.
func (lc *LagrangeCoeffs) ComputeEpochSecretKey(epochSecretKeyShares []*EpochSecretKeyShare) (*EpochSecretKey, error) {
	if len(epochSecretKeyShares) != len(lc.lambdas) {
		return nil, fmt.Errorf("got %d shares, expected %d", len(epochSecretKeyShares), len(lc.lambdas))
	}
	skG1 := new(blst.P1)
	for i, share := range epochSecretKeyShares {
		qTimesLambda := new(blst.P1)
		qTimesLambda.FromAffine((*blst.P1Affine)(share))
		qTimesLambda.MultAssign(bigToScalar(lc.lambdas[i]))
		skG1.AddAssign(qTimesLambda)
	}
	return (*EpochSecretKey)(skG1.ToAffine()), nil
}

// ComputeEpochSecretKey computes the epoch secret key from a set of shares.
func ComputeEpochSecretKey(keyperIndices []int, epochSecretKeyShares []*EpochSecretKeyShare, threshold uint64) (*EpochSecretKey, error) {
	if len(keyperIndices) != len(epochSecretKeyShares) {
		return nil, fmt.Errorf("got %d keyper indices, but %d secret shares", len(keyperIndices), len(epochSecretKeyShares))
	}
	if uint64(len(keyperIndices)) != threshold {
		return nil, fmt.Errorf("got %d shares, but threshold is %d", len(keyperIndices), threshold)
	}

	return NewLagrangeCoeffs(keyperIndices).ComputeEpochSecretKey(epochSecretKeyShares)
}

// VerifyEpochSecretKeyShare checks that an epoch sk share published by a keyper is correct.
func VerifyEpochSecretKeyShare(epochSecretKeyShare *EpochSecretKeyShare, eonPublicKeyShare *EonPublicKeyShare, epochID *EpochID) bool {
	// TODO: This could be optimized by inverting one of the arguments in one of the pairings,
	// multiplying the results and comparing the result to 1. This would improve performance as it
	// avoids one of the final exponentiations.
	p1 := blst.Fp12MillerLoop(blst.P2Generator().ToAffine(), (*blst.P1Affine)(epochSecretKeyShare))
	p1.FinalExp()
	p2 := blst.Fp12MillerLoop((*blst.P2Affine)(eonPublicKeyShare), (*blst.P1Affine)(epochID))
	p2.FinalExp()
	return p1.Equals(p2)
}

// VerifyEpochSecretKey checks that an epoch secret key is the correct key for an epoch given the
// eon public key.
func VerifyEpochSecretKey(epochSecretKey *EpochSecretKey, eonPublicKey *EonPublicKey, epochIDBytes []byte) (bool, error) {
	sigma, err := RandomSigma(rand.Reader)
	if err != nil {
		return false, err
	}
	message := make([]byte, 32)
	_, err = rand.Read(message)
	if err != nil {
		return false, err
	}
	return VerifyEpochSecretKeyDeterministic(epochSecretKey, eonPublicKey, epochIDBytes, sigma, message)
}

// VerifyEpochSecretKeyDeterministic checks that an epoch secret key is the correct key for an
// epoch given the eon public key and random inputs for sigma and message.
func VerifyEpochSecretKeyDeterministic(epochSecretKey *EpochSecretKey, eonPublicKey *EonPublicKey, epochIDBytes []byte, sigma Block, message []byte) (bool, error) {
	epochID := ComputeEpochID(epochIDBytes)
	encryptedMessage := Encrypt(message, eonPublicKey, epochID, sigma)
	decryptedMessage, err := encryptedMessage.Decrypt(epochSecretKey)
	if err != nil {
		return false, nil
	}
	return bytes.Equal(decryptedMessage, message), nil
}

func lagrangeCoefficientFactor(k int, keyperIndex int) *big.Int {
	xj := KeyperX(keyperIndex)
	xk := KeyperX(k)
	dx := new(big.Int).Sub(xk, xj)
	dx.Mod(dx, order)
	dxInv := invert(dx)
	lambdaK := new(big.Int).Mul(xk, dxInv)
	lambdaK.Mod(lambdaK, order)
	return lambdaK
}

func lagrangeCoefficient(keyperIndex int, keyperIndices []int) *big.Int {
	lambda := big.NewInt(1)
	for _, k := range keyperIndices {
		if k == keyperIndex {
			continue
		}
		lambdaK := lagrangeCoefficientFactor(k, keyperIndex)
		lambda.Mul(lambda, lambdaK)
		lambda.Mod(lambda, order)
	}
	return lambda
}

func invert(x *big.Int) *big.Int {
	orderMinus2 := new(big.Int).Sub(order, big.NewInt(2))
	return new(big.Int).Exp(x, orderMinus2, order)
}
