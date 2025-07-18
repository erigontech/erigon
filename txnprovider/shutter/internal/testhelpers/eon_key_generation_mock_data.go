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

package testhelpers

import (
	"crypto/ecdsa"
	"crypto/rand"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/txnprovider/shutter"
	shuttercrypto "github.com/erigontech/erigon/txnprovider/shutter/internal/crypto"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
)

type EonKeyGeneration struct {
	EonIndex        shutter.EonIndex
	ActivationBlock uint64
	Threshold       uint64
	Keypers         []Keyper
	MaliciousKeyper Keyper
	EonPublicKey    *shuttercrypto.EonPublicKey
}

func (ekg EonKeyGeneration) Eon() shutter.Eon {
	members := make([]common.Address, len(ekg.Keypers))
	for i, keyper := range ekg.Keypers {
		members[i] = keyper.Address()
	}

	return shutter.Eon{
		Index:           ekg.EonIndex,
		ActivationBlock: ekg.ActivationBlock,
		Threshold:       ekg.Threshold,
		Members:         members,
		Key:             ekg.EonPublicKey.Marshal(),
	}
}

func (ekg EonKeyGeneration) DecryptionKeys(signers []Keyper, ips shutter.IdentityPreimages) ([]*proto.Key, error) {
	keys := make([]*proto.Key, len(ips))
	for i, ip := range ips {
		epochSecretKey, err := ekg.EpochSecretKey(signers, ip)
		if err != nil {
			return nil, err
		}

		keys[i] = &proto.Key{
			IdentityPreimage: ip[:],
			Key:              epochSecretKey.Marshal(),
		}
	}

	return keys, nil
}

func (ekg EonKeyGeneration) EpochSecretKey(signers []Keyper, ip *shutter.IdentityPreimage) (*shuttercrypto.EpochSecretKey, error) {
	epochSecretKeyShares := make([]*shuttercrypto.EpochSecretKeyShare, len(signers))
	keyperIndices := make([]int, len(signers))
	for i, keyper := range signers {
		keyperIndices[i] = keyper.Index
		epochSecretKeyShares[i] = keyper.EpochSecretKeyShare(ip)
	}

	return shuttercrypto.ComputeEpochSecretKey(keyperIndices, epochSecretKeyShares, ekg.Threshold)
}

func (ekg EonKeyGeneration) Members() []common.Address {
	members := make([]common.Address, len(ekg.Keypers))
	for i, keyper := range ekg.Keypers {
		members[i] = keyper.Address()
	}
	return members
}

type Keyper struct {
	Index             int
	PrivateKey        *ecdsa.PrivateKey
	EonSecretKeyShare *shuttercrypto.EonSecretKeyShare
	EonPublicKeyShare *shuttercrypto.EonPublicKeyShare
}

func (k Keyper) PublicKey() ecdsa.PublicKey {
	return k.PrivateKey.PublicKey
}

func (k Keyper) Address() common.Address {
	return crypto.PubkeyToAddress(k.PublicKey())
}

func (k Keyper) EpochSecretKeyShare(ip *shutter.IdentityPreimage) *shuttercrypto.EpochSecretKeyShare {
	id := shuttercrypto.ComputeEpochID(ip[:])
	return shuttercrypto.ComputeEpochSecretKeyShare(k.EonSecretKeyShare, id)
}

func MockEonKeyGeneration(idx shutter.EonIndex, threshold, numKeypers, activationBlock uint64) (EonKeyGeneration, error) {
	keypers := make([]Keyper, numKeypers)
	polynomials := make([]*shuttercrypto.Polynomial, numKeypers)
	gammas := make([]*shuttercrypto.Gammas, numKeypers)
	for i := 0; i < int(numKeypers); i++ {
		polynomial, err := shuttercrypto.RandomPolynomial(rand.Reader, threshold-1)
		if err != nil {
			return EonKeyGeneration{}, err
		}

		polynomials[i] = polynomial
		gammas[i] = polynomial.Gammas()
	}

	for i := 0; i < int(numKeypers); i++ {
		privKey, err := crypto.GenerateKey()
		if err != nil {
			return EonKeyGeneration{}, err
		}

		keyperX := shuttercrypto.KeyperX(i)
		polynomialEvals := make([]*big.Int, numKeypers)
		for j := 0; j < int(numKeypers); j++ {
			polynomialEvals[j] = polynomials[j].Eval(keyperX)
		}

		keypers[i] = Keyper{
			Index:             i,
			PrivateKey:        privKey,
			EonSecretKeyShare: shuttercrypto.ComputeEonSecretKeyShare(polynomialEvals),
			EonPublicKeyShare: shuttercrypto.ComputeEonPublicKeyShare(i, gammas),
		}
	}

	eonPublicKey := shuttercrypto.ComputeEonPublicKey(gammas)

	// generate 1 malicious keyper for last index
	privKey, err := crypto.GenerateKey()
	if err != nil {
		return EonKeyGeneration{}, err
	}

	lastIdx := int(numKeypers - 1)
	polynomials[lastIdx], err = shuttercrypto.RandomPolynomial(rand.Reader, threshold-1)
	if err != nil {
		return EonKeyGeneration{}, err
	}

	gammas[lastIdx] = polynomials[lastIdx].Gammas()
	keyperX := shuttercrypto.KeyperX(lastIdx)
	polynomialEvals := make([]*big.Int, numKeypers)
	for j := 0; j < int(numKeypers); j++ {
		polynomialEvals[j] = polynomials[j].Eval(keyperX)
	}
	maliciousKeyper := Keyper{
		Index:             lastIdx,
		PrivateKey:        privKey,
		EonSecretKeyShare: shuttercrypto.ComputeEonSecretKeyShare(polynomialEvals),
		EonPublicKeyShare: shuttercrypto.ComputeEonPublicKeyShare(lastIdx, gammas),
	}

	ekg := EonKeyGeneration{
		EonIndex:        idx,
		ActivationBlock: activationBlock,
		Threshold:       threshold,
		Keypers:         keypers,
		MaliciousKeyper: maliciousKeyper,
		EonPublicKey:    eonPublicKey,
	}

	return ekg, nil
}

func TestMustGenerateDecryptionKeys(t *testing.T, ekg EonKeyGeneration, signers []Keyper, ips shutter.IdentityPreimages) []*proto.Key {
	keys, err := ekg.DecryptionKeys(signers, ips)
	require.NoError(t, err)
	return keys
}
