// Copyright 2024 The Erigon Authors
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

package bor_test

import (
	"math/big"
	"sort"
	"testing"

	"github.com/maticnetwork/crand"
	"github.com/stretchr/testify/require"

	common "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/valset"
)

const (
	numVals = 100
)

func TestGetSignerSuccessionNumber_ProposerIsSigner(t *testing.T) {
	t.Parallel()

	validators := buildRandomValidatorSet(numVals)
	validatorSet := valset.NewValidatorSet(validators)
	snap := bor.Snapshot{
		ValidatorSet: validatorSet,
	}

	// proposer is signer
	signer := validatorSet.Proposer.Address
	successionNumber, err := snap.GetSignerSuccessionNumber(signer)
	if err != nil {
		t.Fatalf("%s", err)
	}

	require.Equal(t, 0, successionNumber)
}

func TestGetSignerSuccessionNumber_SignerIndexIsLarger(t *testing.T) {
	t.Parallel()

	validators := buildRandomValidatorSet(numVals)

	// sort validators by address, which is what NewValidatorSet also does
	sort.Sort(valset.ValidatorsByAddress(validators))

	proposerIndex := 32
	signerIndex := 56
	// give highest ProposerPriority to a particular val, so that they become the proposer
	validators[proposerIndex].VotingPower = 200
	snap := bor.Snapshot{
		ValidatorSet: valset.NewValidatorSet(validators),
	}

	// choose a signer at an index greater than proposer index
	signer := snap.ValidatorSet.Validators[signerIndex].Address
	successionNumber, err := snap.GetSignerSuccessionNumber(signer)
	if err != nil {
		t.Fatalf("%s", err)
	}

	require.Equal(t, signerIndex-proposerIndex, successionNumber)
}

func TestGetSignerSuccessionNumber_SignerIndexIsSmaller(t *testing.T) {
	t.Parallel()

	validators := buildRandomValidatorSet(numVals)
	proposerIndex := 98
	signerIndex := 11
	// give highest ProposerPriority to a particular val, so that they become the proposer
	validators[proposerIndex].VotingPower = 200
	snap := bor.Snapshot{
		ValidatorSet: valset.NewValidatorSet(validators),
	}

	// choose a signer at an index greater than proposer index
	signer := snap.ValidatorSet.Validators[signerIndex].Address
	successionNumber, err := snap.GetSignerSuccessionNumber(signer)
	if err != nil {
		t.Fatalf("%s", err)
	}

	require.Equal(t, signerIndex+numVals-proposerIndex, successionNumber)
}

func TestGetSignerSuccessionNumber_ProposerNotFound(t *testing.T) {
	t.Parallel()

	validators := buildRandomValidatorSet(numVals)
	snap := bor.Snapshot{
		ValidatorSet: valset.NewValidatorSet(validators),
	}

	dummyProposerAddress := randomAddress()
	snap.ValidatorSet.Proposer = &valset.Validator{Address: dummyProposerAddress}

	// choose any signer
	signer := snap.ValidatorSet.Validators[3].Address

	_, err := snap.GetSignerSuccessionNumber(signer)
	require.Error(t, err)

	e, ok := err.(*valset.UnauthorizedProposerError)
	require.True(t, ok)
	require.Equal(t, dummyProposerAddress.Bytes(), e.Proposer)
}

func TestGetSignerSuccessionNumber_SignerNotFound(t *testing.T) {
	t.Parallel()

	validators := buildRandomValidatorSet(numVals)
	snap := bor.Snapshot{
		ValidatorSet: valset.NewValidatorSet(validators),
	}

	dummySignerAddress := randomAddress()
	_, err := snap.GetSignerSuccessionNumber(dummySignerAddress)
	require.Error(t, err)

	e, ok := err.(*valset.UnauthorizedSignerError)
	require.True(t, ok)
	require.Equal(t, dummySignerAddress.Bytes(), e.Signer)
}

func buildRandomValidatorSet(numVals int) []*valset.Validator {
	validators := make([]*valset.Validator, numVals)
	for i := 0; i < numVals; i++ {
		power := crand.BigInt(big.NewInt(99))
		// cannot process validators with voting power 0, hence +1
		powerN := power.Int64() + 1
		validators[i] = &valset.Validator{
			Address:     randomAddress(),
			VotingPower: powerN,
		}
	}

	// sort validators by address, which is what NewValidatorSet also does
	sort.Sort(valset.ValidatorsByAddress(validators))
	return validators
}

func randomAddress() common.Address {
	return crand.NewRand().Address()
}
