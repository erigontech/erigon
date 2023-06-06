package bor

import (
	"math/big"
	"sort"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/consensus/bor/valset"
	"github.com/ledgerwatch/log/v3"
	crand "github.com/maticnetwork/crand"
	"github.com/stretchr/testify/require"
)

const (
	numVals = 100
)

func TestGetSignerSuccessionNumber_ProposerIsSigner(t *testing.T) {
	t.Parallel()

	validators := buildRandomValidatorSet(numVals)
	validatorSet := valset.NewValidatorSet(validators, log.New())
	snap := Snapshot{
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
	snap := Snapshot{
		ValidatorSet: valset.NewValidatorSet(validators, log.New()),
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
	snap := Snapshot{
		ValidatorSet: valset.NewValidatorSet(validators, log.New()),
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
	snap := Snapshot{
		ValidatorSet: valset.NewValidatorSet(validators, log.New()),
	}

	dummyProposerAddress := randomAddress()
	snap.ValidatorSet.Proposer = &valset.Validator{Address: dummyProposerAddress}

	// choose any signer
	signer := snap.ValidatorSet.Validators[3].Address

	_, err := snap.GetSignerSuccessionNumber(signer)
	require.NotNil(t, err)

	e, ok := err.(*UnauthorizedProposerError)
	require.True(t, ok)
	require.Equal(t, dummyProposerAddress.Bytes(), e.Proposer)
}

func TestGetSignerSuccessionNumber_SignerNotFound(t *testing.T) {
	t.Skip("TODO: fixme please")
	t.Parallel()

	validators := buildRandomValidatorSet(numVals)
	snap := Snapshot{
		ValidatorSet: valset.NewValidatorSet(validators, log.New()),
	}

	dummySignerAddress := randomAddress()
	_, err := snap.GetSignerSuccessionNumber(dummySignerAddress)
	require.NotNil(t, err)

	e, ok := err.(*UnauthorizedSignerError)
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

func randomAddress() libcommon.Address {
	return crand.NewRand().Address()
}
