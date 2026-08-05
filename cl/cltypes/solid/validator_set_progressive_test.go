package solid

import (
	"strconv"
	"testing"

	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/stretchr/testify/require"
)

func TestValidatorSetProgressiveRootMatchesReference(t *testing.T) {
	for _, count := range []int{0, 1, 5, 21, 85} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			validators := NewValidatorSet(1_000_000)
			for i := range count {
				validator := make(Validator, validatorSize)
				validator[0] = byte(i + 1)
				validators.Append(validator)
			}

			want := validatorSetProgressiveRootReference(t, validators)
			got, err := validators.HashSSZProgressive()
			require.NoError(t, err)
			require.Equal(t, want, got)

			if count > 0 {
				validator := make(Validator, validatorSize)
				validator[0] = 0xff
				validators.Set(count/2, validator)
				want = validatorSetProgressiveRootReference(t, validators)
				got, err = validators.HashSSZProgressive()
				require.NoError(t, err)
				require.Equal(t, want, got)
			}

			appended := make(Validator, validatorSize)
			appended[0] = 0xee
			validators.Append(appended)
			want = validatorSetProgressiveRootReference(t, validators)
			got, err = validators.HashSSZProgressive()
			require.NoError(t, err)
			require.Equal(t, want, got)

			copied := NewValidatorSet(validators.c)
			validators.CopyTo(copied)
			got, err = copied.HashSSZProgressive()
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestValidatorSetSwitchesMerkleCacheMode(t *testing.T) {
	validators := NewValidatorSet(16)
	validators.Append(make(Validator, validatorSize))
	_, err := validators.HashSSZ()
	require.NoError(t, err)
	require.NotNil(t, validators.MerkleTree)

	validators.SetProgressiveHashing(true)
	require.Nil(t, validators.MerkleTree)
	_, err = validators.HashSSZProgressive()
	require.NoError(t, err)
	require.NotNil(t, validators.progressiveTrees)

	validators.SetProgressiveHashing(false)
	require.Nil(t, validators.progressiveTrees)
	_, err = validators.HashSSZ()
	require.NoError(t, err)
	require.NotNil(t, validators.MerkleTree)
}

func validatorSetProgressiveRootReference(t *testing.T, validators *ValidatorSet) [32]byte {
	roots := make([][32]byte, validators.l)
	hashBuffer := make([]byte, 8*32)
	for i := range roots {
		require.NoError(t, validators.Get(i).CopyHashBufferTo(hashBuffer))
		require.NoError(t, merkle_tree.MerkleRootFromFlatLeaves(hashBuffer, roots[i][:]))
	}
	root, err := merkle_tree.ProgressiveListRoot(roots, uint64(validators.l))
	require.NoError(t, err)
	return root
}
