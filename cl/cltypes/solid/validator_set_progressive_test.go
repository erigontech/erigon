package solid

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/merkle_tree"
)

func TestValidatorSetProgressiveRootMatchesReference(t *testing.T) {
	for _, count := range []int{0, 1, 5, 21, 85, 341} {
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

func TestValidatorSetCopyToReusesProgressiveTrees(t *testing.T) {
	for _, testCase := range []struct {
		sourceCount int
		targetCount int
	}{
		{sourceCount: 1, targetCount: 2},
		{sourceCount: 20, targetCount: 21},
		{sourceCount: 84, targetCount: 85},
		{sourceCount: 340, targetCount: 341},
		{sourceCount: 10_000, targetCount: 10_001},
		{sourceCount: 341, targetCount: 1},
		{sourceCount: 5, targetCount: 341},
	} {
		t.Run(fmt.Sprintf("%d_into_%d", testCase.sourceCount, testCase.targetCount), func(t *testing.T) {
			source := validatorSetWithCount(testCase.sourceCount)
			_, err := source.HashSSZProgressive()
			require.NoError(t, err)
			target := validatorSetWithCount(testCase.targetCount)
			_, err = target.HashSSZProgressive()
			require.NoError(t, err)
			oldTrees := append([]*merkle_tree.MerkleTree(nil), target.progressiveTrees...)

			source.CopyTo(target)

			require.Len(t, target.progressiveTrees, len(source.progressiveTrees))
			for i := range min(len(oldTrees), len(target.progressiveTrees)) {
				require.Same(t, oldTrees[i], target.progressiveTrees[i])
			}
			require.Equal(t, validatorSetProgressiveRootReference(t, source), mustProgressiveRoot(t, target))

			sourceRoot := mustProgressiveRoot(t, source)
			target.Set(testCase.sourceCount/2, validatorForProgressiveTest(0xee))
			for index := range 2 {
				target.Append(validatorForProgressiveTest(byte(testCase.sourceCount + index + 1)))
			}
			require.Equal(t, sourceRoot, mustProgressiveRoot(t, source))
			require.Equal(t, validatorSetProgressiveRootReference(t, target), mustProgressiveRoot(t, target))

			targetRoot := mustProgressiveRoot(t, target)
			source.Set(testCase.sourceCount/2, validatorForProgressiveTest(0xdd))
			require.Equal(t, targetRoot, mustProgressiveRoot(t, target))
			require.Equal(t, validatorSetProgressiveRootReference(t, source), mustProgressiveRoot(t, source))
		})
	}
}

func TestValidatorSetCopyToSwitchesHashingModes(t *testing.T) {
	fulu := validatorSetWithCount(5)
	fuluReference := validatorSetWithCount(5)
	wantFuluRoot, err := fuluReference.HashSSZ()
	require.NoError(t, err)
	_, err = fulu.HashSSZ()
	require.NoError(t, err)
	gloas := validatorSetWithCount(5)
	_, err = gloas.HashSSZProgressive()
	require.NoError(t, err)

	fulu.CopyTo(gloas)
	require.Nil(t, gloas.progressiveTrees)
	require.NotNil(t, gloas.MerkleTree)
	gotFuluRoot, err := gloas.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, wantFuluRoot, gotFuluRoot)

	gloas.SetProgressiveHashing(true)
	_, err = gloas.HashSSZProgressive()
	require.NoError(t, err)
	gloas.CopyTo(fulu)
	require.Nil(t, fulu.MerkleTree)
	require.NotNil(t, fulu.progressiveTrees)
	require.Equal(t, validatorSetProgressiveRootReference(t, gloas), mustProgressiveRoot(t, fulu))
}

func validatorSetWithCount(count int) *ValidatorSet {
	validators := NewValidatorSet(1_000_000)
	for i := range count {
		validators.Append(validatorForProgressiveTest(byte(i + 1)))
	}
	return validators
}

func validatorForProgressiveTest(firstByte byte) Validator {
	validator := make(Validator, validatorSize)
	validator[0] = firstByte
	return validator
}

func mustProgressiveRoot(t *testing.T, validators *ValidatorSet) [32]byte {
	t.Helper()
	root, err := validators.HashSSZProgressive()
	require.NoError(t, err)
	return root
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
