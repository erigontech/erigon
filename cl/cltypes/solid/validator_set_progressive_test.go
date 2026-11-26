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
			validators.SetProgressiveHashing(true)
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

			copied := NewValidatorSet(validators.Cap())
			validators.CopyTo(copied)
			got, err = copied.HashSSZProgressive()
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestValidatorSetSwitchesHashingLayout(t *testing.T) {
	validators := NewValidatorSet(16)
	validators.Append(make(Validator, validatorSize))
	want, err := validators.HashSSZ()
	require.NoError(t, err)

	validators.SetProgressiveHashing(true)
	_, err = validators.HashSSZ()
	require.ErrorIs(t, err, errValidatorLayout)
	require.Equal(t, validatorSetProgressiveRootReference(t, validators), mustProgressiveRoot(t, validators))

	validators.SetProgressiveHashing(false)
	_, err = validators.HashSSZProgressive()
	require.ErrorIs(t, err, errValidatorLayout)
	got, err := validators.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestValidatorSetCopyToSharesChunksAndReusesSlots(t *testing.T) {
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
			oldSlots := &target.list.chunks[:1][0]
			slotsFit := len(source.list.chunks) <= cap(target.list.chunks)

			source.CopyTo(target)

			requireSlotsCacheChunkData(t, &source.list, "source after copy")
			requireSlotsCacheChunkData(t, &target.list, "target after copy")
			require.Len(t, target.list.chunks, len(source.list.chunks))
			for i := range source.list.chunks {
				require.Same(t, source.list.chunks[i].Load(), target.list.chunks[i].Load())
			}
			if slotsFit {
				require.Same(t, oldSlots, &target.list.chunks[:1][0])
			}
			require.Equal(t, validatorSetProgressiveRootReference(t, source), mustProgressiveRoot(t, target))

			sourceRoot := mustProgressiveRoot(t, source)
			target.Set(testCase.sourceCount/2, validatorForProgressiveTest(0xee))
			for index := range 2 {
				target.Append(validatorForProgressiveTest(byte(testCase.sourceCount + index + 1)))
			}
			require.Equal(t, sourceRoot, mustProgressiveRoot(t, source))
			require.Equal(t, validatorSetProgressiveRootReference(t, target), mustProgressiveRoot(t, target))
			requireSlotsCacheChunkData(t, &target.list, "target after writes")

			targetRoot := mustProgressiveRoot(t, target)
			source.Set(testCase.sourceCount/2, validatorForProgressiveTest(0xdd))
			require.Equal(t, targetRoot, mustProgressiveRoot(t, target))
			require.Equal(t, validatorSetProgressiveRootReference(t, source), mustProgressiveRoot(t, source))
			requireSlotsCacheChunkData(t, &source.list, "source after writes")
		})
	}
}

func TestValidatorSetCopyToSwitchesHashingModes(t *testing.T) {
	fulu := validatorSetWithCount(5)
	fulu.SetProgressiveHashing(false)
	fuluReference := validatorSetWithCount(5)
	fuluReference.SetProgressiveHashing(false)
	wantFuluRoot, err := fuluReference.HashSSZ()
	require.NoError(t, err)
	_, err = fulu.HashSSZ()
	require.NoError(t, err)
	gloas := validatorSetWithCount(5)
	_, err = gloas.HashSSZProgressive()
	require.NoError(t, err)

	fulu.CopyTo(gloas)
	_, err = gloas.HashSSZProgressive()
	require.ErrorIs(t, err, errValidatorLayout)
	gotFuluRoot, err := gloas.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, wantFuluRoot, gotFuluRoot)

	gloas.SetProgressiveHashing(true)
	_, err = gloas.HashSSZProgressive()
	require.NoError(t, err)
	gloas.CopyTo(fulu)
	_, err = fulu.HashSSZ()
	require.ErrorIs(t, err, errValidatorLayout)
	require.Equal(t, validatorSetProgressiveRootReference(t, gloas), mustProgressiveRoot(t, fulu))
}

func validatorSetWithCount(count int) *ValidatorSet {
	validators := NewValidatorSet(1_000_000)
	validators.SetProgressiveHashing(true)
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
	roots := make([][32]byte, validators.Length())
	hashBuffer := make([]byte, 8*32)
	for i := range roots {
		require.NoError(t, validators.Get(i).Clone().CopyHashBufferTo(hashBuffer))
		require.NoError(t, merkle_tree.MerkleRootFromFlatLeaves(hashBuffer, roots[i][:]))
	}
	root, err := merkle_tree.ProgressiveListRoot(roots, uint64(validators.Length()))
	require.NoError(t, err)
	return root
}
