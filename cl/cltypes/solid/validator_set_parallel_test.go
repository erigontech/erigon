package solid

import (
	"math/rand"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/common/crypto"
)

func TestValidatorRootWorkerPoolTracksGOMAXPROCS(t *testing.T) {
	previous := runtime.GOMAXPROCS(0)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	workers := newDefaultValidatorRootWorkerPool()

	runtime.GOMAXPROCS(2)
	require.Equal(t, 1, workers.workerCount())
	runtime.GOMAXPROCS(16)
	require.Equal(t, 8, workers.workerCount())
}

func TestValidatorRootWorkerPoolLetsOverlappingBuildsRunInParallel(t *testing.T) {
	previous := runtime.GOMAXPROCS(0)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })
	runtime.GOMAXPROCS(16)
	workers := newDefaultValidatorRootWorkerPool()

	first := workers.acquireHelpers(parallelMinChunks)
	second := workers.acquireHelpers(parallelMinChunks)
	third := workers.acquireHelpers(parallelMinChunks)
	require.Equal(t, []int{7, 7, 2}, []int{first, second, third})

	workers.releaseHelpers(first)
	workers.releaseHelpers(second)
	workers.releaseHelpers(third)
	require.Zero(t, workers.liveHelpers.Load())
}

func TestValidatorRootWorkerPoolHonorsRuntimeBudgetAcrossBuilds(t *testing.T) {
	workers := newValidatorRootWorkerPool(8)
	workers.workerLimit = func() int { return 4 }
	workers.totalHelperLimit = func() int { return 5 }
	var liveHelpers atomic.Int64
	workers.helperHook = func(delta int) {
		liveHelpers.Add(int64(delta))
	}

	acquired := []int{
		workers.acquireHelpers(parallelMinChunks),
		workers.acquireHelpers(parallelMinChunks),
		workers.acquireHelpers(parallelMinChunks),
	}
	require.Equal(t, []int{3, 2, 0}, acquired)
	require.Equal(t, int64(5), liveHelpers.Load())

	for _, helperCount := range acquired {
		workers.releaseHelpers(helperCount)
	}
	require.Zero(t, liveHelpers.Load())
}

func TestValidatorSetParallelWorkerBudget(t *testing.T) {
	oldWorkers := validatorRootWorkers
	workers := newValidatorRootWorkerPool(3)
	validatorRootWorkers = workers
	t.Cleanup(func() { validatorRootWorkers = oldWorkers })

	releaseHelpers := make(chan struct{})
	helpersAcquired := make(chan struct{})
	var liveHelpers atomic.Int64
	var maxHelpers atomic.Int64
	var holderBlocked atomic.Bool
	workers.helperHook = func(delta int) {
		live := liveHelpers.Add(int64(delta))
		for maximum := maxHelpers.Load(); live > maximum && !maxHelpers.CompareAndSwap(maximum, live); maximum = maxHelpers.Load() {
		}
		if delta > 0 && live == 2 && holderBlocked.CompareAndSwap(false, true) {
			close(helpersAcquired)
			<-releaseHelpers
		}
	}

	validators := testValidatorSet(parallelMinValidatorCount)
	want := validatorSetRootReference(t, validators)
	type hashResult struct {
		root [32]byte
		err  error
	}
	holderResult := make(chan hashResult, 1)
	go func() {
		root, err := validators.HashSSZ()
		holderResult <- hashResult{root: root, err: err}
	}()
	select {
	case <-helpersAcquired:
	case <-time.After(10 * time.Second):
		t.Fatal("cold build did not acquire the expected helpers")
	}

	const competingBuilds = 6
	results := make(chan hashResult, competingBuilds)
	for range competingBuilds {
		go func() {
			root, err := testValidatorSet(parallelMinValidatorCount).HashSSZ()
			results <- hashResult{root: root, err: err}
		}()
	}
	for range competingBuilds {
		select {
		case result := <-results:
			require.NoError(t, result.err)
			require.Equal(t, want, result.root)
		case <-time.After(10 * time.Second):
			t.Fatal("cold build waited for the worker budget")
		}
	}
	require.Equal(t, int64(2), maxHelpers.Load())
	require.Equal(t, int64(2), liveHelpers.Load())

	close(releaseHelpers)
	select {
	case result := <-holderResult:
		require.NoError(t, result.err)
		require.Equal(t, want, result.root)
	case <-time.After(10 * time.Second):
		t.Fatal("cold build did not finish after helpers were released")
	}
	require.Zero(t, liveHelpers.Load())
}

func TestValidatorSetParallelColdHashSharesChunksWithSibling(t *testing.T) {
	for _, mode := range []struct {
		name        string
		progressive bool
		hash        func(*ValidatorSet) ([32]byte, error)
		reference   func(*testing.T, *ValidatorSet) [32]byte
	}{
		{name: "fulu", hash: (*ValidatorSet).HashSSZ, reference: validatorSetRootReference},
		{name: "gloas", progressive: true, hash: (*ValidatorSet).HashSSZProgressive, reference: validatorSetProgressiveRootReference},
	} {
		t.Run(mode.name, func(t *testing.T) {
			oldWorkers := validatorRootWorkers
			workers := newValidatorRootWorkerPool(4)
			validatorRootWorkers = workers
			t.Cleanup(func() { validatorRootWorkers = oldWorkers })

			helpersAcquired := make(chan struct{})
			releaseHelpers := make(chan struct{})
			var holderBlocked atomic.Bool
			workers.helperHook = func(delta int) {
				if delta > 0 && holderBlocked.CompareAndSwap(false, true) {
					close(helpersAcquired)
					<-releaseHelpers
				}
			}

			validators := testValidatorSet(parallelMinValidatorCount)
			validators.SetProgressiveHashing(mode.progressive)
			want := mode.reference(t, validators)
			sibling := NewValidatorSet(validators.Cap())
			validators.CopyTo(sibling)

			type hashResult struct {
				root [32]byte
				err  error
			}
			holderResult := make(chan hashResult, 1)
			holderDone := make(chan struct{})
			go func() {
				defer close(holderDone)
				root, err := mode.hash(validators)
				holderResult <- hashResult{root: root, err: err}
			}()
			t.Cleanup(func() {
				select {
				case <-releaseHelpers:
				default:
					close(releaseHelpers)
				}
				<-holderDone
			})

			select {
			case <-helpersAcquired:
			case <-time.After(10 * time.Second):
				t.Fatal("cold build did not acquire a helper")
			}

			siblingRoot, err := mode.hash(sibling)
			require.NoError(t, err)
			require.Equal(t, want, siblingRoot)

			require.NotPanics(t, func() { mutateValidator(sibling, sibling.Length()/2) })
			assertValidatorSetRoot(t, sibling, mode.hash, mode.reference)

			close(releaseHelpers)
			result := <-holderResult
			require.NoError(t, result.err)
			require.Equal(t, want, result.root)
			assertValidatorSetRoot(t, validators, mode.hash, mode.reference)
		})
	}
}

func TestValidatorSetParallelFanOutIsBoundedByDirtyChunks(t *testing.T) {
	for _, mode := range []struct {
		name        string
		progressive bool
		hash        func(*ValidatorSet) ([32]byte, error)
	}{
		{name: "fulu", hash: (*ValidatorSet).HashSSZ},
		{name: "gloas", progressive: true, hash: (*ValidatorSet).HashSSZProgressive},
	} {
		t.Run(mode.name, func(t *testing.T) {
			oldWorkers := validatorRootWorkers
			workers := newValidatorRootWorkerPool(3)
			validatorRootWorkers = workers
			t.Cleanup(func() { validatorRootWorkers = oldWorkers })

			var helpersStarted atomic.Int64
			workers.helperHook = func(delta int) {
				if delta > 0 {
					helpersStarted.Add(1)
				}
			}

			small := testValidatorSet(parallelMinValidatorCount - 2*256)
			small.SetProgressiveHashing(mode.progressive)
			_, err := mode.hash(small)
			require.NoError(t, err)
			require.Zero(t, helpersStarted.Load())

			large := testValidatorSet(parallelMinValidatorCount)
			large.SetProgressiveHashing(mode.progressive)
			_, err = mode.hash(large)
			require.NoError(t, err)
			require.Positive(t, helpersStarted.Load())

			helpersStarted.Store(0)
			mutateValidator(large, 0)
			_, err = mode.hash(large)
			require.NoError(t, err)
			require.Zero(t, helpersStarted.Load())

			for i := range 100 {
				mutateValidator(large, i)
			}
			_, err = mode.hash(large)
			require.NoError(t, err)
			require.Zero(t, helpersStarted.Load())

			for i := 0; i < large.Length(); i += 128 {
				mutateValidator(large, i)
			}
			_, err = mode.hash(large)
			require.NoError(t, err)
			require.Positive(t, helpersStarted.Load())
		})
	}
}

func TestValidatorRootMatchesSerialReference(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var chunks [8][32]byte
	for range 1_000 {
		validator := make(Validator, validatorSize)
		_, err := rng.Read(validator)
		require.NoError(t, err)

		var hashBuffer [8 * 32]byte
		require.NoError(t, validator.CopyHashBufferTo(hashBuffer[:]))
		var want [32]byte
		require.NoError(t, merkle_tree.MerkleRootFromFlatLeaves(hashBuffer[:], want[:]))

		var got [32]byte
		validatorRoot(validator, got[:], &chunks)
		require.Equal(t, want, got)
	}
}

func TestValidatorSetRootsMatchSerialReferences(t *testing.T) {
	helpersStarted := useFixedValidatorRootWorkerPool(t)
	type rootMode struct {
		name        string
		progressive bool
		hash        func(*ValidatorSet) ([32]byte, error)
		reference   func(*testing.T, *ValidatorSet) [32]byte
	}
	modes := []rootMode{
		{
			name: "fulu",
			hash: func(validators *ValidatorSet) ([32]byte, error) {
				return validators.HashSSZ()
			},
			reference: validatorSetRootReference,
		},
		{
			name:        "gloas",
			progressive: true,
			hash: func(validators *ValidatorSet) ([32]byte, error) {
				return validators.HashSSZProgressive()
			},
			reference: validatorSetProgressiveRootReference,
		},
	}
	counts := []int{
		0, 1, 2, 3, 4, 5, 6, 7,
		20, 21, 22, 83, 84, 85, 86,
		340, 341, 342, 1_364, 1_365, 1_366,
		21_845, 21_846,
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			for _, count := range counts {
				t.Run(strconv.Itoa(count), func(t *testing.T) {
					validators := randomValidatorSet(count)
					validators.SetProgressiveHashing(mode.progressive)
					helpersBefore := helpersStarted.Load()
					assertValidatorSetRoot(t, validators, mode.hash, mode.reference)
					if count >= parallelMinValidatorCount {
						require.Greater(t, helpersStarted.Load(), helpersBefore)
					}
					assertValidatorSetRoot(t, validators, mode.hash, mode.reference)

					if count > 0 {
						mutateValidator(validators, count/2)
						assertValidatorSetRoot(t, validators, mode.hash, mode.reference)

						for i := range min(count, 1_024) {
							mutateValidator(validators, i)
						}
						assertValidatorSetRoot(t, validators, mode.hash, mode.reference)
					}

					originalRoot, err := mode.hash(validators)
					require.NoError(t, err)
					copied := NewValidatorSet(validators.Cap())
					validators.CopyTo(copied)
					if count == 0 {
						copied.Append(NewValidator())
					} else {
						mutateValidator(copied, count-1)
					}
					assertValidatorSetRoot(t, copied, mode.hash, mode.reference)
					gotOriginal, err := mode.hash(validators)
					require.NoError(t, err)
					require.Equal(t, originalRoot, gotOriginal)
				})
			}
		})
	}
}

func TestValidatorSetProgressiveAppendAcrossSegmentBoundary(t *testing.T) {
	for _, count := range []int{0, 4, 20, 84, 340, 1_364} {
		t.Run(strconv.Itoa(count+1), func(t *testing.T) {
			validators := randomValidatorSet(count)
			validators.SetProgressiveHashing(true)
			_, err := validators.HashSSZProgressive()
			require.NoError(t, err)
			validators.Append(randomValidatorSet(1).Get(0).Clone())
			assertValidatorSetRoot(t, validators, (*ValidatorSet).HashSSZProgressive, validatorSetProgressiveRootReference)
		})
	}
}

func TestValidatorSetParallelColdHashThenAppend(t *testing.T) {
	helpersStarted := useFixedValidatorRootWorkerPool(t)
	for _, mode := range []struct {
		name        string
		progressive bool
		hash        func(*ValidatorSet) ([32]byte, error)
		reference   func(*testing.T, *ValidatorSet) [32]byte
	}{
		{name: "fulu", hash: (*ValidatorSet).HashSSZ, reference: validatorSetRootReference},
		{name: "gloas", progressive: true, hash: (*ValidatorSet).HashSSZProgressive, reference: validatorSetProgressiveRootReference},
	} {
		t.Run(mode.name, func(t *testing.T) {
			for _, count := range []int{21_845, 50_003} {
				t.Run(strconv.Itoa(count+1), func(t *testing.T) {
					validators := randomValidatorSet(count)
					validators.SetProgressiveHashing(mode.progressive)
					helpersBefore := helpersStarted.Load()
					assertValidatorSetRoot(t, validators, mode.hash, mode.reference)
					require.Greater(t, helpersStarted.Load(), helpersBefore)
					validators.Append(randomValidatorSet(1).Get(0).Clone())
					assertValidatorSetRoot(t, validators, mode.hash, mode.reference)
				})
			}
		})
	}
}

func TestValidatorSetDecodedColdRootsAndCacheModeSwitches(t *testing.T) {
	helpersStarted := useFixedValidatorRootWorkerPool(t)
	source := randomValidatorSet(21_845)
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(t, err)
	validators := NewValidatorSet(source.Cap())
	require.NoError(t, validators.DecodeSSZ(encoded, 0))

	helpersBefore := helpersStarted.Load()
	assertValidatorSetRoot(t, validators, (*ValidatorSet).HashSSZ, validatorSetRootReference)
	require.Greater(t, helpersStarted.Load(), helpersBefore)
	validators.SetProgressiveHashing(true)
	helpersBefore = helpersStarted.Load()
	assertValidatorSetRoot(t, validators, (*ValidatorSet).HashSSZProgressive, validatorSetProgressiveRootReference)
	require.Greater(t, helpersStarted.Load(), helpersBefore)
	validators.SetProgressiveHashing(false)
	helpersBefore = helpersStarted.Load()
	assertValidatorSetRoot(t, validators, (*ValidatorSet).HashSSZ, validatorSetRootReference)
	require.Greater(t, helpersStarted.Load(), helpersBefore)
}

func useFixedValidatorRootWorkerPool(t *testing.T) *atomic.Int64 {
	t.Helper()
	oldWorkers := validatorRootWorkers
	workers := newValidatorRootWorkerPool(4)
	validatorRootWorkers = workers
	t.Cleanup(func() { validatorRootWorkers = oldWorkers })

	helpersStarted := new(atomic.Int64)
	workers.helperHook = func(delta int) {
		if delta > 0 {
			helpersStarted.Add(1)
		}
	}
	return helpersStarted
}

const parallelMinValidatorCount = parallelMinChunks * 256

func testValidatorSet(count int) *ValidatorSet {
	encoded := make([]byte, count*validatorSize)
	for i := range encoded {
		encoded[i] = byte(i*31 + i/validatorSize)
	}
	validators := NewValidatorSet(1_000_000)
	if err := validators.DecodeSSZ(encoded, 0); err != nil {
		panic(err)
	}
	return validators
}

func randomValidatorSet(count int) *ValidatorSet {
	encoded := make([]byte, count*validatorSize)
	rng := rand.New(rand.NewSource(int64(count) + 1))
	if _, err := rng.Read(encoded); err != nil {
		panic(err)
	}
	validators := NewValidatorSet(1_000_000)
	if err := validators.DecodeSSZ(encoded, 0); err != nil {
		panic(err)
	}
	return validators
}

func mutateValidator(validators *ValidatorSet, index int) {
	validator := validators.Get(index).Clone()
	validator[index%validatorSize]++
	validators.Set(index, validator)
}

func assertValidatorSetRoot(
	t *testing.T,
	validators *ValidatorSet,
	hash func(*ValidatorSet) ([32]byte, error),
	reference func(*testing.T, *ValidatorSet) [32]byte,
) {
	t.Helper()
	want := reference(t, validators)
	got, err := hash(validators)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func validatorSetRootReference(t *testing.T, validators *ValidatorSet) [32]byte {
	roots := make([][32]byte, validators.Length())
	var hashBuffer [8 * 32]byte
	for i := range roots {
		require.NoError(t, validators.Get(i).Clone().CopyHashBufferTo(hashBuffer[:]))
		require.NoError(t, merkle_tree.MerkleRootFromFlatLeaves(hashBuffer[:], roots[i][:]))
	}
	root, err := merkle_tree.MerkleizeVector(roots, uint64(validators.Cap()))
	require.NoError(t, err)
	lengthRoot := merkle_tree.Uint64Root(uint64(validators.Length()))
	return crypto.Sha256(root[:], lengthRoot[:])
}
