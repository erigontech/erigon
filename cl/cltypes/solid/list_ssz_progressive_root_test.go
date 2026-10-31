// Copyright 2026 The Erigon Authors
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

package solid

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/merkle_tree"
)

const progressiveRootTestLimit = 2048

func newProgressiveRootTestList(values ...uint64) *ListSSZ[*PendingPartialWithdrawal] {
	list := NewStaticListSSZ[*PendingPartialWithdrawal](progressiveRootTestLimit, SizePendingPartialWithdrawal)
	for _, value := range values {
		list.Append(&PendingPartialWithdrawal{ValidatorIndex: value, Amount: value + 1, WithdrawableEpoch: value + 2})
	}
	return list
}

func progressiveRootTestValues(count int) []uint64 {
	values := make([]uint64, count)
	for i := range values {
		values[i] = uint64(i + 1)
	}
	return values
}

func requireProgressiveRoot(t *testing.T, list *ListSSZ[*PendingPartialWithdrawal]) [32]byte {
	t.Helper()
	root, err := list.HashSSZProgressive(nil)
	require.NoError(t, err)
	return root
}

func TestListSSZProgressiveRootMemoized(t *testing.T) {
	list := newProgressiveRootTestList(progressiveRootTestValues(1024)...)
	want := requireProgressiveRoot(t, list)

	var got [32]byte
	var err error
	allocs := testing.AllocsPerRun(100, func() {
		got, err = list.HashSSZProgressive(nil)
	})
	require.Zero(t, allocs)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestListSSZSteadyStateAppendDoesNotAllocate(t *testing.T) {
	const capacity = 1024
	list := NewStaticListSSZFromList(
		make([]*PendingPartialWithdrawal, 0, capacity),
		progressiveRootTestLimit,
		SizePendingPartialWithdrawal,
	)
	element := &PendingPartialWithdrawal{}

	allocs := testing.AllocsPerRun(100, func() {
		list.Append(element)
	})
	require.Zero(t, allocs)
}

func TestListSSZMutationInvalidatesProgressiveRoot(t *testing.T) {
	tests := []struct {
		name   string
		want   []uint64
		mutate func(*ListSSZ[*PendingPartialWithdrawal]) error
	}{
		{
			name: "set",
			want: []uint64{1, 9, 3},
			mutate: func(list *ListSSZ[*PendingPartialWithdrawal]) error {
				list.Set(1, &PendingPartialWithdrawal{ValidatorIndex: 9, Amount: 10, WithdrawableEpoch: 11})
				return nil
			},
		},
		{
			name: "append",
			want: []uint64{1, 2, 3, 4},
			mutate: func(list *ListSSZ[*PendingPartialWithdrawal]) error {
				list.Append(&PendingPartialWithdrawal{ValidatorIndex: 4, Amount: 5, WithdrawableEpoch: 6})
				return nil
			},
		},
		{
			name: "clear",
			want: nil,
			mutate: func(list *ListSSZ[*PendingPartialWithdrawal]) error {
				list.Clear()
				return nil
			},
		},
		{
			name: "truncate",
			want: []uint64{1, 2},
			mutate: func(list *ListSSZ[*PendingPartialWithdrawal]) error {
				list.Truncate(2)
				return nil
			},
		},
		{
			name: "cut",
			want: []uint64{2, 3},
			mutate: func(list *ListSSZ[*PendingPartialWithdrawal]) error {
				list.Cut(1)
				return nil
			},
		},
		{
			name: "decode SSZ",
			want: []uint64{4, 5},
			mutate: func(list *ListSSZ[*PendingPartialWithdrawal]) error {
				encoded, err := newProgressiveRootTestList(4, 5).EncodeSSZ(nil)
				if err != nil {
					return err
				}
				return list.DecodeSSZ(encoded, 0)
			},
		},
		{
			name: "decode SSZ strict",
			want: []uint64{6, 7},
			mutate: func(list *ListSSZ[*PendingPartialWithdrawal]) error {
				encoded, err := newProgressiveRootTestList(6, 7).EncodeSSZ(nil)
				if err != nil {
					return err
				}
				return list.DecodeSSZStrict(encoded, 0)
			},
		},
		{
			name: "unmarshal JSON array",
			want: []uint64{8, 9},
			mutate: func(list *ListSSZ[*PendingPartialWithdrawal]) error {
				encoded, err := json.Marshal(newProgressiveRootTestList(8, 9))
				if err != nil {
					return err
				}
				return list.UnmarshalJSON(encoded)
			},
		},
		{
			name: "unmarshal JSON null",
			want: nil,
			mutate: func(list *ListSSZ[*PendingPartialWithdrawal]) error {
				return list.UnmarshalJSON([]byte("null"))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			list := newProgressiveRootTestList(1, 2, 3)
			oldRoot := requireProgressiveRoot(t, list)
			require.NoError(t, test.mutate(list))

			want := requireProgressiveRoot(t, newProgressiveRootTestList(test.want...))
			require.NotEqual(t, oldRoot, want)
			require.Equal(t, want, requireProgressiveRoot(t, list))
		})
	}
}

func TestListSSZShallowCopyProgressiveRoot(t *testing.T) {
	t.Run("mutate original after warm copy", func(t *testing.T) {
		original := newProgressiveRootTestList(1, 2, 3)
		oldRoot := requireProgressiveRoot(t, original)
		cpy := original.ShallowCopy()

		original.Append(&PendingPartialWithdrawal{ValidatorIndex: 4, Amount: 5, WithdrawableEpoch: 6})

		require.Equal(t, oldRoot, requireProgressiveRoot(t, cpy))
		require.Equal(t, requireProgressiveRoot(t, newProgressiveRootTestList(1, 2, 3, 4)), requireProgressiveRoot(t, original))
	})

	t.Run("mutate copy after warm copy", func(t *testing.T) {
		original := newProgressiveRootTestList(1, 2, 3)
		oldRoot := requireProgressiveRoot(t, original)
		cpy := original.ShallowCopy()

		cpy.Cut(1)

		require.Equal(t, oldRoot, requireProgressiveRoot(t, original))
		require.Equal(t, requireProgressiveRoot(t, newProgressiveRootTestList(2, 3)), requireProgressiveRoot(t, cpy))
	})

	for _, originalFirst := range []bool{true, false} {
		name := "copy first"
		if originalFirst {
			name = "original first"
		}
		t.Run("copy before warm, "+name, func(t *testing.T) {
			original := newProgressiveRootTestList(1, 2, 3)
			cpy := original.ShallowCopy()
			original.Set(0, &PendingPartialWithdrawal{ValidatorIndex: 9, Amount: 10, WithdrawableEpoch: 11})

			wantOriginal := requireProgressiveRoot(t, newProgressiveRootTestList(9, 2, 3))
			wantCopy := requireProgressiveRoot(t, newProgressiveRootTestList(1, 2, 3))
			if originalFirst {
				require.Equal(t, wantOriginal, requireProgressiveRoot(t, original))
				require.Equal(t, wantCopy, requireProgressiveRoot(t, cpy))
			} else {
				require.Equal(t, wantCopy, requireProgressiveRoot(t, cpy))
				require.Equal(t, wantOriginal, requireProgressiveRoot(t, original))
			}
		})
	}

	t.Run("hash cold copy before mutating original", func(t *testing.T) {
		original := newProgressiveRootTestList(1, 2, 3)
		cpy := original.ShallowCopy()
		require.Equal(t, requireProgressiveRoot(t, newProgressiveRootTestList(1, 2, 3)), requireProgressiveRoot(t, cpy))

		original.Append(&PendingPartialWithdrawal{ValidatorIndex: 4, Amount: 5, WithdrawableEpoch: 6})

		require.Equal(t, requireProgressiveRoot(t, newProgressiveRootTestList(1, 2, 3, 4)), requireProgressiveRoot(t, original))
	})
}

func TestListSSZValueCopyProgressiveRoot(t *testing.T) {
	tests := []struct {
		name           string
		warm           bool
		mutateOriginal bool
		originalFirst  bool
	}{
		{name: "cold copy, mutate copy, copy first"},
		{name: "cold copy, mutate copy, original first", originalFirst: true},
		{name: "cold copy, mutate original, copy first", mutateOriginal: true},
		{name: "cold copy, mutate original, original first", mutateOriginal: true, originalFirst: true},
		{name: "warm copy, mutate copy", warm: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := newProgressiveRootTestList(1, 2, 3)
			if test.warm {
				requireProgressiveRoot(t, original)
			}
			cpy := *original

			wantOriginal := newProgressiveRootTestList(1, 2, 3)
			wantCopy := newProgressiveRootTestList(1, 2, 3)
			if test.mutateOriginal {
				original.Append(&PendingPartialWithdrawal{ValidatorIndex: 4, Amount: 5, WithdrawableEpoch: 6})
				wantOriginal.Append(&PendingPartialWithdrawal{ValidatorIndex: 4, Amount: 5, WithdrawableEpoch: 6})
			} else {
				cpy.Append(&PendingPartialWithdrawal{ValidatorIndex: 4, Amount: 5, WithdrawableEpoch: 6})
				wantCopy.Append(&PendingPartialWithdrawal{ValidatorIndex: 4, Amount: 5, WithdrawableEpoch: 6})
			}

			if test.originalFirst {
				require.Equal(t, requireProgressiveRoot(t, wantOriginal), requireProgressiveRoot(t, original))
				require.Equal(t, requireProgressiveRoot(t, wantCopy), requireProgressiveRoot(t, &cpy))
			} else {
				require.Equal(t, requireProgressiveRoot(t, wantCopy), requireProgressiveRoot(t, &cpy))
				require.Equal(t, requireProgressiveRoot(t, wantOriginal), requireProgressiveRoot(t, original))
			}
		})
	}
}

func TestListSSZWarmShallowCopyReusesProgressiveRoot(t *testing.T) {
	const runs = 100
	original := newProgressiveRootTestList(progressiveRootTestValues(1024)...)
	want := requireProgressiveRoot(t, original)
	copies := make([]*ListSSZ[*PendingPartialWithdrawal], runs+1)
	for i := range copies {
		copies[i] = original.ShallowCopy()
	}
	require.NotSame(t, original.progressiveRoot, copies[0].progressiveRoot)
	require.Same(t, copies[0], copies[0].progressiveRoot.owner)
	require.Same(t, original.progressiveRoot.root.Load(), copies[0].progressiveRoot.root.Load())

	var got [32]byte
	var err error
	nextCopy := 0
	allocs := testing.AllocsPerRun(runs, func() {
		got, err = copies[nextCopy].HashSSZProgressive(nil)
		nextCopy++
	})
	require.Zero(t, allocs)
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestListSSZColdShallowCopyCachesProgressiveRoot(t *testing.T) {
	sources := map[string]func() *ListSSZ[*PendingPartialWithdrawal]{
		"holder without root": func() *ListSSZ[*PendingPartialWithdrawal] {
			return newProgressiveRootTestList(progressiveRootTestValues(1024)...)
		},
		"no holder": func() *ListSSZ[*PendingPartialWithdrawal] {
			return NewStaticListSSZFromList(newProgressiveRootTestList(progressiveRootTestValues(1024)...).list, progressiveRootTestLimit, SizePendingPartialWithdrawal)
		},
	}
	for name, newSource := range sources {
		t.Run(name, func(t *testing.T) {
			source := newSource()
			want := requireProgressiveRoot(t, newProgressiveRootTestList(progressiveRootTestValues(1024)...))

			cpy := source.ShallowCopy()
			require.Equal(t, want, requireProgressiveRoot(t, cpy))
			var got [32]byte
			var err error
			allocs := testing.AllocsPerRun(100, func() {
				got, err = cpy.HashSSZProgressive(nil)
			})
			require.Zero(t, allocs)
			require.NoError(t, err)
			require.Equal(t, want, got)

			copyOfCopy := cpy.ShallowCopy()
			allocs = testing.AllocsPerRun(100, func() {
				got, err = copyOfCopy.HashSSZProgressive(nil)
			})
			require.Zero(t, allocs)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestListSSZProgressiveAndLegacyRootsAreSeparate(t *testing.T) {
	values := []uint64{1, 2, 3}
	reference := newProgressiveRootTestList(values...)
	wantLegacy, err := merkle_tree.ListObjectSSZRoot(reference.list, uint64(reference.limit))
	require.NoError(t, err)
	elementRoots := make([][32]byte, reference.Len())
	for i := range reference.Len() {
		elementRoots[i], err = reference.Get(i).HashSSZ()
		require.NoError(t, err)
	}
	wantProgressive, err := merkle_tree.ProgressiveListRoot(elementRoots, uint64(len(elementRoots)))
	require.NoError(t, err)
	require.NotEqual(t, wantLegacy, wantProgressive)

	t.Run("legacy first", func(t *testing.T) {
		list := newProgressiveRootTestList(values...)
		legacy, err := list.HashSSZ()
		require.NoError(t, err)
		require.Equal(t, wantLegacy, legacy)
		require.Equal(t, wantProgressive, requireProgressiveRoot(t, list))
	})

	t.Run("progressive first", func(t *testing.T) {
		list := newProgressiveRootTestList(values...)
		require.Equal(t, wantProgressive, requireProgressiveRoot(t, list))
		legacy, err := list.HashSSZ()
		require.NoError(t, err)
		require.Equal(t, wantLegacy, legacy)
	})
}

func TestListSSZCustomElementHasherBypassesProgressiveRootCache(t *testing.T) {
	customHasher := func(element *PendingPartialWithdrawal) ([32]byte, error) {
		return [32]byte{byte(element.ValidatorIndex + 10)}, nil
	}

	t.Run("warm cache", func(t *testing.T) {
		list := newProgressiveRootTestList(1, 2, 3)
		want := requireProgressiveRoot(t, list)

		custom, err := list.HashSSZProgressive(customHasher)
		require.NoError(t, err)
		require.NotEqual(t, want, custom)
		require.Equal(t, want, requireProgressiveRoot(t, list))
	})

	t.Run("cold cache", func(t *testing.T) {
		list := newProgressiveRootTestList(1, 2, 3)
		custom, err := list.HashSSZProgressive(customHasher)
		require.NoError(t, err)
		require.Nil(t, list.progressiveRoot.root.Load())

		want := requireProgressiveRoot(t, newProgressiveRootTestList(1, 2, 3))
		require.NotEqual(t, want, custom)
		require.Equal(t, want, requireProgressiveRoot(t, list))
	})
}

func TestListSSZConcurrentProgressiveRoot(t *testing.T) {
	list := newProgressiveRootTestList(progressiveRootTestValues(1024)...)
	const goroutines = 64
	start := make(chan struct{})
	results := make(chan [32]byte, goroutines)
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			<-start
			root, err := list.HashSSZProgressive(nil)
			results <- root
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	want := requireProgressiveRoot(t, list)
	for root := range results {
		require.Equal(t, want, root)
	}
}

func TestListSSZCloneHasEmptyProgressiveRoot(t *testing.T) {
	source := newProgressiveRootTestList(1, 2, 3)
	requireProgressiveRoot(t, source)

	clone := source.Clone().(*ListSSZ[*PendingPartialWithdrawal])
	require.Zero(t, clone.Len())
	require.Equal(t, requireProgressiveRoot(t, newProgressiveRootTestList()), requireProgressiveRoot(t, clone))
}
