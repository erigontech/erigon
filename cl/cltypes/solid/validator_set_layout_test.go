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
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidatorSetWrongLayoutHashReturnsError(t *testing.T) {
	for _, count := range []int{0, 1, 300} {
		binary := allTestSet(t, count, false).set
		_, err := binary.HashSSZProgressive()
		require.ErrorIs(t, err, errValidatorLayout)
		_, err = binary.HashSSZ()
		require.NoError(t, err)

		progressive := allTestSet(t, count, true).set
		_, err = progressive.HashSSZ()
		require.ErrorIs(t, err, errValidatorLayout)
		_, err = progressive.HashSSZProgressive()
		require.NoError(t, err)
	}
}

func TestValidatorSetLayoutSwitchRechunks(t *testing.T) {
	for _, count := range []int{0, 1, 84, 85, 86, 340, 341, 342, 1000, 1365} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			live := allTestSet(t, count, false)
			live.check(t, "binary")
			sibling := &liveSet{set: NewValidatorSet(1 << 20), model: newValidatorModel(1 << 20)}
			live.copyInto(sibling)

			live.progressive = true
			live.set.SetProgressiveHashing(true)
			live.check(t, "switched to progressive")
			_, err := live.set.HashSSZ()
			require.ErrorIs(t, err, errValidatorLayout)
			sibling.check(t, "frozen sibling after the source switched")

			live.progressive = false
			live.set.SetProgressiveHashing(false)
			live.check(t, "switched back to binary")
			sibling.check(t, "frozen sibling after switching back")
		})
	}
}

func TestValidatorSetDirectProgressiveDecode(t *testing.T) {
	for _, count := range []int{0, 1, 85, 86, 341, 5000} {
		source := allTestSet(t, count, true)
		encoded, err := source.set.EncodeSSZ(nil)
		require.NoError(t, err)

		decoded := NewValidatorSet(1 << 20)
		decoded.SetProgressiveHashing(true)
		require.NoError(t, decoded.DecodeSSZ(encoded, 0))
		got, err := decoded.HashSSZProgressive()
		require.NoError(t, err)
		require.Equal(t, source.model.root(t, true), got)
	}
}

func TestValidatorSetReusedSpareTakesLayoutFromSource(t *testing.T) {
	rng := rand.New(rand.NewSource(23))
	for _, tc := range []struct {
		name                   string
		sourceProg, spareProg  bool
		sourceCount, spareSize int
	}{
		{"fulu into gloas spare", false, true, 700, 300},
		{"gloas into fulu spare", true, false, 700, 300},
		{"fulu into longer gloas spare", false, true, 90, 2000},
		{"gloas into longer fulu spare", true, false, 90, 2000},
		{"empty fulu into gloas spare", false, true, 0, 300},
		{"empty gloas into fulu spare", true, false, 0, 300},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := newLiveSet(1<<20, tc.sourceProg)
			for range tc.sourceCount {
				source.appendRandom(rng)
			}
			spare := newLiveSet(1<<20, tc.spareProg)
			for range tc.spareSize {
				spare.appendRandom(rng)
			}
			spare.check(t, "spare before copy")

			source.copyInto(spare)
			spare.check(t, "spare after copy")
			source.check(t, "source after copy")
			_, err := spare.set.HashSSZ()
			if tc.sourceProg {
				require.ErrorIs(t, err, errValidatorLayout)
			} else {
				require.NoError(t, err)
			}

			spare.appendRandom(rng)
			spare.applyRandomSetter(rng)
			spare.check(t, "spare mutated")
			source.check(t, "source after spare mutated")
		})
	}
}

func TestValidatorSetCloneKeepsCapacityAndLayout(t *testing.T) {
	for _, progressive := range []bool{false, true} {
		live := allTestSet(t, 10, progressive)
		clone := live.set.Clone().(*ValidatorSet)
		require.Zero(t, clone.Length())
		require.Equal(t, live.set.Cap(), clone.Cap())
		require.Equal(t, live.set.list.layout, clone.list.layout)
	}
}

func TestValidatorSetCopyClearsTruncatedTopTrees(t *testing.T) {
	rng := rand.New(rand.NewSource(29))
	spare := newLiveSet(1<<20, true)
	for range 2000 {
		spare.appendRandom(rng)
	}
	_, err := spare.hash()
	require.NoError(t, err)
	require.Greater(t, len(spare.set.list.top), 1)

	source := newLiveSet(1<<20, false)
	for range 700 {
		source.appendRandom(rng)
	}
	_, err = source.hash()
	require.NoError(t, err)

	source.copyInto(spare)
	spare.check(t, "spare after copy")
	top := spare.set.list.top
	for i, tree := range top[len(top):cap(top)] {
		require.Nil(t, tree, "stale top tree %d", len(top)+i)
	}
}
