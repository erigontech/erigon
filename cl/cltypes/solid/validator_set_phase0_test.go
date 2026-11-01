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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidatorSetWithoutPhase0DataStaysLazy(t *testing.T) {
	tests := map[string]func(*testing.T) *ValidatorSet{
		"constructed": func(t *testing.T) *ValidatorSet {
			return NewValidatorSetWithLength(4, 2)
		},
		"decoded": func(t *testing.T) *ValidatorSet {
			set := NewValidatorSet(4)
			require.NoError(t, set.DecodeSSZ(make([]byte, 2*validatorSize), 0))
			return set
		},
	}

	for name, newSet := range tests {
		t.Run(name, func(t *testing.T) {
			set := newSet(t)
			require.Nil(t, set.phase0Data)

			copySet := NewValidatorSet(4)
			set.CopyTo(copySet)
			require.Nil(t, copySet.phase0Data)

			set.Append(NewValidator())
			copySet.Append(NewValidator())
			require.Nil(t, set.phase0Data)
			require.Nil(t, copySet.phase0Data)
		})
	}
}

func TestValidatorSetMissingPhase0DataReadsNilAndChecksBounds(t *testing.T) {
	set := NewValidatorSetWithLength(4, 2)
	for i := range set.Length() {
		require.Nil(t, set.MinCurrentInclusionDelayAttestation(i))
		require.Nil(t, set.MinPreviousInclusionDelayAttestation(i))
	}
	for _, i := range []int{-1, set.Length()} {
		require.Panics(t, func() { set.MinCurrentInclusionDelayAttestation(i) })
		require.Panics(t, func() { set.MinPreviousInclusionDelayAttestation(i) })
	}
}

func TestValidatorSetPhase0DataSettersCheckBounds(t *testing.T) {
	set := NewValidatorSetWithLength(4, 2)
	attestation := &PendingAttestation{}
	panicValue := "ValidatorSet -- phase0ForWrite: out of bounds"

	require.PanicsWithValue(t, panicValue, func() {
		set.SetMinCurrentInclusionDelayAttestation(set.Length(), attestation)
	})
	require.PanicsWithValue(t, panicValue, func() {
		set.SetMinPreviousInclusionDelayAttestation(set.Length(), attestation)
	})
}

func TestValidatorSetPhase0DataFirstWrite(t *testing.T) {
	set := NewValidatorSetWithLength(4, 2)
	current := &PendingAttestation{InclusionDelay: 1}
	previous := &PendingAttestation{InclusionDelay: 2}
	set.SetMinCurrentInclusionDelayAttestation(0, current)

	require.Len(t, set.phase0Data, set.Length())

	set.SetMinPreviousInclusionDelayAttestation(1, previous)

	require.Same(t, current, set.MinCurrentInclusionDelayAttestation(0))
	require.Same(t, previous, set.MinPreviousInclusionDelayAttestation(1))
}

func TestValidatorSetPhase0DataCopyTo(t *testing.T) {
	set, current, previous := newValidatorSetWithPhase0Data()

	copySet := NewValidatorSetWithLength(4, 2)
	copySet.SetMinCurrentInclusionDelayAttestation(0, &PendingAttestation{})
	copyBacking := &copySet.phase0Data[0]
	set.CopyTo(copySet)
	require.Same(t, copyBacking, &copySet.phase0Data[0])
	require.Same(t, current, copySet.MinCurrentInclusionDelayAttestation(0))
	require.Same(t, previous, copySet.MinPreviousInclusionDelayAttestation(1))
}

func TestValidatorSetPhase0DataAppend(t *testing.T) {
	set, current, previous := newValidatorSetWithPhase0Data()
	copySet := NewValidatorSetWithLength(4, 2)
	set.CopyTo(copySet)

	set.Append(NewValidator())
	copySet.Append(NewValidator())
	for _, validators := range []*ValidatorSet{set, copySet} {
		require.Len(t, validators.phase0Data, validators.Length())
		require.Nil(t, validators.MinCurrentInclusionDelayAttestation(2))
		require.Nil(t, validators.MinPreviousInclusionDelayAttestation(2))
		require.Same(t, current, validators.MinCurrentInclusionDelayAttestation(0))
		require.Same(t, previous, validators.MinPreviousInclusionDelayAttestation(1))
	}
}

func TestValidatorSetPhase0DataCopyWriteDoesNotChangeSource(t *testing.T) {
	set, current, _ := newValidatorSetWithPhase0Data()
	copySet := NewValidatorSetWithLength(4, 2)
	set.CopyTo(copySet)

	replacement := &PendingAttestation{InclusionDelay: 3}
	copySet.SetMinCurrentInclusionDelayAttestation(0, replacement)
	require.Same(t, current, set.MinCurrentInclusionDelayAttestation(0))
	require.Same(t, replacement, copySet.MinCurrentInclusionDelayAttestation(0))
}

func newValidatorSetWithPhase0Data() (*ValidatorSet, *PendingAttestation, *PendingAttestation) {
	set := NewValidatorSetWithLength(4, 2)
	current := &PendingAttestation{InclusionDelay: 1}
	previous := &PendingAttestation{InclusionDelay: 2}
	set.SetMinCurrentInclusionDelayAttestation(0, current)
	set.SetMinPreviousInclusionDelayAttestation(1, previous)
	return set, current, previous
}

func TestValidatorSetCopyWithoutPhase0DataClearsTarget(t *testing.T) {
	source := NewValidatorSetWithLength(4, 2)
	target := NewValidatorSetWithLength(4, 2)
	target.SetMinCurrentInclusionDelayAttestation(0, &PendingAttestation{InclusionDelay: 1})
	target.SetMinPreviousInclusionDelayAttestation(1, &PendingAttestation{InclusionDelay: 2})

	source.CopyTo(target)

	require.Nil(t, target.phase0Data)
	for i := range target.Length() {
		require.Nil(t, target.MinCurrentInclusionDelayAttestation(i))
		require.Nil(t, target.MinPreviousInclusionDelayAttestation(i))
	}
}

func TestValidatorSetClearPhase0Data(t *testing.T) {
	set := NewValidatorSetWithLength(4, 2)
	set.SetMinCurrentInclusionDelayAttestation(0, &PendingAttestation{InclusionDelay: 1})

	set.ClearPhase0Data()

	require.Equal(t, 2, set.Length())
	require.Nil(t, set.phase0Data)
	require.Nil(t, set.MinCurrentInclusionDelayAttestation(0))
}

func TestValidatorSetClearClearsPhase0Data(t *testing.T) {
	set := NewValidatorSetWithLength(4, 2)
	set.SetMinPreviousInclusionDelayAttestation(1, &PendingAttestation{InclusionDelay: 2})
	set.Clear()
	require.Zero(t, set.Length())
	require.Nil(t, set.phase0Data)
}

func TestValidatorSetDecodeSSZClearsPhase0Data(t *testing.T) {
	set := NewValidatorSetWithLength(4, 2)
	set.SetMinCurrentInclusionDelayAttestation(0, &PendingAttestation{InclusionDelay: 1})
	require.NotNil(t, set.phase0Data)

	require.NoError(t, set.DecodeSSZ(make([]byte, 3*validatorSize), 0))

	require.Nil(t, set.phase0Data)
	for i := range set.Length() {
		require.Nil(t, set.MinCurrentInclusionDelayAttestation(i))
		require.Nil(t, set.MinPreviousInclusionDelayAttestation(i))
	}
}
