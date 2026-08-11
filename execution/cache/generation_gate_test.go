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

package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGenerationPublicationApplyPanicConsumesPublication(t *testing.T) {
	var gate GenerationGate
	publisher := gate.Publisher()
	initial := StateGeneration(1, 0, 0, 0)
	publisher.Initialize(initial, nil)
	view := gate.View(initial)
	require.True(t, view.Current())

	publication := publisher.Begin()
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		publication.Publish(StateGeneration(2, 0, 0, 0), func() {
			panic("apply failed")
		}, nil)
	}()

	require.Equal(t, "apply failed", recovered)
	require.Nil(t, publication.gate)
	require.False(t, view.Current())

	publication.Abort()
	next := publisher.Begin()
	next.Abort()
}

func TestGenerationPublicationApplyPanicClearsPartialChanges(t *testing.T) {
	var gate GenerationGate
	publisher := gate.Publisher()
	publisher.Initialize(StateGeneration(1, 0, 0, 0), nil)
	publication := publisher.Begin()

	entries := []string{"old"}
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		publication.Publish(StateGeneration(2, 0, 0, 0), func() {
			entries = append(entries, "partial")
			panic("apply failed")
		}, func() {
			entries = nil
		})
	}()

	require.Equal(t, "apply failed", recovered)
	require.Empty(t, entries)
	require.False(t, gate.View(StateGeneration(2, 0, 0, 0)).Current())
}

func TestGenerationPublicationRejectsOlderStateVersion(t *testing.T) {
	var gate GenerationGate
	publisher := gate.Publisher()
	newer := StateGeneration(3, 0, 0, 0)
	publisher.Initialize(newer, nil)
	newerView := gate.View(newer)
	require.True(t, newerView.Current())

	applied := false
	publication := publisher.Begin()
	publication.Publish(StateGeneration(2, 0, 0, 0), func() {
		applied = true
	}, nil)

	require.False(t, applied, "an older publication must not apply its updates")
	require.True(t, newerView.Current(), "an older publication must restore the newer token")
	require.False(t, gate.View(StateGeneration(2, 0, 0, 0)).Current())
}
