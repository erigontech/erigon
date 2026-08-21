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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

func TestDomainFileFamily_Construct_Aligned(t *testing.T) {
	t.Parallel()
	values := newFilesItem(0, 100)
	index := newFilesItem(0, 100)
	history := newFilesItem(0, 100)

	family, err := NewDomainFileFamily(kv.AccountsDomain, values, index, history)
	require.NoError(t, err)
	require.NotNil(t, family)

	start, end := family.Range()
	require.Equal(t, uint64(0), start)
	require.Equal(t, uint64(100), end)
	require.Equal(t, kv.AccountsDomain, family.Domain)
	require.Same(t, values, family.Values)
	require.Same(t, index, family.Index)
	require.Same(t, history, family.History)
}

func TestDomainFileFamily_Construct_RejectsMismatchedRange(t *testing.T) {
	t.Parallel()
	base := newFilesItem(0, 100)

	cases := []struct {
		name    string
		values  *FilesItem
		index   *FilesItem
		history *FilesItem
	}{
		{"index shifted", base, newFilesItem(0, 200), newFilesItem(0, 100)},
		{"history shifted", base, newFilesItem(0, 100), newFilesItem(50, 100)},
		{"all three differ", newFilesItem(0, 100), newFilesItem(0, 200), newFilesItem(0, 300)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			family, err := NewDomainFileFamily(kv.AccountsDomain, tc.values, tc.index, tc.history)
			require.ErrorIs(t, err, ErrFileFamilyRangeMismatch)
			require.Nil(t, family)
		})
	}
}

func TestDomainFileFamily_Construct_RejectsNilMember(t *testing.T) {
	t.Parallel()
	values := newFilesItem(0, 100)
	index := newFilesItem(0, 100)
	history := newFilesItem(0, 100)

	cases := []struct {
		name    string
		v, i, h *FilesItem
	}{
		{"nil values", nil, index, history},
		{"nil index", values, nil, history},
		{"nil history", values, index, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			family, err := NewDomainFileFamily(kv.AccountsDomain, tc.v, tc.i, tc.h)
			require.ErrorIs(t, err, ErrFileFamilyMissingMember)
			require.Nil(t, family)
		})
	}
}

func TestDomainFileFamily_StepRange(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(100)
	family, err := NewDomainFileFamily(kv.StorageDomain,
		newFilesItem(200, 500), newFilesItem(200, 500), newFilesItem(200, 500))
	require.NoError(t, err)

	fromStep, toStep := family.StepRange(stepSize)
	require.Equal(t, kv.Step(2), fromStep)
	require.Equal(t, kv.Step(5), toStep)
}

func TestDomainFileFamily_Close_NilSafe(t *testing.T) {
	t.Parallel()
	var family *DomainFileFamily
	require.NotPanics(t, func() { family.Close() })
	require.NotPanics(t, func() { family.CloseAndRemove() })
}

func TestDomainFileFamily_Close_ClearsMembers(t *testing.T) {
	t.Parallel()
	values := newFilesItem(0, 100)
	index := newFilesItem(0, 100)
	history := newFilesItem(0, 100)
	family, err := NewDomainFileFamily(kv.AccountsDomain, values, index, history)
	require.NoError(t, err)

	require.NotPanics(t, func() { family.Close() })
	require.Nil(t, values.decompressor)
	require.Nil(t, index.decompressor)
	require.Nil(t, history.decompressor)
}
