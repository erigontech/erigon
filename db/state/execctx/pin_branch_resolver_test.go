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

package execctx

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

type fakeTemporalGetter struct {
	vals map[string][]byte
	err  error
}

func (f *fakeTemporalGetter) GetLatest(_ kv.Domain, k []byte) ([]byte, kv.Step, error) {
	return f.vals[string(k)], 0, f.err
}

func (f *fakeTemporalGetter) HasPrefix(kv.Domain, []byte) ([]byte, []byte, bool, error) {
	return nil, nil, false, nil
}

func (f *fakeTemporalGetter) StepsInFiles(...kv.Domain) kv.Step { return 0 }

// TestPinBranchResolver_ReturnsAuthoritativeLatest pins the trunk-preload resolver
// contract: its output is pinned into the branch cache as the latest branch value,
// so it must come from the authoritative GetLatest — a files-only read here pinned
// stale data whenever the newest value was still in the DB.
func TestPinBranchResolver_ReturnsAuthoritativeLatest(t *testing.T) {
	t.Parallel()
	latest := []byte("branch-latest")
	getter := &fakeTemporalGetter{vals: map[string][]byte{"\x0a\x0b": latest}}
	resolve := pinBranchResolver(getter)
	vals, err := resolve([][]byte{[]byte{0x0a, 0x0b}, []byte{0x0c}})
	require.NoError(t, err)
	require.Len(t, vals, 2)
	require.Equal(t, latest, vals[0])
	require.Nil(t, vals[1], "absent keys must resolve to nil so the preload skips pinning them")
	latest[0] = 'X'
	require.Equal(t, []byte("branch-latest"), vals[0], "resolved values must be copies, detached from the getter's buffer")
}

func TestPinBranchResolver_PropagatesReadErrors(t *testing.T) {
	t.Parallel()
	readErr := errors.New("read failed")
	getter := &fakeTemporalGetter{vals: map[string][]byte{"\x0a": []byte("v")}, err: readErr}
	resolve := pinBranchResolver(getter)
	_, err := resolve([][]byte{[]byte{0x0a}})
	require.ErrorIs(t, err, readErr)
}
