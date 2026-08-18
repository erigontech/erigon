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

package app

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

type caplinStateRetireRecorder struct {
	calls    []string
	buildErr error
}

func (r *caplinStateRetireRecorder) BuildMissingIndices(context.Context, log.Logger) error {
	r.calls = append(r.calls, "build-indices")
	return r.buildErr
}

func (r *caplinStateRetireRecorder) RemoveOverlaps(func([]string) error) error {
	r.calls = append(r.calls, "remove-overlaps")
	return nil
}

func TestPrepareCaplinStateSnapshotsForRetireBuildsIndicesFirst(t *testing.T) {
	recorder := &caplinStateRetireRecorder{}

	require.NoError(t, prepareCaplinStateSnapshotsForRetire(context.Background(), recorder, log.New()))
	require.Equal(t, []string{"build-indices", "remove-overlaps"}, recorder.calls)
}

func TestPrepareCaplinStateSnapshotsForRetireStopsOnIndexError(t *testing.T) {
	expected := errors.New("index failed")
	recorder := &caplinStateRetireRecorder{buildErr: expected}

	require.ErrorIs(t, prepareCaplinStateSnapshotsForRetire(context.Background(), recorder, log.New()), expected)
	require.Equal(t, []string{"build-indices"}, recorder.calls)
}
