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

package storage

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// stubBlockRetire implements only the one method the index builder calls.
// The embedded nil interface makes any other call panic, which is the point:
// the test would rather fail loudly than silently exercise a different path.
type stubBlockRetire struct {
	dbservices.BlockRetire
	err error
}

func (s stubBlockRetire) BuildMissedIndicesIfNeed(_ context.Context, _ string, _ dbservices.DBEventNotifier) error {
	return s.err
}

// The index salt lands as its own file during bootstrap, so a scan that runs
// before it arrives fails for a reason that resolves itself. Reported as a
// plain error it counts against the per-file quarantine, and a quarantined
// file never advances — that is what left the post-Indexed gate blocked with
// head at 0.
func TestProductionIndexBuilder_SaltNotYetPresentPauses(t *testing.T) {
	b := &productionIndexBuilder{
		blockRetire: stubBlockRetire{err: fmt.Errorf("build indices: %w", snaptype.ErrSaltNotFound)},
	}

	err := b.BuildMissedIndices(context.Background(), nil)
	require.ErrorIs(t, err, validation.ErrPause,
		"a scan that ran before the salt arrived must not count toward quarantine")
}

func TestProductionIndexBuilder_RealFailureIsNotAPause(t *testing.T) {
	wantErr := errors.New("corrupt segment")
	b := &productionIndexBuilder{blockRetire: stubBlockRetire{err: wantErr}}

	err := b.BuildMissedIndices(context.Background(), nil)
	require.ErrorIs(t, err, wantErr)
	require.NotErrorIs(t, err, validation.ErrPause)
}
