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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// ResetVersionedIO rebinds the per-tx read/write sets for the next tx; a
// state-read error recorded for one tx must not stick to the next. A sticky
// error would fail a later tx that read cleanly, and (with the EIP-161 removal
// bail) would suppress a legitimate empty-account deletion.
func TestResetVersionedIO_ClearsStateReadError(t *testing.T) {
	t.Parallel()

	ibs := New(NewNoopReader())
	ibs.recordStateReadError(errors.New("transient read failure"))
	require.Error(t, ibs.StateReadError(), "precondition: error recorded")

	ibs.ResetVersionedIO()

	require.NoError(t, ibs.StateReadError(), "ResetVersionedIO must clear the per-tx state-read error")
}
