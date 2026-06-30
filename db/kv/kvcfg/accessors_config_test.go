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

package kvcfg

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv/memdb"
)

// TestSnapModeFlag_FirstRunPersists verifies that the first call against a
// fresh datadir persists the CLI-supplied value and reports notChanged=true.
// This is what allows a new datadir to be initialised with any flag value.
func TestSnapModeFlag_FirstRunPersists(t *testing.T) {
	for _, key := range []ConfigKey{SnapP2PManifest, SnapLifecycleDrivenByStorage, SnapBootstrapFromPreverified} {
		for _, value := range []bool{true, false} {
			_, tx := memdb.NewTestTx(t)
			notChanged, enabled, err := key.EnsureNotChanged(tx, value)
			require.NoError(t, err)
			require.True(t, notChanged, "first run must report notChanged=true (key=%s value=%v)", key, value)
			require.Equal(t, value, enabled, "first run must persist + report the passed value (key=%s)", key)

			// Re-read to confirm persistence.
			persisted, err := key.Enabled(tx)
			require.NoError(t, err)
			require.Equal(t, value, persisted, "first run must persist on disk (key=%s)", key)
		}
	}
}

// TestSnapModeFlag_MatchingRerunOK verifies that a re-run with the same CLI
// value as the persisted value is accepted (notChanged=true).
func TestSnapModeFlag_MatchingRerunOK(t *testing.T) {
	for _, key := range []ConfigKey{SnapP2PManifest, SnapLifecycleDrivenByStorage, SnapBootstrapFromPreverified} {
		for _, value := range []bool{true, false} {
			_, tx := memdb.NewTestTx(t)
			// Seed the datadir.
			require.NoError(t, key.ForceWrite(tx, value))

			notChanged, enabled, err := key.EnsureNotChanged(tx, value)
			require.NoError(t, err)
			require.True(t, notChanged, "matching re-run must report notChanged=true (key=%s value=%v)", key, value)
			require.Equal(t, value, enabled)
		}
	}
}

// TestSnapModeFlag_MismatchedRerunSignalsChange is the load-bearing case: a
// re-run where the CLI value contradicts the persisted value must report
// notChanged=false so the caller (backend.go's snapModes loop) refuses
// startup. This pins the change-detection guarantee — without it a future
// refactor could silently degrade the policy to "warn and accept", and a
// soak with the wrong launcher flags would once again wedge several iters
// in instead of failing fast at startup.
//
// Coverage: every combination of persisted ∈ {true,false} ≠ cli ∈ {true,false}
// for every snap-mode key.
func TestSnapModeFlag_MismatchedRerunSignalsChange(t *testing.T) {
	for _, key := range []ConfigKey{SnapP2PManifest, SnapLifecycleDrivenByStorage, SnapBootstrapFromPreverified} {
		for _, persisted := range []bool{true, false} {
			cli := !persisted
			_, tx := memdb.NewTestTx(t)
			require.NoError(t, key.ForceWrite(tx, persisted))

			notChanged, enabled, err := key.EnsureNotChanged(tx, cli)
			require.NoError(t, err, "EnsureNotChanged itself must not error — the caller fails startup on notChanged=false (key=%s persisted=%v cli=%v)", key, persisted, cli)
			require.False(t, notChanged, "mismatched re-run must report notChanged=false (key=%s persisted=%v cli=%v)", key, persisted, cli)
			require.Equal(t, persisted, enabled, "EnsureNotChanged must report the persisted value (not the CLI value) to the caller (key=%s)", key)

			// Verify the persisted value was NOT silently overwritten.
			stillPersisted, err := key.Enabled(tx)
			require.NoError(t, err)
			require.Equal(t, persisted, stillPersisted, "mismatched re-run must not overwrite the persisted value (key=%s)", key)
		}
	}
}
