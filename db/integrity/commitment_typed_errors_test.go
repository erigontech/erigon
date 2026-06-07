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

package integrity

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCommitmentTypedErrors_WrapErrIntegrity pins that every typed
// commitment-validation error sentinel wraps ErrIntegrity. The
// integrity tool's classifier (CheckCommitmentRoot at line ~87) uses
// errors.Is(err, ErrIntegrity) to distinguish "this file failed its
// integrity invariants" (which can be logged-and-continued under
// failFast=false) from "the system itself broke" (which propagates).
//
// If a future refactor accidentally introduces a typed error that
// doesn't wrap ErrIntegrity, the system-error branch would swallow
// integrity failures — invisible regression. This test pins the
// invariant.
func TestCommitmentTypedErrors_WrapErrIntegrity(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
	}{
		{"ErrCommitmentRecordInvalid", ErrCommitmentRecordInvalid},
		{"ErrAnchorHeaderMissing", ErrAnchorHeaderMissing},
		{"ErrAnchorBodyMissing", ErrAnchorBodyMissing},
		{"ErrCommitmentHeaderMismatch", ErrCommitmentHeaderMismatch},
		{"ErrCommitmentTxNumRange", ErrCommitmentTxNumRange},
		{"ErrCommitmentReplayMismatch", ErrCommitmentReplayMismatch},
		{"ErrCommitmentReplayNoHistory", ErrCommitmentReplayNoHistory},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.True(t, errors.Is(c.err, ErrIntegrity),
				"%s must wrap ErrIntegrity so the integrity tool's classifier treats it as an integrity error, not a system error",
				c.name)
		})
	}
}

// TestCommitmentTypedErrors_DistinctIdentities pins that the typed
// errors are distinguishable from each other — callers branch on
// errors.Is to apply different recovery (skip-validate vs pause vs
// quarantine), so silently collapsing two classes would change runtime
// behaviour.
func TestCommitmentTypedErrors_DistinctIdentities(t *testing.T) {
	t.Parallel()
	sentinels := []error{
		ErrCommitmentRecordInvalid,
		ErrAnchorHeaderMissing,
		ErrAnchorBodyMissing,
		ErrCommitmentHeaderMismatch,
		ErrCommitmentTxNumRange,
		ErrCommitmentReplayMismatch,
		ErrCommitmentReplayNoHistory,
	}
	for i, a := range sentinels {
		for j, b := range sentinels {
			if i == j {
				continue
			}
			require.False(t, errors.Is(a, b),
				"sentinel %v unexpectedly matches sentinel %v — callers branching on errors.Is would conflate them", a, b)
		}
	}
}
