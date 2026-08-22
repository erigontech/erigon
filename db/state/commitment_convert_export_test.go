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

// Test-only bridge: convertCommitmentFile and its sentinels are package-private,
// but the full-aggregator round-trip tests live in package state_test (the
// aggregator-setup helpers — testDbAggregatorWithFiles, etc. — are defined
// there). Re-exporting these via test-only vars avoids leaking the symbol from
// the production API while still giving the black-box tests something to call.

var (
	ConvertCommitmentFileForTest = convertCommitmentFile
	ErrSkipForTest               = errSkip
)

// SetConvertPhase1AfterFileHookForTest installs (or clears, via nil) the
// after-file hook called by convertPhase1 once per loop iteration. Used by the
// resume integration test to cancel mid-Phase-1 deterministically.
func SetConvertPhase1AfterFileHookForTest(fn func(idx int)) {
	convertPhase1AfterFileHook = fn
}
