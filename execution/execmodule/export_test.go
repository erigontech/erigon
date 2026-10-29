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

package execmodule

import "testing"

// SetInMemHistoryReads overrides the ERIGON_IN_MEM_HISTORY flag for the test.
// Callers must not run in parallel, since the flag is package-global.
func SetInMemHistoryReads(t *testing.T, enabled bool) {
	prev := inMemHistoryReads
	inMemHistoryReads = enabled
	t.Cleanup(func() { inMemHistoryReads = prev })
}
