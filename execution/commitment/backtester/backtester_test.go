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

package backtester

import (
	"strings"
	"testing"

	"github.com/erigontech/erigon/db/state/statecfg"
)

func TestBacktesterRejectsCommitmentV3(t *testing.T) {
	previous := statecfg.ExperimentalCommitmentV3
	statecfg.ExperimentalCommitmentV3 = true
	t.Cleanup(func() { statecfg.ExperimentalCommitmentV3 = previous })

	err := (Backtester{}).run(t.Context(), nil, 1, 1)
	if err == nil || !strings.Contains(err.Error(), "not supported on commitment v3") {
		t.Fatalf("expected v3 unsupported error, got %v", err)
	}
}
