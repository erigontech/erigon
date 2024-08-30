// Copyright 2024 The Erigon Authors
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

package heimdall_test

import (
	"testing"

	"github.com/erigontech/erigon/polygon/heimdall"
)

func TestEnumeration(t *testing.T) {

	if heimdall.BorEvents.Enum() != heimdall.Enums.BorEvents {
		t.Fatal("enum mismatch", heimdall.BorEvents, heimdall.BorEvents.Enum(), heimdall.Enums.BorEvents)
	}

	if heimdall.BorSpans.Enum() != heimdall.Enums.BorSpans {
		t.Fatal("enum mismatch", heimdall.BorSpans, heimdall.BorSpans.Enum(), heimdall.Enums.BorSpans)
	}

	if heimdall.BorCheckpoints.Enum() != heimdall.Enums.BorCheckpoints {
		t.Fatal("enum mismatch", heimdall.BorCheckpoints, heimdall.BorCheckpoints.Enum(), heimdall.Enums.BorCheckpoints)
	}

	if heimdall.BorMilestones.Enum() != heimdall.Enums.BorMilestones {
		t.Fatal("enum mismatch", heimdall.BorMilestones, heimdall.BorMilestones.Enum(), heimdall.Enums.BorMilestones)
	}
}

func TestNames(t *testing.T) {

	if heimdall.BorEvents.Name() != heimdall.Enums.BorEvents.String() {
		t.Fatal("name mismatch", heimdall.BorEvents, heimdall.BorEvents.Name(), heimdall.Enums.BorEvents.String())
	}

	if heimdall.BorSpans.Name() != heimdall.Enums.BorSpans.String() {
		t.Fatal("name mismatch", heimdall.BorSpans, heimdall.BorSpans.Name(), heimdall.Enums.BorSpans.String())
	}

	if heimdall.BorCheckpoints.Name() != heimdall.Enums.BorCheckpoints.String() {
		t.Fatal("name mismatch", heimdall.BorCheckpoints, heimdall.BorCheckpoints.Name(), heimdall.Enums.BorCheckpoints.String())
	}

	if heimdall.BorMilestones.Name() != heimdall.Enums.BorMilestones.String() {
		t.Fatal("name mismatch", heimdall.BorMilestones, heimdall.BorMilestones.Name(), heimdall.Enums.BorMilestones.String())
	}
}
