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

package snaptype_test

import (
	"testing"

	"github.com/erigontech/erigon/polygon/bor/snaptype"
)

func TestEnumeration(t *testing.T) {

	if snaptype.BorEvents.Enum() != snaptype.Enums.BorEvents {
		t.Fatal("enum mismatch", snaptype.BorEvents, snaptype.BorEvents.Enum(), snaptype.Enums.BorEvents)
	}

	if snaptype.BorSpans.Enum() != snaptype.Enums.BorSpans {
		t.Fatal("enum mismatch", snaptype.BorSpans, snaptype.BorSpans.Enum(), snaptype.Enums.BorSpans)
	}

	if snaptype.BorCheckpoints.Enum() != snaptype.Enums.BorCheckpoints {
		t.Fatal("enum mismatch", snaptype.BorCheckpoints, snaptype.BorCheckpoints.Enum(), snaptype.Enums.BorCheckpoints)
	}

	if snaptype.BorMilestones.Enum() != snaptype.Enums.BorMilestones {
		t.Fatal("enum mismatch", snaptype.BorMilestones, snaptype.BorMilestones.Enum(), snaptype.Enums.BorMilestones)
	}
}

func TestNames(t *testing.T) {

	if snaptype.BorEvents.Name() != snaptype.Enums.BorEvents.String() {
		t.Fatal("name mismatch", snaptype.BorEvents, snaptype.BorEvents.Name(), snaptype.Enums.BorEvents.String())
	}

	if snaptype.BorSpans.Name() != snaptype.Enums.BorSpans.String() {
		t.Fatal("name mismatch", snaptype.BorSpans, snaptype.BorSpans.Name(), snaptype.Enums.BorSpans.String())
	}

	if snaptype.BorCheckpoints.Name() != snaptype.Enums.BorCheckpoints.String() {
		t.Fatal("name mismatch", snaptype.BorCheckpoints, snaptype.BorCheckpoints.Name(), snaptype.Enums.BorCheckpoints.String())
	}

	if snaptype.BorMilestones.Name() != snaptype.Enums.BorMilestones.String() {
		t.Fatal("name mismatch", snaptype.BorMilestones, snaptype.BorMilestones.Name(), snaptype.Enums.BorMilestones.String())
	}
}
