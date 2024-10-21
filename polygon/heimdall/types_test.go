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

	if heimdall.Events.Enum() != heimdall.Enums.Events {
		t.Fatal("enum mismatch", heimdall.Events, heimdall.Events.Enum(), heimdall.Enums.Events)
	}

	if heimdall.Spans.Enum() != heimdall.Enums.Spans {
		t.Fatal("enum mismatch", heimdall.Spans, heimdall.Spans.Enum(), heimdall.Enums.Spans)
	}

	if heimdall.Checkpoints.Enum() != heimdall.Enums.Checkpoints {
		t.Fatal("enum mismatch", heimdall.Checkpoints, heimdall.Checkpoints.Enum(), heimdall.Enums.Checkpoints)
	}

	if heimdall.Milestones.Enum() != heimdall.Enums.Milestones {
		t.Fatal("enum mismatch", heimdall.Milestones, heimdall.Milestones.Enum(), heimdall.Enums.Milestones)
	}
}

func TestNames(t *testing.T) {

	if heimdall.Events.Name() != heimdall.Enums.Events.String() {
		t.Fatal("name mismatch", heimdall.Events, heimdall.Events.Name(), heimdall.Enums.Events.String())
	}

	if heimdall.Spans.Name() != heimdall.Enums.Spans.String() {
		t.Fatal("name mismatch", heimdall.Spans, heimdall.Spans.Name(), heimdall.Enums.Spans.String())
	}

	if heimdall.Checkpoints.Name() != heimdall.Enums.Checkpoints.String() {
		t.Fatal("name mismatch", heimdall.Checkpoints, heimdall.Checkpoints.Name(), heimdall.Enums.Checkpoints.String())
	}

	if heimdall.Milestones.Name() != heimdall.Enums.Milestones.String() {
		t.Fatal("name mismatch", heimdall.Milestones, heimdall.Milestones.Name(), heimdall.Enums.Milestones.String())
	}
}
