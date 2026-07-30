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

package snaptype_test

import (
	"slices"
	"testing"

	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/polygon/heimdall"
)

// heimdall.SnapshotTypes() omits Checkpoints and Milestones depending on
// configuration, so the bor list is spelled out to cover the full enum range.
var borSnapshotTypes = []snaptype.Type{heimdall.Events, heimdall.Spans, heimdall.Checkpoints, heimdall.Milestones}

// Salt belongs to neither exported snaptype2 slice, so it is listed explicitly.
var coreSnapshotTypes = slices.Concat(
	[]snaptype.Type{snaptype2.Salt},
	snaptype2.BlockSnapshotTypes,
	snaptype2.E3StateTypes,
)

func allSnapshotTypes() []snaptype.Type {
	return slices.Concat(
		coreSnapshotTypes,
		borSnapshotTypes,
		snaptype.CaplinSnapshotTypes,
	)
}

func TestEnumRoundTrip(t *testing.T) {
	for _, typ := range allSnapshotTypes() {
		name := typ.Name()
		enum := typ.Enum()
		if got := enum.String(); got != name {
			t.Errorf("type %q: Enum().String() = %q (enum %d)", name, got, enum)
		}
		parsed, ok := snaptype.ParseEnum(name)
		if !ok || parsed != enum {
			t.Errorf("type %q: ParseEnum = (%d, %v), want (%d, true)", name, parsed, ok, enum)
		}
		if got := enum.Type().Name(); got != name {
			t.Errorf("type %q: Enum().Type().Name() = %q (enum %d)", name, got, enum)
		}
	}
}

func TestEnumUniqueness(t *testing.T) {
	byEnum := map[snaptype.Enum]string{}
	for _, typ := range allSnapshotTypes() {
		enum, name := typ.Enum(), typ.Name()
		if prev, taken := byEnum[enum]; taken {
			t.Errorf("enum %d shared by %q and %q", enum, prev, name)
			continue
		}
		byEnum[enum] = name
	}
}

func expectRegisterPanic(t *testing.T, enum snaptype.Enum, name string) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Errorf("RegisterType(%d, %q) did not panic", enum, name)
		}
	}()
	snaptype.RegisterType(enum, name, snaptype.Versions{}, nil, nil, nil)
}

func TestRegisterTypePanicsOnDuplicateEnum(t *testing.T) {
	expectRegisterPanic(t, snaptype2.Enums.Headers, "duplicateenum")
}

func TestRegisterTypePanicsOnCaplinRangeEnum(t *testing.T) {
	expectRegisterPanic(t, snaptype.CaplinEnums.BeaconBlocks, "caplinrange")
}

func TestRegisterTypePanicsOnDuplicateName(t *testing.T) {
	expectRegisterPanic(t, snaptype.Enum(snaptype.MaxEnum), "headers")
}

// Caplin names resolve via ParseEnum's switch, not namedTypes, so the name
// guard must reject them even though they never pass through RegisterType.
func TestRegisterTypePanicsOnCaplinName(t *testing.T) {
	expectRegisterPanic(t, snaptype.Enum(snaptype.MaxEnum), "beaconblocks")
}

// An enum outside [MinCoreEnum, MaxEnum) would index past the MaxEnum-sized
// file slices at runtime, so registration must fail at init instead.
func TestRegisterTypePanicsOnOutOfRangeEnum(t *testing.T) {
	expectRegisterPanic(t, snaptype.Unknown, "belowrange")
	expectRegisterPanic(t, snaptype.Enum(snaptype.MaxEnum), "aboverange")
}

func TestEnumRangeDisjointness(t *testing.T) {
	for _, typ := range coreSnapshotTypes {
		if e := typ.Enum(); e < snaptype.MinCoreEnum || e >= snaptype.MinCaplinEnum {
			t.Errorf("core type %q enum %d outside [%d, %d)", typ.Name(), e, snaptype.MinCoreEnum, snaptype.MinCaplinEnum)
		}
	}
	for _, typ := range snaptype.CaplinSnapshotTypes {
		if e := typ.Enum(); e < snaptype.MinCaplinEnum || e >= snaptype.MinBorEnum {
			t.Errorf("caplin type %q enum %d outside [%d, %d)", typ.Name(), e, snaptype.MinCaplinEnum, snaptype.MinBorEnum)
		}
	}
	for _, typ := range borSnapshotTypes {
		if e := typ.Enum(); e < snaptype.MinBorEnum || e >= snaptype.MaxEnum {
			t.Errorf("bor type %q enum %d outside [%d, %d)", typ.Name(), e, snaptype.MinBorEnum, snaptype.MaxEnum)
		}
	}
}
