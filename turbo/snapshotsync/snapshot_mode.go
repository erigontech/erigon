package snapshotsync

import "fmt"

var DefaultSnapshotMode = SnapshotMode{}

type SnapshotMode struct {
	Headers  bool
	Bodies   bool
	State    bool
	Receipts bool
}

func (m SnapshotMode) ToString() string {
	var mode string
	if m.Headers {
		mode += "h"
	}
	if m.Bodies {
		mode += "b"
	}
	if m.State {
		mode += "s"
	}
	if m.Receipts {
		mode += "r"
	}
	return mode
}

func (m SnapshotMode) ToSnapshotTypes() []SnapshotType {
	var types []SnapshotType
	if m.Headers {
		types = append(types, SnapshotType_headers)
	}
	if m.Bodies {
		types = append(types, SnapshotType_bodies)
	}
	if m.State {
		types = append(types, SnapshotType_state)
	}
	if m.Receipts {
		types = append(types, SnapshotType_receipts)
	}
	return types
}

func FromSnapshotTypes(st []SnapshotType) SnapshotMode {
	var mode SnapshotMode
	for i := range st {
		switch st[i] {
		case SnapshotType_headers:
			mode.Headers = true
		case SnapshotType_bodies:
			mode.Bodies = true
		case SnapshotType_state:
			mode.State = true
		case SnapshotType_receipts:
			mode.Receipts = true
		}
	}
	return mode
}
func SnapshotModeFromString(flags string) (SnapshotMode, error) {
	mode := SnapshotMode{}
	for _, flag := range flags {
		switch flag {
		case 'h':
			mode.Headers = true
		case 'b':
			mode.Bodies = true
		case 's':
			mode.State = true
		case 'r':
			mode.Receipts = true
		default:
			return mode, fmt.Errorf("unexpected flag found: %c", flag)
		}
	}
	return mode, nil
}
