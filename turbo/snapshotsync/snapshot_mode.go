package snapshotsync

/*
import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/snapshotsync"
)

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

func (m SnapshotMode) ToSnapshotTypes() []snapshotsync.Type {
	var types []snapshotsync.Type
	if m.Headers {
		types = append(types, snapshotsync.SnapshotType_headers)
	}
	if m.Bodies {
		types = append(types, snapshotsync.SnapshotType_bodies)
	}
	if m.State {
		types = append(types, snapshotsync.SnapshotType_state)
	}
	if m.Receipts {
		types = append(types, snapshotsync.SnapshotType_receipts)
	}
	return types
}

func FromSnapshotTypes(st []snapshotsync.Type) SnapshotMode {
	var mode SnapshotMode
	for i := range st {
		switch st[i] {
		case snapshotsync.SnapshotType_headers:
			mode.Headers = true
		case snapshotsync.SnapshotType_bodies:
			mode.Bodies = true
		case snapshotsync.SnapshotType_state:
			mode.State = true
		case snapshotsync.SnapshotType_receipts:
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
*/
