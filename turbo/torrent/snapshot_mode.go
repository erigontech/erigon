package torrent

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
