// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package downloader

import "fmt"

// SyncMode represents the synchronisation mode of the downloader.
type SyncMode int

const (
	FullSync   SyncMode = iota // Synchronise the entire blockchain history from full blocks
	FastSync                   // Quickly download the headers, full sync only at the chain head
	LightSync                  // Download only the headers and terminate afterwards
	StagedSync                 // Full sync but done in stages
	MgrSync                    // MarryGoRound sync
)

const (
	FullSyncName   = "full"
	FastSyncName   = "fast"
	LightSyncName  = "light"
	StagedSyncName = "staged"
	MgrSyncName    = "mgr"
)

func (mode SyncMode) IsValid() bool {
	return mode == FullSync || mode == StagedSync
}

// String implements the stringer interface.
func (mode SyncMode) String() string {
	switch mode {
	case FullSync:
		return FullSyncName
	case FastSync:
		return FastSyncName
	case LightSync:
		return LightSyncName
	case StagedSync:
		return StagedSyncName
	case MgrSync:
		return MgrSyncName
	default:
		return "unknown"
	}
}

func (mode SyncMode) MarshalText() ([]byte, error) {
	switch mode {
	case FullSync:
		return []byte(FullSyncName), nil
	case FastSync:
		return []byte(FastSyncName), nil
	case LightSync:
		return []byte(LightSyncName), nil
	case StagedSync:
		return []byte(StagedSyncName), nil
	case MgrSync:
		return []byte(MgrSyncName), nil
	default:
		return nil, fmt.Errorf("unknown sync mode %d", mode)
	}
}

func (mode *SyncMode) UnmarshalText(text []byte) error {
	switch string(text) {
	case FullSyncName:
		*mode = FullSync
	case FastSyncName:
		*mode = FastSync
	case LightSyncName:
		*mode = LightSync
	case StagedSyncName:
		*mode = StagedSync
	case MgrSyncName:
		*mode = MgrSync
	default:
		return fmt.Errorf(`unknown sync mode %q, want "%s", "%s", "%s" or "%s" or "%s"`,
			FullSyncName, FastSyncName, LightSyncName, StagedSyncName, MgrSyncName, text)
	}
	return nil
}
