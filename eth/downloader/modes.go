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
// It is a uint32 as it is used with atomic operations.
type SyncMode uint32

const (
	FullSync   SyncMode = iota // Synchronise the entire blockchain history from full blocks
	StagedSync                 // Full sync but done in stages
	MgrSync                    // MarryGoRound sync

	// FIXME: these are kept for simplicity of rebasing
	FastSync  // (not supported by turbo-geth)
	LightSync // (not supported by turbo-geth)
)

const (
	FullSyncName   = "full"
	StagedSyncName = "staged"
	MgrSyncName    = "mgr"
)

const MiningEnabled = true

func (mode SyncMode) IsValid() bool {
	return mode == StagedSync || mode == FullSync // needed for some (turbo-geth/console) tests
}

// String implements the stringer interface.
func (mode SyncMode) String() string {
	switch mode {
	case FullSync:
		return FullSyncName
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
	case StagedSyncName:
		*mode = StagedSync
	case MgrSyncName:
		*mode = MgrSync
	default:
		return fmt.Errorf(`unknown sync mode %q, want one of %s`, text, []string{FullSyncName, StagedSyncName, MgrSyncName})
	}
	return nil
}
