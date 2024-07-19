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

package synced_data

import (
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

//go:generate mockgen -typed=true -destination=./mock_services/synced_data_mock.go -package=mock_services . SyncedData
type SyncedData interface {
	OnHeadState(newState *state.CachingBeaconState) error
	HeadState() *state.CachingBeaconState
	HeadStateReader() abstract.BeaconStateReader
	HeadStateMutator() abstract.BeaconStateMutator
	Syncing() bool
	HeadSlot() uint64
}
