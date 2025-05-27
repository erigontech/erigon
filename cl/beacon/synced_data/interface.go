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
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
)

type CancelFn func()
type ViewHeadStateFn func(headState *state.CachingBeaconState) error

//go:generate mockgen -typed=true -destination=./mock_services/synced_data_mock.go -package=mock_services . SyncedData
type SyncedData interface {
	OnHeadState(newState *state.CachingBeaconState) error
	UnsetHeadState()
	ViewHeadState(fn ViewHeadStateFn) error
	ViewPreviousHeadState(fn ViewHeadStateFn) error
	Syncing() bool
	HeadSlot() uint64
	HeadRoot() common.Hash
	CommitteeCount(epoch uint64) uint64
	ValidatorPublicKeyByIndex(index int) (common.Bytes48, error)
	ValidatorIndexByPublicKey(pubkey common.Bytes48) (uint64, bool, error)
	HistoricalRootElementAtIndex(index int) (common.Hash, error)
	HistoricalSummaryElementAtIndex(index int) (*cltypes.HistoricalSummary, error)
}
