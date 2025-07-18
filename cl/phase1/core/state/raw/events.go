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

package raw

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

type Events struct {
	OnNewBlockRoot                           func(index int, root common.Hash) error
	OnNewStateRoot                           func(index int, root common.Hash) error
	OnRandaoMixChange                        func(index int, mix [32]byte) error
	OnNewValidator                           func(index int, v solid.Validator, balance uint64) error
	OnNewValidatorBalance                    func(index int, balance uint64) error
	OnNewValidatorEffectiveBalance           func(index int, balance uint64) error
	OnNewValidatorActivationEpoch            func(index int, epoch uint64) error
	OnNewValidatorExitEpoch                  func(index int, epoch uint64) error
	OnNewValidatorWithdrawableEpoch          func(index int, epoch uint64) error
	OnNewValidatorSlashed                    func(index int, slashed bool) error
	OnNewValidatorActivationEligibilityEpoch func(index int, epoch uint64) error
	OnNewValidatorWithdrawalCredentials      func(index int, wc []byte) error
	OnNewSlashingSegment                     func(index int, segment uint64) error
	OnEpochBoundary                          func(epoch uint64) error
	OnNewNextSyncCommittee                   func(committee *solid.SyncCommittee) error
	OnNewCurrentSyncCommittee                func(committee *solid.SyncCommittee) error
	OnAppendEth1Data                         func(data *cltypes.Eth1Data) error
	OnResetParticipation                     func(previousParticipation *solid.ParticipationBitList) error
}
