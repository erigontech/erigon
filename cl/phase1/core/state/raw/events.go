package raw

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

type Events struct {
	OnNewBlockRoot                           func(index int, root libcommon.Hash) error
	OnNewStateRoot                           func(index int, root libcommon.Hash) error
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
	OnResetParticipation                     func(previousParticipation *solid.BitList) error
}
