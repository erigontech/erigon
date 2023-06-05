package transition

import (
	"fmt"

	state2 "github.com/ledgerwatch/erigon/cl/phase1/core/state"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
)

func ProcessOperations(state *state2.BeaconState, blockBody *cltypes.BeaconBody, fullValidation bool) error {

	return nil
}

func maximumDeposits(state *state2.BeaconState) (maxDeposits uint64) {
	maxDeposits = state.Eth1Data().DepositCount - state.Eth1DepositIndex()
	if maxDeposits > state.BeaconConfig().MaxDeposits {
		maxDeposits = state.BeaconConfig().MaxDeposits
	}
	return
}

// ProcessExecutionPayload sets the latest payload header accordinly.
func (I *impl) ProcessExecutionPayload(s *state2.BeaconState, payload *cltypes.Eth1Block) error {
	if state2.IsMergeTransitionComplete(s.BeaconState) {
		if payload.ParentHash != s.LatestExecutionPayloadHeader().BlockHash {
			return fmt.Errorf("ProcessExecutionPayload: invalid eth1 chain. mismatching parent")
		}
	}
	if payload.PrevRandao != s.GetRandaoMixes(state2.Epoch(s.BeaconState)) {
		return fmt.Errorf("ProcessExecutionPayload: randao mix mismatches with mix digest")
	}
	if payload.Time != state2.ComputeTimestampAtSlot(s.BeaconState, s.Slot()) {
		return fmt.Errorf("ProcessExecutionPayload: invalid Eth1 timestamp")
	}
	payloadHeader, err := payload.PayloadHeader()
	if err != nil {
		return err
	}
	s.SetLatestExecutionPayloadHeader(payloadHeader)
	return nil
}

func executionEnabled(s *state2.BeaconState, payload *cltypes.Eth1Block) bool {
	return (!state2.IsMergeTransitionComplete(s.BeaconState) && payload.BlockHash != libcommon.Hash{}) || state2.IsMergeTransitionComplete(s.BeaconState)
}
