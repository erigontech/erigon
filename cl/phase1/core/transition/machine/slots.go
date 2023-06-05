package machine

import "github.com/ledgerwatch/erigon/cl/phase1/core/state"

type SlotProcessor interface {
	ProcessSlots(s *state.BeaconState, slot uint64) error
}
