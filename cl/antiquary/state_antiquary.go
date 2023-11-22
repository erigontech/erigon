package antiquary

import (
	"context"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/clparams/initial_state"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	state_accessors "github.com/ledgerwatch/erigon/cl/persistence/state"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/transition"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
)

type stateAntiquary struct {
	db              kv.RwDB
	log             log.Logger
	currentState    *state.CachingBeaconState // this starts at nil and is updated by the state processor.
	cfg             *clparams.BeaconChainConfig
	snapshotsReader freezeblocks.BeaconSnapshotReader
}

func (s *stateAntiquary) loop(ctx context.Context) {
	// Execute this each second
	reqRetryTimer := time.NewTicker(3 * time.Second)
	defer reqRetryTimer.Stop()
	if !initial_state.IsGenesisStateSupported(clparams.NetworkType(s.cfg.DepositNetworkID)) {
		s.log.Warn("Genesis state is not supported for this network, no historical states data will be available")
		return
	}

	for {
		select {
		// Check if we are behind finalized
		case <-reqRetryTimer.C:
			// Check if we are behind finalized
			progress, finalized, err := s.readHistoricalProcessingProgress(ctx)
			if err != nil {
				s.log.Error("Failed to read historical processing progress", "err", err)
				continue
			}
			if progress >= finalized {
				continue
			}
			if err := s.incrementBeaconState(ctx, finalized); err != nil {
				s.log.Error("Failed to increment beacon state", "err", err)
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func (s *stateAntiquary) readHistoricalProcessingProgress(ctx context.Context) (progress, finalized uint64, err error) {
	var tx kv.Tx
	tx, err = s.db.BeginRo(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback()
	progress, err = state_accessors.GetStateProcessingProgress(tx)
	if err != nil {
		return
	}

	finalized, err = beacon_indicies.ReadHighestFinalized(tx)
	if err != nil {
		return
	}
	return
}

func (s *stateAntiquary) incrementBeaconState(ctx context.Context, to uint64) error {
	var tx kv.Tx
	tx, err := s.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// TODO(Giulio2002): start from last progress
	if s.currentState == nil {
		s.currentState, err = initial_state.GetGenesisState(clparams.NetworkType(s.cfg.DepositNetworkID))
		if err != nil {
			return err
		}
	}
	log.Info("Starting state processing", "from", s.currentState.Slot(), "to", to)
	// Set up a timer to log progress
	progressTimer := time.NewTicker(1 * time.Minute)
	defer progressTimer.Stop()
	prevSlot := s.currentState.Slot()

	for slot := s.currentState.Slot() + 1; slot < to; slot++ {
		block, err := s.snapshotsReader.ReadBlockBySlot(ctx, tx, slot)
		if err != nil {
			return err
		}
		// If we have a missed block, we just skip it.
		if block == nil {
			continue
		}
		// We sanity check the state every 100k slots.
		if err := transition.TransitionState(s.currentState, block, slot%100_000 == 0); err != nil {
			return err
		}
		select {
		case <-progressTimer.C:
			log.Info("State processing progress", "slot", slot, "blk/sec", fmt.Sprint("%.2f", float64(slot-prevSlot)/60))
			prevSlot = slot
		default:
		}
	}
	log.Info("State processing finished", "slot", s.currentState.Slot())
	tx.Rollback()
	log.Info("Stopping Caplin to load states")

	rwTx, err := s.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer rwTx.Rollback()
	if err := state_accessors.SetStateProcessingProgress(rwTx, s.currentState.Slot()); err != nil {
		return err
	}
	log.Info("Restarting Caplin")
	return rwTx.Commit()
}
