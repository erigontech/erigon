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
	"github.com/ledgerwatch/erigon/cl/transition"
	"github.com/ledgerwatch/log/v3"
)

func (s *Antiquary) loopStates(ctx context.Context) {
	// Execute this each second
	reqRetryTimer := time.NewTicker(3 * time.Second)
	defer reqRetryTimer.Stop()
	if !initial_state.IsGenesisStateSupported(clparams.NetworkType(s.cfg.DepositNetworkID)) {
		s.logger.Warn("Genesis state is not supported for this network, no historical states data will be available")
		return
	}

	for {
		select {
		// Check if we are behind finalized
		case <-reqRetryTimer.C:
			if !s.backfilled.Load() {
				continue
			}
			// Check if we are behind finalized
			progress, finalized, err := s.readHistoricalProcessingProgress(ctx)
			if err != nil {
				s.logger.Error("Failed to read historical processing progress", "err", err)
				continue
			}
			if progress >= finalized {
				continue
			}
			if err := s.incrementBeaconState(ctx, finalized); err != nil {
				s.logger.Error("Failed to increment beacon state", "err", err)
				return
			}

		case <-ctx.Done():
			return
		}
	}
}

func (s *Antiquary) readHistoricalProcessingProgress(ctx context.Context) (progress, finalized uint64, err error) {
	var tx kv.Tx
	tx, err = s.mainDB.BeginRo(ctx)
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

func (s *Antiquary) incrementBeaconState(ctx context.Context, to uint64) error {
	var tx kv.Tx
	tx, err := s.mainDB.BeginRo(ctx)
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
		block, err := s.snReader.ReadBlockBySlot(ctx, tx, slot)
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
			log.Info("State processing progress", "slot", slot, "blk/sec", fmt.Sprintf("%.2f", float64(slot-prevSlot)/60))
			prevSlot = slot
		default:
		}
	}
	log.Info("State processing finished", "slot", s.currentState.Slot())
	tx.Rollback()
	log.Info("Stopping Caplin to load states")

	rwTx, err := s.mainDB.BeginRw(ctx)
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
