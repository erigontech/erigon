package transition

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/state"
)

// ProcessEpoch process epoch transition.
func ProcessEpoch(state *state.BeaconState) error {
	if err := ProcessJustificationBitsAndFinality(state); err != nil {
		return err
	}
	if state.Version() >= clparams.AltairVersion {
		if err := ProcessInactivityScores(state); err != nil {
			return err
		}
	}
	if err := ProcessRewardsAndPenalties(state); err != nil {
		return err
	}
	if err := ProcessRegistryUpdates(state); err != nil {
		return err
	}
	if err := ProcessSlashings(state); err != nil {
		return err
	}
	ProcessEth1DataReset(state)
	if err := ProcessEffectiveBalanceUpdates(state); err != nil {
		return err
	}
	ProcessSlashingsReset(state)
	ProcessRandaoMixesReset(state)
	if err := ProcessHistoricalRootsUpdate(state); err != nil {
		return err
	}
	if state.Version() == clparams.Phase0Version {
		if err := ProcessParticipationRecordUpdates(state); err != nil {
			return err
		}
	}

	if state.Version() >= clparams.AltairVersion {
		ProcessParticipationFlagUpdates(state)
		if err := ProcessSyncCommitteeUpdate(state); err != nil {
			return err
		}
	}
	return nil

	/*def process_epoch(state: BeaconState) -> None:
	  process_justification_and_finalization(state)
	  process_rewards_and_penalties(state)
	  process_registry_updates(state)
	  process_slashings(state)
	  process_eth1_data_reset(state)
	  process_effective_balance_updates(state)
	  process_slashings_reset(state)
	  process_randao_mixes_reset(state)
	  process_historical_roots_update(state)
	  process_participation_record_updates(state)*/
	/*
			 def process_epoch(state: BeaconState) -> None:
		    process_justification_and_finalization(state)  # [Modified in Altair]
		    process_inactivity_updates(state)  # [New in Altair]
		    process_rewards_and_penalties(state)  # [Modified in Altair]
		    process_registry_updates(state)
		    process_slashings(state)  # [Modified in Altair]
		    process_eth1_data_reset(state)
		    process_effective_balance_updates(state)
		    process_slashings_reset(state)
		    process_randao_mixes_reset(state)
		    process_historical_roots_update(state)
		    process_participation_flag_updates(state)  # [New in Altair]
		    process_sync_committee_updates(state)  # [New in Altair]
	*/
}

func ProcessParticipationRecordUpdates(state *state.BeaconState) error {
	panic("not implemented")
}
