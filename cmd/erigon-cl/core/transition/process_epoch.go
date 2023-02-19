package transition

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
)

// ProcessEpoch process epoch transition.
func (s *StateTransistor) ProcessEpoch() error {
	if err := s.ProcessJustificationBitsAndFinality(); err != nil {
		return err
	}
	if s.state.Version() >= clparams.AltairVersion {
		if err := s.ProcessInactivityScores(); err != nil {
			return err
		}
	}
	if err := s.ProcessRewardsAndPenalties(); err != nil {
		return err
	}
	if err := s.ProcessRegistryUpdates(); err != nil {
		return err
	}
	if err := s.ProcessSlashings(); err != nil {
		return err
	}
	s.ProcessEth1DataReset()
	if err := s.ProcessEffectiveBalanceUpdates(); err != nil {
		return err
	}
	s.ProcessSlashingsReset()
	s.ProcessRandaoMixesReset()
	if err := s.ProcessHistoricalRootsUpdate(); err != nil {
		return err
	}
	if s.state.Version() == clparams.Phase0Version {
		if err := s.ProcessParticipationRecordUpdates(); err != nil {
			return err
		}
	}

	if s.state.Version() >= clparams.AltairVersion {
		s.ProcessParticipationFlagUpdates()
		if err := s.ProcessSyncCommitteeUpdate(); err != nil {
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

func (s *StateTransistor) ProcessParticipationRecordUpdates() error {
	panic("not implemented")
}
