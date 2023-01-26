package transition

func (s *StateTransistor) ProcessEth1DataReset() {
	nextEpoch := s.state.Epoch() + 1
	if nextEpoch%s.beaconConfig.EpochsPerEth1VotingPeriod == 0 {
		s.state.ResetEth1DataVotes()
	}
}

func (s *StateTransistor) ProcessSlashingsReset() {
	s.state.SetSlashingSegmentAt(int(s.state.Epoch()+1)%int(s.beaconConfig.EpochsPerSlashingsVector), 0)

}

func (s *StateTransistor) ProcessRandaoMixesReset() {
	currentEpoch := s.state.Epoch()
	nextEpoch := s.state.Epoch() + 1
	s.state.SetRandaoMixAt(int(nextEpoch%s.beaconConfig.EpochsPerHistoricalVector), s.state.GetRandaoMixes(currentEpoch))
}
