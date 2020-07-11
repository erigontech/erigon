package stagedsync

type TxPoolStartStopper struct {
	Start func() error
	Stop  func() error
}

func spawnTxPool(s *StageState, start func() error) error {
	if err := start(); err != nil {
		return err
	}

	s.Done()
	return nil
}
