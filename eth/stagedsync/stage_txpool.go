package stagedsync

func spawnTxPool(s *StageState, blockchain BlockChain) error {
	type startTxPool interface {
		StartTxPool() error
	}
	txPoolStarter, ok := blockchain.Engine().(startTxPool)
	if !ok {
		return nil
	}

	if err := txPoolStarter.StartTxPool(); err != nil {
		return err
	}

	s.Done()
	return nil
}

func unwindTxPool(blockchain BlockChain) error {
	type stopTxPool interface {
		StopTxPool()
	}
	txPoolStopper, ok := blockchain.Engine().(stopTxPool)
	if !ok {
		return nil
	}
	txPoolStopper.StopTxPool()
	return nil
}
