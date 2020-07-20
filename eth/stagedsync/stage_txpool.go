package stagedsync

import (
	"github.com/ledgerwatch/turbo-geth/log"
)

type TxPoolStartStopper struct {
	Start func() error
	Stop  func() error
}

func spawnTxPool(s *StageState, start func() error) error {
	if err := start(); err != nil {
		log.Info("transaction pool can't be started", "err", err)
	}

	s.Done()
	return nil
}

func unwindTxPool(stop func() error) error {
	if err := stop(); err != nil {
		log.Info("transaction pool can't be stopped", "err", err)
	}

	return nil
}
