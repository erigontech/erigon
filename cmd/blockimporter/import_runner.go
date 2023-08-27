package main

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/log/v3"
)

type Settings struct {
	DBPath        string
	Logger        log.Logger
	Terminated    chan struct{}
	RetryCount    uint64
	RetryInterval time.Duration
	PollInterval  time.Duration
}

func RunImport(settings *Settings, blockSource BlockSource) error {
	db, err := NewDB(settings.DBPath, settings.Logger)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	initBalances, err := blockSource.GetInitialBalances()
	if err != nil {
		panic(err)
	}

	state, err := NewState(db, initBalances)
	if err != nil {
		panic(err)
	}

	blockNum := state.BlockNum()
	blockSource = makeBlockSource(settings, blockSource)
	for {
		select {
		case <-settings.Terminated:
			{
				return nil
			}

		default:
			{
				blocks, err := blockSource.PollBlocks(blockNum)
				if err != nil {
					return fmt.Errorf("failed to poll blocks: %w", err)
				}

				for _, block := range blocks {
					if err := state.ProcessBlock(block); err != nil {
						return fmt.Errorf("failed to process block: %w", err)
					}

					blockNum += 1
				}
			}
		}
	}
}

func makeBlockSource(settings *Settings, blockSource BlockSource) BlockSource {
	if settings.RetryCount > 0 {
		blockSource = WithRetries(blockSource, settings.RetryCount, settings.RetryInterval, settings.Terminated)
	}

	if settings.PollInterval > 0 {
		blockSource = WithPollInterval(blockSource, settings.PollInterval, settings.Terminated)
	}

	return blockSource
}
