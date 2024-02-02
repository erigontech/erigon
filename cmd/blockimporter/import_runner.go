package main

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
	"github.com/pion/udp/pkg/sync"
)

type Settings struct {
	DBPath          string
	Logger          log.Logger
	Terminated      chan struct{}
	RetryCount      uint64
	RetryInterval   time.Duration
	PollInterval    time.Duration
	SaveHistoryData bool
}

func RunImport(settings *Settings, blockSource BlockSource, secondaryBlocksSource BlockSource) error {
	db, err := NewDB(settings.DBPath, settings.Logger)
	if err != nil {
		return err
	}
	defer db.Close()

	initBalances, err := blockSource.GetInitialBalances()
	if err != nil {
		return err
	}

	chainID, err := blockSource.GetChainID()
	if err != nil {
		return err
	}

	state, err := NewState(db, initBalances, chainID)
	if err != nil {
		panic(err)
	}

	blockSource = makeBlockSource(settings, blockSource, secondaryBlocksSource)

	blocksChan := make(chan []types.Block, 10)

	var resultErr error
	wg := sync.NewWaitGroup()
	wg.Add(2)

	go func() {
		defer wg.Done()

		blockNum := state.BlockNum()

		for {
			blocks, err := blockSource.PollBlocks(blockNum)
			if err != nil {
				resultErr = fmt.Errorf("failed to poll blocks: %w", err)
				close(blocksChan)
				close(settings.Terminated)
				return
			}

			blockNum += uint64(len(blocks))

			select {
			case blocksChan <- blocks:
			case <-settings.Terminated:
				close(blocksChan)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()

		for {
			select {
			case blocks, ok := <-blocksChan:
				{
					if !ok {
						return
					}

					for {
						select {
						case newBlocks, ok := <-blocksChan:
							{
								if !ok {
									return
								}

								blocks = append(blocks, newBlocks...)
							}
						default:
							{
								if err := state.ProcessBlocks(blocks, settings.SaveHistoryData); err != nil {
									resultErr = fmt.Errorf("failed to process block: %w", err)
									close(settings.Terminated)
									return
								}

								blocks = nil

								break
							}
						}
					}
				}
			case <-settings.Terminated:
				{
					return
				}
			}
		}
	}()

	wg.Wait()
	return resultErr
}

func makeBlockSource(settings *Settings, blockSource BlockSource, secondaryBlockSource BlockSource) BlockSource {
	if secondaryBlockSource != nil {
		blockSource = WithSecondaryBlocksSource(blockSource, secondaryBlockSource)
	}

	return makeSingleBlockSource(settings, blockSource)

}

func makeSingleBlockSource(settings *Settings, blockSource BlockSource) BlockSource {
	if settings.RetryCount > 0 {
		blockSource = WithRetries(blockSource, settings.RetryCount, settings.RetryInterval, settings.Terminated)
	}

	if settings.PollInterval > 0 {
		blockSource = WithPollInterval(blockSource, settings.PollInterval, settings.Terminated)
	}

	return blockSource
}
