package downloader

import (
	"fmt"
	//"os"
	//"runtime/pprof"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/log"
)

const (
	logInterval = 5 // seconds
)

type progressLogger struct {
	timer    *time.Ticker
	quit     chan struct{}
	interval int
}

func NewProgressLogger(intervalInSeconds int) *progressLogger {
	return &progressLogger{
		timer:    time.NewTicker(time.Duration(intervalInSeconds) * time.Second),
		quit:     make(chan struct{}),
		interval: intervalInSeconds,
	}
}

func (l *progressLogger) Start(numberRef *uint64) {
	go func() {
		prev := atomic.LoadUint64(numberRef)
		printFunc := func() {
			now := atomic.LoadUint64(numberRef)
			speed := float64(now-prev) / float64(l.interval)
			log.Info("Executed blocks:", "currentBlock", now, "speed (blk/second)", speed)
			prev = now
		}
		for {
			select {
			case <-l.timer.C:
				printFunc()
			case <-l.quit:
				printFunc()
				return
			}
		}
	}()
}

func (l *progressLogger) Stop() {
	l.timer.Stop()
	close(l.quit)
}

func (d *Downloader) spawnExecuteBlocksStage() (uint64, error) {
	lastProcessedBlockNumber, err := GetStageProgress(d.stateDB, Execution)
	if err != nil {
		return 0, err
	}

	nextBlockNumber := uint64(0)

	atomic.StoreUint64(&nextBlockNumber, lastProcessedBlockNumber+1)

	/*
	profileNumber := atomic.LoadUint64(&nextBlockNumber)
	f, err := os.Create(fmt.Sprintf("cpu-%d.prof", profileNumber))
	if err != nil {
		log.Error("could not create CPU profile", "error", err)
		return lastProcessedBlockNumber, err
	}
	if err1 := pprof.StartCPUProfile(f); err1 != nil {
		log.Error("could not start CPU profile", "error", err1)
		return lastProcessedBlockNumber, err
	}
	*/

	mutation := d.stateDB.NewBatch()
	defer func() {
		_, dbErr := mutation.Commit()
		if dbErr != nil {
			log.Error("Sync (Execution): failed to write db commit", "err", dbErr)
		}
	}()

	progressLogger := NewProgressLogger(logInterval)
	progressLogger.Start(&nextBlockNumber)
	defer progressLogger.Stop()

	chainConfig := d.blockchain.Config()
	engine := d.blockchain.Engine()
	for {
		blockNum := atomic.LoadUint64(&nextBlockNumber)

		block := d.blockchain.GetBlockByNumber(blockNum)
		if block == nil {
			fmt.Printf("block %d nil\n", nextBlockNumber)
			break
		}
		stateReader := state.NewDbStateReader(mutation)
		stateWriter := state.NewDbStateWriter(mutation, blockNum)

		// where the magic happens
		err = core.ExecuteBlockEuphemerally(chainConfig, d.blockchain, engine, block, stateReader, stateWriter)
		if err != nil {
			return 0, err
		}

		if err = SaveStageProgress(mutation, Execution, blockNum); err != nil {
			return 0, err
		}

		atomic.AddUint64(&nextBlockNumber, 1)

		if mutation.BatchSize() >= mutation.IdealBatchSize() {
			if _, err = mutation.Commit(); err != nil {
				return 0, err
			}
			mutation = d.stateDB.NewBatch()
		}

		if blockNum-profileNumber == 100000 {
			// Flush the profiler
			pprof.StopCPUProfile()
		}
		/*
			if nextBlockNumber-profileNumber == 100000 {
				// Flush the profiler
				pprof.StopCPUProfile()
			}
		*/
	}

	return atomic.LoadUint64(&nextBlockNumber) - 1 /* the last processed block */, nil
}

func (d *Downloader) unwindExecutionStage(unwindPoint uint64) error {
	return fmt.Errorf("unwindExecutionStage not implemented")
}
