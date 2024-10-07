package stages

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/log/v3"
)

type DatastreamClientRunner struct {
	dsClient   DatastreamClient
	logPrefix  string
	stopRunner atomic.Bool
	isReading  atomic.Bool
}

func NewDatastreamClientRunner(dsClient DatastreamClient, logPrefix string) *DatastreamClientRunner {
	return &DatastreamClientRunner{
		dsClient:  dsClient,
		logPrefix: logPrefix,
	}
}

func (r *DatastreamClientRunner) StartRead() error {
	if r.isReading.Load() {
		return fmt.Errorf("tried starting datastream client runner thread while another is running")
	}

	go func() {
		routineId := rand.Intn(1000000)

		log.Info(fmt.Sprintf("[%s] Started downloading L2Blocks routine ID: %d", r.logPrefix, routineId))
		defer log.Info(fmt.Sprintf("[%s] Ended downloading L2Blocks routine ID: %d", r.logPrefix, routineId))

		r.isReading.Store(true)
		defer r.isReading.Store(false)

		for {
			if r.stopRunner.Load() {
				log.Info(fmt.Sprintf("[%s] Downloading L2Blocks routine stopped intentionally", r.logPrefix))
				break
			}

			// start routine to download blocks and push them in a channel
			if !r.dsClient.GetStreamingAtomic().Load() {
				log.Info(fmt.Sprintf("[%s] Starting stream", r.logPrefix))
				// this will download all blocks from datastream and push them in a channel
				// if no error, break, else continue trying to get them
				// Create bookmark

				if err := r.connectDatastream(); err != nil {
					log.Error(fmt.Sprintf("[%s] Error connecting to datastream", r.logPrefix), "error", err)
				}

				if err := r.dsClient.ReadAllEntriesToChannel(); err != nil {
					log.Error(fmt.Sprintf("[%s] Error downloading blocks from datastream", r.logPrefix), "error", err)
				}
			}
		}
	}()

	return nil
}

func (r *DatastreamClientRunner) StopRead() {
	r.stopRunner.Store(true)
}

func (r *DatastreamClientRunner) RestartReadFromBlock(fromBlock uint64) error {
	r.StopRead()

	//wait for the old routine to be finished before continuing
	counter := 0
	for {
		if !r.isReading.Load() {
			break
		}
		counter++
		if counter > 100 {
			return fmt.Errorf("failed to stop reader routine correctly")
		}
		time.Sleep(100 * time.Millisecond)
	}

	// set new block
	r.dsClient.GetProgressAtomic().Store(fromBlock)

	log.Info(fmt.Sprintf("[%s] Restarting datastream from block %d", r.logPrefix, fromBlock))

	return r.StartRead()
}

func (r *DatastreamClientRunner) connectDatastream() (err error) {
	var connected bool
	for i := 0; i < 5; i++ {
		if connected, err = r.dsClient.EnsureConnected(); err != nil {
			log.Error(fmt.Sprintf("[%s] Error connecting to datastream", r.logPrefix), "error", err)
			continue
		}
		if connected {
			return nil
		}
	}

	return fmt.Errorf("failed to connect to datastream")
}
