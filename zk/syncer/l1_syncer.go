package syncer

import (
	"context"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"

	ethereum "github.com/ledgerwatch/erigon"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	ethTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/types"
)

var (
	sequencedBatchTopic = common.HexToHash("0x303446e6a8cb73c83dff421c0b1d5e5ce0719dab1bff13660fc254e58cc17fce")
	verificationTopic   = common.HexToHash("0xcb339b570a7f0b25afa7333371ff11192092a0aeace12b671f4c212f2815c6fe")

	batchWorkers = 2
)

type IEtherman interface {
	BlockByNumber(ctx context.Context, blockNumber *big.Int) (*ethTypes.Block, error)
	FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]ethTypes.Log, error)
}

type fetchJob struct {
	From uint64
	To   uint64
}

type jobResult struct {
	Size  uint64
	Error error
	Logs  []ethTypes.Log
}

type L1Syncer struct {
	em                IEtherman
	l1ContractAddress common.Address
	blockRange        uint64

	latestL1Block uint64

	// atomic
	isSyncStarted      atomic.Bool
	isDownloading      atomic.Bool
	lastCheckedL1Block atomic.Uint64

	// Channels
	verificationsChan   chan types.L1BatchInfo
	sequencesChan       chan types.L1BatchInfo
	progressMessageChan chan string
}

func NewL1Syncer(em IEtherman, l1ContractAddress common.Address, blockRange uint64) *L1Syncer {
	return &L1Syncer{
		em:                  em,
		l1ContractAddress:   l1ContractAddress,
		blockRange:          blockRange,
		verificationsChan:   make(chan types.L1BatchInfo, 1000),
		sequencesChan:       make(chan types.L1BatchInfo, 1000),
		progressMessageChan: make(chan string),
	}
}

func (s *L1Syncer) IsSyncStarted() bool {
	return s.isSyncStarted.Load()
}

func (s *L1Syncer) IsDownloading() bool {
	return s.isDownloading.Load()
}

func (s *L1Syncer) GetLastCheckedL1Block() uint64 {
	return s.lastCheckedL1Block.Load()
}

// Channels
func (s *L1Syncer) GetVerificationsChan() chan types.L1BatchInfo {
	return s.verificationsChan
}

func (s *L1Syncer) GetSequencesChan() chan types.L1BatchInfo {
	return s.sequencesChan
}

func (s *L1Syncer) GetProgressMessageChan() chan string {
	return s.progressMessageChan
}

func (s *L1Syncer) Run(lastCheckedBlock uint64) {
	//if already started, don't start another thread
	if s.isSyncStarted.Load() {
		return
	}

	// set it to true to catch the first cycle run case where the check can pass before the latest block is checked
	s.isDownloading.Store(true)
	s.lastCheckedL1Block.Store(lastCheckedBlock)

	//start a thread to cheack for new l1 block in interval
	go func() {
		s.isSyncStarted.Store(true)
		defer s.isSyncStarted.Store(false)

		log.Info("Starting L1 syncer thread")
		defer log.Info("Stopping L1 syncer thread")

		for {
			latestL1Block, err := s.getLatestL1Block()
			if err != nil {
				log.Error("Error getting latest L1 block", "err", err)
				continue
			}

			if latestL1Block > s.lastCheckedL1Block.Load() {
				s.isDownloading.Store(true)
				if err := s.queryBlocks(); err != nil {
					log.Error("Error querying blocks", "err", err)
					continue
				}
				s.lastCheckedL1Block.Store(latestL1Block)
			}

			s.isDownloading.Store(false)
		}
	}()
}

func (s *L1Syncer) getLatestL1Block() (uint64, error) {
	latestBlock, err := s.em.BlockByNumber(context.Background(), nil)
	if err != nil {
		return 0, err
	}

	latest := latestBlock.NumberU64()
	s.latestL1Block = latest

	return latest, nil
}

func (s *L1Syncer) queryBlocks() error {
	startBlock := s.lastCheckedL1Block.Load()

	log.Debug("GetHighestSequence", "startBlock", s.lastCheckedL1Block.Load())

	// define the blocks we're going to fetch up front
	fetches := make([]fetchJob, 0)
	low := startBlock
	for {
		high := low + s.blockRange
		if high > s.latestL1Block {
			// at the end of our search
			high = s.latestL1Block
		}

		fetches = append(fetches, fetchJob{
			From: low,
			To:   high,
		})

		if high == s.latestL1Block {
			break
		}
		low += s.blockRange + 1
	}

	stop := make(chan bool)
	jobs := make(chan fetchJob, len(fetches))
	results := make(chan jobResult, len(fetches))

	for i := 0; i < batchWorkers; i++ {
		go s.getSequencedLogs(jobs, results, stop)
	}

	for _, fetch := range fetches {
		jobs <- fetch
	}
	close(jobs)

	ticker := time.NewTicker(10 * time.Second)
	var progress uint64 = 0
	aimingFor := s.latestL1Block - startBlock
	complete := 0
loop:
	for {
		select {
		case res := <-results:
			complete++
			if res.Error != nil {
				close(stop)
				return res.Error
			}
			progress += res.Size
			if len(res.Logs) > 0 {
				for _, l := range res.Logs {
					info := convertResultToBatchInfo(&l)
					if l.Topics[0] == sequencedBatchTopic {
						s.sequencesChan <- info
					} else if l.Topics[0] == verificationTopic {

						stateRootData := l.Data[:32]
						stateRoot := common.BytesToHash(stateRootData)
						info.StateRoot = stateRoot
						s.verificationsChan <- info
					} else {
						log.Warn("L1 Syncer unknown topic", "topic", l.Topics[0])
					}
				}
			}

			if complete == len(fetches) {
				// we've got all the results we need
				close(stop)
				break loop
			}
		case <-ticker.C:
			if aimingFor == 0 {
				continue
			}
			s.progressMessageChan <- fmt.Sprintf("Progress: %d/%d (%d%%)", progress, aimingFor, (progress*100)/aimingFor)
		}
	}

	return nil
}

func convertResultToBatchInfo(log *ethTypes.Log) types.L1BatchInfo {
	batchNumber := new(big.Int).SetBytes(log.Topics[1].Bytes())
	l1TxHash := common.BytesToHash(log.TxHash.Bytes())
	blockNumber := log.BlockNumber
	return types.L1BatchInfo{
		BatchNo:   batchNumber.Uint64(),
		L1BlockNo: blockNumber,
		L1TxHash:  l1TxHash,
	}
}

func (s *L1Syncer) getSequencedLogs(jobs <-chan fetchJob, results chan jobResult, stop chan bool) {
	for {
		select {
		case <-stop:
			return
		case j, ok := <-jobs:
			if !ok {
				return
			}
			query := ethereum.FilterQuery{
				FromBlock: big.NewInt(int64(j.From)),
				ToBlock:   big.NewInt(int64(j.To)),
				Addresses: []common.Address{s.l1ContractAddress},
				Topics:    [][]common.Hash{{sequencedBatchTopic, verificationTopic}},
			}

			var logs []ethTypes.Log
			var err error
			retry := 0
			for {
				logs, err = s.em.FilterLogs(context.Background(), query)
				if err != nil {
					log.Debug("getSequencedLogs retry error", "err", err)
					retry++
					if retry > 5 {
						results <- jobResult{
							Error: err,
							Logs:  nil,
						}
						return
					}
					time.Sleep(time.Duration(retry*2) * time.Second)
					continue
				}
				break
			}

			results <- jobResult{
				Size:  j.To - j.From,
				Error: nil,
				Logs:  logs,
			}
		}
	}
}
