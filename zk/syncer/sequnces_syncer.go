package syncer

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	ethereum "github.com/ledgerwatch/erigon"
	ethTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/types"
)

var (
	sequencedBatchTopic          = common.HexToHash("0x303446e6a8cb73c83dff421c0b1d5e5ce0719dab1bff13660fc254e58cc17fce")
	blockRange            uint64 = 20000
	sequencedBatchWorkers        = 2
)

type SequencesSyncer struct {
	em                IEtherman
	l1ContractAddress common.Address
}

func NewSequencesSyncer(em IEtherman, l1ContractAddress common.Address) *SequencesSyncer {
	return &SequencesSyncer{
		em:                em,
		l1ContractAddress: l1ContractAddress,
	}
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

func (s *SequencesSyncer) GetSequences(logPrefix string, startBlock uint64) ([]types.L1BatchInfo, uint64, error) {
	log.Debug("GetHighestSequence", "startBlock", startBlock)

	latestBlock, err := s.em.BlockByNumber(context.Background(), nil)
	if err != nil {
		return nil, 0, err
	}

	latest := latestBlock.NumberU64()
	log.Info(fmt.Sprintf("[%s] Latest block: %d", logPrefix, latest))

	// define the blocks we're going to fetch up front
	fetches := make([]fetchJob, 0)
	low := startBlock
	for {
		high := low + blockRange
		if high > latest {
			// at the end of our search
			high = latest
		}

		fetches = append(fetches, fetchJob{
			From: low,
			To:   high,
		})

		if high == latest {
			break
		}
		low += blockRange + 1
	}

	stop := make(chan bool)
	jobs := make(chan fetchJob, len(fetches))
	results := make(chan jobResult, len(fetches))

	for i := 0; i < sequencedBatchWorkers; i++ {
		go s.getSequencedLogs(jobs, results, stop)
	}

	for _, fetch := range fetches {
		jobs <- fetch
	}
	close(jobs)

	ticker := time.NewTicker(10 * time.Second)
	batchInfos := make([]types.L1BatchInfo, 0)
	var progress uint64 = 0
	aimingFor := latest - startBlock
	complete := 0
loop:
	for {
		select {
		case res := <-results:
			complete++
			if res.Error != nil {
				close(stop)
				return nil, 0, res.Error
			}
			progress += res.Size
			if len(res.Logs) > 0 {
				infos := convertResultToBatchInfo(res)
				batchInfos = append(batchInfos, infos...)
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
			log.Info(fmt.Sprintf("[%s] Progress: %d/%d (%d%%)", logPrefix, progress, aimingFor, (progress*100)/aimingFor))
		}
	}

	log.Info(fmt.Sprintf("[%s] Progress: %d/%d (%d%%)", logPrefix, aimingFor, aimingFor, 100))

	// sort by batch number lowest first
	sort.Slice(batchInfos, func(i, j int) bool {
		return batchInfos[i].BatchNo < batchInfos[j].BatchNo
	})

	return batchInfos, latest, nil
}

func convertResultToBatchInfo(res jobResult) []types.L1BatchInfo {
	infos := make([]types.L1BatchInfo, len(res.Logs))
	for i, l := range res.Logs {
		batchNumber := new(big.Int).SetBytes(l.Topics[1].Bytes())
		l1TxHash := common.BytesToHash(l.TxHash.Bytes())
		blockNumber := l.BlockNumber
		infos[i] = types.L1BatchInfo{
			BatchNo:   batchNumber.Uint64(),
			L1BlockNo: blockNumber,
			L1TxHash:  l1TxHash,
		}
	}
	return infos
}

func (s *SequencesSyncer) getSequencedLogs(jobs <-chan fetchJob, results chan jobResult, stop chan bool) {
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
				Topics:    [][]common.Hash{{sequencedBatchTopic}},
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
