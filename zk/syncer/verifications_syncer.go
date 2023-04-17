package syncer

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/time/rate"

	ethereum "github.com/ledgerwatch/erigon"
	ethTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zk/types"
)

type IEtherman interface {
	BlockByNumber(ctx context.Context, blockNumber *big.Int) (*ethTypes.Block, error)
	FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]ethTypes.Log, error)
}

type VerificationsSyncer struct {
	em                IEtherman
	l1ContractAddress common.Address
}

type Result struct {
	BatchNumber   *big.Int
	L1BlockNumber uint64
	StateRoot     common.Hash
	L1TxHash      common.Hash
}

func NewVerificationsSyncer(etherMan IEtherman, l1ContractAddr common.Address) *VerificationsSyncer {
	return &VerificationsSyncer{
		em:                etherMan,
		l1ContractAddress: l1ContractAddr,
	}
}

func (s *VerificationsSyncer) GetVerifications(logPrefix string, startBlock uint64) (verifications []types.L1BatchInfo, highestL1Block uint64, err error) {
	log.Debug("GetVerifications", "startBlock", startBlock)

	latestBlock, err := s.em.BlockByNumber(context.Background(), nil)
	if err != nil {
		return nil, 0, err
	}
	latestL1Block := latestBlock.NumberU64()
	log.Info(fmt.Sprintf("[%s] Latest block: %d", logPrefix, latestL1Block))

	eventTopic := common.HexToHash("0xcb339b570a7f0b25afa7333371ff11192092a0aeace12b671f4c212f2815c6fe")

	numWorkers := 5
	rateLimit := rate.Limit(2)
	limiter := rate.NewLimiter(rateLimit, 1)

	// progress printer
	stateCt := latestL1Block - startBlock
	progress := make(chan uint64)
	ctDone := make(chan bool)

	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		var pc uint64
		var pct uint64

		for {
			select {
			case newPc := <-progress:
				pc += newPc
				if pc > stateCt {
					return
				}
				if stateCt > 0 {
					pct = (pc * 100) / stateCt
				}
			case <-ticker.C:
				log.Info(fmt.Sprintf("[%s] Progress: %d/%d (%d%%)", logPrefix, pc, stateCt, pct))
			case <-ctDone:
				return
			}
		}
	}()

	var wg sync.WaitGroup

	jobs := make(chan *big.Int)
	results := make(chan Result)

	if latestL1Block-startBlock < 20000 {
		numWorkers = 1
	}

	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go worker(w, jobs, results, progress, s.em, s.l1ContractAddress, eventTopic, limiter, &wg)
	}

	verifications = []types.L1BatchInfo{}

	go func() {
		for result := range results {
			l1Info := types.L1BatchInfo{
				L1BlockNo: result.L1BlockNumber,
				BatchNo:   result.BatchNumber.Uint64(),
				L1TxHash:  common.BytesToHash(result.L1TxHash.Bytes()),
				StateRoot: common.BytesToHash(result.StateRoot.Bytes()),
			}
			verifications = append(verifications, l1Info)
		}
	}()

	for i := startBlock; i <= latestL1Block; i += 20000 {
		jobs <- big.NewInt(int64(i))
	}
	close(jobs)

	wg.Wait()
	close(results)

	close(progress)
	close(ctDone)

	sort.Slice(verifications, func(i, j int) bool {
		return verifications[i].L1BlockNo < verifications[j].L1BlockNo
	})

	if len(verifications) == 0 {
		return nil, startBlock, nil
	}

	// get the highest l1 block with verification from the slice
	highestL1Block = verifications[len(verifications)-1].L1BlockNo

	return verifications, highestL1Block, nil
}

func worker(id int, jobs <-chan *big.Int, results chan<- Result, progress chan<- uint64, client IEtherman, contractAddress common.Address, eventTopic common.Hash, limiter *rate.Limiter, wg *sync.WaitGroup) {
	defer wg.Done()

	for startBlock := range jobs {
		err := limiter.Wait(context.Background())
		if err != nil {
			log.Error("limiter error", "err", err)
		}

		endBlock := new(big.Int).Add(startBlock, big.NewInt(20000))

		query := ethereum.FilterQuery{
			FromBlock: startBlock,
			ToBlock:   endBlock,
			Addresses: []common.Address{contractAddress},
			Topics:    [][]common.Hash{{eventTopic}},
		}

		var logs []ethTypes.Log
		retryCount := 0

		for {
			logs, err = client.FilterLogs(context.Background(), query)
			if err == nil {
				break
			}
			if retryCount > 3 {
				return
			}

			retryCount++
			time.Sleep(time.Duration(retryCount*2) * time.Second)
		}

		for _, logEntry := range logs {
			batchNumber := new(big.Int).SetBytes(logEntry.Topics[1].Bytes())
			l1TxHash := common.BytesToHash(logEntry.TxHash.Bytes())
			stateRootData := logEntry.Data[:32]
			stateRoot := common.BytesToHash(stateRootData)

			res := Result{BatchNumber: batchNumber, L1BlockNumber: logEntry.BlockNumber, StateRoot: stateRoot, L1TxHash: l1TxHash}

			results <- res
		}
		progress <- 20000
	}
}
