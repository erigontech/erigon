package syncer

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	ethereum "github.com/ledgerwatch/erigon"
	"github.com/ledgerwatch/log/v3"

	"encoding/binary"

	ethTypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rpc"
)

var (
	batchWorkers = 2
)

var errorShortResponseLT32 = fmt.Errorf("response too short to contain hash data")
var errorShortResponseLT96 = fmt.Errorf("response too short to contain last batch number data")

const rollupSequencedBatchesSignature = "0x25280169" // hardcoded abi signature
const globalExitRootManager = "0xd02103ca"
const rollupManager = "0x49b7b802"
const admin = "0xf851a440"
const trustedSequencer = "0xcfa8ed47"

type IEtherman interface {
	HeaderByNumber(ctx context.Context, blockNumber *big.Int) (*ethTypes.Header, error)
	BlockByNumber(ctx context.Context, blockNumber *big.Int) (*ethTypes.Block, error)
	FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]ethTypes.Log, error)
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
	TransactionByHash(ctx context.Context, hash common.Hash) (ethTypes.Transaction, bool, error)
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*ethTypes.Receipt, error)
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
	ctx                 context.Context
	etherMans           []IEtherman
	ethermanIndex       uint8
	ethermanMtx         *sync.Mutex
	l1ContractAddresses []common.Address
	topics              [][]common.Hash
	blockRange          uint64
	queryDelay          uint64

	latestL1Block uint64

	// atomic
	isSyncStarted      atomic.Bool
	isDownloading      atomic.Bool
	lastCheckedL1Block atomic.Uint64
	wgRunLoopDone      sync.WaitGroup
	flagStop           atomic.Bool

	// Channels
	logsChan         chan []ethTypes.Log
	logsChanProgress chan string

	highestBlockType string // finalized, latest, safe
}

func NewL1Syncer(ctx context.Context, etherMans []IEtherman, l1ContractAddresses []common.Address, topics [][]common.Hash, blockRange, queryDelay uint64, highestBlockType string) *L1Syncer {
	return &L1Syncer{
		ctx:                 ctx,
		etherMans:           etherMans,
		ethermanIndex:       0,
		ethermanMtx:         &sync.Mutex{},
		l1ContractAddresses: l1ContractAddresses,
		topics:              topics,
		blockRange:          blockRange,
		queryDelay:          queryDelay,
		logsChan:            make(chan []ethTypes.Log),
		logsChanProgress:    make(chan string),
		highestBlockType:    highestBlockType,
	}
}

func (s *L1Syncer) getNextEtherman() IEtherman {
	s.ethermanMtx.Lock()
	defer s.ethermanMtx.Unlock()

	if s.ethermanIndex >= uint8(len(s.etherMans)) {
		s.ethermanIndex = 0
	}

	etherman := s.etherMans[s.ethermanIndex]
	s.ethermanIndex++

	return etherman
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

func (s *L1Syncer) StopQueryBlocks() {
	s.flagStop.Store(true)
}

func (s *L1Syncer) ConsumeQueryBlocks() {
	for {
		select {
		case <-s.logsChan:
		case <-s.logsChanProgress:
		default:
			if !s.isSyncStarted.Load() {
				return
			}
			time.Sleep(time.Second)
		}
	}
}

func (s *L1Syncer) WaitQueryBlocksToFinish() {
	s.wgRunLoopDone.Wait()
}

// Channels
func (s *L1Syncer) GetLogsChan() chan []ethTypes.Log {
	return s.logsChan
}

func (s *L1Syncer) GetProgressMessageChan() chan string {
	return s.logsChanProgress
}

func (s *L1Syncer) RunQueryBlocks(lastCheckedBlock uint64) {
	//if already started, don't start another thread
	if s.isSyncStarted.Load() {
		return
	}

	s.isSyncStarted.Store(true)

	// set it to true to catch the first cycle run case where the check can pass before the latest block is checked
	s.isDownloading.Store(true)
	s.lastCheckedL1Block.Store(lastCheckedBlock)

	s.wgRunLoopDone.Add(1)
	s.flagStop.Store(false)

	//start a thread to cheack for new l1 block in interval
	go func() {
		defer s.isSyncStarted.Store(false)
		defer s.wgRunLoopDone.Done()

		log.Info("Starting L1 syncer thread")
		defer log.Info("Stopping L1 syncer thread")

		for {
			if s.flagStop.Load() {
				return
			}

			latestL1Block, err := s.getLatestL1Block()
			if err != nil {
				log.Error("Error getting latest L1 block", "err", err)
			} else {
				if latestL1Block > s.lastCheckedL1Block.Load() {
					s.isDownloading.Store(true)
					if err := s.queryBlocks(); err != nil {
						log.Error("Error querying blocks", "err", err)
					} else {
						s.lastCheckedL1Block.Store(latestL1Block)
					}
				}
			}

			s.isDownloading.Store(false)
			time.Sleep(time.Duration(s.queryDelay) * time.Millisecond)
		}
	}()
}

func (s *L1Syncer) GetHeader(number uint64) (*ethTypes.Header, error) {
	em := s.getNextEtherman()
	return em.HeaderByNumber(context.Background(), new(big.Int).SetUint64(number))
}

func (s *L1Syncer) GetBlock(number uint64) (*ethTypes.Block, error) {
	em := s.getNextEtherman()
	return em.BlockByNumber(context.Background(), new(big.Int).SetUint64(number))
}

func (s *L1Syncer) GetTransaction(hash common.Hash) (ethTypes.Transaction, bool, error) {
	em := s.getNextEtherman()
	return em.TransactionByHash(context.Background(), hash)
}

func (s *L1Syncer) GetOldAccInputHash(ctx context.Context, addr *common.Address, rollupId, batchNum uint64) (common.Hash, error) {
	h, _, err := s.callGetRollupSequencedBatches(ctx, addr, rollupId, batchNum)
	if err != nil {
		return common.Hash{}, err
	}

	return h, nil
}

func (s *L1Syncer) GetL1BlockTimeStampByTxHash(ctx context.Context, txHash common.Hash) (uint64, error) {
	em := s.getNextEtherman()
	r, err := em.TransactionReceipt(ctx, txHash)
	if err != nil {
		return 0, err
	}

	header, err := em.HeaderByNumber(context.Background(), r.BlockNumber)
	if err != nil {
		return 0, err
	}

	return header.Time, nil
}

func (s *L1Syncer) L1QueryHeaders(logs []ethTypes.Log) (map[uint64]*ethTypes.Header, error) {
	logsSize := len(logs)

	// queue up all the logs
	logQueue := make(chan *ethTypes.Log, logsSize)
	defer close(logQueue)
	for i := 0; i < logsSize; i++ {
		logQueue <- &logs[i]
	}

	var wg sync.WaitGroup
	wg.Add(logsSize)

	headersQueue := make(chan *ethTypes.Header, logsSize)

	process := func(em IEtherman) {
		ctx := context.Background()
		for {
			l, ok := <-logQueue
			if !ok {
				break
			}
			header, err := em.HeaderByNumber(ctx, new(big.Int).SetUint64(l.BlockNumber))
			if err != nil {
				log.Error("Error getting block", "err", err)
				// assume a transient error and try again
				time.Sleep(1 * time.Second)
				logQueue <- l
				continue
			}
			headersQueue <- header
			wg.Done()
		}
	}

	// launch the workers - some endpoints might be faster than others so will consume more of the queue
	// but, we really don't care about that.  We want the data as fast as possible
	mans := s.etherMans
	for i := 0; i < len(mans); i++ {
		go process(mans[i])
	}

	wg.Wait()
	close(headersQueue)

	headersMap := map[uint64]*ethTypes.Header{}
	for header := range headersQueue {
		headersMap[header.Number.Uint64()] = header
	}

	return headersMap, nil
}

func (s *L1Syncer) getLatestL1Block() (uint64, error) {
	em := s.getNextEtherman()

	var blockNumber *big.Int

	switch s.highestBlockType {
	case "finalized":
		blockNumber = big.NewInt(rpc.FinalizedBlockNumber.Int64())
	case "safe":
		blockNumber = big.NewInt(rpc.SafeBlockNumber.Int64())
	case "latest":
		blockNumber = nil
	}

	latestBlock, err := em.BlockByNumber(context.Background(), blockNumber)
	if err != nil {
		return 0, err
	}

	latest := latestBlock.NumberU64()
	s.latestL1Block = latest

	return latest, nil
}

func (s *L1Syncer) queryBlocks() error {
	// Fixed receiving duplicate log events.
	// lastCheckedL1Block means that it has already been checked in the previous cycle.
	// It should not be checked again in the new cycle, so +1 is added here.
	startBlock := s.lastCheckedL1Block.Load() + 1

	log.Debug("GetHighestSequence", "startBlock", startBlock)

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

	wg := sync.WaitGroup{}
	stop := make(chan bool)
	jobs := make(chan fetchJob, len(fetches))
	results := make(chan jobResult, len(fetches))
	defer close(results)

	wg.Add(batchWorkers)
	for i := 0; i < batchWorkers; i++ {
		go s.getSequencedLogs(jobs, results, stop, &wg)
	}

	for _, fetch := range fetches {
		jobs <- fetch
	}
	close(jobs)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	var err error
	var progress uint64 = 0

	aimingFor := s.latestL1Block - startBlock
	complete := 0
loop:
	for {
		select {
		case <-s.ctx.Done():
			break loop
		case res := <-results:
			if s.flagStop.Load() {
				break loop
			}

			complete++
			if res.Error != nil {
				err = res.Error
				break loop
			}
			progress += res.Size
			if len(res.Logs) > 0 {
				s.logsChan <- res.Logs
			}

			if complete == len(fetches) {
				// we've got all the results we need
				break loop
			}
		case <-ticker.C:
			if aimingFor == 0 {
				continue
			}
			s.logsChanProgress <- fmt.Sprintf("L1 Blocks processed progress (amounts): %d/%d (%d%%)", progress, aimingFor, (progress*100)/aimingFor)
		}
	}

	close(stop)
	wg.Wait()

	return err
}

func (s *L1Syncer) getSequencedLogs(jobs <-chan fetchJob, results chan jobResult, stop chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-stop:
			return
		case j, ok := <-jobs:
			if !ok {
				return
			}
			query := ethereum.FilterQuery{
				FromBlock: new(big.Int).SetUint64(j.From),
				ToBlock:   new(big.Int).SetUint64(j.To),
				Addresses: s.l1ContractAddresses,
				Topics:    s.topics,
			}

			var logs []ethTypes.Log
			var err error
			retry := 0
			for {
				em := s.getNextEtherman()
				logs, err = em.FilterLogs(context.Background(), query)
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

func (s *L1Syncer) callGetRollupSequencedBatches(ctx context.Context, addr *common.Address, rollupId, batchNum uint64) (common.Hash, uint64, error) {
	rollupID := fmt.Sprintf("%064x", rollupId)
	batchNumber := fmt.Sprintf("%064x", batchNum)

	em := s.getNextEtherman()
	resp, err := em.CallContract(ctx, ethereum.CallMsg{
		To:   addr,
		Data: common.FromHex(rollupSequencedBatchesSignature + rollupID + batchNumber),
	}, nil)

	if err != nil {
		return common.Hash{}, 0, err
	}

	if len(resp) < 32 {
		return common.Hash{}, 0, errorShortResponseLT32
	}
	h := common.BytesToHash(resp[:32])

	if len(resp) < 96 {
		return common.Hash{}, 0, errorShortResponseLT96
	}
	lastBatchNumber := binary.BigEndian.Uint64(resp[88:96])

	return h, lastBatchNumber, nil
}

func (s *L1Syncer) CallAdmin(ctx context.Context, addr *common.Address) (common.Address, error) {
	return s.callGetAddress(ctx, addr, admin)
}

func (s *L1Syncer) CallRollupManager(ctx context.Context, addr *common.Address) (common.Address, error) {
	return s.callGetAddress(ctx, addr, rollupManager)
}

func (s *L1Syncer) CallGlobalExitRootManager(ctx context.Context, addr *common.Address) (common.Address, error) {
	return s.callGetAddress(ctx, addr, globalExitRootManager)
}

func (s *L1Syncer) CallTrustedSequencer(ctx context.Context, addr *common.Address) (common.Address, error) {
	return s.callGetAddress(ctx, addr, trustedSequencer)
}

func (s *L1Syncer) callGetAddress(ctx context.Context, addr *common.Address, data string) (common.Address, error) {
	em := s.getNextEtherman()
	resp, err := em.CallContract(ctx, ethereum.CallMsg{
		To:   addr,
		Data: common.FromHex(data),
	}, nil)

	if err != nil {
		return common.Address{}, err
	}

	if len(resp) < 20 {
		return common.Address{}, errorShortResponseLT32
	}

	return common.BytesToAddress(resp[len(resp)-20:]), nil
}

func (s *L1Syncer) CheckL1BlockFinalized(blockNo uint64) (finalized bool, finalizedBn uint64, err error) {
	em := s.getNextEtherman()
	block, err := em.BlockByNumber(s.ctx, big.NewInt(rpc.FinalizedBlockNumber.Int64()))
	if err != nil {
		return false, 0, err
	}

	return block.NumberU64() >= blockNo, block.NumberU64(), nil
}
