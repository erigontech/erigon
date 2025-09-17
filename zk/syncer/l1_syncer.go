package syncer

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	ethereum "github.com/erigontech/erigon"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/iden3/go-iden3-crypto/keccak256"
	"golang.org/x/sync/singleflight"

	"encoding/binary"

	ethTypes "github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/rpc"
)

const (
	batchWorkers    = 4
	getLogsMaxRetry = 20
	logsChannelSize = 100
)

var errorShortResponseLT32 = fmt.Errorf("response too short to contain hash data")
var errorShortResponseLT96 = fmt.Errorf("response too short to contain last batch number data")

var L1FetchHeaderRetryDelay = time.Duration(1 * time.Second)

const (
	rollupSequencedBatchesSignature = "0x25280169" // hardcoded abi signature
	globalExitRootManager           = "0xd02103ca"
	rollupManager                   = "0x49b7b802"
	admin                           = "0xf851a440"
	trustedSequencer                = "0xcfa8ed47"
	sequencedBatchesMapSignature    = "0xb4d63f58"
)

//go:generate mockgen -typed=true -destination=./mocks/etherman_mock.go -package=mocks . IEtherman

type IEtherman interface {
	HeaderByNumber(ctx context.Context, blockNumber *big.Int) (*ethTypes.Header, error)
	BlockByNumber(ctx context.Context, blockNumber *big.Int) (*ethTypes.Block, error)
	FilterLogs(ctx context.Context, query ethereum.FilterQuery) ([]ethTypes.Log, error)
	CallContract(ctx context.Context, msg ethereum.CallMsg, blockNumber *big.Int) ([]byte, error)
	TransactionByHash(ctx context.Context, hash common.Hash) (ethTypes.Transaction, bool, error)
	TransactionReceipt(ctx context.Context, txHash common.Hash) (*ethTypes.Receipt, error)
	StorageAt(ctx context.Context, account common.Address, key common.Hash, blockNumber *big.Int) ([]byte, error)
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

type LogEvent struct {
	Logs     []ethTypes.Log
	Progress uint64 // set only on Done
	Done     bool
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

	firstL1Block  uint64
	latestL1Block uint64

	// atomic
	isSyncStarted      atomic.Bool
	isDownloading      atomic.Bool
	lastCheckedL1Block atomic.Uint64
	wgRunLoopDone      sync.WaitGroup
	flagStop           atomic.Bool

	// Channels
	logsConsumChan   chan LogEvent
	logsProduceChan  chan LogEvent
	logsSwapMutex    sync.Mutex
	logsChanProgress chan string
	closeLogsChannel atomic.Bool

	highestBlockType string                                   // finalized, latest, safe
	headersCache     *expirable.LRU[uint64, *ethTypes.Header] // cache for headers
	sfGroup          singleflight.Group
	fetchHeaders     bool
}

func NewL1Syncer(ctx context.Context, etherMans []IEtherman, l1ContractAddresses []common.Address, topics [][]common.Hash, blockRange, queryDelay uint64, highestBlockType string, firstL1Block uint64) *L1Syncer {
	headersCache := expirable.NewLRU[uint64, *ethTypes.Header](int(blockRange), nil, time.Minute*10)
	return &L1Syncer{
		ctx:                 ctx,
		etherMans:           etherMans,
		ethermanIndex:       0,
		ethermanMtx:         &sync.Mutex{},
		l1ContractAddresses: l1ContractAddresses,
		topics:              topics,
		blockRange:          blockRange,
		queryDelay:          queryDelay,
		logsProduceChan:     make(chan LogEvent, logsChannelSize),
		logsChanProgress:    make(chan string, logsChannelSize),
		highestBlockType:    highestBlockType,
		firstL1Block:        firstL1Block,
		headersCache:        headersCache,
	}
}

func (s *L1Syncer) getNextEtherman() IEtherman {
	s.ethermanMtx.Lock()
	defer s.ethermanMtx.Unlock()

	// Use modulo to ensure the index wraps around automatically
	etherman := s.etherMans[s.ethermanIndex%uint8(len(s.etherMans))]
	s.ethermanIndex++

	return etherman
}

func (s *L1Syncer) IsSyncStarted() bool {
	return s.isSyncStarted.Load()
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
		case <-s.logsProduceChan:
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
func (s *L1Syncer) GetLogsChan() <-chan LogEvent {
	s.logsSwapMutex.Lock()
	defer s.logsSwapMutex.Unlock()

	s.logsConsumChan = s.logsProduceChan

	// If not downloading, grab existing data and close the channel
	if !s.isDownloading.Load() {
		s.closeLogsChannel.Store(true)
	}
	return s.logsConsumChan
}

func (s *L1Syncer) GetProgressMessageChan() <-chan string {
	return s.logsChanProgress
}

func (s *L1Syncer) RunQueryBlocks(from uint64) {
	//if already started, don't start another thread
	if !s.isSyncStarted.CompareAndSwap(false, true) {
		return
	}

	// set it to true to catch the first cycle run case where the check can pass before the latest block is checked
	s.lastCheckedL1Block.Store(from)

	s.wgRunLoopDone.Add(1)
	s.flagStop.Store(false)

	//start a thread to check for new l1 block in interval
	go func() {
		defer s.isSyncStarted.Store(false)
		defer s.wgRunLoopDone.Done()

		log.Info("Starting L1 syncer thread")
		defer log.Info("Stopping L1 syncer thread")

		ticker := time.NewTicker(time.Duration(s.queryDelay) * time.Millisecond)
		defer ticker.Stop()

		sleepTicker := time.NewTicker(10 * time.Millisecond)
		defer sleepTicker.Stop()

		for {
			if s.flagStop.Load() {
				s.resetChannels()
				return
			}

			select {
			case <-s.ctx.Done():
				s.resetChannels()
				return
			case <-ticker.C:
				latestL1Block, err := s.getLatestL1Block()
				if err != nil {
					log.Error("Error getting latest L1 block", "err", err)
				} else {
					if latestL1Block > s.lastCheckedL1Block.Load() {
						// It should not be checked again in the new cycle, so +1 is added here.
						// Fixed receiving duplicate log events.
						// lastCheckedL1Block means that it has already been checked in the previous cycle.
						startBlock := s.lastCheckedL1Block.Load() + 1
						endBlock := latestL1Block
						if _, err = s.queryBlocks(startBlock, endBlock); err != nil {
							log.Error("Error querying blocks", "err", err)
						} else {
							s.lastCheckedL1Block.Store(latestL1Block)
						}
					}
				}
				s.resetChannels()
			case <-sleepTicker.C:
				if s.closeLogsChannel.Load() && !s.isDownloading.Load() {
					s.resetChannels()
				}
			}
		}
	}()
}

func (s *L1Syncer) GetHeader(number uint64) (*ethTypes.Header, error) {
	em := s.getNextEtherman()
	return s.getHeader(em, number)
}

func (s *L1Syncer) getHeader(em IEtherman, number uint64) (*ethTypes.Header, error) {
	if header, ok := s.headersCache.Get(number); ok {
		return header, nil
	}

	// Deduplicate concurrent requests
	v, err, _ := s.sfGroup.Do(fmt.Sprintf("header-%d", number), func() (any, error) {
		header, err := em.HeaderByNumber(s.ctx, new(big.Int).SetUint64(number))
		if err != nil {
			return nil, err
		}
		s.headersCache.Add(number, header)
		return header, nil
	})

	if err != nil {
		return nil, err
	}
	return v.(*ethTypes.Header), nil
}

func (s *L1Syncer) GetBlock(number uint64) (*ethTypes.Block, error) {
	em := s.getNextEtherman()
	return em.BlockByNumber(s.ctx, new(big.Int).SetUint64(number))
}

func (s *L1Syncer) GetTransaction(hash common.Hash) (ethTypes.Transaction, bool, error) {
	em := s.getNextEtherman()
	return em.TransactionByHash(s.ctx, hash)
}

func (s *L1Syncer) GetPreElderberryAccInputHash(ctx context.Context, addr *common.Address, batchNum uint64) (common.Hash, error) {
	h, err := s.callSequencedBatchesMap(ctx, addr, batchNum)
	if err != nil {
		return common.Hash{}, err
	}

	return h, nil
}

// returns accInputHash only if the batch matches the last batch in sequence
// on Etrrof the rollup contract was changed so data is taken differently
func (s *L1Syncer) GetElderberryAccInputHash(ctx context.Context, addr *common.Address, rollupId, batchNum uint64) (common.Hash, error) {
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

	header, err := s.getHeader(em, r.BlockNumber.Uint64())
	if err != nil {
		return 0, err
	}

	return header.Time, nil
}

func (s *L1Syncer) l1QueryHeaders(logs []ethTypes.Log) error {
	logsSize := len(logs)

	// queue up all the logs
	logQueue := make(chan *ethTypes.Log, logsSize)
	defer close(logQueue)
	for i := 0; i < logsSize; i++ {
		logQueue <- &logs[i]
	}

	var wg sync.WaitGroup
	wg.Add(logsSize)

	process := func(em IEtherman) {
		for {
			l, ok := <-logQueue
			if !ok {
				break
			}

			if s.flagStop.Load() {
				wg.Done()
				continue
			}

			header, err := s.getHeader(em, l.BlockNumber)
			if err != nil {
				log.Debug("Error getting block", "err", err)
				// assume a transient error and try again
				time.Sleep(L1FetchHeaderRetryDelay)
				logQueue <- l
				continue
			}
			s.headersCache.Add(l.BlockNumber, header)
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

	return nil
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

	latestBlock, err := em.BlockByNumber(s.ctx, blockNumber)
	if err != nil {
		return 0, err
	}

	latest := latestBlock.NumberU64()
	s.latestL1Block = latest

	return latest, nil
}

func (s *L1Syncer) queryBlocks(startBlock, endBlock uint64) (numLogs uint64, err error) {
	log.Debug("GetHighestSequence", "startBlock", startBlock, "latestBlock", endBlock)

	s.isDownloading.Store(true)
	defer s.isDownloading.Store(false)

	// define the blocks we're going to fetch up front
	fetches := make([]fetchJob, 0, (endBlock-startBlock)/s.blockRange+1)
	low := startBlock
	for {
		high := low + s.blockRange
		if high > endBlock {
			// at the end of our search
			high = endBlock
		}

		fetches = append(fetches, fetchJob{
			From: low,
			To:   high,
		})

		if high == endBlock {
			break
		}
		low += s.blockRange + 1
	}

	wg := sync.WaitGroup{}
	stop := make(chan bool)
	jobs := make(chan fetchJob, len(fetches))
	results := make(chan jobResult, len(fetches))
	defer close(results)

	workers := min(batchWorkers, len(fetches))
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go s.getSequencedLogs(jobs, results, stop, &wg)
	}

	for _, fetch := range fetches {
		jobs <- fetch
	}
	close(jobs)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	aimingFor := endBlock - startBlock
	complete := 0
	var progress uint64 = 0
	var prevProgress uint64 = 0
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
				numLogs += uint64(len(res.Logs))

				wg.Add(1)
				logs := res.Logs
				go func(logs []ethTypes.Log) {
					defer wg.Done()

					if s.fetchHeaders {
						s.l1QueryHeaders(logs)
					}

					s.logsProduceChan <- LogEvent{Logs: logs}
				}(logs)
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
			if progress > prevProgress {
				prevProgress = progress
				s.logsProduceChan <- LogEvent{Logs: nil} // send a nil log to indicate activity in case no logs for long in big range
			}
		}
	}

	close(stop)
	wg.Wait()

	return numLogs, err
}

func (s *L1Syncer) getSequencedLogs(jobs <-chan fetchJob, results chan jobResult, stop chan bool, wg *sync.WaitGroup) {
	defer wg.Done()

	for j := range jobs {
		query := ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(j.From),
			ToBlock:   new(big.Int).SetUint64(j.To),
			Addresses: s.l1ContractAddresses,
			Topics:    s.topics,
		}

		var logs []ethTypes.Log
		var err error
		retry := 0
	LOOP:
		for {
			select {
			case <-stop:
				break LOOP
			default:
				if s.flagStop.Load() {
					break LOOP
				}

				em := s.getNextEtherman()
				logs, err = em.FilterLogs(s.ctx, query)
				if err != nil {
					log.Debug("getSequencedLogs retry error", "err", err)
					retry++
					time.Sleep(time.Duration(retry*2) * time.Second)
					continue
				}
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

// calls the old rollup contract to get the accInputHash for a certain batch
// returns the accInputHash and lastBatchNumber
func (s *L1Syncer) callSequencedBatchesMap(ctx context.Context, addr *common.Address, batchNum uint64) (accInputHash common.Hash, err error) {
	mapKeyHex := fmt.Sprintf("%064x%064x", batchNum, 114 /* _legacySequencedBatches slot*/)
	mapKey := keccak256.Hash(common.FromHex(mapKeyHex))
	mkh := common.BytesToHash(mapKey)

	em := s.getNextEtherman()

	resp, err := em.StorageAt(ctx, *addr, mkh, nil)
	if err != nil {
		return
	}

	if len(resp) < 32 {
		return
	}
	accInputHash = common.BytesToHash(resp[:32])

	return
}

// calls the rollup contract to get the accInputHash for a certain batch
// returns the accInputHash and lastBatchNumber
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

func (s *L1Syncer) QueryForRootLog(to uint64) (*ethTypes.Log, error) {
	var logs []ethTypes.Log
	var err error
	retry := 0
	for {
		em := s.getNextEtherman()
		query := ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(s.firstL1Block),
			ToBlock:   new(big.Int).SetUint64(to),
			Addresses: s.l1ContractAddresses,
			Topics:    s.topics,
		}
		logs, err = em.FilterLogs(s.ctx, query)
		if err != nil {
			log.Debug("QueryForRootLog retry error", "err", err)
			retry++
			if retry > 5 {
				return nil, err
			}
			time.Sleep(time.Duration(retry*2) * time.Second)
			continue
		}
		break
	}

	if len(logs) != 2 {
		// There should only be 2 logs, the root log and the log from the to block
		// this is called from index 1 on the info tree so we need the root to make index 0
		return nil, fmt.Errorf("did not find the expected number of logs")
	}

	return &logs[0], nil
}

func (s *L1Syncer) SetFetchHeaders(fetchHeaders bool) {
	s.fetchHeaders = fetchHeaders
}

func (s *L1Syncer) resetChannels() {
	s.logsSwapMutex.Lock()
	defer s.logsSwapMutex.Unlock()
	if s.logsConsumChan == s.logsProduceChan {
		s.logsConsumChan <- LogEvent{Logs: nil, Done: true, Progress: s.lastCheckedL1Block.Load()} // send a nil log to indicate activity in case no logs for long in big range
		close(s.logsConsumChan)
		s.logsProduceChan = make(chan LogEvent, logsChannelSize)
	}
	s.closeLogsChannel.Store(false)
}
