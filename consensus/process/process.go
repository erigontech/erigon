package process

import (
	"errors"
	"fmt"
	"math/big"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

type Consensus struct {
	Server         consensus.Verifier
	innerValidate  chan *validateHeaderRequest
	*consensus.API // remote Engine
}

type validateHeaderRequest struct {
	id           uint64
	header       *types.Header
	seal         bool
	knownHeaders []*types.Header
}

const ttl = time.Minute

var (
	errEmptyHeader    = errors.New("an empty header")
	errNothingToAsk   = errors.New("nothing to ask")
	errRequestTimeout = errors.New("request timeout")
)

func NewConsensusProcess(v consensus.Verifier, config *params.ChainConfig, exit chan struct{}) *Consensus {
	c := &Consensus{
		Server:        v,
		API:           consensus.NewAPI(config),
		innerValidate: make(chan *validateHeaderRequest, 65536),
	}

	// event loop
	workers := runtime.NumCPU()

	for i := 0; i < workers; i++ {
		go func() {
		eventLoop:
			for {
				select {
				case req := <-c.API.VerifyHeaderRequests:
					if len(req.Headers) == 0 {
						c.API.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, common.Hash{}, errEmptyHeader}
						continue
					}

					if req.Deadline == nil {
						t := time.Now().Add(ttl)
						req.Deadline = &t
					}

					// copy slices and sort. had a data race with downloader
					reqHeaders := make([]ReqHeader, len(req.Headers))
					for i := range req.Headers {
						reqHeaders[i] = ReqHeader{req.Headers[i], req.Seal[i]}
					}

					SortHeadersAsc(reqHeaders)

					req.Headers = make([]*types.Header, len(reqHeaders))
					req.Seal = make([]bool, len(reqHeaders))
					for i := range reqHeaders {
						req.Headers[i] = reqHeaders[i].header
						req.Seal[i] = reqHeaders[i].seal
					}

					ancestorsReqs := make([]consensus.HeadersRequest, 0, len(req.Headers))

					tn := time.Now()
					for i, header := range req.Headers {
						if header == nil {
							c.API.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, common.Hash{}, errEmptyHeader}
							continue eventLoop
						}

						// Short circuit if the header is known
						if h := c.API.GetCachedHeader(header.HashCache(), header.Number.Uint64()); h != nil {
							c.API.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, header.HashCache(), nil}
							continue
						}

						knownParentsSlice, parentsToValidate, ancestorsReq := c.requestParentHeaders(req.ID, header, req.Headers, req.Seal[i])
						if ancestorsReq == nil {
							continue
						}

						ancestorsReqs = append(ancestorsReqs, *ancestorsReq)

						// todo send first out of given range request immediately
						err := c.verifyByRequest(req.ID, header, req.Seal[i], parentsToValidate, knownParentsSlice)
						if errors.Is(err, errNotAllParents) {
							log.Debug(fmt.Sprint("verifyByRequest errNotAllParents", req.ID, header.Number.Uint64(), len(knownParentsSlice), parentsToValidate))
							c.addVerifyHeaderRequest(req.ID, header, req.Seal[i], req.Deadline, knownParentsSlice, parentsToValidate)
						}
					}

					ancestorsReq, err := sumHeadersRequestsInRange(req.ID, req.Headers[0].Number.Uint64(), ancestorsReqs...)
					if err != nil {
						log.Error("can't request header ancestors", "reqID", req.ID, "number", req.Headers[0].Number.Uint64(), "err", err)
						continue
					}

					log.Debug(fmt.Sprint("NEEDED", ancestorsReq.ID, ancestorsReq.Number, ancestorsReq.HighestBlockNumber, ancestorsReq.HighestHash.String(), time.Since(tn)))

					c.API.HeadersRequests <- ancestorsReq

				case req := <-c.innerValidate:
					err := c.verifyByRequest(req.id, req.header, req.seal, len(req.knownHeaders), req.knownHeaders)
					if err != nil {
						log.Error("it shouldn't happened", req.id, req.header.Number.Uint64(), err)
					}

				case parentResp := <-c.API.HeaderResponses:
					if len(parentResp.Headers) > 0 {
						log.Debug(fmt.Sprint("PARENT-FROM-CLIENT", parentResp.ID, parentResp.Number, parentResp.Headers[0].Number.Uint64()))
					} else {
						log.Debug(fmt.Sprint("PARENT-FROM-CLIENT", parentResp.ID, parentResp.Number, parentResp.Err))
					}
					if parentResp.Err != nil {
						c.API.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{parentResp.ID, parentResp.Hash, parentResp.Err}

						c.API.ProcessingRequestsMu.Lock()
						delete(c.API.ProcessingRequests, parentResp.ID)
						c.API.ProcessingRequestsMu.Unlock()

						continue
					}

					c.VerifyRequestsCommonAncestor(parentResp.ID, parentResp.Headers)

				case <-exit:
					return
				}
			}
		}()
	}

	// cleanup loop
	go func() {
		for {
			select {
			case req := <-c.API.CleanupCh:
				c.cleanupRequest(req.ReqID, req.BlockNumber)

			case <-c.API.CleanupTicker.C:
				// cleanup by timeout
				c.cleanup()

			case <-exit:
				c.API.CleanupTicker.Stop()
				return
			}
		}
	}()

	return c
}

type ReqHeader struct {
	header *types.Header
	seal   bool
}

// Counting in-place sort
func SortHeadersAsc(hs []ReqHeader) {
	if len(hs) == 0 {
		return
	}

	minIdx := 0
	min := hs[minIdx].header.Number
	sorted := true
	var res int

	for i := 1; i < len(hs); i++ {
		res = hs[i].header.Number.Cmp(min)
		if res < 0 {
			min = hs[i].header.Number
			minIdx = i
		}
		if res != 0 {
			sorted = false
		}
	}

	if sorted {
		return
	}

	startIDx := 0
	if minIdx == 0 {
		startIDx = 1
	}

	var newIdx int
	diffWithMin := big.NewInt(0)
	bigI := big.NewInt(0)

	for i := startIDx; i < len(hs); i++ {
		bigI.SetInt64(int64(i))
		for diffWithMin.Sub(hs[i].header.Number, min).Cmp(bigI) != 0 {
			newIdx = int(diffWithMin.Int64())
			hs[newIdx], hs[i] = hs[i], hs[newIdx]
		}
	}
}

func (c *Consensus) cleanup() {
	now := time.Now()

	c.API.ProcessingRequestsMu.RLock()
	for reqID, reqBlocks := range c.API.ProcessingRequests {
		c.API.ProcessingRequestsMu.RUnlock()

		reqBlocks.RLock()
		for _, req := range reqBlocks.Storage {
			if req.Deadline.Before(now) {
				c.API.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, req.Header.HashCache(), errRequestTimeout}

				c.API.ProcessingRequestsMu.Lock()
				delete(c.API.ProcessingRequests, reqID)
				c.API.ProcessingRequestsMu.Unlock()
				break
			}
		}

		reqBlocks.RUnlock()
		c.API.ProcessingRequestsMu.RLock()
	}
	c.API.ProcessingRequestsMu.RUnlock()
}

func (c *Consensus) VerifyRequestsCommonAncestor(reqID uint64, headers []*types.Header) {
	if len(headers) == 0 {
		return
	}

	c.API.ProcessingRequestsMu.RLock()
	reqHeaders, ok := c.API.ProcessingRequests[reqID]
	c.API.ProcessingRequestsMu.RUnlock()
	if !ok {
		return
	}

	reqHeaders.RLock()
	nums := make([]uint64, 0, reqHeaders.Len())
	for num := range reqHeaders.Storage {
		nums = append(nums, num)
	}
	reqHeaders.RUnlock()

	sort.Slice(nums, func(i, j int) bool {
		return nums[i] < nums[j]
	})

	for _, header := range headers {
		c.API.CacheHeader(header)
	}

	knownByRequests := make(map[uint64]map[common.Hash]map[uint64]struct{}) // reqID -> parenthash -> blockToValidate

	for _, num := range nums {
		req, ok := reqHeaders.Get(num)
		if !ok {
			continue
		}

		log.Debug(fmt.Sprint("VerifyRequestsCommonAncestor-XXX-0", reqID, num, len(req.KnownParents)))

		appendAncestors(req, headers, knownByRequests)

		headers = append(headers, req.Header) // todo maybe it's inefficient
	}

	if len(nums) == 1 {
		req, _ := reqHeaders.Get(nums[0])

		log.Debug(fmt.Sprint("VerifyRequestsCommonAncestor-XXX-1", reqID, req.Header.Number.Uint64(), req.KnownParents[0].Number.Uint64(), len(req.KnownParents)))

		_ = c.verifyByRequest(reqID, req.Header, req.Seal, req.ParentsExpected, req.KnownParents)
	} else {
		idx := new(uint32)

		workers := runtime.NumCPU()
		if workers > len(nums) {
			workers = len(nums)
		}

		var wg sync.WaitGroup

		for i := 0; i < workers; i++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for {
					idx := atomic.AddUint32(idx, 1) - 1
					if int(idx) > len(nums)-1 {
						return
					}

					num := nums[idx]

					req, ok := reqHeaders.Get(num)
					if !ok {
						continue
					}

					_ = c.verifyByRequest(reqID, req.Header, req.Seal, req.ParentsExpected, req.KnownParents)
				}
			}()
		}

		wg.Wait()
	}
}

func (c *Consensus) verifyByRequest(reqID uint64, header *types.Header, seal bool, parentsExpected int, knownParents []*types.Header) error {
	if len(knownParents) != parentsExpected {
		return errNotAllParents
	}

	log.Debug(fmt.Sprint("verify", reqID, header.Number.Uint64(), len(knownParents)))
	t := time.Now()
	err := c.Server.Verify(c.API.Chain, header, knownParents, false, seal)
	if err == nil {
		c.API.CacheHeader(header)
	}
	log.Debug(fmt.Sprint("in verifyByRequest-1", reqID, header.Number.Uint64(), time.Since(t)))

	c.API.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, header.HashCache(), err}

	if c.inProcessing(reqID, header.Number.Uint64()) {
		// remove finished request
		finishedRequest := consensus.FinishedRequest{reqID, header.Number.Uint64()}

		select {
		case c.CleanupCh <- finishedRequest:
		default:
			c.cleanupRequest(finishedRequest.ReqID, finishedRequest.BlockNumber)
		}
	}

	return nil
}

// remove finished request
func (c *Consensus) cleanupRequest(reqID uint64, number uint64) {
	if !c.inProcessing(reqID, number) {
		return
	}

	c.API.ProcessingRequestsMu.Lock()
	reqBlocks, ok := c.API.ProcessingRequests[reqID]
	c.API.ProcessingRequestsMu.Unlock()

	if ok {
		_, ok = reqBlocks.Get(number)
		if ok {
			reqBlocks.Remove(number)
		} else {
			return
		}

		if reqBlocks.Len() == 0 {
			c.API.ProcessingRequestsMu.Lock()
			delete(c.API.ProcessingRequests, reqID)
			c.API.ProcessingRequestsMu.Unlock()
		}
	}
}

func toVerifyRequest(reqID uint64, header *types.Header, seal bool, deadline *time.Time, knownParents []*types.Header, parentsToValidate int) *consensus.VerifyRequest {
	return &consensus.VerifyRequest{
		reqID,
		header,
		seal,
		deadline,
		knownParents,
		parentsToValidate,
		header.Number.Uint64() - uint64(parentsToValidate),
		header.Number.Uint64() - uint64(len(knownParents)) - 1,
	}
}

func (c *Consensus) addVerifyHeaderRequest(reqID uint64, header *types.Header, seal bool, deadline *time.Time, knownParentsSlice []*types.Header, parentsToValidate int) {
	c.API.ProcessingRequestsMu.RLock()
	blocks, ok := c.API.ProcessingRequests[reqID]
	c.API.ProcessingRequestsMu.RUnlock()

	if !ok {
		c.API.ProcessingRequestsMu.Lock()
		blocks = consensus.NewRequestStorage()
		c.API.ProcessingRequests[reqID] = blocks
		c.API.ProcessingRequestsMu.Unlock()
	}

	knownStr := "addVerifyHeaderRequest-known: "
	for _, p := range knownParentsSlice {
		knownStr += fmt.Sprintf("%d ", p.Number.Uint64())
	}
	log.Debug(knownStr)

	blocks.Add(header.Number.Uint64(), toVerifyRequest(reqID, header, seal, deadline, knownParentsSlice, parentsToValidate))
}

func appendAncestors(request *consensus.VerifyRequest, ancestors []*types.Header, knownByRequests map[uint64]map[common.Hash]map[uint64]struct{}) {
	blockNumber := request.Header.Number.Uint64()

	ancestorsMap, ok := knownByRequests[request.ID]
	if !ok {
		ancestorsMap = make(map[common.Hash]map[uint64]struct{}, len(request.KnownParents)+len(ancestors))
		for _, p := range request.KnownParents {
			ancestorsMap[p.HashCache()] = map[uint64]struct{}{
				blockNumber: {},
			}
		}
		knownByRequests[request.ID] = ancestorsMap
	}

	for _, parent := range ancestors {
		if parent.Number.Uint64() >= request.From && parent.Number.Uint64() <= request.To {
			parentMap, has := ancestorsMap[parent.HashCache()]
			if !has {
				ancestorsMap[parent.HashCache()] = map[uint64]struct{}{
					blockNumber: {},
				}

				request.KnownParents = append(request.KnownParents, parent)
			} else {
				_, has = parentMap[blockNumber]
				if !has {
					ancestorsMap[parent.HashCache()] = map[uint64]struct{}{
						blockNumber: {},
					}
					request.KnownParents = append(request.KnownParents, parent)
				}
			}
		}
	}
}

func (c *Consensus) HeaderVerification() chan<- consensus.VerifyHeaderRequest {
	return c.API.VerifyHeaderRequests
}

func (c *Consensus) requestParentHeaders(reqID uint64, header *types.Header, reqHeaders []*types.Header, seal bool) ([]*types.Header, int, *consensus.HeadersRequest) {
	parentsToValidate := c.Server.AncestorsNeededForVerification(header)
	if parentsToValidate == 0 {
		return nil, 0, nil
	}

	headerNumber := header.Number.Uint64()
	headerParentHash := header.ParentHash

	from := reqHeaders[0].Number.Uint64()
	to := reqHeaders[len(reqHeaders)-1].Number.Uint64()

	log.Debug(fmt.Sprint("FROM", from, "TO", to))

	headerIdx := int(headerNumber - from)
	if parentsToValidate < headerIdx {
		log.Debug(fmt.Sprint("c.innerValidate <-", reqID, header.Number, reqHeaders[headerIdx].Number.Uint64(), parentsToValidate, headerIdx, from))
		c.innerValidate <- &validateHeaderRequest{reqID, header, seal, reqHeaders[headerIdx-parentsToValidate : headerIdx]}
		return nil, 0, nil
	}

	parentsToAsk := parentsToValidate

	// don't ask for already requested for verification blocks
	if header.Number.Uint64() > from && header.Number.Uint64() <= to {
		if header.Number.Uint64() >= from+uint64(parentsToValidate) {
			// we're inside the requested range
			parentsToAsk = 0
		} else {
			parentsToAsk = int(int64(from) - (header.Number.Int64() - int64(parentsToAsk)))
		}
	}

	if parentsToAsk > 0 {
		headerNumber = from - 1
		headerParentHash = reqHeaders[0].ParentHash
	}

	knownParents, ancestorsReq := c.requestHeadersNotFromRange(reqID, headerNumber, headerParentHash, uint64(parentsToAsk))
	knownParentsFromRange := c.checkHeadersFromRange(header, reqHeaders, uint64(parentsToAsk), uint64(parentsToValidate))

	if len(knownParentsFromRange) > 0 {
		log.Debug(fmt.Sprintf("request %d\n\trequestHeadersNotFromRange %d %d\n\tcheckHeadersFromRange %d(%d-%d)\n",
			header.Number.Uint64(),
			ancestorsReq.HighestBlockNumber, ancestorsReq.Number,
			len(knownParentsFromRange),
			knownParentsFromRange[0].Number.Uint64(), knownParentsFromRange[len(knownParentsFromRange)-1].Number.Uint64()))
	} else {
		log.Debug(fmt.Sprintf("request %d\n\trequestHeadersNotFromRange %d %d\n\tcheckHeadersFromRange %d\n",
			header.Number.Uint64(),
			ancestorsReq.HighestBlockNumber, ancestorsReq.Number,
			len(knownParentsFromRange)))
	}

	knownParents = append(knownParents, knownParentsFromRange...)

	return knownParents, parentsToValidate, ancestorsReq
}

var errNotAllParents = errors.New("not all parents are gathered")

func (c *Consensus) requestHeadersNotFromRange(reqID uint64, highestBlock uint64, highestKnown common.Hash, parentsToGet uint64) ([]*types.Header, *consensus.HeadersRequest) {
	highestParentHash := highestKnown
	highestParentNumber := highestBlock

	var minHeader uint64
	if highestBlock+1 > parentsToGet {
		minHeader = highestBlock + 1 - parentsToGet
	}

	known := make([]*types.Header, 0, parentsToGet+1)

	for parentBlockNum := highestBlock; parentBlockNum >= minHeader; parentBlockNum-- {
		parentBlock := c.API.GetCachedHeader(highestKnown, parentBlockNum)
		if parentBlock == nil {
			break
		}

		highestKnown = parentBlock.ParentHash

		known = append(known, parentBlock)

		if highestParentNumber < parentBlock.Number.Uint64() {
			highestParentNumber = parentBlock.Number.Uint64()
			highestParentHash = parentBlock.HashCache()
		}
	}

	return known, &consensus.HeadersRequest{
		reqID,
		highestParentHash,
		highestParentNumber,
		parentsToGet - uint64(len(known)),
	}
}

func sumHeadersRequestsInRange(reqID uint64, from uint64, reqs ...consensus.HeadersRequest) (consensus.HeadersRequest, error) {
	if len(reqs) == 0 {
		return consensus.HeadersRequest{}, errNothingToAsk
	}

	maxBlockNumber := reqs[0].HighestBlockNumber
	maxBlockHash := reqs[0].HighestHash
	minBlockToGet := maxBlockNumber - reqs[0].Number + 1

	for _, req := range reqs {
		if req.ID != reqID {
			continue
		}

		if req.Number == 0 {
			continue
		}

		if req.HighestBlockNumber > maxBlockNumber && req.HighestBlockNumber < from {
			maxBlockNumber = req.HighestBlockNumber
			maxBlockHash = req.HighestHash
		}

		if req.HighestBlockNumber-req.Number+1 < minBlockToGet {
			minBlockToGet = req.HighestBlockNumber - req.Number + 1
		}
	}

	return consensus.HeadersRequest{
		reqID,
		maxBlockHash,
		maxBlockNumber,
		maxBlockNumber - minBlockToGet + 1,
	}, nil
}

func (c *Consensus) checkHeadersFromRange(highestHeader *types.Header, requestedHeaders []*types.Header, parentsToGet, parentsToValidate uint64) []*types.Header {
	parentsToGet = parentsToValidate - parentsToGet
	if parentsToGet <= 0 {
		return nil
	}

	idx := sort.Search(len(requestedHeaders), func(i int) bool {
		return requestedHeaders[i].Number.Cmp(highestHeader.Number) >= 0
	})

	if idx >= len(requestedHeaders) || requestedHeaders[idx].Number.Cmp(highestHeader.Number) != 0 {
		return nil
	}

	if idx-int(parentsToGet) < 0 {
		return nil
	}

	return requestedHeaders[idx-int(parentsToGet) : idx]
}

func (c *Consensus) VerifyResults() <-chan consensus.VerifyHeaderResponse {
	return c.API.VerifyHeaderResponses
}

func (c *Consensus) inProcessing(reqID uint64, number uint64) bool {
	c.API.ProcessingRequestsMu.RLock()
	reqBlocks, ok := c.API.ProcessingRequests[reqID]
	c.API.ProcessingRequestsMu.RUnlock()
	if !ok {
		return false
	}

	reqBlocks.RLock()
	_, ok = reqBlocks.Storage[number]
	reqBlocks.RUnlock()
	return ok
}
