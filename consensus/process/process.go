package process

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
)

type Consensus struct {
	Server         consensus.Verifier
	*consensus.API // remote Engine
}

const ttl = time.Minute

var (
	errEmptyHeader  = errors.New("an empty header")
	errNothingToAsk = errors.New("nothing to ask")
)

func NewConsensusProcess(v consensus.Verifier, config *params.ChainConfig, exit chan struct{}) *Consensus {
	c := &Consensus{
		Server: v,
		API:    consensus.NewAPI(config),
	}

	// event loop
	go func() {
	eventLoop:
		for {
			select {
			case req := <-c.API.VerifyHeaderRequests:
				t := time.Now()

				if len(req.Headers) == 0 {
					c.API.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, common.Hash{}, errEmptyHeader}
					continue
				}

				if req.Deadline == nil {
					t := time.Now().Add(ttl)
					req.Deadline = &t
				}

				// copy slices and sort. had a data race with downloader
				reqHeaders := make([]reqHeader, len(req.Headers))
				for i := range req.Headers {
					reqHeaders[i] = reqHeader{req.Headers[i], req.Seal[i]}
				}

				sort.Slice(reqHeaders, func(i, j int) bool {
					return reqHeaders[i].header.Number.Cmp(reqHeaders[j].header.Number) == -1
				})

				req.Headers = make([]*types.Header, len(reqHeaders))
				req.Seal = make([]bool, len(reqHeaders))
				for i := range reqHeaders {
					req.Headers[i] = reqHeaders[i].header
					req.Seal[i] = reqHeaders[i].seal
				}

				ancestorsReqs := make([]consensus.HeadersRequest, 0, len(req.Headers))

				fmt.Println("VerifyHeaderRequests-1.Prepare", req.ID, time.Since(t))

				var totalVerify time.Duration
				totalT := time.Now()
				for i, header := range req.Headers {
					t1 := time.Now()
					if header == nil {
						c.API.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, common.Hash{}, errEmptyHeader}
						continue eventLoop
					}

					t2 := time.Now()
					// Short circuit if the header is known
					if h := c.API.GetCachedHeader(header.HashCache(), header.Number.Uint64()); h != nil {
						c.API.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, header.HashCache(), nil}
						continue
					}
					fmt.Println("verify-loop-1", time.Since(t2))
					t2 = time.Now()

					knownParentsSlice, parentsToValidate, ancestorsReq := c.requestParentHeaders(req.ID, header, req.Headers)
					if ancestorsReq != nil {
						ancestorsReqs = append(ancestorsReqs, *ancestorsReq)
					}
					fmt.Println("verify-loop-2", time.Since(t2))
					t2 = time.Now()

					err := c.verifyByRequest(req.ID, header, req.Seal[i], parentsToValidate, knownParentsSlice)
					totalVerify += time.Since(t2)
					fmt.Println("verify-loop-3", time.Since(t2))
					t2 = time.Now()
					if errors.Is(err, errNotAllParents) {
						c.addVerifyHeaderRequest(req.ID, header, req.Seal[i], req.Deadline, knownParentsSlice, parentsToValidate)
						fmt.Println("verify-loop-4", time.Since(t2))
						t2 = time.Now()
					}
					fmt.Println("verify-loop-total", time.Since(t1))
				}

				totalVerifyLoop := time.Since(totalT)
				fmt.Println("verify-loop-RESULT", totalVerifyLoop, totalVerify, totalVerifyLoop-totalVerify)

				tSecond := time.Now()
				t1 := time.Now()
				ancestorsReq, err := sumHeadersRequestsInRange(req.ID, req.Headers[0].Number.Uint64(), ancestorsReqs...)
				if err != nil {
					log.Error("can't request header ancestors", "reqID", req.ID, "number", req.Headers[0].Number.Uint64(), "err", err)
					continue
				}
				fmt.Println("VerifyHeaderRequests-2.sumHeadersRequestsInRange", req.ID, time.Since(t1))

				fmt.Println("VerifyHeaderRequests-3", req.ID, time.Since(t))
				c.API.HeadersRequests <- ancestorsReq
				fmt.Println("verify-loop-total-second-part", time.Since(tSecond))
				fmt.Printf("\n*********************************************************\n\n")

			case parentResp := <-c.API.HeaderResponses:
				t := time.Now()
				if parentResp.Err != nil {
					c.API.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{parentResp.ID, parentResp.Hash, parentResp.Err}

					c.API.ProcessingRequestsMu.Lock()
					delete(c.API.ProcessingRequests, parentResp.ID)
					c.API.ProcessingRequestsMu.Unlock()

					fmt.Println("HeaderResponses-1.VerifyHeaderRequests-Err", parentResp.ID, time.Since(t))
					continue
				}

				c.VerifyRequestsCommonAncestor(parentResp.ID, parentResp.Headers)

				fmt.Println("HeaderResponses-2.VerifyHeaderRequests", parentResp.ID, time.Since(t))

			// cleanup by timeout
			case <-c.API.CleanupTicker.C:
				//fixme debug
				continue

				t := time.Now()
				c.cleanup()
				fmt.Println("cleanup", time.Since(t))

			case <-exit:
				return
			}
		}
	}()

	// cleanup loop
	go func() {
		for {
			select {
			case req := <-c.API.CleanupCh:
				//fixme: debug
				continue

				t := time.Now()
				c.cleanupRequest(req.ReqID, req.BlockNumber)
				fmt.Println("cleanupRequest", req.ReqID, time.Since(t))
			case <-exit:
				return
			}
		}
	}()

	return c
}

type reqHeader struct {
	header *types.Header
	seal   bool
}

func (c *Consensus) cleanup() {
	now := time.Now()

	c.API.ProcessingRequestsMu.Lock()

	for reqID, reqBlocks := range c.API.ProcessingRequests {
		for _, req := range reqBlocks {
			if req.Deadline.Before(now) {
				c.API.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, req.Header.HashCache(), errors.New("timeout")}

				delete(c.API.ProcessingRequests, reqID)
			}
		}
	}

	c.API.ProcessingRequestsMu.Unlock()
}

func (c *Consensus) VerifyRequestsCommonAncestor(reqID uint64, headers []*types.Header) {
	t := time.Now()

	if len(headers) == 0 {
		return
	}

	c.API.ProcessingRequestsMu.Lock()
	reqHeaders, ok := c.API.ProcessingRequests[reqID]
	if !ok {
		c.API.ProcessingRequestsMu.Unlock()
		return
	}

	nums := make([]uint64, 0, len(reqHeaders))
	for num := range reqHeaders {
		nums = append(nums, num)
	}
	c.API.ProcessingRequestsMu.Unlock()

	sort.Slice(nums, func(i, j int) bool {
		return nums[i] < nums[j]
	})

	for _, header := range headers {
		c.API.CacheHeader(header)
	}

	knownByRequests := make(map[uint64]map[common.Hash]map[uint64]struct{}) // reqID -> parenthash -> blockToValidate

	var onlyVerify time.Duration

	for _, num := range nums {
		c.API.ProcessingRequestsMu.Lock()
		req := reqHeaders[num]
		c.API.ProcessingRequestsMu.Unlock()

		appendAncestors(req, headers, knownByRequests)

		t1 := time.Now()
		err := c.verifyByRequest(req.ID, req.Header, req.Seal, req.ParentsExpected, req.KnownParents)
		onlyVerify += time.Since(t1)

		if err == nil {
			headers = append(headers, req.Header)
		}
	}

	total := time.Since(t)
	fmt.Println("VerifyRequestsCommonAncestor", reqID, total, onlyVerify, total - onlyVerify)
}

func (c *Consensus) verifyByRequest(reqID uint64, header *types.Header, seal bool, parentsExpected int, knownParents []*types.Header) error {
	t := time.Now()
	if len(knownParents) != parentsExpected {
		return errNotAllParents
	}

	err := c.Server.Verify(c.API.Chain, header, knownParents, false, seal)
	fmt.Println("verifyByRequest-1", header.Number.Uint64(), reqID, time.Since(t))
	t = time.Now()
	if err == nil {
		c.API.CacheHeader(header)
	}
	fmt.Println("verifyByRequest-2", header.Number.Uint64(), reqID, time.Since(t))
	t = time.Now()

	c.API.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, header.HashCache(), err}
	fmt.Println("verifyByRequest-3", header.Number.Uint64(), reqID, time.Since(t))
	t = time.Now()

	// remove finished request
	//fixme debug
	/*
		finishedRequest := consensus.FinishedRequest{reqID, header.Number.Uint64()}
		select {
		case c.CleanupCh <- finishedRequest:
		default:
			c.cleanupRequest(finishedRequest.ReqID, finishedRequest.BlockNumber)
		}
	*/

	fmt.Println("verifyByRequest-4", header.Number.Uint64(), reqID, time.Since(t))

	return nil
}

// remove finished request
func (c *Consensus) cleanupRequest(reqID uint64, number uint64) {
	c.API.ProcessingRequestsMu.Lock()
	reqBlocks, ok := c.API.ProcessingRequests[reqID]
	if ok {
		//fixme debug
		/*
			_, ok = reqBlocks[number]
			if ok {
				fmt.Println("reqBlocks", len(reqBlocks[number].KnownParents))
			} else {
				for bl, r := range reqBlocks {
					fmt.Println("reqBlocks-1.1", reqID, len(r.KnownParents), r.Header.Number.Uint64(), number, bl)
				}
				fmt.Println("reqBlocks-1", reqID, len(reqBlocks), "\n\n")
			}
		*/

		delete(reqBlocks, number)
		if len(reqBlocks) == 0 {
			delete(c.API.ProcessingRequests, reqID)
		}
	} else {
		//fixme debug - почему-то сюда попадаем.
		fmt.Println("WTF!!!", reqID, number)
	}
	c.API.ProcessingRequestsMu.Unlock()
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
	c.API.ProcessingRequestsMu.Lock()
	blocks, ok := c.API.ProcessingRequests[reqID]
	if !ok {
		blocks = make(map[uint64]*consensus.VerifyRequest)
		c.API.ProcessingRequests[reqID] = blocks
	}

	blocks[header.Number.Uint64()] = toVerifyRequest(reqID, header, seal, deadline, knownParentsSlice, parentsToValidate)

	c.API.ProcessingRequestsMu.Unlock()
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

func (c *Consensus) requestParentHeaders(reqID uint64, header *types.Header, reqHeaders []*types.Header) ([]*types.Header, int, *consensus.HeadersRequest) {
	parentsToValidate := c.Server.AncestorsNeededForVerification(header)
	if parentsToValidate == 0 {
		return nil, 0, nil
	}

	headerNumber := header.Number.Uint64()
	headerParentHash := header.ParentHash

	from := reqHeaders[0].Number.Uint64()
	to := reqHeaders[len(reqHeaders)-1].Number.Uint64()

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
