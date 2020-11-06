package process

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type Consensus struct {
	consensus.Verifier
	*consensus.Process // remote Engine
}

const ttl = time.Minute

var (
	errEmptyHeader  = errors.New("an empty header")
	errNothingToAsk = errors.New("nothing to ask")
)

func NewConsensusProcess(v consensus.Verifier, chain consensus.ChainHeaderReader, exit chan struct{}) *Consensus {
	c := &Consensus{
		Verifier: v,
		Process:  consensus.NewProcess(chain),
	}

	go func() {
	eventLoop:
		for {
			select {
			case req := <-c.VerifyHeaderRequests:
				if req.Deadline == nil {
					t := time.Now().Add(ttl)
					req.Deadline = &t
				}
				if len(req.Headers) == 0 {
					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, common.Hash{}, errEmptyHeader}
					continue
				}

				sort.Slice(req.Headers, func(i, j int) bool {
					return req.Headers[i].Number.Cmp(req.Headers[j].Number) == -1
				})

				ancestorsReqs := make([]consensus.HeadersRequest, 0, len(req.Headers))

				for i, header := range req.Headers {
					if header == nil {
						c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, common.Hash{}, errEmptyHeader}
						continue eventLoop
					}

					// Short circuit if the header is known
					if h := c.GetCachedHeader(header.Hash(), header.Number.Uint64()); h != nil {
						c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, header.Hash(), nil}
						continue
					}

					knownParents, parentsToValidate, ancestorsReq := c.requestParentHeaders(req.ID, header, req.Headers)

					if ancestorsReq != nil {
						ancestorsReqs = append(ancestorsReqs, *ancestorsReq)
					}

					err := c.verifyByRequest(req.ID, header, req.Seal[i], parentsToValidate, knownParents)
					if errors.Is(err, errNotAllParents) {
						c.addVerifyHeaderRequest(req.ID, header, req.Seal[i], req.Deadline, knownParents, parentsToValidate)
					}
				}

				ancestorsReq, err := sumHeadersRequestsInRange(req.ID, req.Headers[0].Number.Uint64(), ancestorsReqs...)
				if err != nil {
					fmt.Println("XXX-err", req.ID, req.Headers[0].Number.Uint64(), err)
					continue
				}
				fmt.Println("XXX-req-parents", req.ID, req.Headers[0].Number.Uint64(), ancestorsReq.Number, ancestorsReq.HighestBlockNumber, ancestorsReq.HighestHash.String())
				c.HeadersRequests <- ancestorsReq

			case parentResp := <-c.HeaderResponses:
				fmt.Println("parentResp-0", parentResp.ID, len(parentResp.Headers), parentResp.Err)
				if parentResp.Err != nil {
					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{parentResp.ID, parentResp.Hash, parentResp.Err}

					c.ProcessingRequestsMu.Lock()
					delete(c.ProcessingRequests, parentResp.ID)
					c.ProcessingRequestsMu.Unlock()

					continue
				}

				c.VerifyRequestsCommonAncestor(parentResp.ID, parentResp.Headers)

			case <-c.CleanupTicker.C:
				c.ProcessingRequestsMu.Lock()

				for reqID, reqBlocks := range c.ProcessingRequests {
					for _, req := range reqBlocks {
						if req.Deadline.Before(time.Now()) {
							c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, req.Header.Hash(), errors.New("timeout")}

							delete(c.ProcessingRequests, reqID)
						}
					}
				}

				c.ProcessingRequestsMu.Unlock()

			case <-exit:
				return
			}
		}
	}()

	return c
}

func (c *Consensus) VerifyRequestsCommonAncestor(reqID uint64, headers []*types.Header) {
	if len(headers) == 0 {
		return
	}

	c.ProcessingRequestsMu.Lock()
	reqHeaders, ok := c.ProcessingRequests[reqID]
	if !ok {
		c.ProcessingRequestsMu.Unlock()
		return
	}

	nums := make([]uint64, 0, len(reqHeaders))
	for num := range reqHeaders {
		nums = append(nums, num)
	}
	c.ProcessingRequestsMu.Unlock()

	sort.Slice(nums, func(i, j int) bool {
		return nums[i] < nums[j]
	})

	for _, header := range headers {
		c.CacheHeader(header)
	}

	for _, num := range nums {
		c.ProcessingRequestsMu.Lock()
		req := reqHeaders[num]
		c.ProcessingRequestsMu.Unlock()

		appendParents(req, headers...)

		err := c.verifyByRequest(req.ID, req.Header, req.Seal, req.ParentsExpected, req.KnownParents)
		if err == nil {
			headers = append(headers, req.Header)
		}
	}
}

func (c *Consensus) verifyByRequest(reqID uint64, header *types.Header, seal bool, parentsExpected int, knownParents []*types.Header) error {
	if len(knownParents) != parentsExpected {
		return errNotAllParents
	}

	err := c.Verify(c.Process.Chain, header, knownParents, false, seal)
	c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, header.Hash(), err}
	if err == nil {
		c.CacheHeader(header)
	}

	// remove finished request
	c.ProcessingRequestsMu.Lock()
	reqBlocks, ok := c.ProcessingRequests[reqID]
	if ok {
		delete(reqBlocks, header.Number.Uint64())
		if len(reqBlocks) == 0 {
			delete(c.ProcessingRequests, reqID)
		}
	}
	c.ProcessingRequestsMu.Unlock()

	return nil
}

func toVerifyRequest(reqID uint64, header *types.Header, seal bool, deadline *time.Time, knownParents []*types.Header, parentsToValidate int) *consensus.VerifyRequest {
	request := &consensus.VerifyRequest{
		reqID,
		header,
		seal,
		deadline,
		knownParents,
		parentsToValidate,
		header.Number.Uint64() - uint64(parentsToValidate),
		header.Number.Uint64() - uint64(len(knownParents)) - 1,
	}

	sort.SliceStable(request.KnownParents, func(i, j int) bool {
		return request.KnownParents[i].Hash().String() < request.KnownParents[j].Hash().String()
	})

	return request
}

func (c *Consensus) addVerifyHeaderRequest(reqID uint64, header *types.Header, seal bool, deadline *time.Time, knownParents []*types.Header, parentsToValidate int) {
	request := toVerifyRequest(reqID, header, seal, deadline, knownParents, parentsToValidate)

	appendParents(request, knownParents...)

	c.ProcessingRequestsMu.Lock()
	blocks, ok := c.ProcessingRequests[reqID]
	if !ok {
		blocks = make(map[uint64]*consensus.VerifyRequest)
	}
	blocks[header.Number.Uint64()] = request
	c.ProcessingRequests[reqID] = blocks
	c.ProcessingRequestsMu.Unlock()
}

func appendParents(request *consensus.VerifyRequest, parents ...*types.Header) {
	for _, parent := range parents {
		if parent.Number.Uint64() >= request.From && parent.Number.Uint64() <= request.To {
			// fixme remove sort and search
			has := types.SearchHeader(request.KnownParents, parent.Hash())
			if !has {
				request.KnownParents = append(request.KnownParents, parent)
			}
		}
	}

	sort.SliceStable(request.KnownParents, func(i, j int) bool {
		if request.KnownParents[i].Number.Uint64() == request.KnownParents[j].Number.Uint64() {
			return request.KnownParents[i].Hash().String() < request.KnownParents[j].Hash().String()
		}
		return request.KnownParents[i].Number.Uint64() < request.KnownParents[j].Number.Uint64()
	})
}

func (c *Consensus) HeaderVerification() chan<- consensus.VerifyHeaderRequest {
	return c.VerifyHeaderRequests
}

func (c *Consensus) requestParentHeaders(reqID uint64, header *types.Header, reqHeaders []*types.Header) ([]*types.Header, int, *consensus.HeadersRequest) {
	parentsToValidate := c.NeededForVerification(header)
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
	sort.Slice(knownParents, func(i, j int) bool {
		return knownParents[i].Number.Cmp(knownParents[j].Number) == -1
	})

	return knownParents, parentsToValidate, &ancestorsReq
}

var errNotAllParents = errors.New("not all parents are gathered")

func (c *Consensus) requestHeadersNotFromRange(reqID uint64, highestBlock uint64, highestKnown common.Hash, parentsToGet uint64) ([]*types.Header, consensus.HeadersRequest) {
	var known []*types.Header
	highestParent := highestBlock

	var minHeader uint64
	if highestBlock > parentsToGet {
		minHeader = highestBlock - parentsToGet + 1
	}

	for parentBlockNum := highestBlock; parentBlockNum >= minHeader; parentBlockNum-- {
		parentBlock := c.GetCachedHeader(highestKnown, parentBlockNum)
		if parentBlock == nil {
			break
		}

		highestKnown = parentBlock.ParentHash
		highestParent = parentBlock.Number.Uint64() - 1

		known = append(known, parentBlock)
	}

	if len(known) != 0 {
		highestKnown = known[0].Hash()
		highestParent = known[0].Number.Uint64()
	}

	return known, consensus.HeadersRequest{
		reqID,
		highestKnown,
		highestParent,
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

	idx := -1
	for i, h := range requestedHeaders {
		if h.Number.Uint64() == highestHeader.Number.Uint64() {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil
	}

	if idx-int(parentsToGet) < 0 {
		return nil
	}

	return requestedHeaders[idx-int(parentsToGet) : idx]
}

func (c *Consensus) VerifyResults() <-chan consensus.VerifyHeaderResponse {
	return c.VerifyHeaderResponses
}
