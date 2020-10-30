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

var ErrEmptyHeader = errors.New("an empty header")

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
				fmt.Println("<-c.VerifyHeaderRequests-1", req.ID, req.Header[0].Number)
				if req.Deadline == nil {
					t := time.Now().Add(ttl)
					req.Deadline = &t
				}
				if len(req.Header) == 0 {
					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, common.Hash{}, ErrEmptyHeader}
					continue
				}

				sort.SliceStable(req.Header, func(i, j int) bool {
					return req.Header[i].Number.Cmp(req.Header[j].Number) == -1
				})

				for i, header := range req.Header {
					if header == nil {
						c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, common.Hash{}, ErrEmptyHeader}
						continue eventLoop
					}

					// Short circuit if the header is known
					if ok := c.GetVerifiedBlock(header.Number.Uint64(), header.Hash()); ok {
						c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, header.Hash(), nil}
						continue
					}

					knownParents, parentsToValidate := c.requestParentHeaders(req.ID, header, req.Header)
					if parentsToValidate == 0 {
						continue
					}

					fmt.Println("<-c.VerifyHeaderRequests-2", header.Number, len(knownParents), parentsToValidate)
					err := c.verifyByRequest(req.ID, header, req.Seal[i], parentsToValidate, knownParents)
					fmt.Println("<-c.VerifyHeaderRequests-3", header.Number, err)
					if errors.Is(err, errNotAllParents) {
						c.addVerifyHeaderRequest(req.ID, header, req.Seal[i], req.Deadline, knownParents, parentsToValidate)
					}
				}
			case parentResp := <-c.HeaderResponses:
				fmt.Println("<-c.HeaderResponses-1", parentResp.Headers == nil, parentResp.Err)
				if parentResp.Err != nil {
					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{parentResp.ID, parentResp.Hash, parentResp.Err}

					c.ProcessingRequestsMu.Lock()
					delete(c.ProcessingRequests, parentResp.ID)
					c.ProcessingRequestsMu.Unlock()

					fmt.Println("<-c.HeaderResponses-1.1")
					continue
				}

				fmt.Println("<-c.HeaderResponses-2")
				c.VerifyRequestsCommonAncestor(parentResp.ID, parentResp.Number, parentResp.Headers)
				fmt.Println("<-c.HeaderResponses-3")
			case <-c.CleanupTicker.C:
				fmt.Println("<-c.CleanupTicker.C-1")
				c.ProcessingRequestsMu.Lock()

				for reqID, reqBlocks := range c.ProcessingRequests {
					for _, req := range reqBlocks {
						if req.Deadline.Before(time.Now()) {
							c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, req.Header.Hash(), errors.New("timeout")}

							c.ProcessingRequestsMu.Lock()
							delete(c.ProcessingRequests, reqID)
							c.ProcessingRequestsMu.Unlock()
						}
					}
				}

				c.ProcessingRequestsMu.Unlock()
				fmt.Println("<-c.CleanupTicker.C-2")
			case <-exit:
				fmt.Println("<-exit")
				return
			}
		}
	}()

	return c
}

func (c *Consensus) VerifyRequestsCommonAncestor(reqID uint64, blockNumber uint64, headers []*types.Header) {
	if len(headers) == 0 {
		return
	}

	c.ProcessingRequestsMu.Lock()
	reqHeaders, ok := c.ProcessingRequests[reqID]
	if !ok {
		c.ProcessingRequestsMu.Unlock()
		return
	}

	req, ok := reqHeaders[blockNumber]
	if !ok {
		c.ProcessingRequestsMu.Unlock()
		return
	}

	appendParents(req, headers...)
	c.ProcessingRequestsMu.Unlock()

	fmt.Println("%%%%% rec", reqID, req.ParentsExpected, len(req.KnownParents), req.ID, req.Header.Number.Uint64(), req.Header.Number.Uint64())
	_ = c.verifyByRequest(req.ID, req.Header, req.Seal, req.ParentsExpected, req.KnownParents)

	fmt.Printf("+++++++++++++++++++++++++++++++++++++++++++++++++_verifyRequestsCommonAncestor-END %d %d\n\n\n", reqID, len(req.KnownParents))
}

func (c *Consensus) verifyByRequest(reqID uint64, header *types.Header, seal bool, parentsExpected int, knownParents []*types.Header) error {
	if len(knownParents) != parentsExpected {
		return errNotAllParents
	}

	err := c.Verify(c.Process.Chain, header, knownParents, false, seal)
	fmt.Println("Verify-1", reqID, header.Number.Uint64(), err)
	c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, header.Hash(), err}
	if err == nil {
		fmt.Println("Verify-2", reqID, header.Number.Uint64(), err)
		c.AddVerifiedBlocks(header)
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

func toVerifyRequest(reqID uint64, header *types.Header, seal bool, deadline *time.Time, knownParents []*types.Header, allParents int) *consensus.VerifyRequest {
	request := &consensus.VerifyRequest{
		reqID,
		header,
		seal,
		deadline,
		knownParents,
		allParents,
		header.Number.Uint64() - uint64(allParents),
		header.Number.Uint64() - uint64(len(knownParents)) - 1,
	}

	sort.SliceStable(request.KnownParents, func(i, j int) bool {
		return request.KnownParents[i].Hash().String() < request.KnownParents[j].Hash().String()
	})

	return request
}

func (c *Consensus) addVerifyHeaderRequest(reqID uint64, header *types.Header, seal bool, deadline *time.Time, knownParents []*types.Header, allParents int) {
	fmt.Println("\n\n================================================ STARTED", reqID, header.Number.Uint64())
	request := toVerifyRequest(reqID, header, seal, deadline, knownParents, allParents)

	appendParents(request, knownParents...)

	c.ProcessingRequestsMu.Lock()
	blocks, ok := c.ProcessingRequests[reqID]
	if !ok {
		blocks = make(map[uint64]*consensus.VerifyRequest)
	}
	blocks[header.Number.Uint64()] = request
	c.ProcessingRequests[reqID] = blocks
	c.ProcessingRequestsMu.Unlock()

	fmt.Println("================================================ DONE\n\n", reqID, header.Number.Uint64())
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

func (c *Consensus) requestParentHeaders(reqID uint64, header *types.Header, reqHeaders []*types.Header) ([]*types.Header, int) {
	parentsToValidate := c.NeededForVerification(header)
	fmt.Println("requestParentHeaders", header.Number, parentsToValidate)
	if parentsToValidate == 0 {
		return nil, 0
	}

	headerNumber := header.Number.Uint64()
	headerParentHash := header.ParentHash

	from := reqHeaders[0].Number.Uint64()
	to := reqHeaders[len(reqHeaders)-1].Number.Uint64()

	// don't ask for already requested for verification blocks
	if header.Number.Uint64() > from && header.Number.Uint64() <= to {
		if header.Number.Uint64()-uint64(parentsToValidate) < from {
			fmt.Printf("----- %d (1) - for block %d\n", reqID, header.Number.Int64())
			parentsToValidate = int(int64(from) - (header.Number.Int64() - int64(parentsToValidate)))
		}
	}

	if parentsToValidate > 0 {
		fmt.Printf("----- %d (2) - for block %d %d\n", reqID, header.Number.Int64(), parentsToValidate)
		headerNumber = from - 1
		headerParentHash = reqHeaders[0].ParentHash
	} else {
		return nil, 0
	}

	fmt.Printf("----- %d - for block %d. req headers are from %d to %d. requesting total %d: back to %d blocks; last one is %d %s\n", reqID, header.Number.Int64(), from, to, c.NeededForVerification(header), headerNumber, parentsToValidate, headerParentHash.String())

	// fixme тут могут быть дубли запросов
	knownParents := c.requestHeaders(reqID, headerNumber, headerParentHash, uint64(parentsToValidate))

	sort.SliceStable(knownParents, func(i, j int) bool {
		return knownParents[i].Number.Cmp(knownParents[j].Number) == -1
	})

	return knownParents, parentsToValidate
}

var errNotAllParents = errors.New("not all parents are gathered")

func (c *Consensus) requestHeaders(reqID uint64, highestBlock uint64, highestKnown common.Hash, parentsToGet uint64) []*types.Header {
	var known []*types.Header
	highestParent := highestBlock

	for parentBlockNum := highestBlock - 1; parentBlockNum >= highestBlock-parentsToGet; parentBlockNum-- {
		parentBlock := c.GetVerifiedBlocks(highestKnown, parentBlockNum)
		if parentBlock == nil {
			break
		}

		highestKnown = parentBlock.ParentHash
		highestParent = parentBlock.Number.Uint64() - 1
		known = append(known, parentBlock)
	}

	c.HeadersRequests <- consensus.HeadersRequest{
		reqID,
		highestKnown,
		highestParent,
		parentsToGet - uint64(len(known)),
	}

	return known
}

func (c *Consensus) VerifyResults() <-chan consensus.VerifyHeaderResponse {
	return c.VerifyHeaderResponses
}
