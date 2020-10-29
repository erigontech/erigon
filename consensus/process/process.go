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
		for {
			select {
			case req := <-c.VerifyHeaderRequests:
				fmt.Println("<-c.VerifyHeaderRequests-1", req.ID, req.Header.Number)
				if req.Deadline == nil {
					t := time.Now().Add(ttl)
					req.Deadline = &t
				}
				if req.Header == nil {
					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, common.Hash{}, ErrEmptyHeader}
				}

				// Short circuit if the header is known
				if ok := c.GetVerifiedBlock(req.Header.Number.Uint64(), req.Header.Hash()); ok {
					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, req.Header.Hash(), nil}
					continue
				}

				knownParents, parentsToValidate := c.requestParentHeaders(req)

				fmt.Println("<-c.VerifyHeaderRequests-2", req.Header.Number, len(knownParents), parentsToValidate)
				err := c.verifyByRequest(toVerifyRequest(req, knownParents, parentsToValidate))
				fmt.Println("<-c.VerifyHeaderRequests-3", req.Header.Number, err)
				if errors.Is(err, errNotAllParents) {
					c.addVerifyHeaderRequest(req, knownParents, parentsToValidate)
				}
			case parentResp := <-c.HeaderResponses:
				fmt.Println("<-c.HeaderResponses-1", parentResp.Headers == nil, parentResp.Err)
				if parentResp.Err != nil {
					c.ProcessingRequestsMu.Lock()

					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{parentResp.ID, parentResp.Hash, parentResp.Err}
					delete(c.ProcessingRequests, parentResp.ID)

					c.ProcessingRequestsMu.Unlock()
					fmt.Println("<-c.HeaderResponses-1.1")
					continue
				}

				fmt.Println("<-c.HeaderResponses-2")
				c.VerifyRequestsCommonAncestor(parentResp.ID, parentResp.Headers)
				fmt.Println("<-c.HeaderResponses-3")
			case <-c.CleanupTicker.C:
				fmt.Println("<-c.CleanupTicker.C-1")
				c.ProcessingRequestsMu.Lock()

				for reqID, req := range c.ProcessingRequests {
					if req.Deadline.Before(time.Now()) {
						c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, req.Header.Hash(), errors.New("timeout")}
						delete(c.ProcessingRequests, reqID)
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

func (c *Consensus) VerifyRequestsCommonAncestor(reqID uint64, headers []*types.Header) {
	if len(headers) == 0 {
		return
	}

	c.ProcessingRequestsMu.RLock()
	req, ok := c.ProcessingRequests[reqID]
	c.ProcessingRequestsMu.RUnlock()
	if !ok {
		return
	}

	appendParents(req, headers...)

	fmt.Println("%%%%% rec", req.ID, req.ParentsExpected, len(req.KnownParents), req.VerifyHeaderRequest.ID, req.Header.Number.Uint64(), req.VerifyHeaderRequest.Header.Number.Uint64())
	_ = c.verifyByRequest(req)

	fmt.Printf("+++++++++++++++++++++++++++++++++++++++++++++++++_verifyRequestsCommonAncestor-END %d %d\n\n\n", reqID, len(req.KnownParents))
}

func (c *Consensus) verifyByRequest(req *consensus.VerifyRequest) error {
	fmt.Println("IN verifyByRequest", req.ID, req.Header.Number.Uint64(), req.ParentsExpected, len(req.KnownParents))
	if uint64(len(req.KnownParents)) > req.Header.Number.Uint64() {
		for _, b := range req.KnownParents {
			fmt.Println("verifyByRequest-2", req.Header.Number.Uint64(), b.Number.Uint64(), b.Hash().String())
		}
	}

	if len(req.KnownParents) != req.ParentsExpected {
		return errNotAllParents
	}

	err := c.Verify(c.Process.Chain, req.Header, req.KnownParents, false, req.Seal)
	fmt.Println("Verify-1", req.ID, req.Header.Number.Uint64(), err)
	c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, req.Header.Hash(), err}
	if err == nil {
		fmt.Println("Verify-2", req.ID, req.Header.Number.Uint64(), err)
		c.AddVerifiedBlocks(req.Header)
	}

	// remove finished request
	c.ProcessingRequestsMu.Lock()
	delete(c.ProcessingRequests, req.ID)
	c.ProcessingRequestsMu.Unlock()

	return nil
}

func toVerifyRequest(req consensus.VerifyHeaderRequest, knownParents []*types.Header, allParents int) *consensus.VerifyRequest {
	request := &consensus.VerifyRequest{
		req,
		knownParents,
		allParents,
		req.Header.Number.Uint64() - uint64(allParents),
		req.Header.Number.Uint64() - uint64(len(knownParents)) - 1,
	}

	sort.SliceStable(request.KnownParents, func(i, j int) bool {
		return request.KnownParents[i].Hash().String() < request.KnownParents[j].Hash().String()
	})

	return request
}

func (c *Consensus) addVerifyHeaderRequest(req consensus.VerifyHeaderRequest, knownParents []*types.Header, allParents int) {
	fmt.Println("\n\n================================================ STARTED", req.ID, req.Header.Number.Uint64())
	request := toVerifyRequest(req, knownParents, allParents)

	appendParents(request, knownParents...)

	c.ProcessingRequestsMu.Lock()
	c.ProcessingRequests[req.ID] = request
	c.ProcessingRequestsMu.Unlock()
	fmt.Println("================================================ DONE\n\n", req.ID, req.Header.Number.Uint64())
}

func appendParents(request *consensus.VerifyRequest, parents ...*types.Header) {
	for _, parent := range parents {
		if parent.Number.Uint64() >= request.From && parent.Number.Uint64() <= request.To {
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

func (c *Consensus) requestParentHeaders(req consensus.VerifyHeaderRequest) ([]*types.Header, int) {
	parentsToValidate := c.NeededForVerification(req.Header)
	fmt.Println("requestParentHeaders", req.Header.Number, parentsToValidate)
	if parentsToValidate == 0 {
		return nil, 0
	}

	knownParents := c.requestHeaders(req.ID, req.Header.Number.Uint64(), req.Header.ParentHash, uint64(parentsToValidate))

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
