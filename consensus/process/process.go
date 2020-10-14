package process

import (
	"errors"
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

				parents, parentsRequested, allParents := c.requestParentHeaders(req)
				err := c.verifyByRequest(toVerifyRequest(req, parents, allParents), req.Header.Number.Uint64())
				if errors.Is(err, errNotAllParents) {
					c.addVerifyHeaderRequest(req, parents, parentsRequested, allParents)
					continue
				}
			case parentResp := <-c.HeaderResponses:
				if parentResp.Err != nil {
					c.RequestsMu.Lock()
					c.DeleteRequestedBlocks(parentResp.Number)

					requests := c.RequestsToParents[parentResp.Number]
					for reqID, req := range requests {
						c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, req.Header.Hash(), parentResp.Err}
						delete(c.RequestsToParents[parentResp.Number], reqID)

						for n := req.From; n <= req.To; n++ {
							c.DeleteRequestedBlocks(n)
						}
					}
					c.RequestsMu.Unlock()
					continue
				}

				c.VerifyRequestsCommonAncestor(parentResp.Number, parentResp.Header)
			case <-c.CleanupTicker.C:
				c.RequestsMu.Lock()
				for blockNum, reqMap := range c.RequestsToParents {
					for reqID, req := range reqMap {
						if req.Deadline.Before(time.Now()) {
							c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, req.Header.Hash(), errors.New("timeout")}
							delete(c.RequestsToParents[blockNum], reqID)

							for n := req.From; n <= req.To; n++ {
								c.DeleteRequestedBlocks(n)
							}
						}
					}
				}
				c.RequestsMu.Unlock()
			case <-exit:
				return
			}
		}
	}()

	return c
}

func (c *Consensus) VerifyRequestsCommonAncestor(blockNumber uint64, header *types.Header) {
	c.RequestsMu.Lock()
	defer c.RequestsMu.Unlock()
	c.verifyRequestsCommonAncestor(blockNumber, header)
}

func (c *Consensus) verifyRequestsCommonAncestor(blockNumber uint64, header *types.Header) {
	toVerify := make(map[uint64][]uint64) // ancestorBlockNum->reqIDs

	for ancestorBlockNum, reqMap := range c.RequestsToParents {
		for reqID, req := range reqMap {
			if req == nil || req.Header == nil {
				continue
			}

			// check if new parent is in the requested range
			if blockNumber >= req.From && blockNumber <= req.To {
				reqMap[reqID].Parents = append(reqMap[reqID].Parents, header)
				toVerify[ancestorBlockNum] = append(toVerify[ancestorBlockNum], reqID)
			}
		}
	}

	for ancestorBlockNum, reqIDs := range toVerify {
		for _, reqID := range reqIDs {
			req, ok := c.RequestsToParents[ancestorBlockNum][reqID]
			if !ok {
				continue
			}

			// recursion
			_ = c.verifyByRequest(req, ancestorBlockNum)
		}
	}

	c.DeleteRequestedBlocks(blockNumber)
}

func (c *Consensus) verifyByRequest(req *consensus.VerifyRequest, ancestorBlockNum uint64) error {
	reqID := req.ID

	parents, err := matchParents(req.Header, req.Parents, req.ParentsCount)
	if err != nil && !errors.Is(err, errNotAllParents) {
		c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, req.Header.Hash(), err}

		// remove finished request
		delete(c.RequestsToParents[ancestorBlockNum], reqID)

		if c.IsRequestedBlocks(req.Header.Number.Uint64()) {
			// recursion
			c.verifyRequestsCommonAncestor(req.Header.Number.Uint64(), req.Header)
		}

		return nil
	}
	if errors.Is(err, errNotAllParents) {
		return err
	}

	err = c.Verify(c.Process.Chain, req.Header, parents, false, req.Seal)
	c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, req.Header.Hash(), err}
	if err == nil {
		c.AddVerifiedBlocks(req.Header)
	}

	// remove finished request
	delete(c.RequestsToParents[ancestorBlockNum], reqID)

	if c.IsRequestedBlocks(req.Header.Number.Uint64()) {
		// recursion
		c.verifyRequestsCommonAncestor(req.Header.Number.Uint64(), req.Header)
	}

	return nil
}

func toVerifyRequest(req consensus.VerifyHeaderRequest, parents []*types.Header, allParents int) *consensus.VerifyRequest {
	request := &consensus.VerifyRequest{
		req,
		parents,
		allParents,
		req.Header.Number.Uint64() - uint64(allParents),
		req.Header.Number.Uint64() - 1,
	}

	sort.SliceStable(request.Parents, func(i, j int) bool {
		return request.Parents[i].Hash().String() < request.Parents[j].Hash().String()
	})

	return request
}

func (c *Consensus) addVerifyHeaderRequest(req consensus.VerifyHeaderRequest, parents []*types.Header, requestedParents []uint64, allParents int) {
	request := toVerifyRequest(req, parents, allParents)

	var toAppend []*types.Header
	for _, parent := range parents {
		has := types.SearchHeader(request.Parents, parent.Hash())
		if !has {
			toAppend = append(toAppend, parent)
		}
	}

	request.Parents = append(request.Parents, toAppend...)
	sort.SliceStable(request.Parents, func(i, j int) bool {
		return request.Parents[i].Hash().String() < request.Parents[j].Hash().String()
	})

	for _, parent := range request.Parents {
		reqMap, ok := c.RequestsToParents[parent.Number.Uint64()]
		if !ok {
			reqMap = make(map[uint64]*consensus.VerifyRequest)
			c.RequestsToParents[parent.Number.Uint64()] = reqMap
		}
		reqMap[request.ID] = request
	}

	for _, parent := range requestedParents {
		reqMap, ok := c.RequestsToParents[parent]
		if !ok {
			reqMap = make(map[uint64]*consensus.VerifyRequest)
			c.RequestsToParents[parent] = reqMap
		}
		reqMap[request.ID] = request
	}
}

func (c *Consensus) HeaderVerification() chan<- consensus.VerifyHeaderRequest {
	return c.VerifyHeaderRequests
}

// returns parents(asc sorted by block number), parentsRequested, error
func (c *Consensus) requestParentHeaders(req consensus.VerifyHeaderRequest) ([]*types.Header, []uint64, int) {
	parentsToGet := c.NeededForVerification(req.Header)
	if parentsToGet == 0 {
		return nil, nil, 0
	}
	parents := make([]*types.Header, 0, parentsToGet)
	parentsRequested := make([]uint64, 0, parentsToGet)
	num := req.Header.Number.Uint64()

	for i := num - uint64(parentsToGet); i <= num-1; i++ {
		requestedParents := c.requestHeaders(i)

		if len(requestedParents) == 0 {
			parentsRequested = append(parentsRequested, i)
			continue
		}
		parents = append(parents, requestedParents...)
	}

	sort.SliceStable(parents, func(i, j int) bool {
		return parents[i].Number.Cmp(parents[j].Number) == -1
	})

	return parents, parentsRequested, parentsToGet
}

var errNotAllParents = errors.New("not all parents are gathered")

// returns matched parents, requested parents block numbers, error
func matchParents(header *types.Header, parents []*types.Header, parentsCount int) ([]*types.Header, error) {
	if len(parents) < parentsCount {
		return nil, errNotAllParents
	}

	num := header.Number.Uint64()
	expectedParentHash := header.ParentHash
	headerParents := make([]*types.Header, 0, parentsCount)
	for i := num - uint64(parentsCount); i <= num-1; i++ {
		blockParents, ok := types.SearchHeadersByNumber(parents, i)
		if !ok || len(blockParents) == 0 {
			return nil, errNotAllParents
		}

		var correctParent bool
		for _, parent := range blockParents {
			if parent.Hash() == expectedParentHash {
				correctParent = true
				expectedParentHash = parent.ParentHash
				headerParents = append(headerParents, parent)
				break
			}
		}

		if !correctParent && len(blockParents) != 0 {
			// old value(reorg) in the cache could cause this value
			return nil, consensus.ErrUnknownAncestor
		}
	}

	return headerParents, nil
}

func (c *Consensus) requestHeaders(parentNum uint64) []*types.Header {
	parentBlocks, ok := c.GetVerifiedBlocks(parentNum)
	if ok {
		return parentBlocks
	}

	alreadyRequested := c.AddRequestedBlocks(parentNum)
	if !alreadyRequested {
		c.HeadersRequests <- consensus.HeadersRequest{parentNum}
	}
	return nil
}

func (c *Consensus) VerifyResults() <-chan consensus.VerifyHeaderResponse {
	return c.VerifyHeaderResponses
}
