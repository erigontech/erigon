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
				matchedParents, reqParents, err := matchParents(req.Header, parents, parentsRequested, allParents)
				if err != nil && !errors.Is(err, errNotAllParents) {
					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, req.Header.Hash(), err}
					continue
				}
				if errors.Is(err, errNotAllParents) {
					c.addVerifyHeaderRequest(req, matchedParents, reqParents, allParents)
					continue
				}

				err = c.Verify(c.Process.Chain, req.Header, matchedParents, false, req.Seal)
				c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, req.Header.Hash(), err}
				if err == nil {
					c.AddVerifiedBlocks(req.Header)

					if c.IsRequestedBlocks(req.Header.Number.Uint64()) {
						c.RequestsMu.Lock()
						c.verifyByParentHeader(consensus.HeaderResponse{req.Header, req.Header.Number.Uint64(), nil})
						c.RequestsMu.Unlock()
					}
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

				c.RequestsMu.Lock()
				c.verifyByParentHeader(parentResp)
				c.RequestsMu.Unlock()
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

func (c *Consensus) verifyByParentHeader(parentResp consensus.HeaderResponse) {
	toVerify := make(map[uint64][]uint64) // blockNum->reqIDs

	for blockNum, reqMap := range c.RequestsToParents {
		for reqID, req := range reqMap {
			if parentResp.Number >= req.From && parentResp.Number <= req.To {
				reqMap[reqID].Parents = append(reqMap[reqID].Parents, parentResp.Header)
				toVerify[blockNum] = append(toVerify[blockNum], reqID)
			}
		}
	}

	for blockNum, reqIDs := range toVerify {
		for _, reqID := range reqIDs {
			req, ok := c.RequestsToParents[blockNum][reqID]
			if !ok || req == nil || req.Header == nil {
				continue
			}

			parents, _, err := matchParentsSlice(req.Header, req.Parents, nil, req.ParentsCount)
			if err != nil && !errors.Is(err, errNotAllParents) {
				c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, req.Header.Hash(), err}
				continue
			}
			if errors.Is(err, errNotAllParents) {
				continue
			}

			err = c.Verify(c.Process.Chain, req.Header, parents, false, req.Seal)
			c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, req.Header.Hash(), err}
			if err == nil {
				c.AddVerifiedBlocks(req.Header)
			}

			// remove finished request
			delete(c.RequestsToParents[blockNum], reqID)

			if c.IsRequestedBlocks(req.Header.Number.Uint64()) {
				c.verifyByParentHeader(consensus.HeaderResponse{req.Header, req.Header.Number.Uint64(), nil})
			}
		}
	}

	c.DeleteRequestedBlocks(parentResp.Number)
}

func (c *Consensus) addVerifyHeaderRequest(req consensus.VerifyHeaderRequest, parents []*types.Header, requestedParents []uint64, allParents int) {
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

	var toAppend []*types.Header
	for _, parent := range parents {
		has := consensus.SearchHeader(request.Parents, parent.Hash())
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

// returns parents, parentsRequested, error
func (c *Consensus) requestParentHeaders(req consensus.VerifyHeaderRequest) (map[uint64][]*types.Header, map[uint64][]*types.Header, int) {
	parentsToGet := c.NeededForVerification(req.Header)
	if parentsToGet == 0 {
		return nil, nil, 0
	}
	parents := make(map[uint64][]*types.Header, parentsToGet)
	parentsRequested := make(map[uint64][]*types.Header, parentsToGet)
	num := req.Header.Number.Uint64()

	for i := num - uint64(parentsToGet); i <= num-1; i++ {
		requestedParents := c.requestHeaders(i)

		if len(requestedParents) == 0 {
			parentsRequested[i] = nil // fixme возможно надо удалить эту строку
			continue
		}
		parents[i] = requestedParents
	}

	return parents, parentsRequested, parentsToGet
}

var errNotAllParents = errors.New("not all parents are gathered")

func matchParentsSlice(header *types.Header, parents []*types.Header, requestedParents []uint64, parentsCount int) ([]*types.Header, []uint64, error) {
	parentsMap := make(map[uint64][]*types.Header, len(parents))
	for _, parent := range parents {
		parentsMap[parent.Number.Uint64()] = append(parentsMap[parent.Number.Uint64()], parent)
	}

	requestedParentsMap := make(map[uint64][]*types.Header, len(parents))
	for _, parent := range requestedParents {
		requestedParentsMap[parent] = nil
	}
	return matchParents(header, parentsMap, requestedParentsMap, parentsCount)
}

// returns matched parents, requested parents block numbers, error
func matchParents(header *types.Header, parents map[uint64][]*types.Header, parentsRequested map[uint64][]*types.Header, parentsCount int) ([]*types.Header, []uint64, error) {
	requestedHeaders := make([]uint64, 0, parentsCount)
	for parent := range parentsRequested {
		requestedHeaders = append(requestedHeaders, parent)
	}

	if len(parents) < parentsCount {
		return nil, requestedHeaders, errNotAllParents
	}

	num := header.Number.Uint64()
	expectedParentHash := header.ParentHash
	headerParents := make([]*types.Header, 0, parentsCount)
	for i := num - uint64(parentsCount); i <= num-1; i++ {
		blockParents, ok := parents[i]
		if !ok || len(blockParents) == 0 {
			return nil, requestedHeaders, errNotAllParents
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
			return nil, requestedHeaders, consensus.ErrUnknownAncestor
		}
	}

	return headerParents, requestedHeaders, nil
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
