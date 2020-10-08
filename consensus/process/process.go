package process

import (
	"errors"
	"sort"
	"time"

	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type Consensus struct {
	consensus.Verifier
	*consensus.Process // remote Engine
}

const ttl = time.Minute

func NewConsensusProcess(v consensus.Verifier, chain consensus.ChainHeaderReader, exit chan struct{}) *Consensus {
	c := &Consensus{
		Verifier: v,
		Process:  consensus.NewProcess(chain),
	}

	// store genesis
	genesis := chain.GetHeaderByNumber(0)
	c.AddVerifiedBlocks(genesis)

	go func() {
		for {
			select {
			case req := <-c.VerifyHeaderRequests:
				if req.Deadline == nil {
					t := time.Now().Add(ttl)
					req.Deadline = &t
				}

				// Short circuit if the header is known
				if ok := c.GetVerifiedBlock(req.Header.Number.Uint64(), req.Header.Hash()); ok {
					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, req.Header.Hash(), nil}
					continue
				}

				parents, allParents := c.requestParentHeaders(req)
				reqParents, err := matchParents(req.Header, parents, allParents)
				if err != nil && !errors.Is(err, errNotAllParents) {
					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, req.Header.Hash(), err}
					continue
				}
				if errors.Is(err, errNotAllParents) {
					c.addVerifyHeaderRequest(req, reqParents, allParents)
					continue
				}

				err = c.Verify(c.Process.Chain, req.Header, reqParents, false, req.Seal)
				c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, req.Header.Hash(), err}
				if err == nil {
					c.AddVerifiedBlocks(req.Header)
				}
			case parentResp := <-c.HeaderResponses:
				toVerify := make(map[uint64][]uint64) // blockNum->reqIDs

				c.RequestsMu.Lock()
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
						if !ok {
							continue
						}

						reqParents, err := matchParentsSlice(req.Header, req.Parents, req.ParentsCount)
						if err != nil && !errors.Is(err, errNotAllParents) {
							c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, req.Header.Hash(), err}

							// remove finished request
							delete(c.RequestsToParents[blockNum], reqID)
							continue
						}
						if errors.Is(err, errNotAllParents) {
							continue
						}

						err = c.Verify(c.Process.Chain, req.Header, reqParents, false, req.Seal)
						c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, req.Header.Hash(), err}
						if err == nil {
							c.AddVerifiedBlocks(req.Header)
						}

						// remove finished request
						delete(c.RequestsToParents[blockNum], reqID)
					}
				}
				c.RequestsMu.Unlock()

				c.DeleteRequestedBlocks(parentResp.Header.Number.Uint64())
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
							continue
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

func (c *Consensus) addVerifyHeaderRequest(req consensus.VerifyHeaderRequest, reqParents []*types.Header, allParents int) {
	request := &consensus.VerifyRequest{
		req,
		reqParents,
		allParents,
		req.Header.Number.Uint64() - uint64(allParents),
		req.Header.Number.Uint64() - 1,
	}

	sort.SliceStable(request.Parents, func(i, j int) bool {
		return request.Parents[i].Hash().String() < request.Parents[j].Hash().String()
	})

	var toAppend []*types.Header
	for _, parent := range reqParents {
		has := consensus.SearchHeader(request.Parents, parent.Hash())
		if !has {
			toAppend = append(toAppend, parent)
		}
	}

	request.Parents = append(request.Parents, toAppend...)
	sort.SliceStable(request.Parents, func(i, j int) bool {
		return request.Parents[i].Hash().String() < request.Parents[j].Hash().String()
	})

	for _, parent := range toAppend {
		reqMap, ok := c.RequestsToParents[parent.Number.Uint64()]
		if !ok {
			reqMap = make(map[uint64]*consensus.VerifyRequest)
			c.RequestsToParents[parent.Number.Uint64()] = reqMap
		}
		reqMap[request.ID] = request
	}
}

func (c *Consensus) HeaderVerification() chan<- consensus.VerifyHeaderRequest {
	return c.VerifyHeaderRequests
}

func (c *Consensus) requestParentHeaders(req consensus.VerifyHeaderRequest) (map[uint64][]*types.Header, int) {
	parentsToGet := c.NeededForVerification(req.Header)
	if parentsToGet == 0 {
		return nil, 0
	}
	parents := make(map[uint64][]*types.Header, parentsToGet)
	header := req.Header
	num := req.Header.Number.Uint64()

	for i := num - 1; i > num+uint64(parentsToGet); i-- {
		requestedParents, err := c.requestHeaders(i)
		if err != nil {
			c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, header.Hash(), consensus.ErrUnknownAncestor}
		}

		parents[i] = requestedParents
	}

	return parents, parentsToGet
}

var errNotAllParents = errors.New("not all parents are gathered")

func matchParentsSlice(header *types.Header, parents []*types.Header, parentsCount int) ([]*types.Header, error) {
	parentsMap := make(map[uint64][]*types.Header, len(parents))
	for _, parent := range parents {
		parentsMap[parent.Number.Uint64()] = append(parentsMap[parent.Number.Uint64()], parent)
	}
	return matchParents(header, parentsMap, parentsCount)
}

func matchParents(header *types.Header, parents map[uint64][]*types.Header, parentsCount int) ([]*types.Header, error) {
	if len(parents) < parentsCount {
		return nil, errNotAllParents
	}

	num := header.Number.Uint64()
	expectedParentHash := header.ParentHash
	headerParents := make([]*types.Header, 0, parentsCount)
	for i := num - 1; i > num+uint64(parentsCount); i-- {
		blockParents, ok := parents[i]
		if !ok {
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

		if !correctParent {
			return nil, consensus.ErrUnknownAncestor
		}
	}

	return headerParents, nil
}

func (c *Consensus) requestHeaders(parentNum uint64) ([]*types.Header, error) {
	parentBlocks, ok := c.GetVerifiedBlocks(parentNum)
	if ok && len(parentBlocks) == 0 {
		return nil, consensus.ErrUnknownAncestor
	}
	if ok {
		return parentBlocks, nil
	}

	alreadyRequested := c.AddRequestedBlocks(parentNum)
	if !alreadyRequested {
		c.HeadersRequests <- consensus.HeadersRequest{parentNum}
	}
	return nil, nil
}

func (c *Consensus) VerifyResults() <-chan consensus.VerifyHeaderResponse {
	return c.VerifyHeaderResponses
}

func (c *Consensus) HeaderRequest() <-chan consensus.HeadersRequest {
	return c.HeadersRequests
}

func (c *Consensus) HeaderResponse() chan<- consensus.HeaderResponse {
	return c.HeaderResponses
}
