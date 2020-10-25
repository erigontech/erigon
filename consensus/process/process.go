package process

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/davecgh/go-spew/spew"

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
				fmt.Println("<-c.VerifyHeaderRequests-1", req.Header.Number)
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
				fmt.Println("<-c.VerifyHeaderRequests-2", req.Header.Number, len(parents), len(parentsRequested), allParents)
				err := c.verifyByRequest(toVerifyRequest(req, parents, allParents), req.Header.Number.Uint64())
				fmt.Println("<-c.VerifyHeaderRequests-3", req.Header.Number, err)
				if errors.Is(err, errNotAllParents) {
					c.addVerifyHeaderRequest(req, parents, parentsRequested, allParents)
				}
			case parentResp := <-c.HeaderResponses:
				// fixme лишнее условие похоже
				fmt.Println("<-c.HeaderResponses-1", parentResp.Number, parentResp.Header == nil, parentResp.Err)
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
					fmt.Println("<-c.HeaderResponses-1.1")
					continue
				}

				fmt.Println("<-c.HeaderResponses-2")
				c.VerifyRequestsCommonAncestor(parentResp.Header)
				fmt.Println("<-c.HeaderResponses-3")
			case <-c.CleanupTicker.C:
				fmt.Println("<-c.CleanupTicker.C-1")
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
				fmt.Println("<-c.CleanupTicker.C-2")
			case <-exit:
				fmt.Println("<-exit")
				return
			}
		}
	}()

	return c
}

func (c *Consensus) VerifyRequestsCommonAncestor(header *types.Header) {
	c.RequestsMu.Lock()
	defer c.RequestsMu.Unlock()
	c.verifyRequestsCommonAncestor(header)
}

type blockByAncestor struct {
	parents          []uint64
	blockNum         uint64
	reqID            uint64
	ancestorBlockNum uint64
}

func (c *Consensus) verifyRequestsCommonAncestor(header *types.Header) {
	if header == nil {
		return
	}
	blockNumber := header.Number.Uint64()
	fmt.Printf("\n\n+++++++++++++++++++++++++++++++++++++++++++++++++_verifyRequestsCommonAncestor-START %d %d\n", blockNumber, header.Number.Uint64())
	toVerify := make(map[uint64]blockByAncestor) // ancestorBlockNum->reqIDs
	toVerifySet := make(map[uint64]map[uint64]struct{})

	for ancestorBlockNum, reqIDsToRequests := range c.RequestsToParents {
		for reqID, request := range reqIDsToRequests {
			if request == nil || request.Header == nil {
				continue
			}

			ancMap, ok := toVerifySet[ancestorBlockNum]
			if !ok {
				ancMap = make(map[uint64]struct{})
				toVerifySet[ancestorBlockNum] = ancMap
			}
			_, ok = ancMap[reqID]

			// check if new parent is in the requested range
			if !ok && blockNumber >= request.From && blockNumber <= request.To {
				appendParents(reqIDsToRequests[reqID], header)

				req, ok := toVerify[ancestorBlockNum]
				if !ok {
					req.blockNum = reqIDsToRequests[reqID].Header.Number.Uint64()
					req.reqID = reqID
					req.ancestorBlockNum = ancestorBlockNum
				}
				req.parents = append(req.parents, reqID)
				toVerify[ancestorBlockNum] = req

				fmt.Printf("TO_VERIFY for %d %d %d\n", req.blockNum, req.ancestorBlockNum, request.ID)

				ancMap[reqID] = struct{}{}
			}
		}
	}

	toVerifySlice := make([]blockByAncestor, 0, len(toVerify))
	for _, reqIDs := range toVerify {
		toVerifySlice = append(toVerifySlice, reqIDs)
	}
	sort.SliceStable(toVerifySlice, func(i, j int) bool {
		return toVerifySlice[i].blockNum < toVerifySlice[j].blockNum
	})

	fmt.Println("XXXXXXXXXXXXXXXXXXXXXX-1", blockNumber)
	spew.Dump(toVerifySlice)

	for _, block := range toVerifySlice {
		req, ok := c.RequestsToParents[block.ancestorBlockNum][block.reqID]
		if !ok || len(req.KnownParents) < req.ParentsExpected {
			continue
		}

		// recursion
		fmt.Println("%%%%% rec", block.ancestorBlockNum, req.ID, req.ParentsExpected, len(req.KnownParents), req.VerifyHeaderRequest.ID, req.Header.Number.Uint64(), req.VerifyHeaderRequest.Header.Number.Uint64())
		_ = c.verifyByRequest(req, block.ancestorBlockNum)
	}

	c.DeleteRequestedBlocks(blockNumber)

	fmt.Printf("+++++++++++++++++++++++++++++++++++++++++++++++++_verifyRequestsCommonAncestor-END %d %d\n\n\n", blockNumber, header.Number.Uint64())
}

func (c *Consensus) verifyByRequest(req *consensus.VerifyRequest, ancestorBlockNum uint64) error {
	fmt.Println("IN verifyByRequest", ancestorBlockNum, req.ID, req.Header.Number.Uint64(), req.ParentsExpected, len(req.KnownParents))
	if uint64(len(req.KnownParents)) > req.Header.Number.Uint64() {
		for _, b := range req.KnownParents {
			fmt.Println("verifyByRequest-2", req.Header.Number.Uint64(), b.Number.Uint64(), b.Hash().String())
		}
	}
	reqID := req.ID

	parents, err := matchParents(req.Header, req.KnownParents, req.ParentsExpected)
	if err != nil && !errors.Is(err, errNotAllParents) {
		c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, req.Header.Hash(), err}

		// remove finished request
		delete(c.RequestsToParents[ancestorBlockNum], reqID)

		if c.IsRequestedBlocks(req.Header.Number.Uint64()) {
			// recursion
			fmt.Println("c.verifyRequestsCommonAncestor-1")
			c.verifyRequestsCommonAncestor(req.Header)
		}

		return nil
	}
	if errors.Is(err, errNotAllParents) {
		return err
	}

	err = c.Verify(c.Process.Chain, req.Header, parents, false, req.Seal)
	fmt.Println("Verify-1", reqID, req.Header.Number.Uint64(), err)
	c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{reqID, req.Header.Hash(), err}
	if err == nil {
		fmt.Println("Verify-2", reqID, req.Header.Number.Uint64(), err)
		c.AddVerifiedBlocks(req.Header)
	}

	// remove finished request
	delete(c.RequestsToParents[ancestorBlockNum], reqID)

	if c.IsRequestedBlocks(req.Header.Number.Uint64()) {
		// recursion
		fmt.Println("Verify-3", reqID, req.Header.Number.Uint64(), err)
		c.verifyRequestsCommonAncestor(req.Header)
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

	sort.SliceStable(request.KnownParents, func(i, j int) bool {
		return request.KnownParents[i].Hash().String() < request.KnownParents[j].Hash().String()
	})

	return request
}

func (c *Consensus) addVerifyHeaderRequest(req consensus.VerifyHeaderRequest, parents []*types.Header, requestedParents []uint64, allParents int) {
	fmt.Println("\n\n================================================ STARTED", req.ID, req.Header.Number.Uint64())
	request := toVerifyRequest(req, parents, allParents)

	appendParents(request, parents...)

	for _, parent := range request.KnownParents {
		reqMap, ok := c.RequestsToParents[parent.Number.Uint64()]
		if !ok {
			reqMap = make(map[uint64]*consensus.VerifyRequest)
			c.RequestsToParents[parent.Number.Uint64()] = reqMap
		}
		reqMap[request.ID] = request
		fmt.Println("addVerifyHeaderRequest-ADD-1.1", parent.Number.Uint64(), request.ID, request.Header.Number.Uint64(), request.ParentsExpected, len(request.KnownParents))
	}

	for _, parent := range requestedParents {
		reqMap, ok := c.RequestsToParents[parent]
		if !ok {
			reqMap = make(map[uint64]*consensus.VerifyRequest)
			c.RequestsToParents[parent] = reqMap
		}
		reqMap[request.ID] = request
		fmt.Println("addVerifyHeaderRequest-ADD-1.2", parent, request.ID, request.Header.Number.Uint64(), request.ParentsExpected, len(request.KnownParents))
	}
	fmt.Println("================================================ DONE\n\n", req.ID, req.Header.Number.Uint64())
}

func appendParents(request *consensus.VerifyRequest, parents ...*types.Header) {
	for _, parent := range parents {
		has := types.SearchHeader(request.KnownParents, parent.Hash())
		if !has {
			request.KnownParents = append(request.KnownParents, parent)
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

// returns parents(asc sorted by block number), parentsRequested, error
func (c *Consensus) requestParentHeaders(req consensus.VerifyHeaderRequest) ([]*types.Header, []uint64, int) {
	parentsToGet := c.NeededForVerification(req.Header)
	fmt.Println("requestParentHeaders", req.Header.Number, parentsToGet)
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
			fmt.Println("+++++++++++++++++++++++++++++++++++++", header.Number.Uint64(), correctParent, len(blockParents), spew.Sdump(blockParents))
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
