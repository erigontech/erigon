package process

import (
	"errors"
	"fmt"
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

func NewConsensusProcess(v consensus.Verifier, chain consensus.ChainHeaderReader, exit chan struct{}) *Consensus {
	c := &Consensus{
		Verifier: v,
		Process:  consensus.NewProcess(chain),
	}

	// store genesis
	genesis := chain.GetHeaderByNumber(0)
	c.AddVerifiedBlocks(genesis, genesis.Hash())

	go func() {
		for {
			select {
			case req := <-c.VerifyHeaderRequests:
				if req.Deadline == nil {
					t := time.Now().Add(ttl)
					req.Deadline = &t
				} else if req.Deadline.Before(time.Now()) {
					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, req.Header.Hash(), errors.New("timeout")}
					continue
				}

				// Short circuit if the header is known
				if _, ok := c.GetVerifiedBlocks(req.Header.Hash()); ok {
					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, req.Header.Hash(), nil}
					continue
				}

				parents, exit := c.requestParentHeaders(req)
				if exit {
					c.RetryVerifyHeaderRequests <- req
					continue
				}

				err := c.Verify(c.Process.Chain, req.Header, parents, false, req.Seal)
				fmt.Println("=== VerifyHeaderResponse-1", req.ID, req.Header.Number.Int64(), len(c.VerifyHeaderResponses))
				c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, req.Header.Hash(), err}
				fmt.Println("=== VerifyHeaderResponse-2", req.ID, req.Header.Number.Int64(), err)
				if err == nil {
					c.AddVerifiedBlocks(req.Header, req.Header.Hash())
				}
			case parentResp := <-c.HeaderResponses:
				c.DeleteRequestedBlocks(parentResp.Hash)
				if parentResp.Header != nil {
					c.AddVerifiedBlocks(parentResp.Header, parentResp.Header.Hash())
				}
			case <-c.RetryVerifyTicker.C:
				select {
				case req := <-c.RetryVerifyHeaderRequests:
					c.VerifyHeaderRequests <- req
				default:
					// nothing to do
				}
			case <-exit:
				return
			}
		}
	}()

	return c
}

func (c *Consensus) HeaderVerification() chan<- consensus.VerifyHeaderRequest {
	return c.VerifyHeaderRequests
}

func (c *Consensus) requestParentHeaders(req consensus.VerifyHeaderRequest) ([]*types.Header, bool) {
	parentsToGet := c.NeededForVerification(req.Header)
	if len(parentsToGet) == 0 {
		return nil, false
	}
	parents := make([]*types.Header, 0, len(parentsToGet))
	header := req.Header

	for _, hash := range parentsToGet {
		if hash == (common.Hash{}) {
			continue
		}
		parent, exit, err := c.requestHeader(hash)
		if err != nil {
			c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.ID, header.Hash(), consensus.ErrUnknownAncestor}
		}
		if exit {
			return nil, true
		}

		parents = append(parents, parent)
		header = parent
	}

	return parents, false
}

func (c *Consensus) requestHeader(parentHash common.Hash) (*types.Header, bool, error) {
	parent, ok := c.GetVerifiedBlocks(parentHash)
	if ok && parent == nil {
		return nil, true, consensus.ErrUnknownAncestor
	}

	if !ok {
		alreadyRequested := c.AddRequestedBlocks(parentHash)
		if !alreadyRequested {
			c.HeadersRequests <- consensus.HeadersRequest{parentHash}
		}
		return nil, true, nil
	}
	return parent, false, nil
}

func (c *Consensus) VerifyResults() chan consensus.VerifyHeaderResponse {
	return c.VerifyHeaderResponses
}

func (c *Consensus) HeaderRequest() <-chan consensus.HeadersRequest {
	return c.HeadersRequests
}

func (c *Consensus) HeaderResponse() chan<- consensus.HeaderResponse {
	return c.HeaderResponses
}
