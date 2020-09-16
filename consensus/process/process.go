package process

import (
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type Consensus struct {
	consensus.Verifier
	*consensus.Process
}

func NewConsensusProcess(v consensus.Verifier, chain consensus.ChainHeaderReader) *Consensus {
	c := &Consensus{
		Verifier: v,
		Process:  consensus.NewProcess(chain),
	}

	go func() {
		// fixme how to close/cancel the goroutine
		// fixme remove records from VerifiedHeaders
		for req := range c.VerifyHeaderRequests {
			// If we're running a full engine faking, accept any input as valid
			if c.IsFake() {
				c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), nil}
				continue
			}

			// Short circuit if the header is known
			if _, ok := c.GetVerifiedBlocks(req.Header.Hash()); ok {
				c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), nil}
				continue
			}

			parent, exit := c.waitParentHeaders(req)
			if exit {
				continue
			}

			err := c.Verify(c.Process.Chain, req.Header, parent, false, req.Seal)
			c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), err}
			if err == nil {
				c.AddVerifiedBlocks(req.Header, req.Header.Hash())
			}
		}
	}()

	return c
}

func (c *Consensus) VerifyHeader() chan<- consensus.VerifyHeaderRequest {
	return c.VerifyHeaderRequests
}

func (c *Consensus) waitParentHeaders(req consensus.VerifyHeaderRequest) ([]*types.Header, bool) {
	parentsToGet := c.NeededForVerification(req.Header)
	parents := make([]*types.Header, 0, parentsToGet)
	header := req.Header

	for i := 0; i < parentsToGet; i++ {
		parent, exit := c.waitHeader(header.Hash(), header.ParentHash)
		if exit {
			return nil, true
		}

		parents = append(parents, parent)
		header = parent
	}

	return parents, false
}

func (c *Consensus) waitHeader(blockHash, parentHash common.Hash) (*types.Header, bool) {
	parent, ok := c.GetVerifiedBlocks(parentHash)
	if ok && parent == nil {
		c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{blockHash, consensus.ErrUnknownAncestor}
		return nil, true
	}

	if !ok {
		c.HeadersRequests <- consensus.HeadersRequest{parentHash}
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

	loop:
		for {
			select {

			case parentResp := <-c.HeaderResponses:
				if parentResp.Header != nil {
					c.AddVerifiedBlocks(parentResp.Header, parentResp.Header.Hash())
				}

				parent, ok = c.GetVerifiedBlocks(parentHash)
				if ok && parent == nil {
					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{blockHash, consensus.ErrUnknownAncestor}
					return nil, true
				} else if ok {
					break loop
				}

				if parentResp.Hash == blockHash {
					if parentResp.Header == nil {
						c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{blockHash, consensus.ErrUnknownAncestor}
						return nil, true
					}

					parent = parentResp.Header
					break loop
				}
			case <-ticker.C:
				parent, ok = c.GetVerifiedBlocks(parentHash)
				if !ok {
					continue
				}
				if parent == nil {
					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{blockHash, consensus.ErrUnknownAncestor}
					return nil, true
				}
				break loop
			}
		}
	}
	return parent, false
}

func (c *Consensus) VerifyHeaderResults() <-chan consensus.VerifyHeaderResponse {
	return c.VerifyHeaderResponses
}

func (c *Consensus) HeaderRequest() <-chan consensus.HeadersRequest {
	return c.HeadersRequests
}

func (c *Consensus) HeaderResponse() chan<- consensus.HeaderResponse {
	return c.HeaderResponses
}
