package process

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type Consensus struct {
	consensus.Verifier
	*consensus.Process // remote Engine
}

func NewConsensusProcess(v consensus.Verifier, chain consensus.ChainHeaderReader, exit chan struct{}) *Consensus {
	c := &Consensus{
		Verifier: v,
		Process:  consensus.NewProcess(chain),
	}

	go func() {
		// fixme remove records from VerifiedHeaders
		for {
			select {
			case <-exit:
				return
			case req := <-c.VerifyHeaderRequests:
				fmt.Println("<-c.VerifyHeaderRequests", req.Header.Number.String())
				// fixme сделать как event loop - чтобы то, что можно, то обрабатывалось сразу, остальное откладывалось
				// If we're running a full engine faking, accept any input as valid
				if c.IsFake() {
					fmt.Println("<-c.VerifyHeaderRequests fake 1")
					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), nil}
					fmt.Println("<-c.VerifyHeaderRequests fake 2")
					continue
				}

				// Short circuit if the header is known
				if _, ok := c.GetVerifiedBlocks(req.Header.Hash()); ok {
					fmt.Println("<-c.VerifyHeaderRequests ok 1")
					c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), nil}
					fmt.Println("<-c.VerifyHeaderRequests ok 2")
					continue
				}

				parents, exit := c.requestParentHeaders(req)
				if exit {
					// fixme добавить задержку? добавить id запросов?
					fmt.Println("<-c.VerifyHeaderRequests rerequest 1")
					c.VerifyHeaderRequests <- req
					fmt.Println("<-c.VerifyHeaderRequests rerequest 2")
					continue
				}

				err := c.Verify(c.Process.Chain, req.Header, parents, false, req.Seal)
				fmt.Println("<-c.VerifyHeaderRequests GOT IT 1")
				c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), err}
				fmt.Println("<-c.VerifyHeaderRequests GOT IT 2", err)
				if err == nil {
					c.AddVerifiedBlocks(req.Header, req.Header.Hash())
				}
				fmt.Println("<-c.VerifyHeaderRequests DONE")
			case parentResp := <-c.HeaderResponses:
				fmt.Println("<-c.HeaderResponses 1")
				if parentResp.Header != nil {
					fmt.Println("<-c.HeaderResponses 2")
					c.AddVerifiedBlocks(parentResp.Header, parentResp.Header.Hash())
				}
				fmt.Println("<-c.HeaderResponses DONE")
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
	parents := make([]*types.Header, 0, len(parentsToGet))
	header := req.Header

	for _, hash := range parentsToGet {
		parent, exit := c.requestHeader(header.Hash(), hash)
		if exit {
			return nil, true
		}

		parents = append(parents, parent)
		header = parent
	}

	return parents, false
}

func (c *Consensus) requestHeader(blockHash, parentHash common.Hash) (*types.Header, bool) {
	parent, ok := c.GetVerifiedBlocks(parentHash)
	if ok && parent == nil {
		c.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{blockHash, consensus.ErrUnknownAncestor}
		return nil, true
	}

	if !ok {
		// fixme - надо проверять, что мы уже запросили и ждём
		c.HeadersRequests <- consensus.HeadersRequest{parentHash}
		return nil, true
	}
	return parent, false
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
