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
	return &Consensus{
		Verifier: v,
		Process:  consensus.NewProcess(chain),
	}
}

func (eth *Consensus) VerifyHeader() chan<- consensus.VerifyHeaderRequest {
	go func() {
		// fixme how to close/cancel the goroutine
		// fixme remove records from VerifiedHeaders
		req := <-eth.VerifyHeaderRequests

		// If we're running a full engine faking, accept any input as valid
		if eth.IsFake() {
			eth.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), nil}
			return
		}

		// Short circuit if the header is known
		if _, ok := eth.GetVerifiedBlocks(req.Header.Hash()); ok {
			eth.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), nil}
			return
		}

		parent, exit := eth.waitParentHeaders(req)
		if exit {
			return
		}

		err := eth.Verify(eth.Process.Chain, req.Header, parent, false, req.Seal)
		eth.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), err}
	}()

	return eth.VerifyHeaderRequests
}

func (eth *Consensus) waitParentHeaders(req consensus.VerifyHeaderRequest) ([]*types.Header, bool) {
	parentsToGet := eth.NeededForVerification(req.Header)
	parents := make([]*types.Header, 0, parentsToGet)
	header := req.Header

	for i := 0; i < parentsToGet; i++ {
		parent, exit := eth.waitHeader(header.Hash(), header.ParentHash)
		if exit {
			return nil, true
		}

		parents = append(parents, parent)
		header = parent
	}

	return parents, false
}

func (eth *Consensus) waitHeader(blockHash, parentHash common.Hash) (*types.Header, bool) {
	parent, ok := eth.GetVerifiedBlocks(parentHash)
	if ok && parent == nil {
		eth.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{blockHash, consensus.ErrUnknownAncestor}
		return nil, true
	}

	if !ok {
		eth.HeadersRequests <- consensus.HeadersRequest{parentHash}
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

	loop:
		for {
			select {

			case parentResp := <-eth.HeaderResponses:
				if parentResp.Header != nil {
					eth.AddVerifiedBlocks(parentResp.Header, parentResp.Header.Hash())
				}

				parent, ok = eth.GetVerifiedBlocks(parentHash)
				if ok && parent == nil {
					eth.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{blockHash, consensus.ErrUnknownAncestor}
					return nil, true
				} else if ok {
					break loop
				}

				if parentResp.Hash == blockHash {
					if parentResp.Header == nil {
						eth.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{blockHash, consensus.ErrUnknownAncestor}
						return nil, true
					}

					parent = parentResp.Header
					break loop
				}
			case <-ticker.C:
				parent, ok = eth.GetVerifiedBlocks(parentHash)
				if !ok {
					continue
				}
				if parent == nil {
					eth.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{blockHash, consensus.ErrUnknownAncestor}
					return nil, true
				}
				break loop
			}
		}
	}
	return parent, false
}

func (eth *Consensus) GetVerifyHeader() <-chan consensus.VerifyHeaderResponse {
	return eth.VerifyHeaderResponses
}

func (eth *Consensus) HeaderRequest() <-chan consensus.HeadersRequest {
	return eth.HeadersRequests
}

func (eth *Consensus) HeaderResponse() chan<- consensus.HeaderResponse {
	return eth.HeaderResponses
}
