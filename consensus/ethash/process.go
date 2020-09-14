package ethash

import (
	"time"

	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type EthashProcess struct {
	Ethash
	*consensus.Process
}

func NewEthashProcess(eth Ethash, chain consensus.ChainHeaderReader) *EthashProcess {
	return &EthashProcess{
		Ethash:  eth,
		Process: consensus.NewProcess(chain),
	}
}

func (eth *EthashProcess) VerifyHeader() chan<- consensus.VerifyHeaderRequest {
	go func() {
		// fixme how to close/cancel the goroutine
		// fixme remove records from VerifiedHeaders
		req := <-eth.VerifyHeaderRequests

		// If we're running a full engine faking, accept any input as valid
		if eth.config.PowMode == ModeFullFake {
			eth.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), nil}
			return
		}

		// Short circuit if the header is known
		if _, ok := eth.GetVerifiedBlocks(req.Header.Hash()); ok {
			eth.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), nil}
			return
		}

		parent, exit := eth.waitHeader(req)
		if exit {
			return
		}

		err := eth.verifyHeader(eth.Process.Chain, req.Header, parent, false, req.Seal)
		eth.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), err}
	}()

	return eth.VerifyHeaderRequests
}

func (eth *EthashProcess) waitHeader(req consensus.VerifyHeaderRequest) (*types.Header, bool) {
	parent, ok := eth.GetVerifiedBlocks(req.Header.ParentHash)
	if ok && parent == nil {
		eth.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), consensus.ErrUnknownAncestor}
		return nil, true
	}

	if !ok {
		eth.HeadersRequests <- consensus.HeadersRequest{req.Header.ParentHash}
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

	loop:
		for {
			select {

			case parentResp := <-eth.HeaderResponses:
				if parentResp.Header != nil {
					eth.AddVerifiedBlocks(parentResp.Header, parentResp.Header.Hash())
				}

				parent, ok = eth.GetVerifiedBlocks(req.Header.ParentHash)
				if ok && parent == nil {
					eth.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), consensus.ErrUnknownAncestor}
					return nil, true
				} else if ok {
					break loop
				}

				if parentResp.Hash == req.Header.Hash() {
					if parentResp.Header == nil {
						eth.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), consensus.ErrUnknownAncestor}
						return nil, true
					}

					parent = parentResp.Header
					break loop
				}
			case <-ticker.C:
				parent, ok = eth.GetVerifiedBlocks(req.Header.ParentHash)
				if !ok {
					continue
				}
				if parent == nil {
					eth.VerifyHeaderResponses <- consensus.VerifyHeaderResponse{req.Header.Hash(), consensus.ErrUnknownAncestor}
					return nil, true
				}
				break loop
			}
		}
	}
	return parent, false
}

func (eth *EthashProcess) GetVerifyHeader() <-chan consensus.VerifyHeaderResponse {
	return eth.VerifyHeaderResponses
}

func (eth *EthashProcess) HeaderRequest() <-chan consensus.HeadersRequest {
	return eth.HeadersRequests
}

func (eth *EthashProcess) HeaderResponse() chan<- consensus.HeaderResponse {
	return eth.HeaderResponses
}
