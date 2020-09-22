package consensus

import (
	"sync"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type VerifyHeaderRequest struct {
	ID       uint64
	Header   *types.Header
	Seal     bool
	Deadline *time.Time
}

type VerifyHeaderResponse struct {
	ID   uint64
	Hash common.Hash
	Err  error
}

type HeadersRequest struct {
	Hash common.Hash
}

type HeaderResponse struct {
	Header *types.Header
	Hash   common.Hash
}

type EngineProcess interface {
	HeaderVerification() chan<- VerifyHeaderRequest
	VerifyResults() chan VerifyHeaderResponse

	HeaderRequest() <-chan HeadersRequest
	HeaderResponse() chan<- HeaderResponse
}

type Process struct {
	Chain                 ChainHeaderReader
	VerifyHeaderRequests  chan VerifyHeaderRequest
	VerifyHeaderResponses chan VerifyHeaderResponse
	HeadersRequests       chan HeadersRequest
	HeaderResponses       chan HeaderResponse

	VerifiedBlocks   map[common.Hash]*types.Header
	VerifiedBlocksMu sync.RWMutex

	RequestedBlocks   map[common.Hash]struct{}
	RequestedBlocksMu sync.RWMutex
}

const size = 128

func NewProcess(chain ChainHeaderReader) *Process {
	return &Process{
		Chain:                 chain,
		VerifyHeaderRequests:  make(chan VerifyHeaderRequest, size),
		VerifyHeaderResponses: make(chan VerifyHeaderResponse, size),
		HeadersRequests:       make(chan HeadersRequest, size),
		HeaderResponses:       make(chan HeaderResponse, size),
		VerifiedBlocks:        make(map[common.Hash]*types.Header, size),
		RequestedBlocks:       make(map[common.Hash]struct{}, size),
	}
}

func (p *Process) GetVerifyHeader() <-chan VerifyHeaderResponse {
	return p.VerifyHeaderResponses
}

func (p *Process) HeaderRequest() <-chan HeadersRequest {
	return p.HeadersRequests
}

func (p *Process) HeaderResponse() chan<- HeaderResponse {
	return p.HeaderResponses
}

func (p *Process) AddVerifiedBlocks(header *types.Header, hash common.Hash) {
	p.VerifiedBlocksMu.Lock()
	defer p.VerifiedBlocksMu.Unlock()
	p.VerifiedBlocks[hash] = header
}

func (p *Process) GetVerifiedBlocks(hash common.Hash) (*types.Header, bool) {
	p.VerifiedBlocksMu.RLock()
	defer p.VerifiedBlocksMu.RUnlock()
	h, ok := p.VerifiedBlocks[hash]
	return h, ok
}

func (p *Process) DeleteVerifiedBlocks(hash common.Hash) {
	p.VerifiedBlocksMu.Lock()
	defer p.VerifiedBlocksMu.Unlock()
	delete(p.VerifiedBlocks, hash)
}

func (p *Process) AddRequestedBlocks(hash common.Hash) bool {
	p.RequestedBlocksMu.Lock()
	defer p.RequestedBlocksMu.Unlock()
	_, ok := p.RequestedBlocks[hash]
	if ok {
		return true
	}
	p.RequestedBlocks[hash] = struct{}{}
	return false
}

func (p *Process) DeleteRequestedBlocks(hash common.Hash) {
	p.RequestedBlocksMu.Lock()
	defer p.RequestedBlocksMu.Unlock()
	delete(p.RequestedBlocks, hash)
}
