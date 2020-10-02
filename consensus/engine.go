package consensus

import (
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru"

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
	Chain                     ChainHeaderReader
	VerifyHeaderRequests      chan VerifyHeaderRequest
	RetryVerifyHeaderRequests chan VerifyHeaderRequest
	RetryVerifyTicker         *time.Ticker
	VerifyHeaderResponses     chan VerifyHeaderResponse
	HeadersRequests           chan HeadersRequest
	HeaderResponses           chan HeaderResponse

	VerifiedBlocks *lru.Cache // common.Hash->*types.Header

	RequestedBlocks   map[common.Hash]struct{}
	RequestedBlocksMu sync.RWMutex
}

const (
	size        = 65536
	storageSize = 60000
	retry       = 10 * time.Millisecond
)

func NewProcess(chain ChainHeaderReader) *Process {
	verifiedBlocks, _ := lru.New(storageSize)
	return &Process{
		Chain:                     chain,
		VerifyHeaderRequests:      make(chan VerifyHeaderRequest, size),
		RetryVerifyHeaderRequests: make(chan VerifyHeaderRequest, size),
		RetryVerifyTicker:         time.NewTicker(retry),
		VerifyHeaderResponses:     make(chan VerifyHeaderResponse, size),
		HeadersRequests:           make(chan HeadersRequest, size),
		HeaderResponses:           make(chan HeaderResponse, size),
		VerifiedBlocks:            verifiedBlocks,
		RequestedBlocks:           make(map[common.Hash]struct{}, size),
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
	p.VerifiedBlocks.Add(hash, header)
}

func (p *Process) GetVerifiedBlocks(hash common.Hash) (*types.Header, bool) {
	h, ok := p.VerifiedBlocks.Get(hash)
	if !ok {
		return nil, false
	}

	header, ok := h.(*types.Header)
	if !ok {
		return nil, false
	}

	return header, true
}

func (p *Process) DeleteVerifiedBlocks(hash common.Hash) {
	p.VerifiedBlocks.Remove(hash)
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
