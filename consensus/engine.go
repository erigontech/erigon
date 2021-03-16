package consensus

import (
	"sync"
	"sync/atomic"
	"time"

	lru "github.com/hashicorp/golang-lru"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/params"
)

type VerifyHeaderRequest struct {
	ID       uint64
	Headers  []*types.Header
	Seal     []bool
	Deadline *time.Time
}

type VerifyHeaderResponse struct {
	ID   uint64
	Hash common.Hash
	Err  error
}

type HeadersRequest struct {
	ID                 uint64
	HighestHash        common.Hash
	HighestBlockNumber uint64
	Number             uint64
}

type HeaderResponse struct {
	ID      uint64
	Headers []*types.Header
	BlockError
}

type BlockError struct {
	Hash   common.Hash
	Number uint64
	Err    error
}

type EngineAPI interface {
	HeaderVerification() chan<- VerifyHeaderRequest
	VerifyResults() <-chan VerifyHeaderResponse

	HeaderRequest() <-chan HeadersRequest
	HeaderResponse() chan<- HeaderResponse
}

type API struct {
	Chain                 ChainHeaderReader
	VerifyHeaderRequests  chan VerifyHeaderRequest
	VerifyHeaderResponses chan VerifyHeaderResponse
	CleanupTicker         *time.Ticker
	CleanupCh             chan FinishedRequest
	HeadersRequests       chan HeadersRequest
	HeaderResponses       chan HeaderResponse

	VerifiedBlocks   *lru.Cache // blockNumber->*types.Header
	VerifiedBlocksMu sync.RWMutex

	ProcessingRequests   map[uint64]*RequestStorage // reqID->blockNumber->*VerifyRequest
	ProcessingRequestsMu sync.RWMutex
}

type RequestStorage struct {
	Storage map[uint64]*VerifyRequest // blockNumber->VerifyRequest
	l       uint64
	sync.RWMutex
}

func NewRequestStorage() *RequestStorage {
	return &RequestStorage{make(map[uint64]*VerifyRequest), 0, sync.RWMutex{}}
}

func (r *RequestStorage) Len() uint64 {
	return atomic.LoadUint64(&r.l)
}

func (r *RequestStorage) Get(n uint64) (*VerifyRequest, bool) {
	r.RLock()
	req, ok := r.Storage[n]
	r.RUnlock()
	return req, ok
}

func (r *RequestStorage) Has(n uint64) bool {
	r.RLock()
	_, ok := r.Storage[n]
	r.RUnlock()
	return ok
}

func (r *RequestStorage) Remove(n uint64) {
	r.Lock()
	delete(r.Storage, n)
	atomic.AddUint64(&r.l, ^uint64(0))
	r.Unlock()
}

func (r *RequestStorage) Add(n uint64, req *VerifyRequest) {
	r.Lock()
	r.Storage[n] = req
	atomic.AddUint64(&r.l, 1)
	r.Unlock()
}

type VerifyRequest struct {
	ID              uint64
	Header          *types.Header
	Seal            bool
	Deadline        *time.Time
	KnownParents    []*types.Header
	ParentsExpected int
	From            uint64
	To              uint64
}

type FinishedRequest struct {
	ReqID       uint64
	BlockNumber uint64
}

const (
	size        = 1000
	storageSize = 60000
	retry       = time.Second
)

func NewAPI(config *params.ChainConfig) *API {
	verifiedBlocks, _ := lru.New(storageSize)
	return &API{
		Chain:                 configGetter{config},
		VerifyHeaderRequests:  make(chan VerifyHeaderRequest, size),
		VerifyHeaderResponses: make(chan VerifyHeaderResponse, size),
		CleanupTicker:         time.NewTicker(retry),
		CleanupCh:             make(chan FinishedRequest, size),
		HeadersRequests:       make(chan HeadersRequest, size),
		HeaderResponses:       make(chan HeaderResponse, size),
		VerifiedBlocks:        verifiedBlocks,
		ProcessingRequests:    make(map[uint64]*RequestStorage, size),
	}
}

func (p *API) GetVerifyHeader() <-chan VerifyHeaderResponse {
	return p.VerifyHeaderResponses
}

func (p *API) HeaderRequest() <-chan HeadersRequest {
	return p.HeadersRequests
}

func (p *API) HeaderResponse() chan<- HeaderResponse {
	return p.HeaderResponses
}

func (p *API) CacheHeader(header *types.Header) {
	if header == nil {
		return
	}

	p.VerifiedBlocksMu.Lock()
	defer p.VerifiedBlocksMu.Unlock()

	blockNum := header.Number.Uint64()
	blocksContainer, ok := p.VerifiedBlocks.Get(blockNum)
	blocks, blocksOk := blocksContainer.([]*types.Header)
	if !ok || !blocksOk || len(blocks) == 0 {
		// single header by a block number case
		p.VerifiedBlocks.Add(blockNum, []*types.Header{header})
		return
	}

	for _, h := range blocks {
		// the block is already stored
		if h.HashCache() == header.HashCache() {
			return
		}
	}

	blocks = append(blocks, header)

	p.VerifiedBlocks.Add(blockNum, blocks)
}

func (p *API) GetCachedHeader(hash common.Hash, blockNum uint64) *types.Header {
	p.VerifiedBlocksMu.RLock()
	defer p.VerifiedBlocksMu.RUnlock()

	h, ok := p.VerifiedBlocks.Get(blockNum)
	if !ok {
		return nil
	}

	headers, ok := h.([]*types.Header)
	if !ok {
		return nil
	}

	for _, h := range headers {
		if h.HashCache() == hash {
			return h
		}
	}
	return nil
}

type configGetter struct {
	config *params.ChainConfig
}

func (c configGetter) Config() *params.ChainConfig {
	return c.config
}

func (c configGetter) CurrentHeader() *types.Header {
	panic("should not be used")
}

func (c configGetter) GetHeader(_ common.Hash, _ uint64) *types.Header {
	panic("should not be used")
}

func (c configGetter) GetHeaderByNumber(_ uint64) *types.Header {
	panic("should not be used")
}

func (c configGetter) GetHeaderByHash(_ common.Hash) *types.Header {
	panic("should not be used")
}
