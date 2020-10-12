package consensus

import (
	"sort"
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
	Number uint64
}

type HeaderResponse struct {
	Header *types.Header
	Number uint64
}

type EngineProcess interface {
	HeaderVerification() chan<- VerifyHeaderRequest
	VerifyResults() <-chan VerifyHeaderResponse

	HeaderRequest() chan HeadersRequest
	HeaderResponse() chan<- HeaderResponse
}

type Process struct {
	Chain                 ChainHeaderReader
	VerifyHeaderRequests  chan VerifyHeaderRequest
	VerifyHeaderResponses chan VerifyHeaderResponse
	CleanupTicker         *time.Ticker
	HeadersRequests       chan HeadersRequest
	HeaderResponses       chan HeaderResponse

	VerifiedBlocks   *lru.Cache // common.Hash->*types.Header
	VerifiedBlocksMu sync.RWMutex

	RequestedBlocks   map[uint64]uint
	RequestedBlocksMu sync.RWMutex

	RequestsToParents map[uint64]map[uint64]*VerifyRequest // BlockNum->reqID->VerifyRequest
	RequestsMu        sync.RWMutex
}

type VerifyRequest struct {
	VerifyHeaderRequest
	Parents      []*types.Header
	ParentsCount int
	From         uint64
	To           uint64
}

const (
	size        = 65536
	storageSize = 60000
	retry       = 100 * time.Millisecond
)

func NewProcess(chain ChainHeaderReader) *Process {
	verifiedBlocks, _ := lru.New(storageSize)
	return &Process{
		Chain:                 chain,
		VerifyHeaderRequests:  make(chan VerifyHeaderRequest, size),
		VerifyHeaderResponses: make(chan VerifyHeaderResponse, size),
		CleanupTicker:         time.NewTicker(retry),
		HeadersRequests:       make(chan HeadersRequest, size),
		HeaderResponses:       make(chan HeaderResponse, size),
		VerifiedBlocks:        verifiedBlocks,
		RequestedBlocks:       make(map[uint64]uint, size),
		RequestsToParents:     make(map[uint64]map[uint64]*VerifyRequest),
	}
}

func (p *Process) GetVerifyHeader() <-chan VerifyHeaderResponse {
	return p.VerifyHeaderResponses
}

func (p *Process) HeaderRequest() chan HeadersRequest {
	return p.HeadersRequests
}

func (p *Process) HeaderResponse() chan<- HeaderResponse {
	return p.HeaderResponses
}

func (p *Process) AddVerifiedBlocks(header *types.Header) {
	p.VerifiedBlocksMu.Lock()
	defer p.VerifiedBlocksMu.Unlock()

	blockNum := header.Number.Uint64()
	blocksContainer, ok := p.VerifiedBlocks.Get(blockNum) // is already sorted
	var blocks []*types.Header
	if ok {
		blocks = blocksContainer.([]*types.Header)
	} else {
		blocks = append(blocks, header)
		p.VerifiedBlocks.Add(blockNum, blocks)
		return
	}

	if ok = SearchHeader(blocks, header.Hash()); ok {
		return
	}

	blocks = append(blocks, header)

	sort.SliceStable(blocks, func(i, j int) bool {
		return blocks[i].Hash().String() < blocks[j].Hash().String()
	})

	p.VerifiedBlocks.Add(blockNum, blocks)
}

func (p *Process) GetVerifiedBlocks(blockNum uint64) ([]*types.Header, bool) {
	p.VerifiedBlocksMu.RLock()
	defer p.VerifiedBlocksMu.RUnlock()

	h, ok := p.VerifiedBlocks.Get(blockNum)
	if !ok {
		return nil, false
	}

	headers, ok := h.([]*types.Header)
	if !ok {
		return nil, false
	}

	res := make([]*types.Header, len(headers))
	copy(res, headers)

	return res, true
}

func (p *Process) GetVerifiedBlock(blockNum uint64, hash common.Hash) bool {
	p.VerifiedBlocksMu.RLock()
	defer p.VerifiedBlocksMu.RUnlock()

	h, ok := p.VerifiedBlocks.Get(blockNum)
	if !ok {
		return false
	}

	headers, ok := h.([]*types.Header)
	if !ok {
		return false
	}

	return SearchHeader(headers, hash)
}

func SearchHeader(blocks []*types.Header, hash common.Hash) bool { //nolint:interfacer
	n := sort.Search(len(blocks), func(i int) bool {
		return blocks[i].Hash().String() >= hash.String()
	})
	if n < len(blocks) && blocks[n].Hash() == hash {
		return true
	}
	return false
}

func (p *Process) AddRequestedBlocks(num uint64) bool {
	p.RequestedBlocksMu.Lock()
	defer p.RequestedBlocksMu.Unlock()

	n, ok := p.RequestedBlocks[num]
	p.RequestedBlocks[num] = n + 1

	return ok
}

func (p *Process) DeleteRequestedBlocks(num uint64) {
	p.RequestedBlocksMu.Lock()
	defer p.RequestedBlocksMu.Unlock()
	n, ok := p.RequestedBlocks[num]
	if !ok {
		return
	}

	n--
	if n == 0 {
		delete(p.RequestedBlocks, num)
	} else {
		p.RequestedBlocks[num] = n
	}
}

func (p *Process) IsRequestedBlocks(num uint64) bool {
	p.RequestedBlocksMu.RLock()
	defer p.RequestedBlocksMu.RUnlock()
	_, ok := p.RequestedBlocks[num]
	return ok
}
