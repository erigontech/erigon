package consensus

import (
	"fmt"
	"sort"
	"sync"
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
	HeadersRequests       chan HeadersRequest
	HeaderResponses       chan HeaderResponse

	VerifiedBlocks   *lru.Cache // common.Hash->*types.Header
	VerifiedBlocksMu sync.RWMutex

	ProcessingRequests   map[uint64]map[uint64]*VerifyRequest // reqID->blockNumber->VerifyRequest
	ProcessingRequestsMu sync.RWMutex
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

const (
	size        = 1000
	storageSize = 60000
	retry       = 100 * time.Millisecond
)

func NewAPI(config *params.ChainConfig) *API {
	verifiedBlocks, _ := lru.New(storageSize)
	return &API{
		Chain:                 configGetter{config},
		VerifyHeaderRequests:  make(chan VerifyHeaderRequest, size),
		VerifyHeaderResponses: make(chan VerifyHeaderResponse, size),
		CleanupTicker:         time.NewTicker(retry),
		HeadersRequests:       make(chan HeadersRequest, size),
		HeaderResponses:       make(chan HeaderResponse, size),
		VerifiedBlocks:        verifiedBlocks,
		ProcessingRequests:    make(map[uint64]map[uint64]*VerifyRequest, size),
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
		fmt.Println("XXX-CacheHeader-1", header.Number.Uint64(), header.Hash().String())
		p.VerifiedBlocks.Add(blockNum, []*types.Header{header})
		return
	}

	for _, h := range blocks {
		// the block is already stored
		if h.Hash() == header.Hash() {
			return
		}
	}

	blocks = append(blocks, header)

	fmt.Println("XXX-CacheHeader-2", header.Number.Uint64(), header.Hash().String())
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
		if h.Hash() == hash {
			return h
		}
	}
	return nil
}

func (p *API) PrintProcessingRequests() {
	p.ProcessingRequestsMu.RLock()
	defer p.ProcessingRequestsMu.RUnlock()

	ids := make([]uint64, 0, len(p.ProcessingRequests))
	for id := range p.ProcessingRequests {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	// reqID->blockNumber->VerifyRequest
	res := "\n***** ProcessingRequests *****\n"

	for _, id := range ids {
		reqMap := p.ProcessingRequests[id]
		res += fmt.Sprintf("request %d\n", id)

		nums := make([]uint64, 0, len(reqMap))
		for num := range reqMap {
			nums = append(nums, num)
		}
		sort.Slice(nums, func(i, j int) bool {
			return nums[i] < nums[j]
		})

		for _, num := range nums {
			req := reqMap[num]
			res += fmt.Sprintf("\tblock num=%d\n", num)

			nums := fmt.Sprintf("\t\tblocks(%d):", req.ParentsExpected)
			for _, known := range req.KnownParents {
				nums += fmt.Sprintf(" %d", known.Number.Uint64())
			}
			nums += "\n"

			res += nums
		}
	}

	fmt.Println(res)
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
