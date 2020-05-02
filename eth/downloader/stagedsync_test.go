package downloader

import (
	"context"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/trie"
	"math/big"
	"sync"
	"testing"
)

type stagedSyncTester struct {
	downloader *Downloader
	db         ethdb.Database
	peers      map[string]*stagedSyncTesterPeer
	lock       sync.RWMutex
}

type stagedSyncTesterPeer struct {
	st    *stagedSyncTester
	id    string
	lock  sync.RWMutex
	chain *stagedTestChain
}

type stagedTestChain struct {
	db       ethdb.Database
	genesis  *types.Block
	chain    []common.Hash
	headerm  map[common.Hash]*types.Header
	blockm   map[common.Hash]*types.Block
	receiptm map[common.Hash][]*types.Receipt
	tdm      map[common.Hash]*big.Int
	cpyLock  sync.Mutex
}

func newStagedSyncTester() *stagedSyncTester {
	tester := &stagedSyncTester{}
	tester.db = ethdb.NewMemDatabase()
	tester.downloader = New(uint64(StagedSync), tester.db, trie.NewSyncBloom(1, tester.db), new(event.TypeMux), tester, nil, tester.dropPeer)
	return tester
}

// dropPeer simulates a hard peer removal from the connection pool.
func (st *stagedSyncTester) dropPeer(id string) {
	st.lock.Lock()
	defer st.lock.Unlock()

	delete(st.peers, id)
	st.downloader.UnregisterPeer(id)
}

// Config is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) Config() *params.ChainConfig {
	panic("")
}

// CurrentBlock is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) CurrentBlock() *types.Block {
	panic("")
}

// CurrentFastBlock is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) CurrentFastBlock() *types.Block {
	panic("")
}

// CurrentHeader is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) CurrentHeader() *types.Header {
	panic("")
}

// ExecuteBlockEuphemerally is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) ExecuteBlockEuphemerally(_ *types.Block, _ state.StateReader, _ *state.DbStateWriter) error {
	panic("")
}

// FastSyncCommitHead is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) FastSyncCommitHead(hash common.Hash) error {
	panic("")
}

// GetBlockByHash is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) GetBlockByHash(hash common.Hash) *types.Block {
	panic("")
}

// GetBlockByNumber is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) GetBlockByNumber(number uint64) *types.Block {
	panic("")
}

// GetHeaderByHash is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) GetHeaderByHash(hash common.Hash) *types.Header {
	panic("")
}

// GetTd is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) GetTd(hash common.Hash, number uint64) *big.Int {
	panic("")
}

// HasBlock is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) HasBlock(hash common.Hash, number uint64) bool {
	panic("")
}

// HasFastBlock is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) HasFastBlock(hash common.Hash, number uint64) bool {
	panic("")
}

// HasHeader is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) HasHeader(hash common.Hash, number uint64) bool {
	panic("")
}

// InsertBodyChain is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) InsertBodyChain(_ context.Context, blocks types.Blocks) (i int, err error) {
	panic("")
}

// InsertChain is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) InsertChain(_ context.Context, blocks types.Blocks) (i int, err error) {
	panic("")
}

// InsertHeaderChainStaged is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) InsertHeaderChainStaged([]*types.Header, int) (int, bool, uint64, error) {
	panic("")
}

// InsertHeaderChain is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) InsertHeaderChain(headers []*types.Header, checkFreq int) (i int, err error) {
	panic("")
}

// InsertReceiptChain is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) InsertReceiptChain(blocks types.Blocks, receipts []types.Receipts, ancientLimit uint64) (i int, err error) {
	panic("")
}

// NotifyHeightKnownBlock is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) NotifyHeightKnownBlock(_ uint64) {
	panic("")
}

// Rollback is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) Rollback(hashes []common.Hash) {
	panic("")
}

func TestUnwind(t *testing.T) {
	newStagedSyncTester()
}
