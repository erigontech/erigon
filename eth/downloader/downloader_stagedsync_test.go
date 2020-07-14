package downloader

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"sync"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/state"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/trie"
)

type stagedSyncTester struct {
	downloader *Downloader
	db         ethdb.Database
	peers      map[string]*stagedSyncTesterPeer
	genesis    *types.Block
	lock       sync.RWMutex
}

func newStagedSyncTester() (*stagedSyncTester, func()) {
	tester := &stagedSyncTester{
		peers:   make(map[string]*stagedSyncTesterPeer),
		genesis: testGenesis,
	}
	tester.db = ethdb.NewMemDatabase()
	// This needs to match the genesis in the file testchain_test.go
	tester.genesis = core.GenesisBlockForTesting(tester.db, testAddress, big.NewInt(1000000000))
	rawdb.WriteTd(tester.db, tester.genesis.Hash(), tester.genesis.NumberU64(), tester.genesis.Difficulty())
	rawdb.WriteBlock(context.Background(), tester.db, testGenesis)
	tester.downloader = New(uint64(StagedSync), tester.db, trie.NewSyncBloom(1, tester.db), new(event.TypeMux), params.TestChainConfig, tester, nil, tester.dropPeer, ethdb.DefaultStorageMode)
	clear := func() {
		tester.db.Close()
	}
	return tester, clear
}

// newPeer registers a new block download source into the downloader.
func (st *stagedSyncTester) newPeer(id string, version int, chain *testChain) error {
	st.lock.Lock()
	defer st.lock.Unlock()

	peer := &stagedSyncTesterPeer{st: st, id: id, chain: chain}
	st.peers[id] = peer
	return st.downloader.RegisterPeer(id, version, peer)
}

// dropPeer simulates a hard peer removal from the connection pool.
func (st *stagedSyncTester) dropPeer(id string) {
	st.lock.Lock()
	defer st.lock.Unlock()

	delete(st.peers, id)
	//nolint:errcheck
	st.downloader.UnregisterPeer(id)
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
	hash := rawdb.ReadHeadHeaderHash(st.db)
	number := rawdb.ReadHeaderNumber(st.db, hash)
	return rawdb.ReadHeader(st.db, hash, *number)
}

// ExecuteBlockEphemerally is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) ExecuteBlockEphemerally(_ *types.Block, _ state.StateReader, _ *state.DbStateWriter) error {
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
	hash := rawdb.ReadCanonicalHash(st.db, number)
	return rawdb.ReadBlock(st.db, hash, number)
}

// GetHeaderByHash is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) GetHeaderByHash(hash common.Hash) *types.Header {
	number := rawdb.ReadHeaderNumber(st.db, hash)
	return rawdb.ReadHeader(st.db, hash, *number)
}

// GetTd is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) GetTd(hash common.Hash, number uint64) *big.Int {
	st.lock.RLock()
	defer st.lock.RUnlock()
	return rawdb.ReadTd(st.db, hash, number)
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
	return rawdb.HasHeader(st.db, hash, number)
}

// InsertBodyChain is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) InsertBodyChain(_ context.Context, blocks types.Blocks) (bool, error) {
	st.lock.Lock()
	defer st.lock.Unlock()
	for _, block := range blocks {
		rawdb.WriteBlock(context.Background(), st.db, block)
	}
	return false, nil
}

// InsertChain is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) InsertChain(_ context.Context, blocks types.Blocks) (i int, err error) {
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
}

// Rollback is part of the implementation of BlockChain interface defined in downloader.go
func (st *stagedSyncTester) Rollback(hashes []common.Hash) {
	fmt.Printf("Rollback %d\n", len(hashes))
	panic("")
}

func (st *stagedSyncTester) Engine() consensus.Engine {
	return ethash.NewFaker()
}

func (st *stagedSyncTester) GetHeader(common.Hash, uint64) *types.Header {
	panic("")
}

func (st *stagedSyncTester) GetVMConfig() *vm.Config {
	return &vm.Config{}
}

// sync starts synchronizing with a remote peer, blocking until it completes.
func (st *stagedSyncTester) sync(id string, td *big.Int) error {
	st.lock.RLock()
	hash := st.peers[id].chain.headBlock().Hash()
	// If no particular TD was requested, load from the peer's blockchain
	if td == nil {
		td = st.peers[id].chain.td(hash)
	}
	st.lock.RUnlock()

	// Synchronise with the chosen peer and ensure proper cleanup afterwards
	err := st.downloader.synchronise(id, hash, td, StagedSync, vm.NewDestsCache(100), getTestTxPoolControl())
	select {
	case <-st.downloader.cancelCh:
		// Ok, downloader fully cancelled after sync cycle
	default:
		// Downloader is still accepting packets, can block a peer up
		panic("downloader active post sync cycle") // panic will be caught by tester
	}
	return err
}

func (st *stagedSyncTester) Stop() {
}

type stagedSyncTesterPeer struct {
	st    *stagedSyncTester
	id    string
	chain *testChain
}

// Head is part of the implementation of Peer interface in peer.go
func (stp *stagedSyncTesterPeer) Head() (common.Hash, *big.Int) {
	b := stp.chain.headBlock()
	return b.Hash(), stp.chain.td(b.Hash())
}

// RequestHeadersByHash is part of the implementation of Peer interface in peer.go
func (stp *stagedSyncTesterPeer) RequestHeadersByHash(origin common.Hash, amount int, skip int, reverse bool) error {
	if reverse {
		panic("reverse header requests not supported")
	}

	result := stp.chain.headersByHash(origin, amount, skip)
	return stp.st.downloader.DeliverHeaders(stp.id, result)
}

// RequestHeadersByNumber is part of the implementation of Peer interface in peer.go
func (stp *stagedSyncTesterPeer) RequestHeadersByNumber(origin uint64, amount int, skip int, reverse bool) error {
	if reverse {
		panic("reverse header requests not supported")
	}

	result := stp.chain.headersByNumber(origin, amount, skip)
	return stp.st.downloader.DeliverHeaders(stp.id, result)
}

// RequestBodies is part of the implementation of Peer interface in peer.go
func (stp *stagedSyncTesterPeer) RequestBodies(hashes []common.Hash) error {
	txs, uncles := stp.chain.bodies(hashes)
	return stp.st.downloader.DeliverBodies(stp.id, txs, uncles)
}

// RequestReceipts is part of the implementation of Peer interface in peer.go
func (stp *stagedSyncTesterPeer) RequestReceipts(hashes []common.Hash) error {
	panic("")
}

func TestStagedBase(t *testing.T) {
	core.UsePlainStateExecution = true // Stage5 unwinds do not support hashed state
	// Same as testChainForkLightA but much shorter
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	tester, clear := newStagedSyncTester()
	defer clear()
	if err := tester.newPeer("peer", 65, testChainBase); err != nil {
		t.Fatal(err)
	}
	if err := tester.sync("peer", nil); err != nil {
		t.Fatal(err)
	}
	currentHeader := tester.CurrentHeader()
	expectedHash := testChainBase.chain[len(testChainBase.chain)-1]
	if int(currentHeader.Number.Uint64()) != len(testChainBase.chain)-1 {
		t.Errorf("last block expected number %d, got %d", len(testChainBase.chain)-1, currentHeader.Number.Uint64())
	}
	if currentHeader.Hash() != expectedHash {
		t.Errorf("last block expected hash %x, got %x", expectedHash, currentHeader.Hash())
	}
}

func TestUnwind(t *testing.T) {
	core.UsePlainStateExecution = true // Stage5 unwinds do not support hashed state
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	tester, clear := newStagedSyncTester()
	defer clear()
	if err := tester.newPeer("peer", 65, testChainForkLightA); err != nil {
		t.Fatal(err)
	}
	if err := tester.sync("peer", nil); err != nil {
		t.Fatal(err)
	}
	fmt.Println("sync heavy")
	if err := tester.newPeer("forkpeer", 65, testChainForkHeavy); err != nil {
		t.Fatal(err)
	}
	if err := tester.sync("forkpeer", nil); err != nil {
		t.Fatal(err)
	}
	// Need to call sync twice, because the first call is terminated by the unwinding
	if err := tester.sync("forkpeer", nil); err != nil {
		t.Fatal(err)
	}
	currentHeader := tester.CurrentHeader()
	expectedHash := testChainForkHeavy.chain[len(testChainForkHeavy.chain)-1]
	if int(currentHeader.Number.Uint64()) != len(testChainForkHeavy.chain)-1 {
		t.Errorf("last block expected number %d, got %d", len(testChainForkHeavy.chain)-1, currentHeader.Number.Uint64())
	}
	if currentHeader.Hash() != expectedHash {
		t.Errorf("last block expected hash %x, got %x", expectedHash, currentHeader.Hash())
	}
}

func getTestTxPoolControl() *stagedsync.TxPoolStartStopper {
	return &stagedsync.TxPoolStartStopper{
		Start: func() error { return nil },
		Stop:  func() error { return nil },
	}
}
