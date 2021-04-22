// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package eth

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/downloader"
	"github.com/ledgerwatch/turbo-geth/eth/fetcher"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	proto_txpool "github.com/ledgerwatch/turbo-geth/gointerfaces/txpool"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/params"
)

const (
	// txChanSize is the size of channel listening to NewTxsEvent.
	// The number is referenced from the size of tx pool.
	txChanSize = 4096
)

// txPool defines the methods needed from a transaction pool implementation to
// support all the operations needed by the Ethereum chain protocols.
type txPool interface {
	// Pending should return pending transactions.
	// The slice should be modifiable by the caller.
	Pending() (types.TransactionsGroupedBySender, error)

	// SubscribeNewTxsEvent should return an event subscription of
	// NewTxsEvent and send events to the given channel.
	SubscribeNewTxsEvent(chan<- core.NewTxsEvent) event.Subscription
}

// handlerConfig is the collection of initialization parameters to create a full
// node network handler.
type handlerConfig struct {
	Database    ethdb.Database // Database for direct sync insertions
	ChainConfig *params.ChainConfig
	vmConfig    *vm.Config
	genesis     *types.Block
	engine      consensus.Engine
	TxPool      txPool // Transaction pool to propagate from
	TxPool2     eth.TxPool
	Network     uint64                    // Network identifier to adfvertise
	BloomCache  uint64                    // Megabytes to alloc for fast sync bloom
	Checkpoint  *params.TrustedCheckpoint // Hard coded checkpoint for sync challenges
	Whitelist   map[uint64]common.Hash    // Hard coded whitelist for sync challenged
}

type handler struct {
	networkID  uint64
	forkFilter forkid.Filter // Fork ID filter, constant across the lifetime of the node

	acceptTxs uint32 // Flag whether we're considered synchronised (enables transaction processing)

	database    ethdb.Database
	txpool      txPool
	txpool2     eth.TxPool
	chainConfig *params.ChainConfig
	vmConfig    *vm.Config
	genesis     *types.Block
	engine      consensus.Engine
	maxPeers    int

	downloader   *downloader.Downloader
	blockFetcher *fetcher.BlockFetcher
	txFetcher    *fetcher.TxFetcher
	peers        *peerSet

	txsCh  chan core.NewTxsEvent
	txsSub event.Subscription

	whitelist map[uint64]common.Hash

	// channels for fetcher, syncer, txsyncLoop
	txsyncCh chan *txsync
	quitSync chan struct{}

	chainSync *chainSyncer
	wg        sync.WaitGroup
	peerWG    sync.WaitGroup

	tmpdir        string
	batchSize     datasize.ByteSize
	stagedSync    *stagedsync.StagedSync
	currentHeight uint64 // Atomic variable to contain chain height
}

// newHandler returns a handler for all Ethereum chain management protocol.
func newHandler(config *handlerConfig) (*handler, error) { //nolint:unparam
	h := &handler{
		networkID:   config.Network,
		database:    config.Database,
		txpool:      config.TxPool,
		txpool2:     config.TxPool2,
		chainConfig: config.ChainConfig,
		vmConfig:    config.vmConfig,
		genesis:     config.genesis,
		engine:      config.engine,
		peers:       newPeerSet(),
		whitelist:   config.Whitelist,
		txsyncCh:    make(chan *txsync),
		quitSync:    make(chan struct{}),
	}
	if headHeight, err := stages.GetStageProgress(config.Database, stages.Finish); err == nil {
		h.currentHeight = headHeight
	} else {
		return nil, fmt.Errorf("could not get Finish stage progress: %v", err)
	}
	heighter := func() uint64 {
		return atomic.LoadUint64(&h.currentHeight)
	}
	h.forkFilter = forkid.NewFilter(config.ChainConfig, config.genesis.Hash(), heighter)
	// Construct the downloader (long sync) and its backing state bloom if fast
	// sync is requested. The downloader is responsible for deallocating the state
	// bloom when it's done.
	sm, err := ethdb.GetStorageModeFromDB(config.Database)
	if err != nil {
		log.Error("Get storage mode", "err", err)
	}
	h.downloader = downloader.New(config.Database, config.ChainConfig, config.engine, config.vmConfig, h.removePeer, sm)
	h.downloader.SetTmpDir(h.tmpdir)
	h.downloader.SetBatchSize(h.batchSize)

	// Construct the fetcher (short sync)
	validator := func(header *types.Header) error {
		return h.engine.VerifyHeader(stagedsync.ChainReader{Cfg: h.chainConfig, Db: h.database}, header, true)
	}
	inserter := func(blocks types.Blocks) (int, error) {
		if err == nil {
			atomic.StoreUint32(&h.acceptTxs, 1) // Mark initial sync done on any fetcher import
		}
		return 0, err
	}
	getBlockByHash := func(hash common.Hash) *types.Block { b, _ := rawdb.ReadBlockByHash(h.database, hash); return b }
	h.blockFetcher = fetcher.NewBlockFetcher(nil, getBlockByHash, validator, h.BroadcastBlock, heighter, nil, inserter, h.removePeer)

	fetchTx := func(peer string, hashes []common.Hash) error {
		p := h.peers.peer(peer)
		if p == nil {
			return errors.New("unknown peer")
		}
		return p.RequestTxs(hashes)
	}
	findUnknownTxs := func(hashes common.Hashes) (common.Hashes, error) {
		reply, err2 := h.txpool2.FindUnknownTransactions(context.Background(), &proto_txpool.TxHashes{Hashes: gointerfaces.ConvertHashesToH256(hashes)})
		if err2 != nil {
			return nil, err2
		}
		return gointerfaces.ConvertH256ToHashes(reply.Hashes), nil
	}
	importTxs := func(txs [][]byte) ([]proto_txpool.ImportResult, error) {
		reply, err2 := h.txpool2.ImportTransactions(context.Background(), &proto_txpool.ImportRequest{Txs: txs})
		if err2 != nil {
			return nil, err2
		}
		return reply.Imported, nil
	}
	h.txFetcher = fetcher.NewTxFetcher(findUnknownTxs, importTxs, fetchTx)
	h.chainSync = newChainSyncer(h)
	return h, nil
}

func (h *handler) SetTmpDir(tmpdir string) {
	h.tmpdir = tmpdir
	if h.downloader != nil {
		h.downloader.SetTmpDir(tmpdir)
	}
}

func (h *handler) SetBatchSize(batchSize datasize.ByteSize) {
	h.batchSize = batchSize
	if h.downloader != nil {
		h.downloader.SetBatchSize(batchSize)
	}
}

func (h *handler) SetStagedSync(stagedSync *stagedsync.StagedSync) {
	h.stagedSync = stagedSync
	if h.downloader != nil {
		h.downloader.SetStagedSync(stagedSync)
	}
}

// runEthPeer registers an eth peer into the joint eth/snap peerset, adds it to
// various subsistems and starts handling messages.
func (h *handler) runEthPeer(peer *eth.Peer, handler eth.Handler) error {
	// TODO(karalabe): Not sure why this is needed
	if !h.chainSync.handlePeerEvent() {
		return p2p.DiscQuitting
	}
	h.peerWG.Add(1)
	defer h.peerWG.Done()

	// Execute the Ethereum handshake
	var (
		genesis = h.genesis
		number  = atomic.LoadUint64(&h.currentHeight)
	)
	hash, err := rawdb.ReadCanonicalHash(h.database, number)
	if err != nil {
		return fmt.Errorf("reading canonical hash for %d: %v", number, err)
	}
	td, err1 := rawdb.ReadTd(h.database, hash, number)
	if err1 != nil {
		return fmt.Errorf("reading td for %d %x: %v", number, hash, err1)
	}
	forkID := forkid.NewID(h.chainConfig, genesis.Hash(), number)
	if err := peer.Handshake(h.networkID, td, hash, genesis.Hash(), forkID, h.forkFilter); err != nil {
		peer.Log().Debug("Ethereum handshake failed", "err", err)
		return err
	}
	reject := false // reserved peer slots
	// Ignore maxPeers if this is a trusted peer
	if !peer.Peer.Info().Network.Trusted {
		if reject || h.peers.len() >= h.maxPeers {
			return p2p.DiscTooManyPeers
		}
	}
	peer.Log().Debug("Ethereum peer connected", "name", peer.Name())

	// Register the peer locally
	if err := h.peers.registerPeer(peer); err != nil {
		peer.Log().Error("Ethereum peer registration failed", "err", err)
		return err
	}
	defer h.removePeer(peer.ID())

	p := h.peers.peer(peer.ID())
	if p == nil {
		return errors.New("peer dropped during handling")
	}
	// Register the peer in the downloader. If the downloader considers it banned, we disconnect
	if err := h.downloader.RegisterPeer(peer.ID(), peer.Version(), peer); err != nil {
		peer.Log().Error("Failed to register peer in eth syncer", "err", err)
		return err
	}
	h.chainSync.handlePeerEvent()

	// Propagate existing transactions. new transactions appearing
	// after this will be sent via broadcasts.
	h.syncTransactions(peer)

	// If we have any explicit whitelist block hashes, request them
	for number := range h.whitelist {
		if err := peer.RequestHeadersByNumber(number, 1, 0, false); err != nil {
			return err
		}
	}
	// Handle incoming messages until the connection is torn down
	return handler(peer)
}

// removePeer unregisters a peer from the downloader and fetchers, removes it from
// the set of tracked peers and closes the network connection to it.
func (h *handler) removePeer(id string) {
	// Create a custom logger to avoid printing the entire id
	var logger log.Logger
	if len(id) < 16 {
		// Tests use short IDs, don't choke on them
		logger = log.New("peer", id)
	} else {
		logger = log.New("peer", id[:8])
	}
	// Abort if the peer does not exist
	peer := h.peers.peer(id)
	if peer == nil {
		logger.Error("Ethereum peer removal failed", "err", errPeerNotRegistered)
		return
	}
	// Remove the `eth` peer if it exists
	logger.Debug("Removing Ethereum peer")

	//nolint:errcheck
	h.downloader.UnregisterPeer(id)
	//nolint:errcheck
	h.txFetcher.Drop(id)

	if err := h.peers.unregisterPeer(id); err != nil {
		logger.Error("Ethereum peer removal failed", "err", err)
	}
	// Hard disconnect at the networking layer
	peer.Peer.Disconnect(p2p.DiscUselessPeer)
}

func (h *handler) Start(maxPeers int) {
	h.maxPeers = maxPeers

	// broadcast transactions
	h.wg.Add(1)
	h.txsCh = make(chan core.NewTxsEvent, txChanSize)
	h.txsSub = h.txpool.SubscribeNewTxsEvent(h.txsCh)
	go h.txBroadcastLoop()

	// start sync handlers
	h.wg.Add(2)
	go h.chainSync.loop()
	go h.txsyncLoop64() // TODO(karalabe): Legacy initial tx echange, drop with eth/64.
}
func (h *handler) Stop() {
	h.txsSub.Unsubscribe() // quits txBroadcastLoop

	// Quit chainSync and txsync64.
	// After this is done, no new peers will be accepted.
	close(h.quitSync)
	h.wg.Wait()

	// Disconnect existing sessions.
	// This also closes the gate for any new registrations on the peer set.
	// sessions which are already established but not added to h.peers yet
	// will exit when they try to register.
	h.peers.close()
	h.peerWG.Wait()

	log.Info("Ethereum protocol stopped")
}

// BroadcastBlock will either propagate a block to a subset of its peers, or
// will only announce its availability (depending what's requested).
func (h *handler) BroadcastBlock(block *types.Block, propagate bool) {
	hash := block.Hash()
	peers := h.peers.peersWithoutBlock(hash)

	// If propagation is requested, send to a subset of the peer
	if propagate {
		// Calculate the TD of the block (it's not imported yet, so block.Td is not valid)
		var td *big.Int

		if rawdb.HasBody(h.database, block.ParentHash(), block.NumberU64()-1) {
			parentTd, _ := rawdb.ReadTd(h.database, block.ParentHash(), block.NumberU64()-1)
			td = new(big.Int).Add(block.Difficulty(), parentTd)
		} else {
			// If the parent's unknown, abort insertion
			log.Error("Propagating dangling block", "number", block.Number(), "hash", hash)
			return
		}
		// Send the block to a subset of our peers
		transfer := peers[:int(math.Sqrt(float64(len(peers))))]
		for _, peer := range transfer {
			peer.AsyncSendNewBlock(block, td)
		}
		log.Trace("Propagated block", "hash", hash, "recipients", len(transfer), "duration", common.PrettyDuration(time.Since(block.ReceivedAt)))
		return
	}
	// Otherwise if the block is indeed in out own chain, announce it
	if rawdb.HasBody(h.database, hash, block.NumberU64()) {
		for _, peer := range peers {
			peer.AsyncSendNewBlockHash(block)
		}
		log.Trace("Announced block", "hash", hash, "recipients", len(peers), "duration", common.PrettyDuration(time.Since(block.ReceivedAt)))
	}
}

// BroadcastTransactions will propagate a batch of transactions
// - To a square root of all peers
// - And, separately, as announcements to all peers which are not known to
// already have the given transaction.
func (h *handler) BroadcastTransactions(txs types.Transactions) {
	var (
		annoCount   int // Count of announcements made
		annoPeers   int
		directCount int // Count of the txs sent directly to peers
		directPeers int // Count of the peers that were sent transactions directly

		txset = make(map[*ethPeer][]common.Hash) // Set peer->hash to transfer directly
		annos = make(map[*ethPeer][]common.Hash) // Set peer->hash to announce

	)
	// Broadcast transactions to a batch of peers not knowing about it
	for _, tx := range txs {
		peers := h.peers.peersWithoutTransaction(tx.Hash())
		// Send the tx unconditionally to a subset of our peers
		numDirect := int(math.Sqrt(float64(len(peers))))
		for _, peer := range peers[:numDirect] {
			txset[peer] = append(txset[peer], tx.Hash())
		}
		// For the remaining peers, send announcement only
		for _, peer := range peers[numDirect:] {
			annos[peer] = append(annos[peer], tx.Hash())
		}
	}
	for peer, hashes := range txset {
		directPeers++
		directCount += len(hashes)
		peer.AsyncSendTransactions(hashes)
	}
	for peer, hashes := range annos {
		annoPeers++
		annoCount += len(hashes)
		peer.AsyncSendPooledTransactionHashes(hashes)
	}
	log.Debug("Transaction broadcast", "txs", len(txs),
		"announce packs", annoPeers, "announced hashes", annoCount,
		"tx packs", directPeers, "broadcast txs", directCount)
}

// txBroadcastLoop announces new transactions to connected peers.
func (h *handler) txBroadcastLoop() {
	defer h.wg.Done()
	for {
		select {
		case event := <-h.txsCh:
			h.BroadcastTransactions(event.Txs)
		case <-h.txsSub.Err():
			return
		}
	}
}
