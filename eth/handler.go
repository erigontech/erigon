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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/downloader"
	"github.com/ledgerwatch/turbo-geth/eth/fetcher"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/enode"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
)

const (
	softResponseLimit = 2 * 1024 * 1024 // Target maximum size of returned blocks, headers or node data.
	estHeaderRlpSize  = 500             // Approximate size of an RLP encoded block header

	// txChanSize is the size of channel listening to NewTxsEvent.
	// The number is referenced from the size of tx pool.
	txChanSize = 4096
)

var (
	syncChallengeTimeout = 15 * time.Second // Time allowance for a node to reply to the sync progress challenge
)

func errResp(code errCode, format string, v ...interface{}) error {
	return fmt.Errorf("%v - %v", code, fmt.Sprintf(format, v...))
}

type ProtocolManager struct {
	networkID  uint64
	forkFilter forkid.Filter // Fork ID filter, constant across the lifetime of the node

	fastSync  uint32 // Flag whether fast sync is enabled (gets disabled if we already have blocks)
	acceptTxs uint32 // Flag whether we're considered synchronised (enables transaction processing)

	checkpointNumber uint64      // Block number for the sync progress validator to cross reference
	checkpointHash   common.Hash // Block hash for the sync progress validator to cross reference

	txpool      txPool
	chainConfig *params.ChainConfig
	blockchain  *core.BlockChain
	chaindb     *ethdb.ObjectDatabase
	maxPeers    int

	stagedSync   *stagedsync.StagedSync
	downloader   *downloader.Downloader
	blockFetcher *fetcher.BlockFetcher
	txFetcher    *fetcher.TxFetcher
	peers        *peerSet

	eventMux      *event.TypeMux
	txsCh         chan core.NewTxsEvent
	txsSub        event.Subscription
	txsSubMu      sync.RWMutex
	minedBlockSub *event.TypeMuxSubscription

	whitelist map[uint64]common.Hash

	// channels for fetcher, syncer, txsyncLoop
	txsyncCh chan *txsync
	quitSync chan struct{}

	chainSync *chainSyncer
	wg        sync.WaitGroup
	peerWG    sync.WaitGroup

	// Test fields or hooks
	broadcastTxAnnouncesOnly bool // Testing field, disable transaction propagation

	mode          downloader.SyncMode // Sync mode passed from the command line
	tmpdir        string
	cacheSize     int
	batchSize     int
	currentHeight uint64 // Atomic variable to contain chain height
}

// NewProtocolManager returns a new Ethereum sub protocol manager. The Ethereum sub protocol manages peers capable
// with the Ethereum network.
func NewProtocolManager(config *params.ChainConfig, checkpoint *params.TrustedCheckpoint, mode downloader.SyncMode, networkID uint64, mux *event.TypeMux, txpool txPool, engine consensus.Engine, blockchain *core.BlockChain, chaindb *ethdb.ObjectDatabase, whitelist map[uint64]common.Hash, stagedSync *stagedsync.StagedSync) (*ProtocolManager, error) {
	// Create the protocol manager with the base fields
	if stagedSync == nil {
		stagedSync = stagedsync.New(stagedsync.DefaultStages(), stagedsync.DefaultUnwindOrder(), stagedsync.OptionalParameters{})
	}
	manager := &ProtocolManager{
		networkID:   networkID,
		forkFilter:  forkid.NewFilter(config, blockchain.Genesis().Hash(), blockchain.CurrentHeader().Number.Uint64()),
		eventMux:    mux,
		txpool:      txpool,
		chainConfig: config,
		blockchain:  blockchain,
		chaindb:     chaindb,
		peers:       newPeerSet(),
		whitelist:   whitelist,
		stagedSync:  stagedSync,
		mode:        mode,
		txsyncCh:    make(chan *txsync),
		quitSync:    make(chan struct{}),
	}

	if mode == downloader.FullSync {
		// The database seems empty as the current block is the genesis. Yet the fast
		// block is ahead, so fast sync was enabled for this node at a certain point.
		// The scenarios where this can happen is
		// * if the user manually (or via a bad block) rolled back a fast sync node
		//   below the sync point.
		// * the last fast sync is not finished while user specifies a full sync this
		//   time. But we don't have any recent state for full sync.
		// In these cases however it's safe to reenable fast sync.
		fullBlock, fastBlock := blockchain.CurrentBlock(), blockchain.CurrentFastBlock()
		if fullBlock.NumberU64() == 0 && fastBlock.NumberU64() > 0 {
			manager.fastSync = uint32(1)
			log.Warn("Switch sync mode from full sync to fast sync")
		}
	} else if mode != downloader.StagedSync {
		if blockchain.CurrentBlock().NumberU64() > 0 {
			// Print warning log if database is not empty to run fast sync.
			log.Warn("Switch sync mode from fast sync to full sync")
		} else {
			// If fast sync was requested and our database is empty, grant it
			manager.fastSync = uint32(1)
		}
	}

	// If we have trusted checkpoints, enforce them on the chain
	if checkpoint != nil {
		manager.checkpointNumber = (checkpoint.SectionIndex+1)*params.CHTFrequency - 1
		manager.checkpointHash = checkpoint.SectionHead
	}

	initPm(manager, engine, config, blockchain, chaindb)

	return manager, nil
}

func (pm *ProtocolManager) SetTmpDir(tmpdir string) {
	pm.tmpdir = tmpdir
	if pm.downloader != nil {
		pm.downloader.SetTmpDir(tmpdir)
	}
}

func (pm *ProtocolManager) SetBatchSize(cacheSize, batchSize int) {
	pm.cacheSize = cacheSize
	pm.batchSize = batchSize
	if pm.downloader != nil {
		pm.downloader.SetBatchSize(cacheSize, batchSize)
	}
}

func initPm(manager *ProtocolManager, engine consensus.Engine, chainConfig *params.ChainConfig, blockchain *core.BlockChain, chaindb *ethdb.ObjectDatabase) {
	sm, err := ethdb.GetStorageModeFromDB(chaindb)
	if err != nil {
		log.Error("Get storage mode", "err", err)
	}
	// Construct the different synchronisation mechanisms
	if manager.downloader != nil {
		manager.downloader.Cancel()
	}
	manager.downloader = downloader.New(manager.checkpointNumber, chaindb, manager.eventMux, chainConfig, blockchain, nil, manager.removePeer, sm)
	manager.downloader.SetTmpDir(manager.tmpdir)
	manager.downloader.SetBatchSize(manager.cacheSize, manager.batchSize)
	manager.downloader.SetStagedSync(manager.stagedSync)

	// Construct the fetcher (short sync)
	validator := func(header *types.Header) error {
		return engine.VerifyHeader(blockchain, header, true)
	}
	heighter := func() uint64 {
		return atomic.LoadUint64(&manager.currentHeight)
	}
	inserter := func(blocks types.Blocks) (int, error) {
		atomic.StoreUint32(&manager.acceptTxs, 1) // Mark initial sync done on any fetcher import
		return 0, err
	}
	if manager.blockFetcher == nil {
		manager.blockFetcher = fetcher.NewBlockFetcher(false, nil, blockchain.GetBlockByHash, validator, manager.BroadcastBlock, heighter, nil, inserter, manager.removePeer)
	}

	if manager.chainSync == nil {
		manager.chainSync = newChainSyncer(manager)
	}
}

func (pm *ProtocolManager) makeDebugProtocol() p2p.Protocol {
	// Initiate Debug protocol
	log.Info("Initialising Debug protocol", "versions", DebugVersions)
	return p2p.Protocol{
		Name:    DebugName,
		Version: DebugVersions[0],
		Length:  DebugLengths[DebugVersions[0]],
		Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
			peer := &debugPeer{Peer: p, rw: rw}
			select {
			case <-pm.quitSync:
				return p2p.DiscQuitting
			default:
				pm.wg.Add(1)
				defer pm.wg.Done()
				return pm.handleDebug(peer)
			}
		},
		NodeInfo: func() interface{} {
			return pm.NodeInfo()
		},
		PeerInfo: func(id enode.ID) interface{} {
			if p := pm.peers.Peer(fmt.Sprintf("%x", id[:8])); p != nil {
				return p.Info()
			}
			return nil
		},
	}
}

func (pm *ProtocolManager) txpoolGet(hash common.Hash) *types.Transaction {
	switch pm.txpool.(type) {
	case nil:
		return nil
	}
	return pm.txpool.Get(hash)
}

func (pm *ProtocolManager) makeProtocol(version uint) p2p.Protocol {
	length, ok := ProtocolLengths[version]
	if !ok {
		panic("makeProtocol for unknown version")
	}

	return p2p.Protocol{
		Name:    ProtocolName,
		Version: version,
		Length:  length,
		Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
			return pm.runPeer(pm.newPeer(int(version), p, rw, pm.txpoolGet))
		},
		NodeInfo: func() interface{} {
			return pm.NodeInfo()
		},
		PeerInfo: func(id enode.ID) interface{} {
			if p := pm.peers.Peer(fmt.Sprintf("%x", id[:8])); p != nil {
				return p.Info()
			}
			return nil
		},
	}
}

func (pm *ProtocolManager) removePeer(id string) {
	// Short circuit if the peer was already removed
	peer := pm.peers.Peer(id)
	if peer == nil {
		return
	}
	log.Debug("Removing Ethereum peer", "peer", id)

	// Unregister the peer from the downloader and Ethereum peer set
	err := pm.downloader.UnregisterPeer(id)
	if err != nil {
		log.Error("Peer unregister failed", "peer", id, "err", err)
	}

	if pm.txFetcher != nil {
		pm.txFetcher.Drop(id) // nolint:errcheck
	}

	if err := pm.peers.Unregister(id); err != nil {
		log.Error("Peer removal failed", "peer", id, "err", err)
	}
	// Hard disconnect at the networking layer
	if peer != nil {
		peer.Peer.Disconnect(p2p.DiscUselessPeer)
	}
}

func (pm *ProtocolManager) Start(maxPeers int, withTxPool bool) error {
	pm.maxPeers = maxPeers

	if withTxPool {
		// broadcast transactions
		if err := pm.StartTxPool(); err != nil {
			return err
		}
	}

	// broadcast mined blocks
	pm.minedBlockSub = pm.eventMux.Subscribe(core.NewMinedBlockEvent{})
	if pm.minedBlockSub != nil {
		pm.wg.Add(1)
		go pm.minedBroadcastLoop()
	}

	// start sync handlers
	if pm.chainSync != nil {
		pm.wg.Add(1)
		go pm.chainSync.loop()
	}

	pm.wg.Add(1)
	go pm.txsyncLoop64() // TODO(karalabe): Legacy initial tx echange, drop with eth/64.

	return nil
}

func (pm *ProtocolManager) StartTxPool() error {
	fetchTx := func(peer string, hashes []common.Hash) error {
		p := pm.peers.Peer(peer)
		if p == nil {
			return errors.New("unknown peer")
		}
		return p.RequestTxs(hashes)
	}
	pm.txFetcher = fetcher.NewTxFetcher(pm.txpool.Has, pm.txpool.AddRemotes, fetchTx)

	if pm.txsCh == nil {
		pm.txsCh = make(chan core.NewTxsEvent, txChanSize)
	}
	pm.txsSubMu.Lock()
	pm.txsSub = pm.txpool.SubscribeNewTxsEvent(pm.txsCh)
	if pm.txsSub != nil {
		pm.wg.Add(1)
		go pm.txBroadcastLoop()
	}
	pm.txsSubMu.Unlock()

	pm.txFetcher.Start()
	return nil
}

func (pm *ProtocolManager) StopTxPool() {
	pm.txsSubMu.RLock()
	ok := pm.txsSub != nil
	pm.txsSubMu.RUnlock()
	if ok {
		pm.txsSub.Unsubscribe() // quits txBroadcastLoop
		pm.txFetcher.Stop()
	}
}

func (pm *ProtocolManager) Stop() {
	pm.StopTxPool()

	if pm.minedBlockSub != nil {
		pm.minedBlockSub.Unsubscribe() // quits blockBroadcastLoop
	}

	// Quit chainSync and txsync64.
	// After this is done, no new peers will be accepted.
	close(pm.quitSync)
	pm.wg.Wait()

	// Disconnect existing sessions.
	// This also closes the gate for any new registrations on the peer set.
	// sessions which are already established but not added to pm.peers yet
	// will exit when they try to register.
	pm.peers.Close()
	pm.peerWG.Wait()

	log.Info("Ethereum protocol stopped")
}

func (pm *ProtocolManager) newPeer(pv int, p *p2p.Peer, rw p2p.MsgReadWriter, getPooledTx func(hash common.Hash) *types.Transaction) *peer {
	return newPeer(pv, p, rw, getPooledTx)
}

func (pm *ProtocolManager) runPeer(p *peer) error {
	if !pm.chainSync.handlePeerEvent(p) {
		return p2p.DiscQuitting
	}
	pm.peerWG.Add(1)
	defer pm.peerWG.Done()
	return pm.handle(p)
}

// handle is the callback invoked to manage the life cycle of an eth peer. When
// this function terminates, the peer is disconnected.
func (pm *ProtocolManager) handle(p *peer) error {
	// Ignore maxPeers if this is a trusted peer
	if pm.peers.Len() >= pm.maxPeers && !p.Peer.Info().Network.Trusted {
		return p2p.DiscTooManyPeers
	}
	p.Log().Debug("Ethereum peer connected", "name", p.Name())

	// Execute the Ethereum handshake
	var (
		genesis = pm.blockchain.Genesis()
		head    = pm.blockchain.CurrentHeader()
		hash    = head.Hash()
		number  = head.Number.Uint64()
		td      = pm.blockchain.GetTd(hash, number)
	)
	forkID := forkid.NewID(pm.blockchain.Config(), pm.blockchain.Genesis().Hash(), pm.blockchain.CurrentHeader().Number.Uint64())
	if err := p.Handshake(pm.networkID, td, hash, genesis.Hash(), forkID, pm.forkFilter); err != nil {
		p.Log().Debug("Ethereum handshake failed", "err", err)
		return err
	}

	// Make sure that we first exchange headers and only then announce transactions
	p.HandshakeOrderMux.Lock()
	// Register the peer locally
	if err := pm.peers.Register(p, pm.removePeer); err != nil {
		p.Log().Error("Ethereum peer registration failed", "err", err)
		p.HandshakeOrderMux.Unlock()
		return err
	}
	defer pm.removePeer(p.id)

	// Register the peer in the downloader. If the downloader considers it banned, we disconnect
	if err := pm.downloader.RegisterPeer(p.id, p.version, p); err != nil {
		p.HandshakeOrderMux.Unlock()
		return err
	}
	pm.chainSync.handlePeerEvent(p)

	// Propagate existing transactions. new transactions appearing
	// after this will be sent via broadcasts.
	// Send request for the head header
	peerHeadHash, _ := p.Head()
	if err := p.RequestHeadersByHash(peerHeadHash, 1, 0, false); err != nil {
		p.HandshakeOrderMux.Unlock()
		return err
	}

	// Allow to handle transaction ordering
	// Unlocking needs to happen before we start waiting for the response to the peer head hash
	// Otherwise, if the peer does not response, it will eventually fill up the tx broadcast
	// channels and the whole system will block
	p.HandshakeOrderMux.Unlock()

	// Handle one message to prevent two peers deadlocking each other
	if err := pm.handleMsg(p); err != nil {
		p.Log().Debug("Ethereum message handling failed", "err", err)
		return err
	}

	pm.syncTransactions(p)

	// If we have a trusted CHT, reject all peers below that (avoid fast sync eclipse)
	if pm.checkpointHash != (common.Hash{}) {
		// Request the peer's checkpoint header for chain height/weight validation
		if err := p.RequestHeadersByNumber(pm.checkpointNumber, 1, 0, false); err != nil {
			return err
		}
		// Start a timer to disconnect if the peer doesn't reply in time
		p.syncDrop = time.AfterFunc(syncChallengeTimeout, func() {
			p.Log().Warn("Checkpoint challenge timed out, dropping", "addr", p.RemoteAddr(), "type", p.Name())
			pm.removePeer(p.id)
		})
		// Make sure it's cleaned up if the peer dies off
		defer func() {
			if p.syncDrop != nil {
				p.syncDrop.Stop()
				p.syncDrop = nil
			}
		}()
	}
	// If we have any explicit whitelist block hashes, request them
	for number := range pm.whitelist {
		if err := p.RequestHeadersByNumber(number, 1, 0, false); err != nil {
			return err
		}
	}
	// Handle incoming messages until the connection is torn down
	for {
		if err := pm.handleMsg(p); err != nil {
			p.Log().Debug("Ethereum message handling failed", "err", err)
			return err
		}
	}
}

func (pm *ProtocolManager) handleDebug(p *debugPeer) error {
	for {
		if err := pm.handleDebugMsg(p); err != nil {
			p.Log().Debug("Debug message handling failed", "err", err)
			return err
		}
	}
}

// handleMsg is invoked whenever an inbound message is received from a remote
// peer. The remote connection is torn down upon returning any error.
func (pm *ProtocolManager) handleMsg(p *peer) error {
	// Read the next message from the remote peer, and ensure it's fully consumed
	msg, err := p.rw.ReadMsg()
	if err != nil {
		return fmt.Errorf("handleMsg p.rw.ReadMsg: %w", err)
	}
	if msg.Size > ProtocolMaxMsgSize {
		return errResp(ErrMsgTooLarge, "%v > %v", msg.Size, ProtocolMaxMsgSize)
	}
	defer msg.Discard()

	// Handle the message depending on its contents
	switch {
	case msg.Code == StatusMsg:
		// Status messages should never arrive after the handshake
		return errResp(ErrExtraStatusMsg, "uncontrolled status message")

	// Block header query, collect the requested headers and reply
	case msg.Code == GetBlockHeadersMsg:
		// Decode the complex header query
		var query GetBlockHeadersData
		if err := msg.Decode(&query); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		hashMode := query.Origin.Hash != (common.Hash{})
		first := true
		maxNonCanonical := uint64(100)

		// Gather headers until the fetch or network limits is reached
		var (
			bytes   common.StorageSize
			headers []*types.Header
			unknown bool
		)
		for !unknown && len(headers) < int(query.Amount) && bytes < softResponseLimit && len(headers) < downloader.MaxHeaderFetch {
			// Retrieve the next header satisfying the query
			var origin *types.Header
			if hashMode {
				if first {
					first = false
					origin = pm.blockchain.GetHeaderByHash(query.Origin.Hash)
					if origin != nil {
						query.Origin.Number = origin.Number.Uint64()
					}
				} else {
					origin = pm.blockchain.GetHeader(query.Origin.Hash, query.Origin.Number)
				}
			} else {
				origin = pm.blockchain.GetHeaderByNumber(query.Origin.Number)
			}
			if origin == nil {
				break
			}
			headers = append(headers, origin)
			bytes += estHeaderRlpSize

			// Advance to the next header of the query
			switch {
			case hashMode && query.Reverse:
				// Hash based traversal towards the genesis block
				ancestor := query.Skip + 1
				if ancestor == 0 {
					unknown = true
				} else {
					query.Origin.Hash, query.Origin.Number = pm.blockchain.GetAncestor(query.Origin.Hash, query.Origin.Number, ancestor, &maxNonCanonical)
					unknown = (query.Origin.Hash == common.Hash{})
				}
			case hashMode && !query.Reverse:
				// Hash based traversal towards the leaf block
				var (
					current = origin.Number.Uint64()
					next    = current + query.Skip + 1
				)
				if next <= current {
					infos, _ := json.MarshalIndent(p.Peer.Info(), "", "  ")
					p.Log().Warn("GetBlockHeaders skip overflow attack", "current", current, "skip", query.Skip, "next", next, "attacker", infos)
					unknown = true
				} else {
					if header := pm.blockchain.GetHeaderByNumber(next); header != nil {
						nextHash := header.Hash()
						expOldHash, _ := pm.blockchain.GetAncestor(nextHash, next, query.Skip+1, &maxNonCanonical)
						if expOldHash == query.Origin.Hash {
							query.Origin.Hash, query.Origin.Number = nextHash, next
						} else {
							unknown = true
						}
					} else {
						unknown = true
					}
				}
			case query.Reverse:
				// Number based traversal towards the genesis block
				if query.Origin.Number >= query.Skip+1 {
					query.Origin.Number -= query.Skip + 1
				} else {
					unknown = true
				}

			case !query.Reverse:
				// Number based traversal towards the leaf block
				query.Origin.Number += query.Skip + 1
			}
		}
		return p.SendBlockHeaders(headers)

	case msg.Code == BlockHeadersMsg:
		// A batch of headers arrived to one of our previous requests
		var headers []*types.Header
		if err := msg.Decode(&headers); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// If no headers were received, but we're expencting a checkpoint header, consider it that
		if len(headers) == 0 && p.syncDrop != nil {
			// Stop the timer either way, decide later to drop or not
			p.syncDrop.Stop()
			p.syncDrop = nil

			// If we're doing a fast sync, we must enforce the checkpoint block to avoid
			// eclipse attacks. Unsynced nodes are welcome to connect after we're done
			// joining the network
			if atomic.LoadUint32(&pm.fastSync) == 1 {
				p.Log().Warn("Dropping unsynced node during fast sync", "addr", p.RemoteAddr(), "type", p.Name())
				return errors.New("unsynced node cannot serve fast sync")
			}
		}
		for _, header := range headers {
			var (
				trueHead   = header.Hash()
				trueNumber = header.Number.Uint64()
			)
			// Update the peer's total difficulty if better than the previous
			if _, number := p.Head(); trueNumber > number {
				p.SetHead(trueHead, trueNumber)
				pm.chainSync.handlePeerEvent(p)
			}
		}
		// Filter out any explicitly requested headers, deliver the rest to the downloader
		filter := len(headers) == 1
		if filter {
			// If it's a potential sync progress check, validate the content and advertised chain weight
			if p.syncDrop != nil && headers[0].Number.Uint64() == pm.checkpointNumber {
				// Disable the sync drop timer
				p.syncDrop.Stop()
				p.syncDrop = nil

				// Validate the header and either drop the peer or continue
				if headers[0].Hash() != pm.checkpointHash {
					return errors.New("checkpoint hash mismatch")
				}
				return nil
			}
			// Otherwise if it's a whitelisted block, validate against the set
			if want, ok := pm.whitelist[headers[0].Number.Uint64()]; ok {
				if hash := headers[0].Hash(); want != hash {
					p.Log().Info("Whitelist mismatch, dropping peer", "number", headers[0].Number.Uint64(), "hash", hash, "want", want)
					return errors.New("whitelist block mismatch")
				}
				p.Log().Debug("Whitelist block verified", "number", headers[0].Number.Uint64(), "hash", want)
			}
			// Irrelevant of the fork checks, send the header to the fetcher just in case
			headers = pm.blockFetcher.FilterHeaders(p.id, headers, time.Now())
		}
		if len(headers) > 0 || !filter {
			err := pm.downloader.DeliverHeaders(p.id, headers)
			if err != nil {
				log.Debug("Failed to deliver headers", "err", err)
			}
		}

	case msg.Code == GetBlockBodiesMsg:
		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		// Gather blocks until the fetch or network limits is reached
		var (
			hash   common.Hash
			bytes  int
			bodies []rlp.RawValue
		)
		for bytes < softResponseLimit && len(bodies) < downloader.MaxBlockFetch {
			// Retrieve the hash of the next block
			if err := msgStream.Decode(&hash); err == rlp.EOL {
				break
			} else if err != nil {
				return errResp(ErrDecode, "msg %v: %v", msg, err)
			}
			// Retrieve the requested block body, stopping if enough was found
			if data := pm.blockchain.GetBodyRLP(hash); len(data) != 0 {
				bodies = append(bodies, data)
				bytes += len(data)
			}
		}
		return p.SendBlockBodiesRLP(bodies)

	case msg.Code == BlockBodiesMsg:
		// A batch of block bodies arrived to one of our previous requests
		var request blockBodiesData
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Deliver them all to the downloader for queuing
		transactions := make([][]*types.Transaction, len(request))
		uncles := make([][]*types.Header, len(request))

		for i, body := range request {
			transactions[i] = body.Transactions
			uncles[i] = body.Uncles
		}
		// Filter out any explicitly requested bodies, deliver the rest to the downloader
		filter := len(transactions) > 0 || len(uncles) > 0
		if filter {
			transactions, uncles = pm.blockFetcher.FilterBodies(p.id, transactions, uncles, time.Now())
		}
		if len(transactions) > 0 || len(uncles) > 0 || !filter {
			err := pm.downloader.DeliverBodies(p.id, transactions, uncles)
			if err != nil {
				log.Debug("Failed to deliver bodies", "err", err)
			}
		}

	case p.version >= eth64 && msg.Code == GetNodeDataMsg:
		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}

		// Obtain the TrieDbState
		if pm.mode == downloader.StagedSync {
			return fmt.Errorf("staged sync mode, no support for GetNodeData")
		}
		tds, err := pm.blockchain.GetTrieDbState()
		if err != nil {
			return err
		}
		if tds == nil {
			return fmt.Errorf("download-only mode, no support for GetNodeData")
		}

		// Gather state data until the fetch or network limits is reached
		var (
			hash  common.Hash
			bytes int
			data  [][]byte
		)
		for bytes < softResponseLimit && len(data) < downloader.MaxStateFetch {
			// Retrieve the hash of the next node
			if err := msgStream.Decode(&hash); err == rlp.EOL {
				break
			} else if err != nil {
				return errResp(ErrDecode, "msg %v: %v", msg, err)
			}

			// First try to get the trie node
			node := tds.GetNodeByHash(hash)
			if len(node) != 0 {
				data = append(data, node)
				bytes += len(node)
				continue
			}

			// Now attempt to get the byte code
			code, err := tds.ReadCodeByHash(hash)
			if err == nil {
				data = append(data, code)
				bytes += len(code)
			} else {
				data = append(data, nil)
			}
		}
		return p.SendNodeData(data)

	case p.version >= eth64 && msg.Code == GetReceiptsMsg:
		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		// Gather state data until the fetch or network limits is reached
		var (
			hash     common.Hash
			bytes    int
			receipts []rlp.RawValue
		)
		for bytes < softResponseLimit && len(receipts) < downloader.MaxReceiptFetch {
			// Retrieve the hash of the next block
			if err := msgStream.Decode(&hash); err == rlp.EOL {
				break
			} else if err != nil {
				return errResp(ErrDecode, "msg %v: %v", msg, err)
			}
			// Retrieve the requested block's receipts, skipping if unknown to us
			results := pm.blockchain.GetReceiptsByHash(hash)
			if results == nil {
				if header := pm.blockchain.GetHeaderByHash(hash); header == nil || header.ReceiptHash != types.EmptyRootHash {
					continue
				}
			}
			// If known, encode and queue for response packet
			if encoded, err := rlp.EncodeToBytes(results); err != nil {
				log.Error("Failed to encode receipt", "err", err)
			} else {
				receipts = append(receipts, encoded)
				bytes += len(encoded)
			}
		}
		return p.SendReceiptsRLP(receipts)

	case p.version >= eth64 && msg.Code == ReceiptsMsg:
		// A batch of receipts arrived to one of our previous requests
		var receipts [][]*types.Receipt
		if err := msg.Decode(&receipts); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Deliver all to the downloader
		if err := pm.downloader.DeliverReceipts(p.id, receipts); err != nil {
			log.Debug("Failed to deliver receipts", "err", err)
		}

	case msg.Code == NewBlockHashesMsg:
		var announces NewBlockHashesData
		if err := msg.Decode(&announces); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		// Mark the hashes as present at the remote node
		for _, block := range announces {
			p.MarkBlock(block.Hash)
		}
		// Schedule all the unknown hashes for retrieval
		unknown := make(NewBlockHashesData, 0, len(announces))
		for _, block := range announces {
			if !pm.blockchain.HasBlock(block.Hash, block.Number) {
				unknown = append(unknown, block)
			}
		}
		for _, block := range unknown {
			pm.blockFetcher.Notify(p.id, block.Hash, block.Number, time.Now(), p.RequestOneHeader, p.RequestBodies) //nolint:errcheck
		}

	case msg.Code == NewBlockMsg:
		// Retrieve and decode the propagated block
		var request NewBlockData
		if err := msg.Decode(&request); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		if hash := types.CalcUncleHash(request.Block.Uncles()); hash != request.Block.UncleHash() {
			log.Warn("Propagated block has invalid uncles", "have", hash, "exp", request.Block.UncleHash())
			break // TODO(karalabe): return error eventually, but wait a few releases
		}
		if hash := types.DeriveSha(request.Block.Transactions()); hash != request.Block.TxHash() {
			log.Warn("Propagated block has invalid body", "have", hash, "exp", request.Block.TxHash())
			break // TODO(karalabe): return error eventually, but wait a few releases
		}
		if err := request.sanityCheck(); err != nil {
			return err
		}
		request.Block.ReceivedAt = msg.ReceivedAt
		request.Block.ReceivedFrom = p

		// Mark the peer as owning the block and schedule it for import
		p.MarkBlock(request.Block.Hash())
		if pm.mode != downloader.StagedSync {
			// Staged sync does not support this yet
			if err := pm.blockFetcher.Enqueue(p.id, request.Block); err != nil {
				return err
			}
		} else {
			log.Debug("Adding block to staged sync prefetch",
				"number", request.Block.NumberU64,
				"hash", request.Block.Hash().Hex(),
			)
			pm.stagedSync.PrefetchedBlocks.Add(request.Block)
		}

		// Assuming the block is importable by the peer, but possibly not yet done so,
		// calculate the head hash and TD that the peer truly must have.
		var (
			trueHead   = request.Block.Hash()
			trueNumber = request.Block.NumberU64()
		)
		// Update the peer's total difficulty if better than the previous
		if _, number := p.Head(); trueNumber > number {
			p.SetHead(trueHead, trueNumber)
			pm.chainSync.handlePeerEvent(p)
		}

	case msg.Code == NewPooledTransactionHashesMsg && p.version >= eth65:
		if pm.txFetcher == nil {
			break
		}
		// New transaction announcement arrived, make sure we have
		// a valid and fresh chain to handle them
		if atomic.LoadUint32(&pm.acceptTxs) == 0 {
			break
		}
		var hashes []common.Hash
		if err := msg.Decode(&hashes); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		// Schedule all the unknown hashes for retrieval
		for _, hash := range hashes {
			p.MarkTransaction(hash)
		}
		pm.txFetcher.Notify(p.id, hashes) // nolint:errcheck

	case msg.Code == GetPooledTransactionsMsg && p.version >= eth65:
		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		// Gather transactions until the fetch or network limits is reached
		var (
			hash   common.Hash
			bytes  int
			hashes []common.Hash
			txs    []rlp.RawValue
		)
		for bytes < softResponseLimit {
			// Retrieve the hash of the next block
			if err := msgStream.Decode(&hash); err == rlp.EOL {
				break
			} else if err != nil {
				return errResp(ErrDecode, "msg %v: %v", msg, err)
			}
			// Retrieve the requested transaction, skipping if unknown to us
			tx := pm.txpool.Get(hash)
			if tx == nil {
				continue
			}
			// If known, encode and queue for response packet
			if encoded, err := rlp.EncodeToBytes(tx); err != nil {
				log.Error("Failed to encode transaction", "err", err)
			} else {
				hashes = append(hashes, hash)
				txs = append(txs, encoded)
				bytes += len(encoded)
			}
		}
		return p.SendPooledTransactionsRLP(hashes, txs)

	case msg.Code == TransactionMsg || (msg.Code == PooledTransactionsMsg && p.version >= eth65):
		if pm.txFetcher == nil {
			break
		}
		// Transactions arrived, make sure we have a valid and fresh chain to handle them
		if atomic.LoadUint32(&pm.acceptTxs) == 0 {
			break
		}
		// Transactions can be processed, parse all of them and deliver to the pool
		var txs []*types.Transaction
		if err := msg.Decode(&txs); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		for i, tx := range txs {
			// Validate and mark the remote transaction
			if tx == nil {
				return errResp(ErrDecode, "transaction %d is nil", i)
			}
			p.MarkTransaction(tx.Hash())
		}
		pm.txFetcher.Enqueue(p.id, txs, msg.Code == PooledTransactionsMsg) // nolint:errcheck

	default:
		return errResp(ErrInvalidMsgCode, "%v", msg.Code)
	}
	return nil
}

func (pm *ProtocolManager) extractAddressHash(addressOrHash []byte) (common.Hash, error) {
	var addrHash common.Hash
	if len(addressOrHash) == common.HashLength {
		addrHash.SetBytes(addressOrHash)
		return addrHash, nil
	} else if len(addressOrHash) == common.AddressLength {
		addrHash = crypto.Keccak256Hash(addressOrHash)
		return addrHash, nil
	} else {
		return addrHash, errResp(ErrDecode, "not an account address or its hash")
	}
}

func (pm *ProtocolManager) handleDebugMsg(p *debugPeer) error {
	msg, readErr := p.rw.ReadMsg()
	if readErr != nil {
		return fmt.Errorf("handleDebugMsg p.rw.ReadMsg: %w", readErr)
	}
	if msg.Size > DebugMaxMsgSize {
		return errResp(ErrMsgTooLarge, "%v > %v", msg.Size, DebugMaxMsgSize)
	}
	defer msg.Discard()

	switch msg.Code {
	case DebugSetGenesisMsg:
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))

		var msgAsJson []byte
		if err := msgStream.Decode(&msgAsJson); err != nil {
			return fmt.Errorf("msgStream.Decode: %w", err)
		}

		var genesis core.Genesis
		if err := json.Unmarshal(msgAsJson, &genesis); err != nil {
			return fmt.Errorf("json.Unmarshal: %w", err)
		}

		pm.chainConfig = genesis.Config
		pm.downloader.SetChainConfig(genesis.Config)

		// hacks to speedup local sync
		downloader.MaxHashFetch = 512 * 10
		downloader.MaxBlockFetch = 128 * 10
		downloader.MaxHeaderFetch = 192 * 10
		downloader.MaxReceiptFetch = 256 * 10

		log.Warn("Succeed to set new chainConfig")
		if err := p2p.Send(p.rw, DebugSetGenesisMsg, "{}"); err != nil {
			return fmt.Errorf("p2p.Send: %w", err)
		}
		log.Warn("Sent back the DebugSetGenesisMsg")
		return nil
	default:
		return errResp(ErrInvalidMsgCode, "%v", msg.Code)
	}
}

// BroadcastBlock will either propagate a block to a subset of its peers, or
// will only announce its availability (depending what's requested).
func (pm *ProtocolManager) BroadcastBlock(block *types.Block, propagate bool) {
	hash := block.Hash()
	peers := pm.peers.PeersWithoutBlock(hash)

	// If propagation is requested, send to a subset of the peer
	if propagate {
		// Calculate the TD of the block (it's not imported yet, so block.Td is not valid)
		var td *big.Int
		if parent := pm.blockchain.GetBlock(block.ParentHash(), block.NumberU64()-1); parent != nil {
			td = new(big.Int).Add(block.Difficulty(), pm.blockchain.GetTd(block.ParentHash(), block.NumberU64()-1))
		} else {
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
	if pm.blockchain.HasBlock(hash, block.NumberU64()) {
		for _, peer := range peers {
			peer.AsyncSendNewBlockHash(block)
		}
		log.Trace("Announced block", "hash", hash, "recipients", len(peers), "duration", common.PrettyDuration(time.Since(block.ReceivedAt)))
	}
}

// BroadcastTransactions will propagate a batch of transactions to all peers which are not known to
// already have the given transaction.
func (pm *ProtocolManager) BroadcastTransactions(txs types.Transactions, propagate bool) {
	var (
		txset = make(map[*peer][]common.Hash)
		annos = make(map[*peer][]common.Hash)
	)
	// Broadcast transactions to a batch of peers not knowing about it
	if propagate {
		for _, tx := range txs {
			peers := pm.peers.PeersWithoutTx(tx.Hash())

			// Send the block to a subset of our peers
			transfer := peers[:int(math.Sqrt(float64(len(peers))))]
			for _, peer := range transfer {
				txset[peer] = append(txset[peer], tx.Hash())
			}
			log.Trace("Broadcast transaction", "hash", tx.Hash(), "recipients", len(peers))
		}
		for peer, hashes := range txset {
			peer.AsyncSendTransactions(hashes)
		}
		return
	}
	// Otherwise only broadcast the announcement to peers
	for _, tx := range txs {
		peers := pm.peers.PeersWithoutTx(tx.Hash())
		for _, peer := range peers {
			annos[peer] = append(annos[peer], tx.Hash())
		}
	}
	for peer, hashes := range annos {
		if peer.version >= eth65 {
			peer.AsyncSendPooledTransactionHashes(hashes)
		} else {
			peer.AsyncSendTransactions(hashes)
		}
	}
}

// minedBroadcastLoop sends mined blocks to connected peers.
func (pm *ProtocolManager) minedBroadcastLoop() {
	defer pm.wg.Done()

	for obj := range pm.minedBlockSub.Chan() {
		if ev, ok := obj.Data.(core.NewMinedBlockEvent); ok {
			pm.BroadcastBlock(ev.Block, true)  // First propagate block to peers
			pm.BroadcastBlock(ev.Block, false) // Only then announce to the rest
		}
	}
}

// txBroadcastLoop announces new transactions to connected peers.
func (pm *ProtocolManager) txBroadcastLoop() {
	defer pm.wg.Done()

	for {
		select {
		case <-pm.quitSync:
			return
		case event := <-pm.txsCh:
			// For testing purpose only, disable propagation
			if pm.broadcastTxAnnouncesOnly {
				pm.BroadcastTransactions(event.Txs, false)
				continue
			}
			pm.BroadcastTransactions(event.Txs, true)  // First propagate transactions to peers
			pm.BroadcastTransactions(event.Txs, false) // Only then announce to the rest

		case <-pm.txsSubErr():
			return
		}
	}
}

func (pm *ProtocolManager) txsSubErr() <-chan error {
	pm.txsSubMu.RLock()
	defer pm.txsSubMu.RUnlock()
	return pm.txsSub.Err()
}

// NodeInfo represents a short summary of the Ethereum sub-protocol metadata
// known about the host peer.
type NodeInfo struct {
	Network    uint64              `json:"network"`    // Ethereum network ID (1=Frontier, 2=Morden, Ropsten=3, Rinkeby=4)
	Difficulty *big.Int            `json:"difficulty"` // Total difficulty of the host's blockchain
	Genesis    common.Hash         `json:"genesis"`    // SHA3 hash of the host's genesis block
	Config     *params.ChainConfig `json:"config"`     // Chain configuration for the fork rules
	Head       common.Hash         `json:"head"`       // SHA3 hash of the host's best owned block
}

// NodeInfo retrieves some protocol metadata about the running host node.
func (pm *ProtocolManager) NodeInfo() *NodeInfo {
	currentBlock := pm.blockchain.CurrentBlock()
	return &NodeInfo{
		Network:    pm.networkID,
		Difficulty: pm.blockchain.GetTd(currentBlock.Hash(), currentBlock.NumberU64()),
		Genesis:    pm.blockchain.Genesis().Hash(),
		Config:     pm.chainConfig,
		Head:       currentBlock.Hash(),
	}
}
