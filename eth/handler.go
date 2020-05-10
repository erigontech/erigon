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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/types/accounts"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/downloader"
	"github.com/ledgerwatch/turbo-geth/eth/fetcher"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/turbo-geth/event"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/enode"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/trie"
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

	txpool     txPool
	blockchain *core.BlockChain
	maxPeers   int

	downloader   *downloader.Downloader
	blockFetcher *fetcher.BlockFetcher
	txFetcher    *fetcher.TxFetcher
	peers        *peerSet

	eventMux      *event.TypeMux
	txsCh         chan core.NewTxsEvent
	txsSub        event.Subscription
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

	mode downloader.SyncMode // Sync mode passed from the command line
}

// NewProtocolManager returns a new Ethereum sub protocol manager. The Ethereum sub protocol manages peers capable
// with the Ethereum network.
func NewProtocolManager(config *params.ChainConfig, checkpoint *params.TrustedCheckpoint, mode downloader.SyncMode, networkID uint64, mux *event.TypeMux, txpool txPool, engine consensus.Engine, blockchain *core.BlockChain, chaindb ethdb.Database, whitelist map[uint64]common.Hash) (*ProtocolManager, error) {
	// Create the protocol manager with the base fields
	manager := &ProtocolManager{
		networkID:  networkID,
		forkFilter: forkid.NewFilter(blockchain),
		eventMux:   mux,
		txpool:     txpool,
		blockchain: blockchain,
		peers:      newPeerSet(),
		whitelist:  whitelist,
		mode:       mode,
		txsyncCh:   make(chan *txsync),
		quitSync:   make(chan struct{}),
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

	initPm(manager, txpool, engine, blockchain, chaindb)

	return manager, nil
}

func initPm(manager *ProtocolManager, txpool txPool, engine consensus.Engine, blockchain *core.BlockChain, chaindb ethdb.Database) {
	// Construct the different synchronisation mechanisms
	manager.downloader = downloader.New(manager.checkpointNumber, chaindb, nil /*stateBloom */, manager.eventMux, blockchain, nil, manager.removePeer)

	// Construct the fetcher (short sync)
	validator := func(header *types.Header) error {
		return engine.VerifyHeader(blockchain, header, true)
	}
	heighter := func() uint64 {
		return blockchain.CurrentBlock().NumberU64()
	}
	inserter := func(blocks types.Blocks) (int, error) {
		// If sync hasn't reached the checkpoint yet, deny importing weird blocks.
		//
		// Ideally we would also compare the head block's timestamp and similarly reject
		// the propagated block if the head is too old. Unfortunately there is a corner
		// case when starting new networks, where the genesis might be ancient (0 unix)
		// which would prevent full nodes from accepting it.
		if manager.blockchain.CurrentBlock().NumberU64() < manager.checkpointNumber {
			log.Warn("Unsynced yet, discarded propagated block", "number", blocks[0].Number(), "hash", blocks[0].Hash())
			return 0, nil
		}
		// If fast sync is running, deny importing weird blocks. This is a problematic
		// clause when starting up a new network, because fast-syncing miners might not
		// accept each others' blocks until a restart. Unfortunately we haven't figured
		// out a way yet where nodes can decide unilaterally whether the network is new
		// or not. This should be fixed if we figure out a solution.
		if atomic.LoadUint32(&manager.fastSync) == 1 {
			log.Warn("Fast syncing, discarded propagated block", "number", blocks[0].Number(), "hash", blocks[0].Hash())
			return 0, nil
		}
		n, err := manager.blockchain.InsertChain(context.Background(), blocks)
		if err == nil {
			atomic.StoreUint32(&manager.acceptTxs, 1) // Mark initial sync done on any fetcher import
		}
		return n, err
	}
	manager.blockFetcher = fetcher.NewBlockFetcher(blockchain.GetBlockByHash, validator, manager.BroadcastBlock, heighter, inserter, manager.removePeer)

	fetchTx := func(peer string, hashes []common.Hash) error {
		p := manager.peers.Peer(peer)
		if p == nil {
			return errors.New("unknown peer")
		}
		return p.RequestTxs(hashes)
	}
	if txpool != nil {
		manager.txFetcher = fetcher.NewTxFetcher(txpool.Has, txpool.AddRemotes, fetchTx)
	}
	manager.chainSync = newChainSyncer(manager)
}

func (pm *ProtocolManager) makeFirehoseProtocol() p2p.Protocol {
	// Initiate Firehose
	log.Info("Initialising Firehose protocol", "versions", FirehoseVersions)
	return p2p.Protocol{
		Name:    FirehoseName,
		Version: FirehoseVersions[0],
		Length:  FirehoseLengths[0],
		Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
			peer := &firehosePeer{Peer: p, rw: rw}
			select {
			case <-pm.quitSync:
				return p2p.DiscQuitting
			default:
				pm.wg.Add(1)
				defer pm.wg.Done()
				return pm.handleFirehose(peer)
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

func (pm *ProtocolManager) makeDebugProtocol() p2p.Protocol {
	// Initiate Debug protocol
	log.Info("Initialising Debug protocol", "versions", FirehoseVersions)
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

func (pm *ProtocolManager) makeMgrProtocol() p2p.Protocol {
	// Initiate Debug protocol
	log.Info("Initialising Debug protocol", "versions", FirehoseVersions)
	return p2p.Protocol{
		Name:    MGRName,
		Version: MGRVersions[0],
		Length:  MGRLengths[MGRVersions[0]],
		Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
			peer := &mgrPeer{Peer: p, rw: rw}
			select {
			case <-pm.quitSync:
				return p2p.DiscQuitting
			default:
				pm.wg.Add(1)
				defer pm.wg.Done()
				return pm.handleMgr(peer)
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
	pm.downloader.UnregisterPeer(id)
	pm.txFetcher.Drop(id) // nolint:errcheck

	if err := pm.peers.Unregister(id); err != nil {
		log.Error("Peer removal failed", "peer", id, "err", err)
	}
	// Hard disconnect at the networking layer
	if peer != nil {
		peer.Peer.Disconnect(p2p.DiscUselessPeer)
	}
}

func (pm *ProtocolManager) Start(maxPeers int) {
	pm.maxPeers = maxPeers

	// broadcast transactions
	pm.wg.Add(1)
	pm.txsCh = make(chan core.NewTxsEvent, txChanSize)
	pm.txsSub = pm.txpool.SubscribeNewTxsEvent(pm.txsCh)
	if pm.txsSub != nil {
		go pm.txBroadcastLoop()
	}

	// broadcast mined blocks
	pm.wg.Add(1)
	pm.minedBlockSub = pm.eventMux.Subscribe(core.NewMinedBlockEvent{})
	go pm.minedBroadcastLoop()

	// start sync handlers
	pm.wg.Add(2)
	go pm.chainSync.loop()
	go pm.txsyncLoop64() // TODO(karalabe): Legacy initial tx echange, drop with eth/64.
}

func (pm *ProtocolManager) Stop() {
	if pm.txsSub != nil {
		pm.txsSub.Unsubscribe() // quits txBroadcastLoop
	}
	pm.minedBlockSub.Unsubscribe() // quits blockBroadcastLoop

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
	if err := p.Handshake(pm.networkID, td, hash, genesis.Hash(), forkid.NewID(pm.blockchain), pm.forkFilter); err != nil {
		p.Log().Debug("Ethereum handshake failed", "err", err)
		return err
	}

	// Register the peer locally
	if err := pm.peers.Register(p); err != nil {
		p.Log().Error("Ethereum peer registration failed", "err", err)
		return err
	}
	defer pm.removePeer(p.id)

	// Register the peer in the downloader. If the downloader considers it banned, we disconnect
	if err := pm.downloader.RegisterPeer(p.id, p.version, p); err != nil {
		return err
	}
	pm.chainSync.handlePeerEvent(p)

	// Propagate existing transactions. new transactions appearing
	// after this will be sent via broadcasts.
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

func (pm *ProtocolManager) handleFirehose(p *firehosePeer) error {
	for {
		if err := pm.handleFirehoseMsg(p); err != nil {
			p.Log().Debug("Firehose message handling failed", "err", err)
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

func (pm *ProtocolManager) handleMgr(p *mgrPeer) error {
	for {
		if err := pm.handleMgrMsg(p); err != nil {
			p.Log().Debug("MGR message handling failed", "err", err)
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
		var query getBlockHeadersData
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
			if body := pm.blockchain.GetBody(hash); body != nil {
				smallBody := &blockBody{Transactions: body.Transactions, Uncles: body.Uncles}
				if data, err := rlp.EncodeToBytes(smallBody); err == nil {
					bodies = append(bodies, data)
					bytes += len(data)
				} else {
					return err
				}
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

	case p.version >= eth63 && msg.Code == GetNodeDataMsg:
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

	case p.version >= eth63 && msg.Code == GetReceiptsMsg:
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

	case p.version >= eth63 && msg.Code == ReceiptsMsg:
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
		if pm.mode == downloader.StagedSync {
			// Staged sync does not support this yet
			return nil
		}
		var announces newBlockHashesData
		if err := msg.Decode(&announces); err != nil {
			return errResp(ErrDecode, "%v: %v", msg, err)
		}
		// Mark the hashes as present at the remote node
		for _, block := range announces {
			p.MarkBlock(block.Hash)
		}
		// Schedule all the unknown hashes for retrieval
		unknown := make(newBlockHashesData, 0, len(announces))
		for _, block := range announces {
			if !pm.blockchain.HasBlock(block.Hash, block.Number) {
				unknown = append(unknown, block)
			}
		}
		for _, block := range unknown {
			pm.blockFetcher.Notify(p.id, block.Hash, block.Number, time.Now(), p.RequestOneHeader, p.RequestBodies) //nolint:errcheck
		}

	case msg.Code == NewBlockMsg:
		if pm.mode == downloader.StagedSync {
			// Staged sync does not support this yet
			return nil
		}
		// Retrieve and decode the propagated block
		var request newBlockData
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
		if err := pm.blockFetcher.Enqueue(p.id, request.Block); err != nil {
			return err
		}

		// Assuming the block is importable by the peer, but possibly not yet done so,
		// calculate the head hash and TD that the peer truly must have.
		var (
			trueHead = request.Block.ParentHash()
			trueTD   = new(big.Int).Sub(request.TD, request.Block.Difficulty())
		)
		// Update the peer's total difficulty if better than the previous
		if _, td := p.Head(); trueTD.Cmp(td) > 0 {
			p.SetHead(trueHead, trueTD)
			pm.chainSync.handlePeerEvent(p)
		}

	case msg.Code == NewPooledTransactionHashesMsg && p.version >= eth65:
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

func (pm *ProtocolManager) handleFirehoseMsg(p *firehosePeer) error {
	msg, readErr := p.rw.ReadMsg()
	if readErr != nil {
		return readErr
	}
	if msg.Size > FirehoseMaxMsgSize {
		return errResp(ErrMsgTooLarge, "%v > %v", msg.Size, FirehoseMaxMsgSize)
	}
	defer msg.Discard()

	switch msg.Code {
	case GetStateRangesCode:
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		var request getStateRangesOrNodes
		if err := msgStream.Decode(&request); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}

		n := len(request.Prefixes)

		var response stateRangesMsg
		response.ID = request.ID
		response.Entries = make([]firehoseAccountRange, n)

		for i := 0; i < n; i++ {
			response.Entries[i].Status = NoData
		}

		block := pm.blockchain.GetBlockByHash(request.Block)
		if block != nil {
			_, dbstate, err := pm.blockchain.StateAt(block.NumberU64())
			if err != nil {
				return err
			}
			for i, responseSize := 0, 0; i < n && responseSize < softResponseLimit; i++ {
				var leaves []accountLeaf
				allTraversed, err := dbstate.WalkRangeOfAccounts(request.Prefixes[i], MaxLeavesPerPrefix,
					func(key common.Hash, value *accounts.Account) {
						leaves = append(leaves, accountLeaf{key, value})
					},
				)
				if err != nil {
					return err
				}
				if allTraversed {
					response.Entries[i].Status = OK
					response.Entries[i].Leaves = leaves
					responseSize += len(leaves)
				} else {
					response.Entries[i].Status = TooManyLeaves
				}
			}
		} else {
			response.AvailableBlocks = pm.blockchain.AvailableBlocks()
		}

		return p2p.Send(p.rw, StateRangesCode, response)

	case StateRangesCode:
		return errResp(ErrNotImplemented, "Not implemented yet")

	case GetStorageRangesCode:
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		var request getStorageRangesOrNodes
		if err := msgStream.Decode(&request); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}

		numReq := len(request.Requests)

		var response storageRangesMsg
		response.ID = request.ID
		response.Entries = make([][]storageRange, numReq)

		block := pm.blockchain.GetBlockByHash(request.Block)
		if block != nil {
			_, dbstate, err := pm.blockchain.StateAt(block.NumberU64())
			if err != nil {
				return err
			}

			for j, responseSize := 0, 0; j < numReq; j++ {
				req := request.Requests[j]

				n := len(req.Prefixes)
				response.Entries[j] = make([]storageRange, n)
				for i := 0; i < n; i++ {
					response.Entries[j][i].Status = NoData
				}

				addrHash, err := pm.extractAddressHash(req.Account)
				if err != nil {
					return err
				}

				for i := 0; i < n && responseSize < softResponseLimit; i++ {
					var leaves []storageLeaf
					allTraversed, err := dbstate.WalkStorageRange(addrHash, req.Prefixes[i], MaxLeavesPerPrefix,
						func(key common.Hash, value big.Int) {
							leaves = append(leaves, storageLeaf{key, value})
						},
					)
					if err != nil {
						return err
					}
					if allTraversed {
						response.Entries[j][i].Status = OK
						response.Entries[j][i].Leaves = leaves
						responseSize += len(leaves)
					} else {
						response.Entries[j][i].Status = TooManyLeaves
					}
				}
			}
		} else {
			response.AvailableBlocks = pm.blockchain.AvailableBlocks()
		}

		return p2p.Send(p.rw, StorageRangesCode, response)

	case StorageRangesCode:
		return errResp(ErrNotImplemented, "Not implemented yet")

	case GetStateNodesCode:
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		var request getStateRangesOrNodes
		if err := msgStream.Decode(&request); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}

		n := len(request.Prefixes)

		var response stateNodesMsg
		response.ID = request.ID
		response.Nodes = make([][]byte, n)

		block := pm.blockchain.GetBlockByHash(request.Block)
		if block != nil {
			tr := trie.New(common.Hash{})

			for i, responseSize := 0, 0; i < n && responseSize < softResponseLimit; i++ {
				prefix := request.Prefixes[i]
				rr := tr.NewResolveRequest(nil, prefix.ToHex(), prefix.Nibbles(), nil)
				rr.RequiresRLP = true

				resolver := trie.NewResolver(tr, block.NumberU64())
				resolver.SetHistorical(true)
				resolver.AddRequest(rr)

				if err2 := resolver.ResolveWithDb(pm.blockchain.ChainDb(), block.NumberU64(), false); err2 != nil {
					return err2
				}

				node := rr.NodeRLP
				response.Nodes[i] = make([]byte, len(node))
				copy(response.Nodes[i], node)
				responseSize += len(node)
			}
		} else {
			response.AvailableBlocks = pm.blockchain.AvailableBlocks()
		}

		return p2p.Send(p.rw, StateNodesCode, response)

	case StateNodesCode:
		return errResp(ErrNotImplemented, "Not implemented yet")

	case GetStorageNodesCode:
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		var request getStorageRangesOrNodes
		if err := msgStream.Decode(&request); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}

		numReq := len(request.Requests)

		var response storageNodesMsg
		response.ID = request.ID
		response.Nodes = make([][][]byte, numReq)

		block := pm.blockchain.GetBlockByHash(request.Block)
		if block != nil {

			for j, responseSize := 0, 0; j < numReq; j++ {
				req := request.Requests[j]

				n := len(req.Prefixes)
				response.Nodes[j] = make([][]byte, n)

				addrHash, err := pm.extractAddressHash(req.Account)
				if err != nil {
					return err
				}

				tr := trie.New(common.Hash{})

				for i := 0; i < n && responseSize < softResponseLimit; i++ {
					contractPrefix := make([]byte, common.HashLength+common.IncarnationLength)
					copy(contractPrefix, addrHash.Bytes())
					binary.BigEndian.PutUint64(contractPrefix[common.HashLength:], ^uint64(1))
					// TODO [Issue 99] support incarnations
					storagePrefix := req.Prefixes[i]
					rr := tr.NewResolveRequest(contractPrefix, storagePrefix.ToHex(), storagePrefix.Nibbles(), nil)
					rr.RequiresRLP = true

					resolver := trie.NewResolver(tr, block.NumberU64())
					resolver.SetHistorical(true)
					resolver.AddRequest(rr)

					if err2 := resolver.ResolveWithDb(pm.blockchain.ChainDb(), block.NumberU64(), false); err2 != nil {
						return err2
					}

					node := rr.NodeRLP
					response.Nodes[j][i] = make([]byte, len(node))
					copy(response.Nodes[j][i], node)
					responseSize += len(node)
				}
			}
		} else {
			response.AvailableBlocks = pm.blockchain.AvailableBlocks()
		}

		return p2p.Send(p.rw, StorageNodesCode, response)

	case StorageNodesCode:
		return errResp(ErrNotImplemented, "Not implemented yet")

	case GetBytecodeCode:
		// Decode the retrieval message
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		if _, err := msgStream.List(); err != nil {
			return err
		}
		var reqID uint64
		if err := msgStream.Decode(&reqID); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		if _, err := msgStream.List(); err != nil {
			return err
		}

		// Gather bytecodes until the fetch or network limit is reached
		var (
			responseSize int
			code         [][]byte
		)
		for responseSize < softResponseLimit && len(code) < downloader.MaxStateFetch {
			var req bytecodeRef
			if err := msgStream.Decode(&req); err == rlp.EOL {
				break
			} else if err != nil {
				return errResp(ErrDecode, "msg %v: %v", msg, err)
			}

			var addr common.Address
			if len(req.Account) == common.AddressLength {
				addr.SetBytes(req.Account)
			} else if len(req.Account) == common.HashLength {
				var preimageErr error
				addr, preimageErr = pm.blockchain.GetAddressFromItsHash(common.BytesToHash(req.Account))
				if preimageErr == core.ErrNotFound {
					code = append(code, []byte{})
					break
				} else if preimageErr != nil {
					return preimageErr
				}
			} else {
				return errResp(ErrDecode, "not an account address or its hash")
			}

			// Retrieve requested byte code, stopping if enough was found
			if entry, err := pm.blockchain.ByteCode(addr); err == nil {
				code = append(code, entry)
				responseSize += len(entry)
			}
		}
		return p.SendByteCode(reqID, code)

	case BytecodeCode:
		return errResp(ErrNotImplemented, "Not implemented yet")

	case GetStorageSizesCode:
		return errResp(ErrNotImplemented, "Not implemented yet")

	case StorageSizesCode:
		return errResp(ErrNotImplemented, "Not implemented yet")

	default:
		return errResp(ErrInvalidMsgCode, "%v", msg.Code)
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

		genesis := core.DefaultGenesisBlock()
		if err := json.Unmarshal(msgAsJson, genesis); err != nil {
			return fmt.Errorf("json.Unmarshal: %w", err)
		}

		_ = os.Remove("simulator")
		ethDb, err := ethdb.NewBoltDatabase("simulator")
		if err != nil {
			return err
		}
		chainConfig, _, _, err := core.SetupGenesisBlock(ethDb, genesis, true /* history */)
		if err != nil {
			return fmt.Errorf("SetupGenesisBlock: %w", err)
		}

		// Clean up: reuse engine... probably we can
		time.Sleep(100 * time.Millisecond) // wait for pm.syncer finish

		engine := pm.blockchain.Engine()
		pm.blockchain.ChainDb().Close()
		blockchain, err := core.NewBlockChain(ethDb, nil, chainConfig, engine, vm.Config{}, nil)
		if err != nil {
			return fmt.Errorf("NewBlockChain: %w", err)
		}
		pm.blockchain.Stop()
		pm.blockchain = blockchain
		pm.forkFilter = forkid.NewFilter(pm.blockchain)
		initPm(pm, pm.txpool, pm.blockchain.Engine(), pm.blockchain, pm.blockchain.ChainDb())
		pm.quitSync = make(chan struct{})
		remotedbserver.StartDeprecated(ethDb.AbstractKV(), "") // hack to make UI work. But need to somehow re-create whole Node or Ethereum objects

		// hacks to speedup local sync
		downloader.MaxHashFetch = 512 * 10
		downloader.MaxBlockFetch = 128 * 10
		downloader.MaxHeaderFetch = 192 * 10
		downloader.MaxReceiptFetch = 256 * 10

		log.Warn("Succeed to set new Genesis")
		if err := p2p.Send(p.rw, DebugSetGenesisMsg, "{}"); err != nil {
			return fmt.Errorf("p2p.Send: %w", err)
		}
		return nil
	default:
		return errResp(ErrInvalidMsgCode, "%v", msg.Code)
	}
	return nil
}

func (pm *ProtocolManager) handleMgrMsg(p *mgrPeer) error {
	msg, readErr := p.rw.ReadMsg()
	if readErr != nil {
		return fmt.Errorf("handleDebugMsg p.rw.ReadMsg: %w", readErr)
	}
	if msg.Size > MGRMaxMsgSize {
		return errResp(ErrMsgTooLarge, "%v > %v", msg.Size, DebugMaxMsgSize)
	}
	defer msg.Discard()

	switch msg.Code {
	case MGRStatus:
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		var knownPrefixes [][]byte
		if err := msgStream.Decode(&knownPrefixes); err != nil {
			return fmt.Errorf("msgStream.Decode: %w", err)
		}
		tds, err := pm.blockchain.GetTrieDbState()
		if err != nil {
			panic(err)
		}

		fmt.Printf("Received MGRStatus. len(knownPrefixes)=%d\n", len(knownPrefixes))
		buf := bytes.NewBuffer([]byte{})
		blockNr := tds.GetBlockNr()
		epoch := blockNr / 4096
		subtree := epoch % 256
		for i := 0; i < 256; i++ { // spread witness of each subtree
			prefix := []byte{byte(subtree), byte(i)}
			witness, err := tds.ExtractWitnessForPrefix(prefix, false, false)
			if err != nil {
				return err
			}
			buf.Reset()
			if _, err := witness.WriteTo(buf); err != nil {
				return err
			}
			//fmt.Printf("Sernding MGRWitness: %x, of %d\n", prefix, buf.Len())
			//for _, o := range witness.Operators {
			//fmt.Printf("%x\n", o)
			//}

			if err := p.rw.WriteMsg(p2p.Msg{Code: MGRWitness, Size: 0, Payload: buf}); err != nil {
				return err
			}
		}
	case MGRWitness:
		msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
		var marshaledWitness []byte
		if err := msgStream.Decode(&marshaledWitness); err != nil {
			return fmt.Errorf("msgStream.Decode: %w", err)
		}

		res, err := trie.NewWitnessFromReader(bytes.NewReader(marshaledWitness), false)
		if err != nil {
			panic(err)
		}

		fmt.Printf("Received MGRWitness: %v\n", res)

		return nil
	default:
		return errResp(ErrInvalidMsgCode, "%v", msg.Code)
	}
	return nil
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
		case event := <-pm.txsCh:
			// For testing purpose only, disable propagation
			if pm.broadcastTxAnnouncesOnly {
				pm.BroadcastTransactions(event.Txs, false)
				continue
			}
			pm.BroadcastTransactions(event.Txs, true)  // First propagate transactions to peers
			pm.BroadcastTransactions(event.Txs, false) // Only then announce to the rest

		case <-pm.txsSub.Err():
			return
		}
	}
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
		Config:     pm.blockchain.Config(),
		Head:       currentBlock.Hash(),
	}
}
