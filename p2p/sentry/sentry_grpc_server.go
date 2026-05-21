// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package sentry

import (
	"bytes"
	"container/heap"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"slices"
	"sort"
	"sync"
	"syscall"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/diagnostics/diaglib"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/grpcutil"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/dnsdisc"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/protocols/wit"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"

	_ "github.com/erigontech/erigon/polygon/chain" // Register Polygon chains
)

const (
	// handshakeTimeout is the maximum allowed time for the `eth` handshake to
	// complete before dropping the connection.= as malicious.
	handshakeTimeout = 5 * time.Second
	// ethProtocolTimeout is the maximum allowed time for the ETH protocol to be ready
	// before dropping the connection. This prevents goroutine leaks and DOS attacks.
	ethProtocolTimeout = 30 * time.Second
	// awaitStatusTimeout caps how long an inbound peer's Protocol.Run waits
	// for the multi-client's first SetStatus to populate ss.statusData. It
	// only matters during the startup window when the shared p2p.Server is
	// already listening but SetStatus hasn't propagated yet.
	awaitStatusTimeout = 10 * time.Second
	maxPermitsPerPeer  = 4 // How many outstanding requests per peer we may have
)

// PeerInfo collects various extra bits of information about the peer,
// for example deadlines that is used for regulating requests sent to the peer
type PeerInfo struct {
	peer             *p2p.Peer
	lock             sync.RWMutex
	deadlines        []time.Time // Request deadlines
	latestDealine    time.Time
	minBlock, height uint64
	// ethRw and witRw are the per-subprotocol MsgReadWriters this peer is
	// using. They live on the SAME RLPx connection but have different code
	// offsets, so writePeer must select the right one for the message's
	// protocol — pinning a single "canonical" rw routes wit frames onto
	// the eth offset (or vice versa) and breaks the receiving side. The
	// respective Protocol.Run sets each via SetEthRw / SetWitRw before
	// announcing the peer as ready.
	ethRw                 p2p.MsgReadWriter
	witRw                 p2p.MsgReadWriter
	protocol, witProtocol uint
	knownWitnesses        *wit.KnownCache // Set of witness hashes (`witness.Headers[0].Hash()`) known to be known by this peer
	ethReady              chan struct{}
	ethReadyOnce          sync.Once

	ctx       context.Context
	ctxCancel context.CancelFunc

	// this channel is closed on Remove()
	removed      chan struct{}
	removeReason *p2p.PeerError
	removeOnce   sync.Once

	// each peer has own worker (goroutine) - all funcs from this queue will execute on this worker
	// if this queue is full (means peer is slow) - old messages will be dropped
	// channel closed on peer remove
	tasks chan func()
}

type PeerRef struct {
	pi     *PeerInfo
	height uint64
}

// WitnessRequest tracks when a witness request was initiated (for deduplication and cleanup)
type WitnessRequest struct {
	RequestedAt time.Time
}

// PeersByMinBlock is the priority queue of peers. Used to select certain number of peers considered to be "best available"
type PeersByMinBlock []PeerRef

// Len (part of heap.Interface) returns the current size of the best peers queue
func (bp PeersByMinBlock) Len() int {
	return len(bp)
}

// Less (part of heap.Interface) compares two peers
func (bp PeersByMinBlock) Less(i, j int) bool {
	return bp[i].height < bp[j].height
}

// Swap (part of heap.Interface) moves two peers in the queue into each other's places.
func (bp PeersByMinBlock) Swap(i, j int) {
	bp[i], bp[j] = bp[j], bp[i]
}

// Push (part of heap.Interface) places a new peer onto the end of queue.
func (bp *PeersByMinBlock) Push(x any) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	p := x.(PeerRef)
	*bp = append(*bp, p)
}

// Pop (part of heap.Interface) removes the first peer from the queue
func (bp *PeersByMinBlock) Pop() any {
	old := *bp
	n := len(old)
	x := old[n-1]
	old[n-1] = PeerRef{} // avoid memory leak
	*bp = old[0 : n-1]
	return x
}

func NewPeerInfo(peer *p2p.Peer) *PeerInfo {
	ctx, cancel := context.WithCancel(context.Background())

	p := &PeerInfo{
		peer:           peer,
		ethReady:       make(chan struct{}),
		knownWitnesses: wit.NewKnownCache(wit.MaxKnownWitnesses),
		removed:        make(chan struct{}),
		tasks:          make(chan func(), 32),
		ctx:            ctx,
		ctxCancel:      cancel,
	}

	p.lock.RLock()
	t := p.tasks
	p.lock.RUnlock()

	go func() { // each peer has own worker, then slow
		for f := range t {
			f()
		}
	}()
	return p
}

func (pi *PeerInfo) Close() {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	if pi.tasks != nil {
		close(pi.tasks)
		// Setting this to nil because other functions detect the closure of the channel by checking pi.tasks == nil
		pi.tasks = nil
	}
}

func (pi *PeerInfo) ID() [64]byte {
	return pi.peer.Pubkey()
}

// SetEthRw stores the eth protoRW for this peer. Called by the eth Run
// after getOrCreatePeer so writePeer can route eth-protocol messages
// through the eth offset on the RLPx connection regardless of which
// Protocol.Run created the shared PeerInfo first.
func (pi *PeerInfo) SetEthRw(rw p2p.MsgReadWriter) {
	pi.lock.Lock()
	pi.ethRw = rw
	pi.lock.Unlock()
}

// EthRw returns the eth-subprotocol MsgReadWriter, or nil if the eth Run
// hasn't attached it yet (which means this peer isn't yet ready for eth
// outbound — writePeer must skip).
func (pi *PeerInfo) EthRw() p2p.MsgReadWriter {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	return pi.ethRw
}

// SetWitRw stores the wit protoRW for this peer (called by the wit Run).
// Outbound wit messages (GET_BLOCK_WITNESS_W0 etc.) must go through this
// rw, never through the eth one — they use a different protocol offset.
func (pi *PeerInfo) SetWitRw(rw p2p.MsgReadWriter) {
	pi.lock.Lock()
	pi.witRw = rw
	pi.lock.Unlock()
}

// WitRw returns the wit-subprotocol MsgReadWriter, or nil if the wit Run
// hasn't attached it yet (e.g. peer doesn't advertise wit). Callers
// targeting wit messages must treat nil as "this peer doesn't speak wit"
// and skip the write.
func (pi *PeerInfo) WitRw() p2p.MsgReadWriter {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	return pi.witRw
}

// AddDeadline adds given deadline to the list of deadlines
// Deadlines must be added in the chronological order for the function
// ClearDeadlines to work correctly (it uses binary search)
func (pi *PeerInfo) AddDeadline(deadline time.Time) {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	pi.deadlines = append(pi.deadlines, deadline)
	pi.latestDealine = deadline
}

func (pi *PeerInfo) Height() uint64 {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	return pi.height
}

// SetEthProtocol sets protocol version and marks the ETH protocol as ready
func (pi *PeerInfo) SetEthProtocol(version uint) {
	if version == 0 {
		return
	}

	pi.lock.Lock()
	pi.protocol = version
	pi.lock.Unlock()

	pi.ethReadyOnce.Do(func() { close(pi.ethReady) })
}

// WaitForEth blocks until the ETH handshake completes or returns a disconnect reason
func (pi *PeerInfo) WaitForEth(ctx context.Context) *p2p.PeerError {
	if !pi.peer.RunningProtocol(eth.ProtocolName) {
		return p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscProtocolError, nil, "wit protocol requires eth capability")
	}

	pi.lock.RLock()
	ready := pi.protocol != 0
	readyCh := pi.ethReady
	pi.lock.RUnlock()

	if ready {
		return nil
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, ethProtocolTimeout)
	defer cancel()

	select {
	case <-readyCh:
		return nil
	case <-pi.removed:
		return pi.RemoveReason()
	case <-timeoutCtx.Done():
		return p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscQuitting, timeoutCtx.Err(), "wit protocol waiting for eth handshake cancelled")
	}
}

// SetIncreasedHeight updates PeerInfo.height only if newHeight is higher (threadsafe)
func (pi *PeerInfo) SetIncreasedHeight(newHeight uint64) {
	pi.lock.Lock()
	if pi.height < newHeight {
		pi.height = newHeight
	}
	pi.lock.Unlock()
}

// MinBlock gets earliest block for eth/69 peers, falls back to height if not available
// We use this to select a peer, fallback behaviour is valid since it will give us potentially
// fewer peers but the peers will still be valid.
func (pi *PeerInfo) MinBlock() uint64 {
	pi.lock.RLock()
	defer pi.lock.RUnlock()

	if pi.minBlock != 0 {
		return pi.minBlock
	}
	return pi.height
}

// SetBlockRange updates minBlock and (monotonically) increases height under a single lock
func (pi *PeerInfo) SetBlockRange(newMinBlock, newHeight uint64) {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	pi.minBlock = newMinBlock
	if pi.height < newHeight {
		pi.height = newHeight
	}
}

// SetMinimumBlock updates PeerInfo.minBlock from BlockRangeUpdate message
func (pi *PeerInfo) SetMinimumBlock(newMinBlock uint64) {
	pi.lock.Lock()
	pi.minBlock = newMinBlock
	pi.lock.Unlock()
}

// ClearDeadlines goes through the deadlines of
// given peers and removes the ones that have passed
// Optionally, it also clears one extra deadline - this is used when response is received
// It returns the number of deadlines left
func (pi *PeerInfo) ClearDeadlines(now time.Time, givePermit bool) int {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	// Look for the first deadline which is not passed yet
	firstNotPassed := sort.Search(len(pi.deadlines), func(i int) bool {
		return pi.deadlines[i].After(now)
	})
	cutOff := firstNotPassed
	if cutOff < len(pi.deadlines) && givePermit {
		cutOff++
	}
	pi.deadlines = pi.deadlines[cutOff:]
	return len(pi.deadlines)
}

func (pi *PeerInfo) LatestDeadline() time.Time {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	return pi.latestDealine
}

func (pi *PeerInfo) Remove(reason *p2p.PeerError) {
	pi.removeOnce.Do(func() {
		pi.removeReason = reason
		close(pi.removed)
		pi.ctxCancel()
		pi.peer.Disconnect(reason)
	})
}

func (pi *PeerInfo) Async(f func(), logger log.Logger) {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	if pi.tasks == nil {
		// Too late, the task channel has been closed
		return
	}
	select {
	case <-pi.removed: // noop if peer removed
	case <-pi.ctx.Done():
		if pi.tasks != nil {
			close(pi.tasks)
			// Setting this to nil because other functions detect the closure of the channel by checking pi.tasks == nil
			pi.tasks = nil
		}
	case pi.tasks <- f:
		if len(pi.tasks) == cap(pi.tasks) { // if channel full - discard old messages
			for i := 0; i < cap(pi.tasks)/2; i++ {
				select {
				case <-pi.tasks:
				default:
				}
			}
			logger.Trace("[sentry] slow peer or too many requests, dropping its old requests", "name", pi.peer.Name())
		}
	}
}

func (pi *PeerInfo) RemoveReason() *p2p.PeerError {
	select {
	case <-pi.removed:
		return pi.removeReason
	default:
		return nil
	}
}

func (pi *PeerInfo) AddKnownWitness(hash common.Hash) {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	pi.knownWitnesses.Add(hash)
}

// ConvertH512ToPeerID ensures the return type is [64]byte
// so that short variable declarations will still be formatted as hex in logs
func ConvertH512ToPeerID(h512 *typesproto.H512) [64]byte {
	return gointerfaces.ConvertH512ToHash(h512)
}

func makeP2PServer(
	p2pConfig p2p.Config,
	bootnodes []string,
	protocols []p2p.Protocol,
) (*p2p.Server, error) {
	// Only fall back to chain-default bootnodes when the caller didn't configure
	// BootstrapNodes at all (nil slice). An empty non-nil slice signals explicit
	// opt-out (e.g., --bootnodes= on the CLI) and must be preserved.
	if p2pConfig.BootstrapNodes == nil && len(bootnodes) > 0 {
		bootstrapNodes, err := enode.ParseNodesFromURLs(bootnodes)
		if err != nil {
			return nil, fmt.Errorf("bad bootnodes option: %w", err)
		}
		p2pConfig.BootstrapNodes = bootstrapNodes
		p2pConfig.BootstrapNodesV5 = bootstrapNodes
	}
	p2pConfig.Protocols = protocols
	return &p2p.Server{Config: p2pConfig}, nil
}

func runPeer(
	ctx context.Context,
	peerID [64]byte,
	cap p2p.Cap,
	rw p2p.MsgReadWriter,
	peerInfo *PeerInfo,
	send func(msgId sentryproto.MessageId, peerID [64]byte, b []byte),
	hasSubscribers func(msgId sentryproto.MessageId) bool,
	logger log.Logger,
) *p2p.PeerError {
	protocol := cap.Version
	printTime := time.Now().Add(time.Minute)
	peerPrinted := false
	defer func() {
		select { // don't print logs if we stopping
		case <-ctx.Done():
			return
		default:
		}
		if peerPrinted {
			logger.Trace("Peer disconnected", "id", hex.EncodeToString(peerID[:]), "name", peerInfo.peer.Fullname())
		}
	}()

	for {
		if !peerPrinted {
			if time.Now().After(printTime) {
				logger.Trace("Peer stable", "id", hex.EncodeToString(peerID[:]), "name", peerInfo.peer.Fullname())
				peerPrinted = true
			}
		}
		if err := common.Stopped(ctx.Done()); err != nil {
			return p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscQuitting, ctx.Err(), "sentry.runPeer: context stopped")
		}
		if err := peerInfo.RemoveReason(); err != nil {
			return err
		}

		msg, err := rw.ReadMsg()
		if err != nil {
			return p2p.NewPeerError(p2p.PeerErrorMessageReceive, p2p.DiscNetworkError, err, "sentry.runPeer: ReadMsg error")
		}

		if msg.Size > eth.ProtocolMaxMsgSize {
			msg.Discard()
			return p2p.NewPeerError(p2p.PeerErrorMessageSizeLimit, p2p.DiscSubprotocolError, nil, fmt.Sprintf("sentry.runPeer: message is too large %d, limit %d", msg.Size, eth.ProtocolMaxMsgSize))
		}

		givePermit := false
		switch msg.Code {
		case eth.StatusMsg:
			msg.Discard()
			// Status messages should never arrive after the handshake
			return p2p.NewPeerError(p2p.PeerErrorStatusUnexpected, p2p.DiscSubprotocolError, nil, "sentry.runPeer: unexpected status message")
		case eth.GetBlockHeadersMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}

			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.BlockHeadersMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			givePermit = true
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.GetBlockBodiesMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.BlockBodiesMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			givePermit = true
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.GetBlockAccessListsMsg:
			// eth/71 (EIP-8159) — inbound BAL request. Mirrors GetBlockBodiesMsg:
			// read-only request, no permit change, forward to subscribers.
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.BlockAccessListsMsg:
			// eth/71 (EIP-8159) — inbound BAL response. Mirrors BlockBodiesMsg:
			// completes an in-flight request so release a request permit.
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			givePermit = true
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.GetReceiptsMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
			//log.Info(fmt.Sprintf("[%s] GetReceiptsMsg", peerID))
		case eth.ReceiptsMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
			//log.Info(fmt.Sprintf("[%s] ReceiptsMsg", peerID))
		case eth.NewBlockHashesMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}
			//log.Debug("NewBlockHashesMsg from", "peerId", fmt.Sprintf("%x", peerID)[:20], "name", peerInfo.peer.Name())
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.NewBlockMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}
			//log.Debug("NewBlockMsg from", "peerId", fmt.Sprintf("%x", peerID)[:20], "name", peerInfo.peer.Name())
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.NewPooledTransactionHashesMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}

			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.GetPooledTransactionsMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}

			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.TransactionsMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}

			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.PooledTransactionsMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}

			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.BlockRangeUpdateMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}

			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error("reading msg into bytes", "peerId", hex.EncodeToString(peerID[:]), "err", err)
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		default:
			logger.Error(fmt.Sprintf("[p2p] Unknown message code: %d, peerID=%v", msg.Code, hex.EncodeToString(peerID[:])))
		}

		msgType := eth.ToProto[protocol][msg.Code]
		msgCap := cap.String()

		trackPeerStatistics(peerInfo.peer.Fullname(), peerInfo.peer.ID().String(), true, msgType.String(), msgCap, int(msg.Size))

		msg.Discard()
		peerInfo.ClearDeadlines(time.Now(), givePermit)
	}
}

func trackPeerStatistics(peerName string, peerID string, inbound bool, msgType string, msgCap string, bytes int) {
	isDiagEnabled := diaglib.TypeOf(diaglib.PeerStatisticMsgUpdate{}).Enabled()
	if isDiagEnabled {
		stats := diaglib.PeerStatisticMsgUpdate{
			PeerName: peerName,
			PeerID:   peerID,
			Inbound:  inbound,
			MsgType:  msgType,
			MsgCap:   msgCap,
			Bytes:    bytes,
			PeerType: "Sentry",
		}

		diaglib.Send(stats)
	}
}
func runWitPeer(
	ctx context.Context,
	peerID [64]byte,
	rw p2p.MsgReadWriter,
	peerInfo *PeerInfo,
	send func(msgId sentryproto.MessageId, peerID [64]byte, b []byte),
	hasSubscribers func(msgId sentryproto.MessageId) bool,
	getWitnessRequest func(hash common.Hash, peerID [64]byte) bool,
	logger log.Logger,
) *p2p.PeerError {
	if err := peerInfo.WaitForEth(ctx); err != nil {
		return err
	}

	protocol := uint(wit.WIT1)
	pubkey := peerInfo.peer.Pubkey()
	logger.Debug("[wit] wit protocol active", "peer", hex.EncodeToString(pubkey[:]), "version", protocol)
	for {
		if err := common.Stopped(ctx.Done()); err != nil {
			return p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscQuitting, ctx.Err(), "sentry.runPeer: context stopped")
		}
		if err := peerInfo.RemoveReason(); err != nil {
			return err
		}

		msg, err := rw.ReadMsg()
		if err != nil {
			return p2p.NewPeerError(p2p.PeerErrorMessageReceive, p2p.DiscNetworkError, err, "sentry.runPeer: ReadMsg error")
		}

		if msg.Size > wit.MaxMessageSize {
			msg.Discard()
			return p2p.NewPeerError(p2p.PeerErrorMessageSizeLimit, p2p.DiscSubprotocolError, nil, fmt.Sprintf("sentry.runPeer: message is too large %d, limit %d", msg.Size, eth.ProtocolMaxMsgSize))
		}

		switch msg.Code {
		case wit.GetWitnessMsg, wit.WitnessMsg:
			if !hasSubscribers(wit.ToProto[protocol][msg.Code]) {
				continue
			}

			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}
			send(wit.ToProto[protocol][msg.Code], peerID, b)
		case wit.NewWitnessMsg:
			// add hashes to peer
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}

			var query wit.NewWitnessPacket
			if err := rlp.DecodeBytes(b, &query); err != nil {
				logger.Error("decoding NewWitnessMsg: %w, data: %x", err, b)
				return p2p.NewPeerError(p2p.PeerErrorInvalidMessage, p2p.DiscSubprotocolError, err, "decoding NewWitnessMsg")
			}

			peerInfo.AddKnownWitness(query.Witness.Header().Hash())

			// send to client to add witness to db
			if !hasSubscribers(wit.ToProto[protocol][msg.Code]) {
				continue
			}
			send(wit.ToProto[protocol][msg.Code], peerID, b)
		case wit.NewWitnessHashesMsg:
			// add hashes to peer
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				logger.Error(fmt.Sprintf("%s: reading msg into bytes: %v", hex.EncodeToString(peerID[:]), err))
			}

			var query wit.NewWitnessHashesPacket
			if err := rlp.DecodeBytes(b, &query); err != nil {
				logger.Error("decoding NewWitnessHashesMsg: %w, data: %x", err, b)
				return p2p.NewPeerError(p2p.PeerErrorInvalidMessage, p2p.DiscSubprotocolError, err, "decoding NewWitnessHashesMsg")
			}

			for _, hash := range query.Hashes {
				peerInfo.AddKnownWitness(hash)
			}

			// process each announced block hash with deduplication
			for _, hash := range query.Hashes {
				shouldRequest := getWitnessRequest(hash, peerID)
				if !shouldRequest {
					continue // already being requested by another peer
				}

				// send GetWitnessMsg request starting from page 0
				getWitnessReq := wit.GetWitnessPacket{
					RequestId: rand.Uint64(),
					GetWitnessRequest: &wit.GetWitnessRequest{
						WitnessPages: []wit.WitnessPageRequest{
							{
								Hash: hash,
								Page: 0,
							},
						},
					},
				}

				reqData, err := rlp.EncodeToBytes(&getWitnessReq)
				if err != nil {
					logger.Error("encoding GetWitnessMsg request", "err", err, "hash", hash)
					continue
				}

				if err := rw.WriteMsg(p2p.Msg{
					Code:    wit.GetWitnessMsg,
					Size:    uint32(len(reqData)),
					Payload: bytes.NewReader(reqData),
				}); err != nil {
					logger.Debug("sending GetWitnessMsg request", "err", err, "hash", hash)
				} else {
					logger.Debug("sent GetWitnessMsg request", "hash", hash, "page", 0, "peer", hex.EncodeToString(peerID[:]))
				}
			}
		default:
			logger.Error(fmt.Sprintf("%s: unknown message code: %d", hex.EncodeToString(pubkey[:]), msg.Code))
		}
	}
}

func grpcSentryServer(ctx context.Context, sentryAddr string, ss *GrpcServer, healthCheck bool) (*grpc.Server, error) {
	// STARTING GRPC SERVER
	ss.logger.Info("Starting Sentry gRPC server", "on", sentryAddr)
	listenConfig := net.ListenConfig{
		Control: func(network, address string, _ syscall.RawConn) error {
			log.Info("[p2p] Sentry gRPC received connection", "via", network, "from", address)
			return nil
		},
	}
	lis, err := listenConfig.Listen(ctx, "tcp", sentryAddr)
	if err != nil {
		return nil, fmt.Errorf("could not create Sentry P2P listener: %w, addr=%s", err, sentryAddr)
	}
	grpcServer := grpcutil.NewServer(100, nil)
	sentryproto.RegisterSentryServer(grpcServer, ss)
	var healthServer *health.Server
	if healthCheck {
		healthServer = health.NewServer()
		grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	}

	go func() {
		if healthCheck {
			defer healthServer.Shutdown()
		}
		if err1 := grpcServer.Serve(lis); err1 != nil {
			ss.logger.Error("Sentry gRPC server fail", "err", err1)
		}
	}()
	return grpcServer, nil
}

func NewGrpcServer(ctx context.Context, dialCandidates func() enode.Iterator, readNodeInfo func() *eth.NodeInfo, cfg *p2p.Config, protocol uint, logger log.Logger, bootnodes []string, dnsNetwork string) *GrpcServer {
	ss := &GrpcServer{
		ctx:                   ctx,
		p2p:                   cfg,
		peersStreams:          NewPeersStreams(),
		logger:                logger,
		peers:                 NewPeerStore(),
		activeWitnessRequests: make(map[common.Hash]*WitnessRequest),
		bootnodes:             bootnodes,
		dnsNetwork:            dnsNetwork,
		statusReady:           make(chan struct{}),
	}

	var disc enode.Iterator
	if dialCandidates != nil {
		disc = dialCandidates()
	} else {
		disc, _ = setupDiscovery(ss.p2p.DiscoveryDNS)
	}

	ss.Protocols = append(ss.Protocols, p2p.Protocol{
		Name:           eth.ProtocolName,
		Version:        protocol,
		Length:         eth.ProtocolLengths[protocol],
		DialCandidates: disc,
		Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) *p2p.PeerError {
			peerID := peer.Pubkey()
			printablePeerID := hex.EncodeToString(peerID[:])
			logger.Trace("[p2p] start with peer", "peerId", printablePeerID)
			// Wait briefly for the multi-client's first SetStatus to land.
			// With a shared p2p.Server (SetP2PServer) the listener can be up
			// before status arrives — without this wait those inbound dials
			// would all bounce off DiscProtocolError.
			status := ss.awaitStatus(awaitStatusTimeout)
			if status == nil {
				return p2p.NewPeerError(p2p.PeerErrorLocalStatusNeeded, p2p.DiscProtocolError, nil, "could not get status message from core")
			}

			var minBlock, latestBlock uint64
			if protocol >= direct.ETH69 {
				statusPacket69, err := handShake[eth.StatusPacket69](ctx, status, rw, protocol, protocol, encodeStatusPacket69, compatStatusPacket69, handshakeTimeout)
				if err != nil {
					return err
				}

				minBlock = statusPacket69.MinimumBlock
				latestBlock = statusPacket69.LatestBlock
			} else {
				statusPacket, err := handShake[eth.StatusPacket](ctx, status, rw, protocol, protocol, encodeStatusPacket, compatStatusPacket, handshakeTimeout)
				if err != nil {
					return err
				}

				peerBestHash := statusPacket.Head

				getBlockHeadersErr := ss.getBlockHeaders(ctx, peerBestHash, peerID)
				if getBlockHeadersErr != nil {
					return p2p.NewPeerError(p2p.PeerErrorFirstMessageSend, p2p.DiscNetworkError, getBlockHeadersErr, "p2p.Protocol.Run getBlockHeaders failure")
				}
			}

			// handshake is successful
			logger.Trace("[p2p] Received status message OK", "peerId", printablePeerID, "name", peer.Name(), "caps", peer.Caps())
			peerInfo, err := ss.getOrCreatePeer(peer, eth.ProtocolName)
			if err != nil {
				return err
			}
			// Attach the eth subprotocol rw so writePeer can route
			// eth-protocol outbound at the right RLPx offset.
			peerInfo.SetEthRw(rw)
			peerInfo.SetEthProtocol(protocol)

			if protocol >= direct.ETH69 {
				peerInfo.SetBlockRange(minBlock, latestBlock)
			}

			peerInfo.protocol = protocol
			ss.sendNewPeerToClients(gointerfaces.ConvertHashToH512(peerID))
			defer ss.sendGonePeerToClients(gointerfaces.ConvertHashToH512(peerID))
			defer peerInfo.Close()
			// note for consistency we want to delete the peer before send the disconnect event via sendGonePeerToClients
			defer ss.deletePeer(peerID)

			cap := p2p.Cap{Name: eth.ProtocolName, Version: protocol}

			return runPeer(
				ctx,
				peerID,
				cap,
				rw,
				peerInfo,
				ss.send,
				ss.hasSubscribers,
				logger,
			)
		},
		NodeInfo: func() any {
			return readNodeInfo()
		},
		PeerInfo: func(peerID [64]byte) any {
			// TODO: remember handshake reply per peer ID and return eth-related Status info (see ethPeerInfo in geth)
			return nil
		},
		//Attributes: []enr.Entry{eth.CurrentENREntry(chainConfig, genesisHash, headHeight)},
		FromProto: eth.FromProto[protocol],
		ToProto:   eth.ToProto[protocol],
	})

	// Add WIT protocol if enabled
	if cfg.EnableWitProtocol {
		log.Debug("[wit] running wit protocol")
		ss.Protocols = append(ss.Protocols, p2p.Protocol{
			Name:           wit.ProtocolName,
			Version:        wit.ProtocolVersions[0],
			Length:         wit.ProtocolLengths[wit.ProtocolVersions[0]],
			DialCandidates: nil,
			Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) *p2p.PeerError {
				peerID := peer.Pubkey()
				peerInfo, err := ss.getOrCreatePeer(peer, wit.ProtocolName)
				if err != nil {
					return err
				}
				peerInfo.SetWitRw(rw)
				peerInfo.witProtocol = wit.ProtocolVersions[0]
				// In shared-Server mode wit/0 is deduped to one sentry; if
				// no eth Run ever fires on this sentry for this peer (which
				// is the common case — wit lives on sentry[0], eth/* on the
				// version sentry), nothing else tears down the goodPeers
				// entry or the PeerInfo task-channel worker. Close is
				// idempotent (it nil-checks pi.tasks) so it's safe even
				// when eth Run also runs and defers Close on its side.
				defer ss.deletePeer(peerID)
				defer peerInfo.Close()

				return runWitPeer(
					ctx,
					peerID,
					rw,
					peerInfo,
					ss.send,
					ss.hasSubscribers,
					ss.getWitnessRequest,
					logger,
				)
			},
			NodeInfo: func() any {
				return readNodeInfo()
			},
			PeerInfo: func(peerID [64]byte) any {
				return nil
			},
			FromProto: wit.FromProto[wit.ProtocolVersions[0]],
			ToProto:   wit.ToProto[wit.ProtocolVersions[0]],
		})
	}

	// start cleanup routine for stale witness requests
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				ss.cleanupOldWitnessRequests()
			}
		}
	}()

	return ss
}

// Sentry creates and runs standalone sentry
func Sentry(ctx context.Context, dirs datadir.Dirs, sentryAddr string, discoveryDNS []string, cfg *p2p.Config, protocolVersion uint, healthCheck bool, logger log.Logger) error {
	dir.MustExist(dirs.DataDir)

	discovery := func() enode.Iterator {
		d, err := setupDiscovery(discoveryDNS)
		if err != nil {
			panic(err)
		}
		return d
	}
	cfg.DiscoveryDNS = discoveryDNS
	sentryServer := NewGrpcServer(ctx, discovery, func() *eth.NodeInfo { return nil }, cfg, protocolVersion, logger, nil, "")

	grpcServer, err := grpcSentryServer(ctx, sentryAddr, sentryServer, healthCheck)
	if err != nil {
		return err
	}

	<-ctx.Done()
	grpcServer.GracefulStop()
	sentryServer.Close()
	return nil
}

// PeerStore is the per-peer state registry shared by GrpcServers that back
// the same p2p.Server. With wit/0 deduped to a single GrpcServer in shared-
// Server mode, the wit Run and the negotiated eth/* Run land on different
// sentries; without a shared store they each create their own PeerInfo and
// wit's WaitForEth never observes the eth handshake complete, so the peer
// would be disconnected after ethProtocolTimeout. The shared store keeps
// one PeerInfo per peer regardless of which sentry's Run touched it first.
//
// Each GrpcServer owns its own PeerStore by default (set up in
// NewGrpcServer); SetSharedPeerStore swaps in a coordinator-supplied store.
type PeerStore struct {
	mu    sync.RWMutex
	peers map[[64]byte]*PeerInfo
}

// NewPeerStore returns an empty PeerStore ready for sentries to share.
func NewPeerStore() *PeerStore {
	return &PeerStore{peers: make(map[[64]byte]*PeerInfo)}
}

type GrpcServer struct {
	sentryproto.UnimplementedSentryServer
	ctx           context.Context
	Protocols     []p2p.Protocol
	peers         *PeerStore
	p2pServer     *p2p.Server
	p2pServerLock sync.RWMutex
	// external is true when p2pServer was injected by an outer coordinator
	// (e.g. node/components/sentry.Provider) via SetP2PServer rather than
	// created lazily inside SetStatus. External Servers are shared across
	// multiple GrpcServer instances and their lifecycle is owned by the
	// coordinator, so this GrpcServer must not Stop() them on Close().
	external bool
	// statusReady is closed by SetStatus the first time it stores a
	// non-empty statusData. Protocol.Run waits on it (with a timeout) so
	// inbound dials that land on the listener before the multi-client has
	// broadcast SetStatus don't get an instant DiscProtocolError disconnect.
	statusReady          chan struct{}
	statusReadyOnce      sync.Once
	statusData           *sentryproto.StatusData
	statusDataLock       sync.RWMutex
	messageStreams       map[sentryproto.MessageId]map[uint64]chan *sentryproto.InboundMessage
	messagesSubscriberID uint64
	messageStreamsLock   sync.RWMutex
	peersStreams         *PeersStreams
	p2p                  *p2p.Config
	logger               log.Logger
	bootnodes            []string // chain-specific bootnodes, used if p2pConfig has none
	dnsNetwork           string   // chain-specific DNS discovery URL
	// witness request tracking
	activeWitnessRequests map[common.Hash]*WitnessRequest
	witnessRequestMutex   sync.RWMutex
}

// SetSharedPeerStore swaps in a coordinator-supplied PeerStore so several
// GrpcServers backing the same p2p.Server can see one PeerInfo per peer.
// This is what makes wit/0 (deduped to a single sentry) and the negotiated
// eth/* (on a different sentry) share the same eth-ready signal — without
// it, wit's WaitForEth would time out and disconnect every peer after
// ethProtocolTimeout.
//
// Must be called BEFORE SetP2PServer / srv.Start; the helpers that look up
// the store don't synchronise on the field itself, so concurrent peer Run
// closures must not be live when this swap happens. NewGrpcServer gives
// each sentry its own store by default — pass nil here to keep that.
func (ss *GrpcServer) SetSharedPeerStore(s *PeerStore) {
	if s == nil {
		return
	}
	ss.peers = s
}

// SetP2PServer injects an externally-managed p2p.Server into this GrpcServer
// so several GrpcServer instances can share one Server (and therefore one
// Node ID, one ENR, one listener port). It bypasses the lazy server
// construction inside SetStatus.
//
// Each GrpcServer reports peers (via Peers / SimplePeerCount) filtered by
// its own eth protocol version — even when SetSharedPeerStore is in
// effect and every sentry can see the same PeerInfo map. That's what
// keeps node/eth.Ethereum.Peers aggregation from N-fold-duplicating the
// result and lets the multi-sentry router map each peer to the correct
// sentry's gRPC client.
//
// Calling order: SetSharedPeerStore (optional), then SetP2PServer, then
// srv.Start. The Provider follows this order in Initialize before any
// peer can connect; LocalNode-touching paths (SetStatus) must still wait
// until after srv.Start because LocalNode is created there. Calling
// SetP2PServer more than once on the same GrpcServer returns an error —
// ownership is decided up front. A nil srv is rejected too: if it were
// accepted, SetStatus would lazily build its own Server (good) but Close
// would still see external=true and skip Stop (leak).
func (ss *GrpcServer) SetP2PServer(srv *p2p.Server) error {
	if srv == nil {
		return errors.New("sentry.GrpcServer: SetP2PServer called with nil *p2p.Server")
	}
	ss.p2pServerLock.Lock()
	defer ss.p2pServerLock.Unlock()
	if ss.p2pServer != nil {
		return errors.New("sentry.GrpcServer: SetP2PServer called when p2pServer is already set")
	}
	ss.p2pServer = srv
	ss.external = true
	return nil
}

// cleanupOldWitnessRequests removes witness requests that have been active for too long
func (ss *GrpcServer) cleanupOldWitnessRequests() {
	ss.witnessRequestMutex.Lock()
	defer ss.witnessRequestMutex.Unlock()

	timeout := 1 * time.Minute
	now := time.Now()

	for hash, req := range ss.activeWitnessRequests {
		if now.Sub(req.RequestedAt) > timeout {
			ss.logger.Debug("cleaning up stale witness request", "hash", hash, "age", now.Sub(req.RequestedAt))
			delete(ss.activeWitnessRequests, hash)
		}
	}
}

// getWitnessRequest checks if we should request a witness
func (ss *GrpcServer) getWitnessRequest(hash common.Hash, peerID [64]byte) bool {
	ss.witnessRequestMutex.Lock()
	defer ss.witnessRequestMutex.Unlock()

	if _, exists := ss.activeWitnessRequests[hash]; exists {
		return false
	}

	witnessReq := &WitnessRequest{
		RequestedAt: time.Now(),
	}
	ss.activeWitnessRequests[hash] = witnessReq

	ss.logger.Debug("initiating new witness request", "hash", hash, "peer", hex.EncodeToString(peerID[:]))
	return true
}

func (ss *GrpcServer) rangePeers(f func(peerInfo *PeerInfo) bool) {
	ss.peers.mu.RLock()
	defer ss.peers.mu.RUnlock()
	for _, peerInfo := range ss.peers.peers {
		if peerInfo == nil {
			continue
		}
		cont := f(peerInfo)
		if !cont {
			break
		}
	}
}

func (ss *GrpcServer) getPeer(peerID [64]byte) (peerInfo *PeerInfo) {
	ss.peers.mu.RLock()
	peerInfo, ok := ss.peers.peers[peerID]
	ss.peers.mu.RUnlock()
	if ok && peerInfo == nil {
		go func() {
			ss.deletePeer(peerID)
		}()
	}
	return peerInfo
}

// getOrCreatePeer gets or creates PeerInfo. The subprotocol-specific
// MsgReadWriter is NOT stored here — callers must follow up with
// peerInfo.SetEthRw or peerInfo.SetWitRw so writePeer can route outbound
// messages to the correct offset (see PeerInfo.ethRw/witRw).
func (ss *GrpcServer) getOrCreatePeer(peer *p2p.Peer, protocolName string) (*PeerInfo, *p2p.PeerError) {
	peerID := peer.Pubkey()

	ss.peers.mu.Lock()
	defer ss.peers.mu.Unlock()

	existingPeerInfo := ss.peers.peers[peerID]
	if existingPeerInfo == nil {
		peerInfo := NewPeerInfo(peer)
		ss.peers.peers[peerID] = peerInfo
		return peerInfo, nil
	}

	// allow one connection per protocol
	if protocolName == eth.ProtocolName {
		existingVersion := existingPeerInfo.protocol
		if existingVersion != 0 {
			return nil, p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscAlreadyConnected, nil, "peer already has connection")
		}

		return existingPeerInfo, nil
	}

	if protocolName == wit.ProtocolName {
		existingVersion := existingPeerInfo.witProtocol
		if existingVersion != 0 {
			return nil, p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscAlreadyConnected, nil, "peer already has connection")
		}

		return existingPeerInfo, nil
	}

	return existingPeerInfo, nil
}

func (ss *GrpcServer) removePeer(peerID [64]byte, reason *p2p.PeerError) {
	if peerInfo, ok := ss.loadAndDeletePeer(peerID); ok {
		if peerInfo != nil {
			peerInfo.Remove(reason)
		}
	}
}

func (ss *GrpcServer) loadAndDeletePeer(peerID [64]byte) (*PeerInfo, bool) {
	ss.peers.mu.Lock()
	defer ss.peers.mu.Unlock()
	peerInfo, ok := ss.peers.peers[peerID]
	if ok {
		delete(ss.peers.peers, peerID)
	}
	return peerInfo, ok
}

func (ss *GrpcServer) deletePeer(peerID [64]byte) {
	ss.peers.mu.Lock()
	defer ss.peers.mu.Unlock()
	delete(ss.peers.peers, peerID)
}

// writePeer sends a message to a peer over the correct RLPx subprotocol
// offset. msgID must be the high-level sentryproto.MessageId — it's used
// to look up the subprotocol (eth vs wit) and pick the corresponding
// PeerInfo.{Eth,Wit}Rw. Routing by numeric msgcode would be wrong:
// eth and wit reuse the same low code values (e.g. msgcode 0x01 is both
// eth.NewBlockHashesMsg and wit.NewWitnessHashesMsg), so a numeric lookup
// can match the wrong subprotocol and emit frames at the wrong offset.
func (ss *GrpcServer) writePeer(logPrefix string, peerInfo *PeerInfo, msgID sentryproto.MessageId, msgcode uint64, data []byte, ttl time.Duration) {
	peerInfo.Async(func() {
		// Async enqueue can win the race against Remove closing pi.removed, so a
		// queued task may be scheduled to run on a peer we have already asked
		// p2p to disconnect. Without this check, one last frame would slip out
		// after Disconnect — which Hive's devp2p BlobViolations asserts against.
		// Matches go-ethereum, where Disconnect runs on the same goroutine as
		// detection so nothing further can be sent.
		select {
		case <-peerInfo.removed:
			return
		default:
		}

		protocolName, protocolVersion := ss.protocolForMessageID(msgID)
		trackPeerStatistics(peerInfo.peer.Fullname(), peerInfo.peer.ID().String(), false, msgID.String(), fmt.Sprintf("%s/%d", protocolName, protocolVersion), len(data))

		// Select the rw for the message's subprotocol. eth and wit live on
		// the same RLPx connection but at different code offsets — writing
		// a wit message via the eth rw (or vice versa) puts it at the wrong
		// offset and the receiver disconnects on protocol error.
		var rw p2p.MsgReadWriter
		switch protocolName {
		case eth.ProtocolName:
			rw = peerInfo.EthRw()
		case wit.ProtocolName:
			rw = peerInfo.WitRw()
		}
		if rw == nil {
			// Peer hasn't (yet) attached the subprotocol this message
			// belongs to. Common case: a wit broadcast aimed at every
			// good peer, some of which negotiated only eth. Treat as a
			// successful no-op rather than disconnecting.
			return
		}
		err := rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(data)), Payload: bytes.NewReader(data)})
		if err != nil {
			ss.removePeer(peerInfo.ID(), p2p.NewPeerError(p2p.PeerErrorMessageSend, p2p.DiscNetworkError, err, fmt.Sprintf("%s writePeer msgcode=%d", logPrefix, msgcode)))
		} else {
			if ttl > 0 {
				peerInfo.AddDeadline(time.Now().Add(ttl))
			}
		}
	}, ss.logger)
}

func (ss *GrpcServer) getBlockHeaders(ctx context.Context, bestHash common.Hash, peerID [64]byte) error {
	b, err := rlp.EncodeToBytes(&eth.GetBlockHeadersPacket66{
		RequestId: rand.Uint64(), // nolint: gosec
		GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
			Amount:  1,
			Reverse: false,
			Skip:    0,
			Origin:  eth.HashOrNumber{Hash: bestHash},
		},
	})
	if err != nil {
		return fmt.Errorf("GrpcServer.getBlockHeaders encode packet failed: %w", err)
	}
	if _, err := ss.SendMessageById(ctx, &sentryproto.SendMessageByIdRequest{
		PeerId: gointerfaces.ConvertHashToH512(peerID),
		Data: &sentryproto.OutboundMessageData{
			Id:   sentryproto.MessageId_GET_BLOCK_HEADERS_66,
			Data: b,
		},
	}); err != nil {
		return err
	}
	return nil
}

func (ss *GrpcServer) PenalizePeer(_ context.Context, req *sentryproto.PenalizePeerRequest) (*emptypb.Empty, error) {
	//log.Warn("Received penalty", "kind", req.GetPenalty().Descriptor().FullName, "from", fmt.Sprintf("%s", req.GetPeerId()))
	peerID := ConvertH512ToPeerID(req.PeerId)
	peerInfo := ss.getPeer(peerID)
	if ss.statusData != nil && peerInfo != nil && !peerInfo.peer.Info().Network.Static && !peerInfo.peer.Info().Network.Trusted {
		ss.removePeer(peerID, p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscRequested, nil, "penalized peer"))
	}
	return &emptypb.Empty{}, nil
}

func (ss *GrpcServer) SetPeerLatestBlock(_ context.Context, req *sentryproto.SetPeerLatestBlockRequest) (*emptypb.Empty, error) {
	peerID := ConvertH512ToPeerID(req.PeerId)
	if peerInfo := ss.getPeer(peerID); peerInfo != nil {
		peerInfo.SetIncreasedHeight(req.LatestBlockHeight)
	}
	return &emptypb.Empty{}, nil
}

func (ss *GrpcServer) SetPeerMinimumBlock(_ context.Context, req *sentryproto.SetPeerMinimumBlockRequest) (*emptypb.Empty, error) {
	peerID := ConvertH512ToPeerID(req.PeerId)
	if peerInfo := ss.getPeer(peerID); peerInfo != nil {
		peerInfo.SetMinimumBlock(req.MinBlockHeight)
	}
	return &emptypb.Empty{}, nil
}

func (ss *GrpcServer) SetPeerBlockRange(_ context.Context, req *sentryproto.SetPeerBlockRangeRequest) (*emptypb.Empty, error) {
	peerID := ConvertH512ToPeerID(req.PeerId)
	if peerInfo := ss.getPeer(peerID); peerInfo != nil {
		peerInfo.SetBlockRange(req.MinBlockHeight, req.LatestBlockHeight)
	}
	return &emptypb.Empty{}, nil
}

func (ss *GrpcServer) findBestPeersWithPermit(peerCount int) []*PeerInfo {
	// Choose peer(s) that we can send this request to, with maximum number of permits
	now := time.Now()
	byMinBlock := make(PeersByMinBlock, 0, peerCount)
	var pokePeer *PeerInfo // Peer with the earliest dealine, to be "poked" by the request
	var pokeDeadline time.Time
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		deadlines := peerInfo.ClearDeadlines(now, false /* givePermit */)
		height := peerInfo.Height()
		//fmt.Printf("%d deadlines for peer %s\n", deadlines, peerID)
		if deadlines < maxPermitsPerPeer {
			heap.Push(&byMinBlock, PeerRef{pi: peerInfo, height: height})
			if byMinBlock.Len() > peerCount {
				// RemoveFile the worst peer
				peerRef := heap.Pop(&byMinBlock).(PeerRef)
				latestDeadline := peerRef.pi.LatestDeadline()
				if pokePeer == nil || latestDeadline.Before(pokeDeadline) {
					pokeDeadline = latestDeadline
					pokePeer = peerInfo
				}
			}
		}
		return true
	})
	var foundPeers []*PeerInfo
	if peerCount == 1 || pokePeer == nil {
		foundPeers = make([]*PeerInfo, len(byMinBlock))
	} else {
		foundPeers = make([]*PeerInfo, len(byMinBlock)+1)
		foundPeers[len(foundPeers)-1] = pokePeer
	}
	for i, peerRef := range byMinBlock {
		foundPeers[i] = peerRef.pi
	}
	return foundPeers
}

func (ss *GrpcServer) findPeerByMinBlock(minBlock uint64) (*PeerInfo, bool) {
	// Choose a peer that we can send this request to, with maximum number of permits
	var foundPeerInfo *PeerInfo
	var maxPermits int
	now := time.Now()
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		if peerInfo.MinBlock() >= minBlock {
			deadlines := peerInfo.ClearDeadlines(now, false /* givePermit */)
			//fmt.Printf("%d deadlines for peer %s\n", deadlines, peerID)
			if deadlines < maxPermitsPerPeer {
				permits := maxPermitsPerPeer - deadlines
				if permits > maxPermits {
					maxPermits = permits
					foundPeerInfo = peerInfo
				}
			}
		}
		return true
	})
	return foundPeerInfo, maxPermits > 0
}

func (ss *GrpcServer) SendMessageByMinBlock(_ context.Context, inreq *sentryproto.SendMessageByMinBlockRequest) (*sentryproto.SentPeers, error) {
	reply := &sentryproto.SentPeers{}
	msgcode := eth.FromProto[ss.Protocols[0].Version][inreq.Data.Id]
	if msgcode != eth.GetBlockHeadersMsg &&
		msgcode != eth.GetBlockBodiesMsg &&
		msgcode != eth.GetPooledTransactionsMsg {
		return reply, fmt.Errorf("sendMessageByMinBlock not implemented for message Id: %s", inreq.Data.Id)
	}
	if inreq.MaxPeers == 1 {
		peerInfo, found := ss.findPeerByMinBlock(inreq.MinBlock)
		if found {
			ss.writePeer("[sentry] sendMessageByMinBlock", peerInfo, inreq.Data.Id, msgcode, inreq.Data.Data, 30*time.Second)
			reply.Peers = []*typesproto.H512{gointerfaces.ConvertHashToH512(peerInfo.ID())}
			return reply, nil
		}
	}
	peerInfos := ss.findBestPeersWithPermit(int(inreq.MaxPeers))
	reply.Peers = make([]*typesproto.H512, len(peerInfos))
	for i, peerInfo := range peerInfos {
		ss.writePeer("[sentry] sendMessageByMinBlock", peerInfo, inreq.Data.Id, msgcode, inreq.Data.Data, 15*time.Second)
		reply.Peers[i] = gointerfaces.ConvertHashToH512(peerInfo.ID())
	}
	return reply, nil
}

func (ss *GrpcServer) SendMessageById(_ context.Context, inreq *sentryproto.SendMessageByIdRequest) (*sentryproto.SentPeers, error) {
	reply := &sentryproto.SentPeers{}

	peerID := ConvertH512ToPeerID(inreq.PeerId)
	peerInfo := ss.getPeer(peerID)
	if peerInfo == nil {
		//TODO: enable after support peer to sentry mapping
		//return reply, fmt.Errorf("peer not found: %s", peerID)
		return reply, nil
	}

	msgcode, protocolVersions := ss.messageCode(inreq.Data.Id)
	if protocolVersions.Cardinality() == 0 {
		return reply, fmt.Errorf("msgcode not found for message Id: %s (peer protocol %d)", inreq.Data.Id, peerInfo.protocol)
	}

	ss.writePeer("[sentry] sendMessageById", peerInfo, inreq.Data.Id, msgcode, inreq.Data.Data, 0)
	reply.Peers = []*typesproto.H512{inreq.PeerId}
	return reply, nil
}

func (ss *GrpcServer) messageCode(id sentryproto.MessageId) (code uint64, protocolVersions mapset.Set[uint]) {
	protocolVersions = mapset.NewSet[uint]()
	for i := 0; i < len(ss.Protocols); i++ {
		version := ss.Protocols[i].Version
		if val, ok := ss.Protocols[i].FromProto[id]; ok {
			code = val // assuming that the code doesn't change between protocol versions
			protocolVersions.Add(version)
		}
	}
	return
}

// protocolForMessageID returns the subprotocol name and version that
// declares the given sentryproto.MessageId. Lookup is by MessageId (which
// is globally unique across subprotocols), unlike a lookup by numeric
// msgcode which would be ambiguous: eth and wit reuse the same low code
// values within their respective offsets.
func (ss *GrpcServer) protocolForMessageID(id sentryproto.MessageId) (protocolName string, protocolVersion uint) {
	for i := 0; i < len(ss.Protocols); i++ {
		if _, ok := ss.Protocols[i].FromProto[id]; ok {
			return ss.Protocols[i].Name, ss.Protocols[i].Version
		}
	}
	return
}

func (ss *GrpcServer) SendMessageToRandomPeers(ctx context.Context, req *sentryproto.SendMessageToRandomPeersRequest) (*sentryproto.SentPeers, error) {
	reply := &sentryproto.SentPeers{}

	msgcode, protocolVersions := ss.messageCode(req.Data.Id)
	if protocolVersions.Cardinality() == 0 ||
		(msgcode != eth.NewBlockMsg &&
			msgcode != eth.NewBlockHashesMsg &&
			msgcode != eth.NewPooledTransactionHashesMsg &&
			msgcode != eth.TransactionsMsg) {
		return reply, fmt.Errorf("sendMessageToRandomPeers not implemented for message Id: %s", req.Data.Id)
	}

	peerInfos := make([]*PeerInfo, 0, 100)
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		if protocolVersions.Contains(peerInfo.protocol) {
			peerInfos = append(peerInfos, peerInfo)
		}
		return true
	})
	rand.Shuffle(len(peerInfos), func(i int, j int) {
		peerInfos[i], peerInfos[j] = peerInfos[j], peerInfos[i]
	})

	var peersToSendCount int
	if req.MaxPeers > 0 {
		peersToSendCount = int(math.Min(float64(req.MaxPeers), float64(len(peerInfos))))
	} else {
		// MaxPeers == 0 means send to all
		peersToSendCount = len(peerInfos)
	}

	// Send the block to a subset of our peers at random
	for _, peerInfo := range peerInfos[:peersToSendCount] {
		ss.writePeer("[sentry] sendMessageToRandomPeers", peerInfo, req.Data.Id, msgcode, req.Data.Data, 0)
		reply.Peers = append(reply.Peers, gointerfaces.ConvertHashToH512(peerInfo.ID()))
	}
	return reply, nil
}

func (ss *GrpcServer) SendMessageToAll(ctx context.Context, req *sentryproto.OutboundMessageData) (*sentryproto.SentPeers, error) {
	reply := &sentryproto.SentPeers{}

	allowedMsgCodes := []uint64{
		eth.NewBlockMsg,
		eth.NewPooledTransactionHashesMsg, // to broadcast new local transactions
		eth.NewBlockHashesMsg,
		eth.BlockRangeUpdateMsg,
	}

	msgcode, protocolVersions := ss.messageCode(req.Id)
	if protocolVersions.Cardinality() == 0 || !slices.Contains(allowedMsgCodes, msgcode) { // this message is not enabled for this protocol, do nothing
		return reply, fmt.Errorf("sendMessageToAll not implemented for message Id: %s", req.Id)
	}

	var lastErr error
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		if protocolVersions.Contains(peerInfo.protocol) {
			ss.writePeer("[sentry] SendMessageToAll", peerInfo, req.Id, msgcode, req.Data, 0)
			reply.Peers = append(reply.Peers, gointerfaces.ConvertHashToH512(peerInfo.ID()))
		}
		return true
	})
	return reply, lastErr
}

func (ss *GrpcServer) HandShake(context.Context, *emptypb.Empty) (*sentryproto.HandShakeReply, error) {
	reply := &sentryproto.HandShakeReply{}
	reply.Protocol = direct.UintToProtocolMap[ss.Protocols[0].Version]

	for _, protocol := range ss.Protocols[1:] { // noop if no extra protocols
		v, ok := direct.UintToSideProtocolMap[protocol.Version]
		if !ok {
			continue
		}

		if _, ok = direct.SupportedSideProtocols[v]; ok {
			reply.SideProtocols = append(reply.SideProtocols, v)
		}
	}

	return reply, nil
}

func (ss *GrpcServer) startP2PServer() (*p2p.Server, error) {
	if !ss.p2p.NoDiscovery {
		if len(ss.p2p.DiscoveryDNS) == 0 {
			if url := ss.dnsNetwork; url != "" {
				ss.p2p.DiscoveryDNS = []string{url}
			}

			for i := range ss.Protocols {
				dialCandidates, err := setupDiscovery(ss.p2p.DiscoveryDNS)
				if err != nil {
					return nil, err
				}
				ss.Protocols[i].DialCandidates = dialCandidates
			}
		}
	}

	srv, err := makeP2PServer(*ss.p2p, ss.bootnodes, ss.Protocols)
	if err != nil {
		return nil, err
	}

	if err = srv.Start(ss.ctx, ss.logger); err != nil {
		srv.Stop()
		return nil, fmt.Errorf("could not start server: %w", err)
	}

	return srv, nil
}

func (ss *GrpcServer) getP2PServer() *p2p.Server {
	ss.p2pServerLock.RLock()
	defer ss.p2pServerLock.RUnlock()
	return ss.p2pServer
}

// GetP2PServer returns the P2P server if it has been started, nil otherwise.
func (ss *GrpcServer) GetP2PServer() *p2p.Server {
	return ss.getP2PServer()
}

func (ss *GrpcServer) SetStatus(ctx context.Context, statusData *sentryproto.StatusData) (*sentryproto.SetStatusReply, error) {
	genesisHash := gointerfaces.ConvertH256ToHash(statusData.ForkData.Genesis)

	reply := &sentryproto.SetStatusReply{}

	ss.p2pServerLock.Lock()
	defer ss.p2pServerLock.Unlock()
	if ss.p2pServer == nil {
		srv, err := ss.startP2PServer()
		if err != nil {
			return reply, err
		}
		ss.p2pServer = srv
	}

	ss.statusDataLock.Lock()
	defer ss.statusDataLock.Unlock()

	ss.p2pServer.LocalNode().Set(eth.CurrentENREntryFromForks(statusData.ForkData.HeightForks, statusData.ForkData.TimeForks, genesisHash, statusData.MaxBlockHeight, statusData.MaxBlockTime))
	if ss.statusData == nil || statusData.MaxBlockHeight != 0 {
		// Not overwrite statusData if the message contains zero MaxBlock (comes from standalone transaction pool)
		ss.statusData = statusData
	}
	// Unblock awaitStatus once we have a status the eth handshake can
	// actually use (statusUsable). A partial first SetStatus (e.g. an early
	// standalone-txpool update) must not wake waiters — Protocol.Run would
	// proceed with garbage, defeating the startup-gap fix. nil-guarded for
	// callers that construct GrpcServer outside NewGrpcServer (close(nil)
	// would panic).
	if statusUsable(ss.statusData) {
		ss.statusReadyOnce.Do(func() {
			if ss.statusReady != nil {
				close(ss.statusReady)
			}
		})
	}
	return reply, nil
}

func (ss *GrpcServer) Peers(_ context.Context, _ *emptypb.Empty) (*sentryproto.PeersReply, error) {
	if ss.getP2PServer() == nil {
		return nil, errors.New("p2p server was not started")
	}

	// Report only peers this sentry actually owns for eth — i.e. the ones
	// whose negotiated eth version matches this GrpcServer's own version.
	// With the shared PeerStore (used in shared-Server mode), every sentry
	// can see every PeerInfo; filtering here is what keeps node/eth
	// Ethereum.Peers aggregation from N-fold-duplicating the result and
	// what lets the multi-sentry router map each peer to the correct
	// sentry's gRPC client. protocol==0 is also dropped (RLPx-only,
	// wit-only, or in-flight handshake entries).
	myVersion := ss.ethProtocolVersion()
	var reply sentryproto.PeersReply
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		if peerInfo.protocol == 0 || peerInfo.protocol != myVersion {
			return true
		}
		peer := peerInfo.peer.Info()
		reply.Peers = append(reply.Peers, &typesproto.PeerInfo{
			Id:             peer.ID,
			Name:           peer.Name,
			Enode:          peer.Enode,
			Enr:            peer.ENR,
			Caps:           peer.Caps,
			ConnLocalAddr:  peer.Network.LocalAddress,
			ConnRemoteAddr: peer.Network.RemoteAddress,
			ConnIsInbound:  peer.Network.Inbound,
			ConnIsTrusted:  peer.Network.Trusted,
			ConnIsStatic:   peer.Network.Static,
		})
		return true
	})

	return &reply, nil
}

func (ss *GrpcServer) SimplePeerCount() map[uint]int {
	myVersion := ss.ethProtocolVersion()
	counts := map[uint]int{}
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		// Per-sentry counting: each sentry reports only the peers whose
		// negotiated eth version matches its own. Mirrors Peers(). Without
		// this filter the Provider's runPeerCountLogger would aggregate
		// every peer once per sentry (shared PeerStore makes the map
		// visible to all sentries).
		if peerInfo.protocol == 0 || peerInfo.protocol != myVersion {
			return true
		}
		counts[peerInfo.protocol]++
		return true
	})
	return counts
}

// ethProtocolVersion returns the eth protocol version this GrpcServer was
// constructed for. NewGrpcServer appends the eth Protocol first, so
// Protocols[0] is the eth entry. A zero return means the eth Protocol
// hasn't been registered yet (only possible during construction).
func (ss *GrpcServer) ethProtocolVersion() uint {
	for i := range ss.Protocols {
		if ss.Protocols[i].Name == eth.ProtocolName {
			return ss.Protocols[i].Version
		}
	}
	return 0
}

func (ss *GrpcServer) PeerCount(_ context.Context, req *sentryproto.PeerCountRequest) (*sentryproto.PeerCountReply, error) {
	counts := ss.SimplePeerCount()
	reply := &sentryproto.PeerCountReply{}
	for protocol, count := range counts {
		reply.Count += uint64(count)
		reply.CountsPerProtocol = append(reply.CountsPerProtocol, &sentryproto.PeerCountPerProtocol{Protocol: sentryproto.Protocol(protocol), Count: uint64(count)})
	}
	return reply, nil
}

func (ss *GrpcServer) PeerById(_ context.Context, req *sentryproto.PeerByIdRequest) (*sentryproto.PeerByIdReply, error) {
	peerID := ConvertH512ToPeerID(req.PeerId)

	var rpcPeer *typesproto.PeerInfo
	sentryPeer := ss.getPeer(peerID)

	if sentryPeer != nil {
		peer := sentryPeer.peer.Info()
		rpcPeer = &typesproto.PeerInfo{
			Id:             peer.ID,
			Name:           peer.Name,
			Enode:          peer.Enode,
			Enr:            peer.ENR,
			Caps:           peer.Caps,
			ConnLocalAddr:  peer.Network.LocalAddress,
			ConnRemoteAddr: peer.Network.RemoteAddress,
			ConnIsInbound:  peer.Network.Inbound,
			ConnIsTrusted:  peer.Network.Trusted,
			ConnIsStatic:   peer.Network.Static,
		}
	}

	return &sentryproto.PeerByIdReply{Peer: rpcPeer}, nil
}

// setupDiscovery creates the node discovery source for the `eth` protocol.
func setupDiscovery(urls []string) (enode.Iterator, error) {
	return SetupDNSDiscovery(urls)
}

// SetupDNSDiscovery exposes the DNS-discovery iterator constructor so outer
// coordinators (e.g. node/components/sentry.Provider) can attach DialCandidates
// to Protocols before handing them to a shared p2p.Server.
func SetupDNSDiscovery(urls []string) (enode.Iterator, error) {
	if len(urls) == 0 {
		return nil, nil
	}
	client := dnsdisc.NewClient(dnsdisc.Config{})
	return client.NewIterator(urls...)
}

func (ss *GrpcServer) GetStatus() *sentryproto.StatusData {
	ss.statusDataLock.RLock()
	defer ss.statusDataLock.RUnlock()
	return ss.statusData
}

// statusUsable reports whether a stored statusData is complete enough for
// Protocol.Run to use as the local side of the eth handshake. A partial
// payload (e.g. an early standalone-txpool update with NetworkId still 0)
// must NOT wake awaitStatus or be returned to handshakers.
func statusUsable(s *sentryproto.StatusData) bool {
	return s != nil && s.NetworkId != 0 && s.ForkData != nil
}

// awaitStatus returns the current statusData, waiting up to maxWait for the
// first usable SetStatus to land if it hasn't yet. The wait absorbs the
// startup gap when a shared p2p.Server's listener is already accepting
// connections but the multi-client hasn't broadcast status to this sentry
// yet. Returns nil if no usable status is available before the deadline
// (or before ss.ctx is cancelled) so the caller can disconnect the peer
// with PeerErrorLocalStatusNeeded.
//
// A debug log fires on timeout to help an operator tell "core didn't send
// status in time" from "core never tried."
func (ss *GrpcServer) awaitStatus(maxWait time.Duration) *sentryproto.StatusData {
	if status := ss.GetStatus(); statusUsable(status) {
		return status
	}
	// Use NewTimer (not time.After) so heavy inbound churn during the
	// startup window doesn't accumulate one Timer-backed goroutine per
	// connection until maxWait elapses.
	timer := time.NewTimer(maxWait)
	defer timer.Stop()
	select {
	case <-ss.statusReady:
	case <-timer.C:
		if ss.logger != nil {
			ss.logger.Debug("[p2p] sentry timed out waiting for first SetStatus; inbound peer will be disconnected", "after", maxWait)
		}
	case <-ss.ctx.Done():
	}
	status := ss.GetStatus()
	if !statusUsable(status) {
		return nil
	}
	return status
}

func (ss *GrpcServer) send(msgID sentryproto.MessageId, peerID [64]byte, b []byte) {
	ss.messageStreamsLock.RLock()
	defer ss.messageStreamsLock.RUnlock()
	req := &sentryproto.InboundMessage{
		PeerId: gointerfaces.ConvertHashToH512(peerID),
		Id:     msgID,
		Data:   b,
	}
	for i := range ss.messageStreams[msgID] {
		ch := ss.messageStreams[msgID][i]
		ch <- req
		before := len(ch)
		libsentry.EvictOldestIfHalfFull(ch)
		if before > cap(ch)/2 {
			ss.logger.Debug("[sentry] consuming is slow, drop oldest 25% of messages", "msgID", msgID.String())
		}
	}
}

func (ss *GrpcServer) hasSubscribers(msgID sentryproto.MessageId) bool {
	ss.messageStreamsLock.RLock()
	defer ss.messageStreamsLock.RUnlock()
	return ss.messageStreams[msgID] != nil && len(ss.messageStreams[msgID]) > 0
	//	log.Error("Sending msg to core P2P failed", "msg", sentryproto.MessageId_name[int32(streamMsg.msgId)], "err", err)
}

func (ss *GrpcServer) addMessagesStream(ids []sentryproto.MessageId, ch chan *sentryproto.InboundMessage) func() {
	ss.messageStreamsLock.Lock()
	defer ss.messageStreamsLock.Unlock()
	if ss.messageStreams == nil {
		ss.messageStreams = map[sentryproto.MessageId]map[uint64]chan *sentryproto.InboundMessage{}
	}

	ss.messagesSubscriberID++
	for _, id := range ids {
		m, ok := ss.messageStreams[id]
		if !ok {
			m = map[uint64]chan *sentryproto.InboundMessage{}
			ss.messageStreams[id] = m
		}
		m[ss.messagesSubscriberID] = ch
	}

	sID := ss.messagesSubscriberID
	return func() {
		ss.messageStreamsLock.Lock()
		defer ss.messageStreamsLock.Unlock()
		for _, id := range ids {
			delete(ss.messageStreams[id], sID)
		}
	}
}

func (ss *GrpcServer) Messages(req *sentryproto.MessagesRequest, server sentryproto.Sentry_MessagesServer) error {
	ss.logger.Trace("[Messages] new subscriber", "to", req.Ids)
	ch := make(chan *sentryproto.InboundMessage, libsentry.MessagesQueueSize)
	defer close(ch)
	clean := ss.addMessagesStream(req.Ids, ch)
	defer clean()

	for {
		select {
		case <-ss.ctx.Done():
			return nil
		case <-server.Context().Done():
			return nil
		case in := <-ch:
			if err := server.Send(in); err != nil {
				ss.logger.Warn("Sending msg to core P2P failed", "msg", in.Id.String(), "err", err)
				return err
			}
		}
	}
}

// Close performs cleanup operations for the sentry. When the p2p.Server is
// externally owned (SetP2PServer was used), the coordinator owns lifecycle
// and we must not stop it here — doing so would tear down the listener for
// every other GrpcServer sharing the same Server.
func (ss *GrpcServer) Close() {
	ss.p2pServerLock.RLock()
	srv, external := ss.p2pServer, ss.external
	ss.p2pServerLock.RUnlock()
	if srv != nil && !external {
		srv.Stop()
	}
}

func (ss *GrpcServer) sendNewPeerToClients(peerID *typesproto.H512) {
	if err := ss.peersStreams.Broadcast(&sentryproto.PeerEvent{PeerId: peerID, EventId: sentryproto.PeerEvent_Connect}); err != nil {
		ss.logger.Warn("Sending new peer notice to core P2P failed", "err", err)
	}
}

func (ss *GrpcServer) sendGonePeerToClients(peerID *typesproto.H512) {
	if err := ss.peersStreams.Broadcast(&sentryproto.PeerEvent{PeerId: peerID, EventId: sentryproto.PeerEvent_Disconnect}); err != nil {
		ss.logger.Warn("Sending gone peer notice to core P2P failed", "err", err)
	}
}

func (ss *GrpcServer) PeerEvents(req *sentryproto.PeerEventsRequest, server sentryproto.Sentry_PeerEventsServer) error {
	clean := ss.peersStreams.Add(server)
	defer clean()
	// replay currently connected peers
	eg, ctx := errgroup.WithContext(server.Context())
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		eg.Go(func() error {
			return server.Send(&sentryproto.PeerEvent{
				PeerId:  gointerfaces.ConvertHashToH512(peerInfo.ID()),
				EventId: sentryproto.PeerEvent_Connect,
			})
		})
		select {
		case <-ctx.Done():
			return false
		default:
			return true
		}
	})
	if err := eg.Wait(); err != nil {
		return err
	}
	select {
	case <-ss.ctx.Done():
		return nil
	case <-server.Context().Done():
		return nil
	}
}

func (ss *GrpcServer) AddPeer(_ context.Context, req *sentryproto.AddPeerRequest) (*sentryproto.AddPeerReply, error) {
	node, err := enode.Parse(enode.ValidSchemes, req.Url)
	if err != nil {
		return nil, err
	}

	p2pServer := ss.getP2PServer()
	if p2pServer == nil {
		return nil, errors.New("p2p server was not started")
	}
	p2pServer.AddPeer(node)

	return &sentryproto.AddPeerReply{Success: true}, nil
}

func (ss *GrpcServer) RemovePeer(_ context.Context, req *sentryproto.RemovePeerRequest) (*sentryproto.RemovePeerReply, error) {
	node, err := enode.Parse(enode.ValidSchemes, req.Url)
	if err != nil {
		return nil, err
	}

	p2pServer := ss.getP2PServer()
	if p2pServer == nil {
		return nil, errors.New("p2p server was not started")
	}
	p2pServer.RemovePeer(node)

	return &sentryproto.RemovePeerReply{Success: true}, nil
}

func (ss *GrpcServer) AddTrustedPeer(_ context.Context, req *sentryproto.AddPeerRequest) (*sentryproto.AddPeerReply, error) {
	node, err := enode.Parse(enode.ValidSchemes, req.Url)
	if err != nil {
		return nil, err
	}

	p2pServer := ss.getP2PServer()
	if p2pServer == nil {
		return nil, errors.New("p2p server was not started")
	}
	p2pServer.AddTrustedPeer(node)

	return &sentryproto.AddPeerReply{Success: true}, nil
}

func (ss *GrpcServer) RemoveTrustedPeer(_ context.Context, req *sentryproto.RemovePeerRequest) (*sentryproto.RemovePeerReply, error) {
	node, err := enode.Parse(enode.ValidSchemes, req.Url)
	if err != nil {
		return nil, err
	}

	p2pServer := ss.getP2PServer()
	if p2pServer == nil {
		return nil, errors.New("p2p server was not started")
	}
	p2pServer.RemoveTrustedPeer(node)

	return &sentryproto.RemovePeerReply{Success: true}, nil
}

func (ss *GrpcServer) NodeInfo(_ context.Context, _ *emptypb.Empty) (*typesproto.NodeInfoReply, error) {
	p2pServer := ss.getP2PServer()
	if p2pServer == nil {
		return nil, errors.New("p2p server was not started")
	}

	// With a shared p2p.Server every sentry returns the same node info
	// (same Node ID, same enode, same listener port). The multi-sentry
	// aggregator in node/eth.NodesInfo deduplicates the resulting list.
	info := p2pServer.NodeInfo()
	ret := &typesproto.NodeInfoReply{
		Id:    info.ID,
		Name:  info.Name,
		Enode: info.Enode,
		Enr:   info.ENR,
		Ports: &typesproto.NodeInfoPorts{
			Discovery: uint32(info.Ports.Discovery),
			Listener:  uint32(info.Ports.Listener),
		},
		ListenerAddr: info.ListenAddr,
	}

	protos, err := json.Marshal(info.Protocols)
	if err != nil {
		return nil, fmt.Errorf("cannot encode protocols map: %w", err)
	}

	ret.Protocols = protos
	return ret, nil
}

// PeersStreams - it's safe to use this class as non-pointer
type PeersStreams struct {
	mu      sync.RWMutex
	id      uint
	streams map[uint]sentryproto.Sentry_PeerEventsServer
}

func NewPeersStreams() *PeersStreams {
	return &PeersStreams{}
}

func (s *PeersStreams) Add(stream sentryproto.Sentry_PeerEventsServer) (remove func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.streams == nil {
		s.streams = make(map[uint]sentryproto.Sentry_PeerEventsServer)
	}
	s.id++
	id := s.id
	s.streams[id] = stream
	return func() { s.remove(id) }
}

func (s *PeersStreams) doBroadcast(reply *sentryproto.PeerEvent) (ids []uint, errs []error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for id, stream := range s.streams {
		err := stream.Send(reply)
		if err != nil {
			select {
			case <-stream.Context().Done():
				ids = append(ids, id)
			default:
			}
			errs = append(errs, err)
		}
	}
	return
}

func (s *PeersStreams) Broadcast(reply *sentryproto.PeerEvent) (errs []error) {
	var ids []uint
	ids, errs = s.doBroadcast(reply)
	if len(ids) > 0 {
		s.mu.Lock()
		defer s.mu.Unlock()
	}
	for _, id := range ids {
		delete(s.streams, id)
	}
	return errs
}

func (s *PeersStreams) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.streams)
}

func (s *PeersStreams) remove(id uint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.streams[id]
	if !ok { // double-unsubscribe support
		return
	}
	delete(s.streams, id)
}
