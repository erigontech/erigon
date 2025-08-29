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
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/grpcutil"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	proto_types "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/diagnostics/diaglib"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/dnsdisc"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/forkid"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/protocols/wit"

	_ "github.com/erigontech/erigon/polygon/chain" // Register Polygon chains
)

const (
	// handshakeTimeout is the maximum allowed time for the `eth` handshake to
	// complete before dropping the connection.= as malicious.
	handshakeTimeout  = 5 * time.Second
	maxPermitsPerPeer = 4 // How many outstanding requests per peer we may have
)

// PeerInfo collects various extra bits of information about the peer,
// for example deadlines that is used for regulating requests sent to the peer
type PeerInfo struct {
	peer                  *p2p.Peer
	lock                  sync.RWMutex
	deadlines             []time.Time // Request deadlines
	latestDealine         time.Time
	height                uint64
	rw                    p2p.MsgReadWriter
	protocol, witProtocol uint
	knownWitnesses        *wit.KnownCache // Set of witness hashes (`witness.Headers[0].Hash()`) known to be known by this peer

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
func (bp *PeersByMinBlock) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	p := x.(PeerRef)
	*bp = append(*bp, p)
}

// Pop (part of heap.Interface) removes the first peer from the queue
func (bp *PeersByMinBlock) Pop() interface{} {
	old := *bp
	n := len(old)
	x := old[n-1]
	old[n-1] = PeerRef{} // avoid memory leak
	*bp = old[0 : n-1]
	return x
}

func NewPeerInfo(peer *p2p.Peer, rw p2p.MsgReadWriter) *PeerInfo {
	ctx, cancel := context.WithCancel(context.Background())

	p := &PeerInfo{
		peer:           peer,
		rw:             rw,
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
	return atomic.LoadUint64(&pi.height)
}

// SetIncreasedHeight atomically updates PeerInfo.height only if newHeight is higher
func (pi *PeerInfo) SetIncreasedHeight(newHeight uint64) {
	for {
		oldHeight := atomic.LoadUint64(&pi.height)
		if oldHeight >= newHeight || atomic.CompareAndSwapUint64(&pi.height, oldHeight, newHeight) {
			break
		}
	}
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

// ConvertH512ToPeerID() ensures the return type is [64]byte
// so that short variable declarations will still be formatted as hex in logs
func ConvertH512ToPeerID(h512 *proto_types.H512) [64]byte {
	return gointerfaces.ConvertH512ToHash(h512)
}

func makeP2PServer(
	p2pConfig p2p.Config,
	genesisHash common.Hash,
	protocols []p2p.Protocol,
) (*p2p.Server, error) {
	if len(p2pConfig.BootstrapNodes) == 0 {
		spec, err := chainspec.ChainSpecByGenesisHash(genesisHash)
		if err != nil {
			return nil, fmt.Errorf("no config for given genesis hash: %w", err)
		}
		bootstrapNodes, err := enode.ParseNodesFromURLs(spec.Bootnodes)
		if err != nil {
			return nil, fmt.Errorf("bad bootnodes option: %w", err)
		}
		p2pConfig.BootstrapNodes = bootstrapNodes
		p2pConfig.BootstrapNodesV5 = bootstrapNodes
	}
	p2pConfig.Protocols = protocols
	return &p2p.Server{Config: p2pConfig}, nil
}

func handShake(
	ctx context.Context,
	status *proto_sentry.StatusData,
	rw p2p.MsgReadWriter,
	version uint,
	minVersion uint,
) (*common.Hash, *p2p.PeerError) {
	// Send out own handshake in a new thread
	errChan := make(chan *p2p.PeerError, 2)
	resultChan := make(chan *eth.StatusPacket, 1)

	ourTD := gointerfaces.ConvertH256ToUint256Int(status.TotalDifficulty)
	// Convert proto status data into the one required by devp2p
	genesisHash := gointerfaces.ConvertH256ToHash(status.ForkData.Genesis)

	go func() {
		defer dbg.LogPanic()
		status := &eth.StatusPacket{
			ProtocolVersion: uint32(version),
			NetworkID:       status.NetworkId,
			TD:              ourTD.ToBig(),
			Head:            gointerfaces.ConvertH256ToHash(status.BestHash),
			Genesis:         genesisHash,
			ForkID:          forkid.NewIDFromForks(status.ForkData.HeightForks, status.ForkData.TimeForks, genesisHash, status.MaxBlockHeight, status.MaxBlockTime),
		}
		err := p2p.Send(rw, eth.StatusMsg, status)

		if err == nil {
			errChan <- nil
		} else {
			errChan <- p2p.NewPeerError(p2p.PeerErrorStatusSend, p2p.DiscNetworkError, err, "sentry.handShake failed to send eth Status")
		}
	}()

	go func() {
		defer dbg.LogPanic()
		status, err := readAndValidatePeerStatusMessage(rw, status, version, minVersion)

		if err == nil {
			resultChan <- status
			errChan <- nil
		} else {
			errChan <- err
		}
	}()

	timeout := time.NewTimer(handshakeTimeout)
	defer timeout.Stop()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errChan:
			if err != nil {
				return nil, err
			}
		case <-timeout.C:
			return nil, p2p.NewPeerError(p2p.PeerErrorStatusHandshakeTimeout, p2p.DiscReadTimeout, nil, "sentry.handShake timeout")
		case <-ctx.Done():
			return nil, p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscQuitting, ctx.Err(), "sentry.handShake ctx.Done")
		}
	}

	peerStatus := <-resultChan
	return &peerStatus.Head, nil
}

func runPeer(
	ctx context.Context,
	peerID [64]byte,
	cap p2p.Cap,
	rw p2p.MsgReadWriter,
	peerInfo *PeerInfo,
	send func(msgId proto_sentry.MessageId, peerID [64]byte, b []byte),
	hasSubscribers func(msgId proto_sentry.MessageId) bool,
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
		case 11:
			// Ignore
			// TODO: Investigate why BSC peers for eth/67 send these messages
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
	send func(msgId proto_sentry.MessageId, peerID [64]byte, b []byte),
	hasSubscribers func(msgId proto_sentry.MessageId) bool,
	getWitnessRequest func(hash common.Hash, peerID [64]byte) bool,
	logger log.Logger,
) *p2p.PeerError {
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
		case wit.GetWitnessMsg | wit.WitnessMsg:
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
			log.Info("Sentry gRPC received connection", "via", network, "from", address)
			return nil
		},
	}
	lis, err := listenConfig.Listen(ctx, "tcp", sentryAddr)
	if err != nil {
		return nil, fmt.Errorf("could not create Sentry P2P listener: %w, addr=%s", err, sentryAddr)
	}
	grpcServer := grpcutil.NewServer(100, nil)
	proto_sentry.RegisterSentryServer(grpcServer, ss)
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

func NewGrpcServer(ctx context.Context, dialCandidates func() enode.Iterator, readNodeInfo func() *eth.NodeInfo, cfg *p2p.Config, protocol uint, logger log.Logger) *GrpcServer {
	ss := &GrpcServer{
		ctx:                   ctx,
		p2p:                   cfg,
		peersStreams:          NewPeersStreams(),
		logger:                logger,
		activeWitnessRequests: make(map[common.Hash]*WitnessRequest),
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
		Length:         17,
		DialCandidates: disc,
		Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) *p2p.PeerError {
			peerID := peer.Pubkey()
			printablePeerID := hex.EncodeToString(peerID[:])
			logger.Trace("[p2p] start with peer", "peerId", printablePeerID)
			peerInfo, err := ss.getOrCreatePeer(peer, rw, eth.ProtocolName)
			if err != nil {
				return err
			}
			peerInfo.protocol = protocol
			defer peerInfo.Close()

			defer ss.GoodPeers.Delete(peerID)

			status := ss.GetStatus()

			if status == nil {
				return p2p.NewPeerError(p2p.PeerErrorLocalStatusNeeded, p2p.DiscProtocolError, nil, "could not get status message from core")
			}

			peerBestHash, err := handShake(ctx, status, rw, protocol, protocol)
			if err != nil {
				return err
			}

			// handshake is successful
			logger.Trace("[p2p] Received status message OK", "peerId", printablePeerID, "name", peer.Name(), "caps", peer.Caps())

			ss.sendNewPeerToClients(gointerfaces.ConvertHashToH512(peerID))
			defer ss.sendGonePeerToClients(gointerfaces.ConvertHashToH512(peerID))
			getBlockHeadersErr := ss.getBlockHeaders(ctx, *peerBestHash, peerID)
			if getBlockHeadersErr != nil {
				return p2p.NewPeerError(p2p.PeerErrorFirstMessageSend, p2p.DiscNetworkError, getBlockHeadersErr, "p2p.Protocol.Run getBlockHeaders failure")
			}

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
		NodeInfo: func() interface{} {
			return readNodeInfo()
		},
		PeerInfo: func(peerID [64]byte) interface{} {
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
				peerInfo, err := ss.getOrCreatePeer(peer, rw, wit.ProtocolName)
				if err != nil {
					return err
				}
				peerInfo.witProtocol = wit.ProtocolVersions[0]

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
			NodeInfo: func() interface{} {
				return readNodeInfo()
			},
			PeerInfo: func(peerID [64]byte) interface{} {
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
	sentryServer := NewGrpcServer(ctx, discovery, func() *eth.NodeInfo { return nil }, cfg, protocolVersion, logger)

	grpcServer, err := grpcSentryServer(ctx, sentryAddr, sentryServer, healthCheck)
	if err != nil {
		return err
	}

	<-ctx.Done()
	grpcServer.GracefulStop()
	sentryServer.Close()
	return nil
}

type GrpcServer struct {
	proto_sentry.UnimplementedSentryServer
	ctx                  context.Context
	Protocols            []p2p.Protocol
	GoodPeers            sync.Map
	TxSubscribed         uint32 // Set to non-zero if downloader is subscribed to transaction messages
	p2pServer            *p2p.Server
	p2pServerLock        sync.RWMutex
	statusData           *proto_sentry.StatusData
	statusDataLock       sync.RWMutex
	messageStreams       map[proto_sentry.MessageId]map[uint64]chan *proto_sentry.InboundMessage
	messagesSubscriberID uint64
	messageStreamsLock   sync.RWMutex
	peersStreams         *PeersStreams
	p2p                  *p2p.Config
	logger               log.Logger
	// Mutex to synchronize PeerInfo creation between protocols
	peerCreationMutex sync.Mutex

	// witness request tracking
	activeWitnessRequests map[common.Hash]*WitnessRequest
	witnessRequestMutex   sync.RWMutex
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
	ss.GoodPeers.Range(func(key, value interface{}) bool {
		peerInfo, _ := value.(*PeerInfo)
		if peerInfo == nil {
			return true
		}
		return f(peerInfo)
	})
}

func (ss *GrpcServer) getPeer(peerID [64]byte) (peerInfo *PeerInfo) {
	if value, ok := ss.GoodPeers.Load(peerID); ok {
		peerInfo := value.(*PeerInfo)
		if peerInfo != nil {
			return peerInfo
		}
		ss.GoodPeers.Delete(peerID)
	}
	return nil
}

// getOrCreatePeer gets or creates PeerInfo
func (ss *GrpcServer) getOrCreatePeer(peer *p2p.Peer, rw p2p.MsgReadWriter, protocolName string) (*PeerInfo, *p2p.PeerError) {
	peerID := peer.Pubkey()

	ss.peerCreationMutex.Lock()
	defer ss.peerCreationMutex.Unlock()

	existingPeerInfo := ss.getPeer(peerID)

	if existingPeerInfo == nil {
		peerInfo := NewPeerInfo(peer, rw)
		ss.GoodPeers.Store(peerID, peerInfo)
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
	if value, ok := ss.GoodPeers.LoadAndDelete(peerID); ok {
		peerInfo := value.(*PeerInfo)
		if peerInfo != nil {
			peerInfo.Remove(reason)
		}
	}
}

func (ss *GrpcServer) writePeer(logPrefix string, peerInfo *PeerInfo, msgcode uint64, data []byte, ttl time.Duration) {
	peerInfo.Async(func() {
		msgType, protocolName, protocolVersion := ss.protoMessageID(msgcode)
		trackPeerStatistics(peerInfo.peer.Fullname(), peerInfo.peer.ID().String(), false, msgType.String(), fmt.Sprintf("%s/%d", protocolName, protocolVersion), len(data))

		err := peerInfo.rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(data)), Payload: bytes.NewReader(data)})
		if err != nil {
			peerInfo.Remove(p2p.NewPeerError(p2p.PeerErrorMessageSend, p2p.DiscNetworkError, err, fmt.Sprintf("%s writePeer msgcode=%d", logPrefix, msgcode)))
			ss.GoodPeers.Delete(peerInfo.ID())
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
	if _, err := ss.SendMessageById(ctx, &proto_sentry.SendMessageByIdRequest{
		PeerId: gointerfaces.ConvertHashToH512(peerID),
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_GET_BLOCK_HEADERS_66,
			Data: b,
		},
	}); err != nil {
		return err
	}
	return nil
}

func (ss *GrpcServer) PenalizePeer(_ context.Context, req *proto_sentry.PenalizePeerRequest) (*emptypb.Empty, error) {
	//log.Warn("Received penalty", "kind", req.GetPenalty().Descriptor().FullName, "from", fmt.Sprintf("%s", req.GetPeerId()))
	peerID := ConvertH512ToPeerID(req.PeerId)
	peerInfo := ss.getPeer(peerID)
	if ss.statusData != nil && peerInfo != nil && !peerInfo.peer.Info().Network.Static && !peerInfo.peer.Info().Network.Trusted {
		ss.removePeer(peerID, p2p.NewPeerError(p2p.PeerErrorDiscReason, p2p.DiscRequested, nil, "penalized peer"))
	}
	return &emptypb.Empty{}, nil
}

func (ss *GrpcServer) PeerMinBlock(_ context.Context, req *proto_sentry.PeerMinBlockRequest) (*emptypb.Empty, error) {
	peerID := ConvertH512ToPeerID(req.PeerId)
	if peerInfo := ss.getPeer(peerID); peerInfo != nil {
		peerInfo.SetIncreasedHeight(req.MinBlock)
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
		if peerInfo.Height() >= minBlock {
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

func (ss *GrpcServer) SendMessageByMinBlock(_ context.Context, inreq *proto_sentry.SendMessageByMinBlockRequest) (*proto_sentry.SentPeers, error) {
	reply := &proto_sentry.SentPeers{}
	msgcode := eth.FromProto[ss.Protocols[0].Version][inreq.Data.Id]
	if msgcode != eth.GetBlockHeadersMsg &&
		msgcode != eth.GetBlockBodiesMsg &&
		msgcode != eth.GetPooledTransactionsMsg {
		return reply, fmt.Errorf("sendMessageByMinBlock not implemented for message Id: %s", inreq.Data.Id)
	}
	if inreq.MaxPeers == 1 {
		peerInfo, found := ss.findPeerByMinBlock(inreq.MinBlock)
		if found {
			ss.writePeer("[sentry] sendMessageByMinBlock", peerInfo, msgcode, inreq.Data.Data, 30*time.Second)
			reply.Peers = []*proto_types.H512{gointerfaces.ConvertHashToH512(peerInfo.ID())}
			return reply, nil
		}
	}
	peerInfos := ss.findBestPeersWithPermit(int(inreq.MaxPeers))
	reply.Peers = make([]*proto_types.H512, len(peerInfos))
	for i, peerInfo := range peerInfos {
		ss.writePeer("[sentry] sendMessageByMinBlock", peerInfo, msgcode, inreq.Data.Data, 15*time.Second)
		reply.Peers[i] = gointerfaces.ConvertHashToH512(peerInfo.ID())
	}
	return reply, nil
}

func (ss *GrpcServer) SendMessageById(_ context.Context, inreq *proto_sentry.SendMessageByIdRequest) (*proto_sentry.SentPeers, error) {
	reply := &proto_sentry.SentPeers{}

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

	ss.writePeer("[sentry] sendMessageById", peerInfo, msgcode, inreq.Data.Data, 0)
	reply.Peers = []*proto_types.H512{inreq.PeerId}
	return reply, nil
}

func (ss *GrpcServer) messageCode(id proto_sentry.MessageId) (code uint64, protocolVersions mapset.Set[uint]) {
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

func (ss *GrpcServer) protoMessageID(code uint64) (id proto_sentry.MessageId, protocolName string, protocolVersion uint) {
	for i := 0; i < len(ss.Protocols); i++ {
		if val, ok := ss.Protocols[i].ToProto[code]; ok {
			return val, ss.Protocols[i].Name, ss.Protocols[i].Version
		}
	}
	return
}

func (ss *GrpcServer) SendMessageToRandomPeers(ctx context.Context, req *proto_sentry.SendMessageToRandomPeersRequest) (*proto_sentry.SentPeers, error) {
	reply := &proto_sentry.SentPeers{}

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
		ss.writePeer("[sentry] sendMessageToRandomPeers", peerInfo, msgcode, req.Data.Data, 0)
		reply.Peers = append(reply.Peers, gointerfaces.ConvertHashToH512(peerInfo.ID()))
	}
	return reply, nil
}

func (ss *GrpcServer) SendMessageToAll(ctx context.Context, req *proto_sentry.OutboundMessageData) (*proto_sentry.SentPeers, error) {
	reply := &proto_sentry.SentPeers{}

	msgcode, protocolVersions := ss.messageCode(req.Id)
	if protocolVersions.Cardinality() == 0 ||
		(msgcode != eth.NewBlockMsg &&
			msgcode != eth.NewPooledTransactionHashesMsg && // to broadcast new local transactions
			msgcode != eth.NewBlockHashesMsg) {
		return reply, fmt.Errorf("sendMessageToAll not implemented for message Id: %s", req.Id)
	}

	var lastErr error
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		if protocolVersions.Contains(peerInfo.protocol) {
			ss.writePeer("[sentry] SendMessageToAll", peerInfo, msgcode, req.Data, 0)
			reply.Peers = append(reply.Peers, gointerfaces.ConvertHashToH512(peerInfo.ID()))
		}
		return true
	})
	return reply, lastErr
}

func (ss *GrpcServer) HandShake(context.Context, *emptypb.Empty) (*proto_sentry.HandShakeReply, error) {
	reply := &proto_sentry.HandShakeReply{}
	switch ss.Protocols[0].Version {
	case direct.ETH67:
		reply.Protocol = proto_sentry.Protocol_ETH67
	case direct.ETH68:
		reply.Protocol = proto_sentry.Protocol_ETH68
	}
	return reply, nil
}

func (ss *GrpcServer) startP2PServer(genesisHash common.Hash) (*p2p.Server, error) {
	if !ss.p2p.NoDiscovery {
		if len(ss.p2p.DiscoveryDNS) == 0 {
			s, err := chainspec.ChainSpecByGenesisHash(genesisHash)
			if err != nil {
				ss.logger.Debug("[sentry] Could not get chain spec for genesis hash", "genesisHash", genesisHash, "err", err)
			} else {
				if url := s.DNSNetwork; url != "" {
					ss.p2p.DiscoveryDNS = []string{url}
				}
			}

			for _, p := range ss.Protocols {
				dialCandidates, err := setupDiscovery(ss.p2p.DiscoveryDNS)
				if err != nil {
					return nil, err
				}
				p.DialCandidates = dialCandidates
			}
		}
	}

	srv, err := makeP2PServer(*ss.p2p, genesisHash, ss.Protocols)
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

func (ss *GrpcServer) SetStatus(ctx context.Context, statusData *proto_sentry.StatusData) (*proto_sentry.SetStatusReply, error) {
	genesisHash := gointerfaces.ConvertH256ToHash(statusData.ForkData.Genesis)

	reply := &proto_sentry.SetStatusReply{}

	ss.p2pServerLock.Lock()
	defer ss.p2pServerLock.Unlock()
	if ss.p2pServer == nil {
		srv, err := ss.startP2PServer(genesisHash)
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
	return reply, nil
}

func (ss *GrpcServer) Peers(_ context.Context, _ *emptypb.Empty) (*proto_sentry.PeersReply, error) {
	p2pServer := ss.getP2PServer()
	if p2pServer == nil {
		return nil, errors.New("p2p server was not started")
	}

	peers := p2pServer.PeersInfo()

	var reply proto_sentry.PeersReply
	reply.Peers = make([]*proto_types.PeerInfo, 0, len(peers))

	for _, peer := range peers {
		rpcPeer := proto_types.PeerInfo{
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
		reply.Peers = append(reply.Peers, &rpcPeer)
	}

	return &reply, nil
}

func (ss *GrpcServer) SimplePeerCount() map[uint]int {
	counts := map[uint]int{}
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		counts[peerInfo.protocol]++
		return true
	})
	return counts
}

func (ss *GrpcServer) PeerCount(_ context.Context, req *proto_sentry.PeerCountRequest) (*proto_sentry.PeerCountReply, error) {
	counts := ss.SimplePeerCount()
	reply := &proto_sentry.PeerCountReply{}
	for protocol, count := range counts {
		reply.Count += uint64(count)
		reply.CountsPerProtocol = append(reply.CountsPerProtocol, &proto_sentry.PeerCountPerProtocol{Protocol: proto_sentry.Protocol(protocol), Count: uint64(count)})
	}
	return reply, nil
}

func (ss *GrpcServer) PeerById(_ context.Context, req *proto_sentry.PeerByIdRequest) (*proto_sentry.PeerByIdReply, error) {
	peerID := ConvertH512ToPeerID(req.PeerId)

	var rpcPeer *proto_types.PeerInfo
	sentryPeer := ss.getPeer(peerID)

	if sentryPeer != nil {
		peer := sentryPeer.peer.Info()
		rpcPeer = &proto_types.PeerInfo{
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

	return &proto_sentry.PeerByIdReply{Peer: rpcPeer}, nil
}

// setupDiscovery creates the node discovery source for the `eth` protocol.
func setupDiscovery(urls []string) (enode.Iterator, error) {
	if len(urls) == 0 {
		return nil, nil
	}
	client := dnsdisc.NewClient(dnsdisc.Config{})
	return client.NewIterator(urls...)
}

func (ss *GrpcServer) GetStatus() *proto_sentry.StatusData {
	ss.statusDataLock.RLock()
	defer ss.statusDataLock.RUnlock()
	return ss.statusData
}

func (ss *GrpcServer) send(msgID proto_sentry.MessageId, peerID [64]byte, b []byte) {
	ss.messageStreamsLock.RLock()
	defer ss.messageStreamsLock.RUnlock()
	req := &proto_sentry.InboundMessage{
		PeerId: gointerfaces.ConvertHashToH512(peerID),
		Id:     msgID,
		Data:   b,
	}
	for i := range ss.messageStreams[msgID] {
		ch := ss.messageStreams[msgID][i]
		ch <- req
		if len(ch) > MessagesQueueSize/2 {
			ss.logger.Debug("[sentry] consuming is slow, drop 50% of old messages", "msgID", msgID.String())
			// evict old messages from channel
			for j := 0; j < MessagesQueueSize/4; j++ {
				select {
				case <-ch:
				default:
				}
			}
		}
	}
}

func (ss *GrpcServer) hasSubscribers(msgID proto_sentry.MessageId) bool {
	ss.messageStreamsLock.RLock()
	defer ss.messageStreamsLock.RUnlock()
	return ss.messageStreams[msgID] != nil && len(ss.messageStreams[msgID]) > 0
	//	log.Error("Sending msg to core P2P failed", "msg", proto_sentry.MessageId_name[int32(streamMsg.msgId)], "err", err)
}

func (ss *GrpcServer) addMessagesStream(ids []proto_sentry.MessageId, ch chan *proto_sentry.InboundMessage) func() {
	ss.messageStreamsLock.Lock()
	defer ss.messageStreamsLock.Unlock()
	if ss.messageStreams == nil {
		ss.messageStreams = map[proto_sentry.MessageId]map[uint64]chan *proto_sentry.InboundMessage{}
	}

	ss.messagesSubscriberID++
	for _, id := range ids {
		m, ok := ss.messageStreams[id]
		if !ok {
			m = map[uint64]chan *proto_sentry.InboundMessage{}
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

const MessagesQueueSize = 1024 // one such queue per client of .Messages stream
func (ss *GrpcServer) Messages(req *proto_sentry.MessagesRequest, server proto_sentry.Sentry_MessagesServer) error {
	ss.logger.Trace("[Messages] new subscriber", "to", req.Ids)
	ch := make(chan *proto_sentry.InboundMessage, MessagesQueueSize)
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

// Close performs cleanup operations for the sentry
func (ss *GrpcServer) Close() {
	p2pServer := ss.getP2PServer()
	if p2pServer != nil {
		p2pServer.Stop()
	}
}

func (ss *GrpcServer) sendNewPeerToClients(peerID *proto_types.H512) {
	if err := ss.peersStreams.Broadcast(&proto_sentry.PeerEvent{PeerId: peerID, EventId: proto_sentry.PeerEvent_Connect}); err != nil {
		ss.logger.Warn("Sending new peer notice to core P2P failed", "err", err)
	}
}

func (ss *GrpcServer) sendGonePeerToClients(peerID *proto_types.H512) {
	if err := ss.peersStreams.Broadcast(&proto_sentry.PeerEvent{PeerId: peerID, EventId: proto_sentry.PeerEvent_Disconnect}); err != nil {
		ss.logger.Warn("Sending gone peer notice to core P2P failed", "err", err)
	}
}

func (ss *GrpcServer) PeerEvents(req *proto_sentry.PeerEventsRequest, server proto_sentry.Sentry_PeerEventsServer) error {
	clean := ss.peersStreams.Add(server)
	defer clean()
	select {
	case <-ss.ctx.Done():
		return nil
	case <-server.Context().Done():
		return nil
	}
}

func (ss *GrpcServer) AddPeer(_ context.Context, req *proto_sentry.AddPeerRequest) (*proto_sentry.AddPeerReply, error) {
	node, err := enode.Parse(enode.ValidSchemes, req.Url)
	if err != nil {
		return nil, err
	}

	p2pServer := ss.getP2PServer()
	if p2pServer == nil {
		return nil, errors.New("p2p server was not started")
	}
	p2pServer.AddPeer(node)

	return &proto_sentry.AddPeerReply{Success: true}, nil
}

func (ss *GrpcServer) RemovePeer(_ context.Context, req *proto_sentry.RemovePeerRequest) (*proto_sentry.RemovePeerReply, error) {
	node, err := enode.Parse(enode.ValidSchemes, req.Url)
	if err != nil {
		return nil, err
	}

	p2pServer := ss.getP2PServer()
	if p2pServer == nil {
		return nil, errors.New("p2p server was not started")
	}
	p2pServer.RemovePeer(node)

	return &proto_sentry.RemovePeerReply{Success: true}, nil
}

func (ss *GrpcServer) NodeInfo(_ context.Context, _ *emptypb.Empty) (*proto_types.NodeInfoReply, error) {
	p2pServer := ss.getP2PServer()
	if p2pServer == nil {
		return nil, errors.New("p2p server was not started")
	}

	info := p2pServer.NodeInfo()
	ret := &proto_types.NodeInfoReply{
		Id:    info.ID,
		Name:  info.Name,
		Enode: info.Enode,
		Enr:   info.ENR,
		Ports: &proto_types.NodeInfoPorts{
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
	streams map[uint]proto_sentry.Sentry_PeerEventsServer
}

func NewPeersStreams() *PeersStreams {
	return &PeersStreams{}
}

func (s *PeersStreams) Add(stream proto_sentry.Sentry_PeerEventsServer) (remove func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.streams == nil {
		s.streams = make(map[uint]proto_sentry.Sentry_PeerEventsServer)
	}
	s.id++
	id := s.id
	s.streams[id] = stream
	return func() { s.remove(id) }
}

func (s *PeersStreams) doBroadcast(reply *proto_sentry.PeerEvent) (ids []uint, errs []error) {
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

func (s *PeersStreams) Broadcast(reply *proto_sentry.PeerEvent) (errs []error) {
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
