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

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/datadir"
	"github.com/gateway-fm/cdk-erigon-lib/common/dir"
	"github.com/gateway-fm/cdk-erigon-lib/gointerfaces"
	"github.com/gateway-fm/cdk-erigon-lib/gointerfaces/grpcutil"
	proto_sentry "github.com/gateway-fm/cdk-erigon-lib/gointerfaces/sentry"
	proto_types "github.com/gateway-fm/cdk-erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/dnsdisc"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
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
	peer          *p2p.Peer
	lock          sync.RWMutex
	deadlines     []time.Time // Request deadlines
	latestDealine time.Time
	height        uint64
	rw            p2p.MsgReadWriter
	protocol      uint

	removed    chan struct{} // close this channel on remove
	ctx        context.Context
	ctxCancel  context.CancelFunc
	removeOnce sync.Once
	// each peer has own worker (goroutine) - all funcs from this queue will execute on this worker
	// if this queue is full (means peer is slow) - old messages will be dropped
	// channel closed on peer remove
	tasks chan func()
}

type PeerRef struct {
	pi     *PeerInfo
	height uint64
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
	old[n-1] = PeerRef{}
	*bp = old[0 : n-1]
	return x
}

func NewPeerInfo(peer *p2p.Peer, rw p2p.MsgReadWriter) *PeerInfo {
	ctx, cancel := context.WithCancel(context.Background())

	p := &PeerInfo{peer: peer, rw: rw, removed: make(chan struct{}), tasks: make(chan func(), 16), ctx: ctx, ctxCancel: cancel}

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

func (pi *PeerInfo) Remove() {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	pi.removeOnce.Do(func() {
		close(pi.removed)
		pi.ctxCancel()
	})
}

func (pi *PeerInfo) Async(f func()) {
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
			log.Debug("slow peer or too many requests, dropping its old requests", "name", pi.peer.Name())
		}
	}
}

func (pi *PeerInfo) Removed() bool {
	select {
	case <-pi.removed:
		return true
	default:
		return false
	}
}

// ConvertH512ToPeerID() ensures the return type is [64]byte
// so that short variable declarations will still be formatted as hex in logs
func ConvertH512ToPeerID(h512 *proto_types.H512) [64]byte {
	return gointerfaces.ConvertH512ToHash(h512)
}

func makeP2PServer(
	p2pConfig p2p.Config,
	genesisHash libcommon.Hash,
	protocols []p2p.Protocol,
) (*p2p.Server, error) {
	var urls []string
	chainConfig := params.ChainConfigByGenesisHash(genesisHash)
	if chainConfig != nil {
		urls = params.BootnodeURLsOfChain(chainConfig.ChainName)
	}
	if len(p2pConfig.BootstrapNodes) == 0 {
		bootstrapNodes, err := utils.ParseNodesFromURLs(urls)
		if err != nil {
			return nil, fmt.Errorf("bad option %s: %w", utils.BootnodesFlag.Name, err)
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
	peerID [64]byte,
	rw p2p.MsgReadWriter,
	version uint,
	minVersion uint,
	startSync func(bestHash libcommon.Hash) error,
) error {
	if status == nil {
		return fmt.Errorf("could not get status message from core for peer %s connection", peerID)
	}

	// Send out own handshake in a new thread
	errc := make(chan error, 2)

	ourTD := gointerfaces.ConvertH256ToUint256Int(status.TotalDifficulty)
	// Convert proto status data into the one required by devp2p
	genesisHash := gointerfaces.ConvertH256ToHash(status.ForkData.Genesis)

	go func() {
		defer debug.LogPanic()
		s := &eth.StatusPacket{
			ProtocolVersion: uint32(version),
			NetworkID:       status.NetworkId,
			TD:              ourTD.ToBig(),
			Head:            gointerfaces.ConvertH256ToHash(status.BestHash),
			Genesis:         genesisHash,
			ForkID:          forkid.NewIDFromForks(status.ForkData.HeightForks, status.ForkData.TimeForks, genesisHash, status.MaxBlockHeight, status.MaxBlockTime),
		}
		errc <- p2p.Send(rw, eth.StatusMsg, s)
	}()

	go func() {
		reply, err := readAndValidatePeerStatusMessage(rw, status, version, minVersion)
		if (err == nil) && (startSync != nil) {
			err = startSync(reply.Head)
		}

		errc <- err
	}()

	timeout := time.NewTimer(handshakeTimeout)
	defer timeout.Stop()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errc:
			if err != nil {
				return err
			}
		case <-timeout.C:
			return p2p.DiscReadTimeout
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func runPeer(
	ctx context.Context,
	peerID [64]byte,
	protocol uint,
	rw p2p.MsgReadWriter,
	peerInfo *PeerInfo,
	send func(msgId proto_sentry.MessageId, peerID [64]byte, b []byte),
	hasSubscribers func(msgId proto_sentry.MessageId) bool,
) error {
	printTime := time.Now().Add(time.Minute)
	peerPrinted := false
	defer func() {
		select { // don't print logs if we stopping
		case <-ctx.Done():
			return
		default:
		}
		if peerPrinted {
			log.Trace("Peer disconnected", "id", peerID, "name", peerInfo.peer.Fullname())
		}
	}()
	for {
		if !peerPrinted {
			if time.Now().After(printTime) {
				log.Trace("Peer stable", "id", peerID, "name", peerInfo.peer.Fullname())
				peerPrinted = true
			}
		}
		if err := libcommon.Stopped(ctx.Done()); err != nil {
			return err
		}
		if peerInfo.Removed() {
			return fmt.Errorf("peer removed")
		}
		msg, err := rw.ReadMsg()
		if err != nil {
			return fmt.Errorf("reading message: %w", err)
		}
		if msg.Size > eth.ProtocolMaxMsgSize {
			msg.Discard()
			return fmt.Errorf("message is too large %d, limit %d", msg.Size, eth.ProtocolMaxMsgSize)
		}
		givePermit := false
		switch msg.Code {
		case eth.StatusMsg:
			msg.Discard()
			// Status messages should never arrive after the handshake
			return fmt.Errorf("uncontrolled status message")
		case eth.GetBlockHeadersMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}

			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				log.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.BlockHeadersMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			givePermit = true
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				log.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.GetBlockBodiesMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				log.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.BlockBodiesMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			givePermit = true
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				log.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.GetNodeDataMsg:
			if protocol >= eth.ETH67 {
				msg.Discard()
				return fmt.Errorf("unexpected GetNodeDataMsg from %s in eth/%d", peerID, protocol)
			}
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				log.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
			//log.Info(fmt.Sprintf("[%s] GetNodeData", peerID))
		case eth.GetReceiptsMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				log.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
			//log.Info(fmt.Sprintf("[%s] GetReceiptsMsg", peerID))
		case eth.ReceiptsMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				log.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
			//log.Info(fmt.Sprintf("[%s] ReceiptsMsg", peerID))
		case eth.NewBlockHashesMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				log.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			//log.Debug("NewBlockHashesMsg from", "peerId", fmt.Sprintf("%x", peerID)[:20], "name", peerInfo.peer.Name())
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.NewBlockMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				log.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			//log.Debug("NewBlockMsg from", "peerId", fmt.Sprintf("%x", peerID)[:20], "name", peerInfo.peer.Name())
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.NewPooledTransactionHashesMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}

			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				log.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.GetPooledTransactionsMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}

			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				log.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.TransactionsMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}

			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				log.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.PooledTransactionsMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}

			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				log.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case 11:
			// Ignore
			// TODO: Investigate why BSC peers for eth/67 send these messages
		default:
			log.Error(fmt.Sprintf("[p2p] Unknown message code: %d, peerID=%x", msg.Code, peerID))
		}
		msg.Discard()
		peerInfo.ClearDeadlines(time.Now(), givePermit)
	}
}

func grpcSentryServer(ctx context.Context, sentryAddr string, ss *GrpcServer, healthCheck bool) (*grpc.Server, error) {
	// STARTING GRPC SERVER
	log.Info("Starting Sentry gRPC server", "on", sentryAddr)
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
			log.Error("Sentry gRPC server fail", "err", err1)
		}
	}()
	return grpcServer, nil
}

func NewGrpcServer(ctx context.Context, dialCandidates func() enode.Iterator, readNodeInfo func() *eth.NodeInfo, cfg *p2p.Config, protocol uint) *GrpcServer {
	ss := &GrpcServer{
		ctx:          ctx,
		p2p:          cfg,
		peersStreams: NewPeersStreams(),
	}

	protocols := []uint{protocol}
	if protocol == eth.ETH67 {
		protocols = append(protocols, eth.ETH66)
	}

	for _, p := range protocols {
		protocol := p
		var disc enode.Iterator
		if dialCandidates != nil {
			disc = dialCandidates()
		}
		ss.Protocols = append(ss.Protocols, p2p.Protocol{
			Name:           eth.ProtocolName,
			Version:        protocol,
			Length:         17,
			DialCandidates: disc,
			Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) error {
				peerID := peer.Pubkey()
				printablePeerID := hex.EncodeToString(peerID[:])[:20]
				if ss.getPeer(peerID) != nil {
					log.Trace("[p2p] peer already has connection", "peerId", printablePeerID)
					return nil
				}
				log.Debug("[p2p] start with peer", "peerId", printablePeerID)

				peerInfo := NewPeerInfo(peer, rw)
				peerInfo.protocol = protocol
				defer peerInfo.Close()

				defer ss.GoodPeers.Delete(peerID)
				err := handShake(ctx, ss.GetStatus(), peerID, rw, protocol, protocol, func(bestHash libcommon.Hash) error {
					ss.GoodPeers.Store(peerID, peerInfo)
					ss.sendNewPeerToClients(gointerfaces.ConvertHashToH512(peerID))
					return ss.startSync(ctx, bestHash, peerID)
				})
				if err != nil {
					log.Debug("[p2p] Handshake failure", "peer", printablePeerID, "err", err)
					return fmt.Errorf("[p2p]handshake to peer %s: %w", printablePeerID, err)
				}
				log.Trace("[p2p] Received status message OK", "peerId", printablePeerID, "name", peer.Name())

				err = runPeer(
					ctx,
					peerID,
					protocol,
					rw,
					peerInfo,
					ss.send,
					ss.hasSubscribers,
				) // runPeer never returns a nil error
				log.Trace("[p2p] error while running peer", "peerId", printablePeerID, "err", err)
				ss.sendGonePeerToClients(gointerfaces.ConvertHashToH512(peerID))
				return nil
			},
			NodeInfo: func() interface{} {
				return readNodeInfo()
			},
			PeerInfo: func(peerID [64]byte) interface{} {
				// TODO: remember handshake reply per peer ID and return eth-related Status info (see ethPeerInfo in geth)
				return nil
			},
			//Attributes: []enr.Entry{eth.CurrentENREntry(chainConfig, genesisHash, headHeight)},
		})
	}

	return ss
}

// Sentry creates and runs standalone sentry
func Sentry(ctx context.Context, dirs datadir.Dirs, sentryAddr string, discoveryDNS []string, cfg *p2p.Config, protocolVersion uint, healthCheck bool) error {
	dir.MustExist(dirs.DataDir)

	discovery := func() enode.Iterator {
		d, err := setupDiscovery(discoveryDNS)
		if err != nil {
			panic(err)
		}
		return d
	}
	sentryServer := NewGrpcServer(ctx, discovery, func() *eth.NodeInfo { return nil }, cfg, protocolVersion)
	sentryServer.discoveryDNS = discoveryDNS

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
	discoveryDNS         []string
	GoodPeers            sync.Map
	statusData           *proto_sentry.StatusData
	P2pServer            *p2p.Server
	TxSubscribed         uint32 // Set to non-zero if downloader is subscribed to transaction messages
	lock                 sync.RWMutex
	messageStreams       map[proto_sentry.MessageId]map[uint64]chan *proto_sentry.InboundMessage
	messagesSubscriberID uint64
	messageStreamsLock   sync.RWMutex
	peersStreams         *PeersStreams
	p2p                  *p2p.Config
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

func (ss *GrpcServer) removePeer(peerID [64]byte) {
	if value, ok := ss.GoodPeers.LoadAndDelete(peerID); ok {
		peerInfo := value.(*PeerInfo)
		if peerInfo != nil {
			peerInfo.Remove()
		}
	}
}

func (ss *GrpcServer) writePeer(logPrefix string, peerInfo *PeerInfo, msgcode uint64, data []byte, ttl time.Duration) {
	peerInfo.Async(func() {
		err := peerInfo.rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(data)), Payload: bytes.NewReader(data)})
		if err != nil {
			peerInfo.Remove()
			ss.GoodPeers.Delete(peerInfo.ID())
			if !errors.Is(err, p2p.ErrShuttingDown) {
				log.Debug(logPrefix, "msgcode", msgcode, "err", err)
			}
		} else {
			if ttl > 0 {
				peerInfo.AddDeadline(time.Now().Add(ttl))
			}
		}
	})
}

func (ss *GrpcServer) startSync(ctx context.Context, bestHash libcommon.Hash, peerID [64]byte) error {
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
		return fmt.Errorf("startSync encode packet failed: %w", err)
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
		ss.removePeer(peerID)
		printablePeerID := hex.EncodeToString(peerID[:])[:8]
		log.Debug("[p2p] Penalized peer", "peerId", printablePeerID, "name", peerInfo.peer.Name())
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
				// Remove the worst peer
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
			ss.writePeer("sendMessageByMinBlock", peerInfo, msgcode, inreq.Data.Data, 30*time.Second)
			reply.Peers = []*proto_types.H512{gointerfaces.ConvertHashToH512(peerInfo.ID())}
			return reply, nil
		}
	}
	peerInfos := ss.findBestPeersWithPermit(int(inreq.MaxPeers))
	reply.Peers = make([]*proto_types.H512, len(peerInfos))
	for i, peerInfo := range peerInfos {
		ss.writePeer("sendMessageByMinBlock", peerInfo, msgcode, inreq.Data.Data, 15*time.Second)
		reply.Peers[i] = gointerfaces.ConvertHashToH512(peerInfo.ID())
	}
	return reply, nil
}

func (ss *GrpcServer) SendMessageById(_ context.Context, inreq *proto_sentry.SendMessageByIdRequest) (*proto_sentry.SentPeers, error) {
	reply := &proto_sentry.SentPeers{}
	msgcode := eth.FromProto[ss.Protocols[0].Version][inreq.Data.Id]
	if msgcode != eth.GetBlockHeadersMsg &&
		msgcode != eth.BlockHeadersMsg &&
		msgcode != eth.BlockBodiesMsg &&
		msgcode != eth.GetReceiptsMsg &&
		msgcode != eth.ReceiptsMsg &&
		msgcode != eth.NewPooledTransactionHashesMsg &&
		msgcode != eth.PooledTransactionsMsg &&
		msgcode != eth.GetPooledTransactionsMsg {
		return reply, fmt.Errorf("sendMessageById not implemented for message Id: %s", inreq.Data.Id)
	}

	peerID := ConvertH512ToPeerID(inreq.PeerId)
	peerInfo := ss.getPeer(peerID)
	if peerInfo == nil {
		//TODO: enable after support peer to sentry mapping
		//return reply, fmt.Errorf("peer not found: %s", peerID)
		return reply, nil
	}

	ss.writePeer("sendMessageById", peerInfo, msgcode, inreq.Data.Data, 0)
	reply.Peers = []*proto_types.H512{inreq.PeerId}
	return reply, nil
}

func (ss *GrpcServer) SendMessageToRandomPeers(ctx context.Context, req *proto_sentry.SendMessageToRandomPeersRequest) (*proto_sentry.SentPeers, error) {
	reply := &proto_sentry.SentPeers{}

	msgcode := eth.FromProto[ss.Protocols[0].Version][req.Data.Id]
	if msgcode != eth.NewBlockMsg &&
		msgcode != eth.NewBlockHashesMsg &&
		msgcode != eth.NewPooledTransactionHashesMsg &&
		msgcode != eth.TransactionsMsg {
		return reply, fmt.Errorf("sendMessageToRandomPeers not implemented for message Id: %s", req.Data.Id)
	}

	peerInfos := make([]*PeerInfo, 0, 32) // 32 gives capacity for 1024 peers, well beyond default
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		peerInfos = append(peerInfos, peerInfo)
		return true
	})
	rand.Shuffle(len(peerInfos), func(i int, j int) {
		peerInfos[i], peerInfos[j] = peerInfos[j], peerInfos[i]
	})
	peersToSendCount := len(peerInfos)
	if peersToSendCount > 0 {
		peerCountConstrained := math.Min(float64(len(peerInfos)), float64(req.MaxPeers))
		// Ensure we have at least 1 peer during our sqrt operation
		peersToSendCount = int(math.Max(math.Sqrt(peerCountConstrained), 1.0))
	}

	var lastErr error
	// Send the block to a subset of our peers at random
	for _, peerInfo := range peerInfos[:peersToSendCount] {
		ss.writePeer("sendMessageToRandomPeers", peerInfo, msgcode, req.Data.Data, 0)
		reply.Peers = append(reply.Peers, gointerfaces.ConvertHashToH512(peerInfo.ID()))
	}
	return reply, lastErr
}

func (ss *GrpcServer) SendMessageToAll(ctx context.Context, req *proto_sentry.OutboundMessageData) (*proto_sentry.SentPeers, error) {
	reply := &proto_sentry.SentPeers{}

	msgcode := eth.FromProto[ss.Protocols[0].Version][req.Id]
	if msgcode != eth.NewBlockMsg &&
		msgcode != eth.NewPooledTransactionHashesMsg && // to broadcast new local transactions
		msgcode != eth.NewBlockHashesMsg {
		return reply, fmt.Errorf("sendMessageToAll not implemented for message Id: %s", req.Id)
	}

	var lastErr error
	ss.rangePeers(func(peerInfo *PeerInfo) bool {
		ss.writePeer("SendMessageToAll", peerInfo, msgcode, req.Data, 0)
		reply.Peers = append(reply.Peers, gointerfaces.ConvertHashToH512(peerInfo.ID()))
		return true
	})
	return reply, lastErr
}

func (ss *GrpcServer) HandShake(context.Context, *emptypb.Empty) (*proto_sentry.HandShakeReply, error) {
	reply := &proto_sentry.HandShakeReply{}
	switch ss.Protocols[0].Version {
	case eth.ETH66:
		reply.Protocol = proto_sentry.Protocol_ETH66
	case eth.ETH67:
		reply.Protocol = proto_sentry.Protocol_ETH67
	case eth.ETH68:
		reply.Protocol = proto_sentry.Protocol_ETH68
	}
	return reply, nil
}

func (ss *GrpcServer) SetStatus(ctx context.Context, statusData *proto_sentry.StatusData) (*proto_sentry.SetStatusReply, error) {
	genesisHash := gointerfaces.ConvertH256ToHash(statusData.ForkData.Genesis)

	ss.lock.Lock()
	defer ss.lock.Unlock()
	reply := &proto_sentry.SetStatusReply{}

	if ss.P2pServer == nil {
		var err error
		if !ss.p2p.NoDiscovery {
			if len(ss.discoveryDNS) == 0 {
				if url := params.KnownDNSNetwork(genesisHash, "all"); url != "" {
					ss.discoveryDNS = []string{url}
				}
			}
			for _, p := range ss.Protocols {
				p.DialCandidates, err = setupDiscovery(ss.discoveryDNS)
				if err != nil {
					return nil, err
				}
			}
		}

		srv, err := makeP2PServer(*ss.p2p, genesisHash, ss.Protocols)
		if err != nil {
			return reply, err
		}

		// Add protocol
		if err = srv.Start(ss.ctx); err != nil {
			srv.Stop()
			return reply, fmt.Errorf("could not start server: %w", err)
		}

		ss.P2pServer = srv
	}

	ss.P2pServer.LocalNode().Set(eth.CurrentENREntryFromForks(statusData.ForkData.HeightForks, statusData.ForkData.TimeForks, genesisHash, statusData.MaxBlockHeight, statusData.MaxBlockTime))
	if ss.statusData == nil || statusData.MaxBlockHeight != 0 {
		// Not overwrite statusData if the message contains zero MaxBlock (comes from standalone transaction pool)
		ss.statusData = statusData
	}
	return reply, nil
}

func (ss *GrpcServer) Peers(_ context.Context, _ *emptypb.Empty) (*proto_sentry.PeersReply, error) {
	if ss.P2pServer == nil {
		return nil, errors.New("p2p server was not started")
	}

	peers := ss.P2pServer.PeersInfo()

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

// setupDiscovery creates the node discovery source for the `eth` and `snap`
// protocols.
func setupDiscovery(urls []string) (enode.Iterator, error) {
	if len(urls) == 0 {
		return nil, nil
	}
	client := dnsdisc.NewClient(dnsdisc.Config{})
	return client.NewIterator(urls...)
}

func (ss *GrpcServer) GetStatus() *proto_sentry.StatusData {
	ss.lock.RLock()
	defer ss.lock.RUnlock()
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
			log.Debug("[sentry] consuming is slow, drop 50% of old messages", "msgID", msgID.String())
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
	log.Trace("[Messages] new subscriber", "to", req.Ids)
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
				log.Warn("Sending msg to core P2P failed", "msg", in.Id.String(), "err", err)
				return err
			}
		}
	}
}

// Close performs cleanup operations for the sentry
func (ss *GrpcServer) Close() {
	if ss.P2pServer != nil {
		ss.P2pServer.Stop()
	}
}

func (ss *GrpcServer) sendNewPeerToClients(peerID *proto_types.H512) {
	if err := ss.peersStreams.Broadcast(&proto_sentry.PeerEvent{PeerId: peerID, EventId: proto_sentry.PeerEvent_Connect}); err != nil {
		log.Warn("Sending new peer notice to core P2P failed", "err", err)
	}
}

func (ss *GrpcServer) sendGonePeerToClients(peerID *proto_types.H512) {
	if err := ss.peersStreams.Broadcast(&proto_sentry.PeerEvent{PeerId: peerID, EventId: proto_sentry.PeerEvent_Disconnect}); err != nil {
		log.Warn("Sending gone peer notice to core P2P failed", "err", err)
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

func (ss *GrpcServer) NodeInfo(_ context.Context, _ *emptypb.Empty) (*proto_types.NodeInfoReply, error) {
	if ss.P2pServer == nil {
		return nil, errors.New("p2p server was not started")
	}

	info := ss.P2pServer.NodeInfo()
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
