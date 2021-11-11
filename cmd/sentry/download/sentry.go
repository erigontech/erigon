package download

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"path"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	proto_types "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/dnsdisc"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
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
	peer      *p2p.Peer
	lock      sync.RWMutex
	deadlines []time.Time // Request deadlines
	height    uint64
	rw        p2p.MsgReadWriter
	removed   bool
}

// AddDeadline adds given deadline to the list of deadlines
// Deadlines must be added in the chronological order for the function
// ClearDeadlines to work correctly (it uses binary search)
func (pi *PeerInfo) AddDeadline(deadline time.Time) {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	pi.deadlines = append(pi.deadlines, deadline)
}

func (pi *PeerInfo) Height() uint64 {
	return atomic.LoadUint64(&pi.height)
}
func (pi *PeerInfo) SetHeight(h uint64) {
	atomic.StoreUint64(&pi.height, h)
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

func (pi *PeerInfo) Remove() {
	pi.lock.Lock()
	defer pi.lock.Unlock()
	pi.removed = true
}

func (pi *PeerInfo) Removed() bool {
	pi.lock.RLock()
	defer pi.lock.RUnlock()
	return pi.removed
}

func makeP2PServer(
	p2pConfig p2p.Config,
	genesisHash common.Hash,
	protocol p2p.Protocol,
) (*p2p.Server, error) {
	var urls []string
	switch genesisHash {
	case params.MainnetGenesisHash:
		urls = params.MainnetBootnodes
	case params.RopstenGenesisHash:
		urls = params.RopstenBootnodes
	case params.GoerliGenesisHash:
		urls = params.GoerliBootnodes
	case params.RinkebyGenesisHash:
		urls = params.RinkebyBootnodes
	case params.SokolGenesisHash:
		urls = params.SokolBootnodes
	case params.KovanGenesisHash:
		urls = params.KovanBootnodes
	case params.FermionGenesisHash:
		urls = params.FermionBootnodes
	}
	p2pConfig.BootstrapNodes, _ = utils.GetUrlListNodes(urls, utils.BootnodesFlag.Name, log.Crit)
	p2pConfig.BootstrapNodesV5 = p2pConfig.BootstrapNodes
	p2pConfig.Protocols = []p2p.Protocol{protocol}
	return &p2p.Server{Config: p2pConfig}, nil
}

func handShake(
	ctx context.Context,
	status *proto_sentry.StatusData,
	peerID string,
	rw p2p.MsgReadWriter,
	version uint,
	minVersion uint,
	startSync func(bestHash common.Hash) error,
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
			ForkID:          forkid.NewIDFromForks(status.ForkData.Forks, genesisHash, status.MaxBlock),
		}
		errc <- p2p.Send(rw, eth.StatusMsg, s)
	}()
	var readStatus = func() error {
		forks := make([]uint64, len(status.ForkData.Forks)) // copy because forkid.NewFilterFromForks will write into this slice
		copy(forks, status.ForkData.Forks)
		forkFilter := forkid.NewFilterFromForks(forks, genesisHash, status.MaxBlock)
		networkID := status.NetworkId
		// Read handshake message
		msg, err1 := rw.ReadMsg()
		if err1 != nil {
			return err1
		}

		if msg.Code != eth.StatusMsg {
			msg.Discard()
			return fmt.Errorf("first msg has code %x (!= %x)", msg.Code, eth.StatusMsg)
		}
		if msg.Size > eth.ProtocolMaxMsgSize {
			msg.Discard()
			return fmt.Errorf("message is too large %d, limit %d", msg.Size, eth.ProtocolMaxMsgSize)
		}
		// Decode the handshake and make sure everything matches
		var reply eth.StatusPacket
		if err1 = msg.Decode(&reply); err1 != nil {
			msg.Discard()
			return fmt.Errorf("decode message %v: %w", msg, err1)
		}
		msg.Discard()
		if reply.NetworkID != networkID {
			return fmt.Errorf("network id does not match: theirs %d, ours %d", reply.NetworkID, networkID)
		}
		if uint(reply.ProtocolVersion) < minVersion {
			return fmt.Errorf("version is less than allowed minimum: theirs %d, min %d", reply.ProtocolVersion, minVersion)
		}
		if uint(reply.ProtocolVersion) > version {
			return fmt.Errorf("version is more than what this senty supports: theirs %d, max %d", reply.ProtocolVersion, version)
		}
		if reply.Genesis != genesisHash {
			return fmt.Errorf("genesis hash does not match: theirs %x, ours %x", reply.Genesis, genesisHash)
		}
		if err1 = forkFilter(reply.ForkID); err1 != nil {
			return fmt.Errorf("%w", err1)
		}

		if startSync != nil {
			if err := startSync(reply.Head); err != nil {
				return err
			}
		}

		return nil
	}
	go func() {
		errc <- readStatus()
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
	peerID string,
	protocol uint,
	rw p2p.MsgReadWriter,
	peerInfo *PeerInfo,
	send func(msgId proto_sentry.MessageId, peerID string, b []byte),
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
			//log.Info(fmt.Sprintf("[%s] GetNodeData", peerID))
		case eth.GetReceiptsMsg:
			//log.Info(fmt.Sprintf("[%s] GetReceiptsMsg", peerID))
		case eth.ReceiptsMsg:
			//log.Info(fmt.Sprintf("[%s] ReceiptsMsg", peerID))
		case eth.NewBlockHashesMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				log.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
			send(eth.ToProto[protocol][msg.Code], peerID, b)
		case eth.NewBlockMsg:
			if !hasSubscribers(eth.ToProto[protocol][msg.Code]) {
				continue
			}
			b := make([]byte, msg.Size)
			if _, err := io.ReadFull(msg.Payload, b); err != nil {
				log.Error(fmt.Sprintf("%s: reading msg into bytes: %v", peerID, err))
			}
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
		default:
			log.Error(fmt.Sprintf("[%s] Unknown message code: %d", peerID, msg.Code))
		}
		msg.Discard()
		peerInfo.ClearDeadlines(time.Now(), givePermit)
	}
}

func rootContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer debug.LogPanic()
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(ch)

		select {
		case <-ch:
			log.Info("Got interrupt, shutting down...")
		case <-ctx.Done():
		}

		cancel()
	}()
	return ctx
}

func grpcSentryServer(ctx context.Context, sentryAddr string, ss *SentryServerImpl) (*grpc.Server, error) {
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
	go func() {
		if err1 := grpcServer.Serve(lis); err1 != nil {
			log.Error("Sentry gRPC server fail", "err", err1)
		}
	}()
	return grpcServer, nil
}

func NewSentryServer(ctx context.Context, dialCandidates enode.Iterator, readNodeInfo func() *eth.NodeInfo, cfg *p2p.Config, protocol uint) *SentryServerImpl {
	ss := &SentryServerImpl{
		ctx:          ctx,
		p2p:          cfg,
		peersStreams: NewPeersStreams(),
	}

	if protocol != eth.ETH66 {
		panic(fmt.Errorf("unexpected p2p protocol: %d", protocol))
	}

	ss.Protocol = p2p.Protocol{
		Name:           eth.ProtocolName,
		Version:        protocol,
		Length:         17,
		DialCandidates: dialCandidates,
		Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) error {
			peerID := peer.ID().String()
			if _, ok := ss.GoodPeers.Load(peerID); ok {
				log.Trace(fmt.Sprintf("[%s] Peer already has connection", peerID))
				return nil
			}
			log.Trace(fmt.Sprintf("[%s] Start with peer", peerID))

			peerInfo := &PeerInfo{
				peer: peer,
				rw:   rw,
			}

			defer ss.GoodPeers.Delete(peerID)
			err := handShake(ctx, ss.GetStatus(), peerID, rw, protocol, protocol, func(bestHash common.Hash) error {
				ss.GoodPeers.Store(peerID, peerInfo)
				ss.sendNewPeerToClients(gointerfaces.ConvertBytesToH512([]byte(peerID)))
				return ss.startSync(ctx, bestHash, peerID)
			})
			if err != nil {
				return fmt.Errorf("handshake to peer %s: %w", peerID, err)
			}
			log.Trace(fmt.Sprintf("[%s] Received status message OK", peerID), "name", peer.Name())

			if err := runPeer(
				ctx,
				peerID,
				protocol,
				rw,
				peerInfo,
				ss.send,
				ss.hasSubscribers,
			); err != nil {
				log.Trace(fmt.Sprintf("[%s] Error while running peer: %v", peerID, err))
			}
			return nil
		},
		NodeInfo: func() interface{} {
			return readNodeInfo()
		},
		PeerInfo: func(id enode.ID) interface{} {
			p, ok := ss.GoodPeers.Load(id.String())
			if !ok {
				return nil
			}
			return p.(*PeerInfo).peer.Info()
		},
		//Attributes: []enr.Entry{eth.CurrentENREntry(chainConfig, genesisHash, headHeight)},
	}

	return ss
}

// Sentry creates and runs standalone sentry
func Sentry(datadir string, sentryAddr string, discoveryDNS []string, cfg *p2p.Config, protocolVersion uint) error {
	if err := os.MkdirAll(path.Join(datadir, "erigon"), 0744); err != nil {
		return fmt.Errorf("could not create dir: %s, %w", datadir, err)
	}
	ctx := rootContext()
	sentryServer := NewSentryServer(ctx, nil, func() *eth.NodeInfo { return nil }, cfg, protocolVersion)
	sentryServer.discoveryDNS = discoveryDNS

	grpcServer, err := grpcSentryServer(ctx, sentryAddr, sentryServer)
	if err != nil {
		return err
	}

	<-ctx.Done()
	grpcServer.GracefulStop()
	sentryServer.Close()
	return nil
}

type SentryServerImpl struct {
	proto_sentry.UnimplementedSentryServer
	ctx                  context.Context
	Protocol             p2p.Protocol
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

func (ss *SentryServerImpl) startSync(ctx context.Context, bestHash common.Hash, peerID string) error {
	switch ss.Protocol.Version {
	case eth.ETH66:
		b, err := rlp.EncodeToBytes(&eth.GetBlockHeadersPacket66{
			RequestId: rand.Uint64(),
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
			PeerId: gointerfaces.ConvertBytesToH512([]byte(peerID)),
			Data: &proto_sentry.OutboundMessageData{
				Id:   proto_sentry.MessageId_GET_BLOCK_HEADERS_66,
				Data: b,
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func (ss *SentryServerImpl) PenalizePeer(_ context.Context, req *proto_sentry.PenalizePeerRequest) (*emptypb.Empty, error) {
	//log.Warn("Received penalty", "kind", req.GetPenalty().Descriptor().FullName, "from", fmt.Sprintf("%s", req.GetPeerId()))
	strId := string(gointerfaces.ConvertH512ToBytes(req.PeerId))
	if x, ok := ss.GoodPeers.Load(strId); ok {
		peerInfo := x.(*PeerInfo)
		if peerInfo != nil {
			peerInfo.Remove()
		}
	}
	ss.GoodPeers.Delete(strId)
	return &emptypb.Empty{}, nil
}

func (ss *SentryServerImpl) PeerMinBlock(_ context.Context, req *proto_sentry.PeerMinBlockRequest) (*emptypb.Empty, error) {
	peerID := string(gointerfaces.ConvertH512ToBytes(req.PeerId))
	x, _ := ss.GoodPeers.Load(peerID)
	peerInfo, _ := x.(*PeerInfo)
	if peerInfo == nil {
		return &emptypb.Empty{}, nil
	}
	if req.MinBlock > peerInfo.Height() {
		peerInfo.SetHeight(req.MinBlock)
	}
	return &emptypb.Empty{}, nil
}

func (ss *SentryServerImpl) findPeer(minBlock uint64) (string, *PeerInfo, bool) {
	// Choose a peer that we can send this request to, with maximum number of permits
	var foundPeerID string
	var foundPeerInfo *PeerInfo
	var maxPermits int
	now := time.Now()
	ss.GoodPeers.Range(func(key, value interface{}) bool {
		peerID := key.(string)
		peerInfo, _ := value.(*PeerInfo)
		if peerInfo == nil {
			return true
		}
		if peerInfo.Height() >= minBlock {
			deadlines := peerInfo.ClearDeadlines(now, false /* givePermit */)
			//fmt.Printf("%d deadlines for peer %s\n", deadlines, peerID)
			if deadlines < maxPermitsPerPeer {
				permits := maxPermitsPerPeer - deadlines
				if permits > maxPermits {
					maxPermits = permits
					foundPeerID = peerID
					foundPeerInfo = peerInfo
				}
			}
		}
		return true
	})
	return foundPeerID, foundPeerInfo, maxPermits > 0
}

func (ss *SentryServerImpl) SendMessageByMinBlock(_ context.Context, inreq *proto_sentry.SendMessageByMinBlockRequest) (*proto_sentry.SentPeers, error) {
	peerID, peerInfo, found := ss.findPeer(inreq.MinBlock)
	if !found {
		return &proto_sentry.SentPeers{}, nil
	}
	msgcode := eth.FromProto[ss.Protocol.Version][inreq.Data.Id]
	if msgcode != eth.GetBlockHeadersMsg &&
		msgcode != eth.GetBlockBodiesMsg &&
		msgcode != eth.GetPooledTransactionsMsg {
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageByMinBlock not implemented for message Id: %s", inreq.Data.Id)
	}
	if err := peerInfo.rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(inreq.Data.Data)), Payload: bytes.NewReader(inreq.Data.Data)}); err != nil {
		if x, ok := ss.GoodPeers.Load(peerID); ok {
			peerInfo := x.(*PeerInfo)
			if peerInfo != nil {
				peerInfo.Remove()
			}
		}
		ss.GoodPeers.Delete(peerID)
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageByMinBlock to peer %s: %w", peerID, err)
	}
	peerInfo.AddDeadline(time.Now().Add(30 * time.Second))
	return &proto_sentry.SentPeers{Peers: []*proto_types.H512{gointerfaces.ConvertBytesToH512([]byte(peerID))}}, nil
}

func (ss *SentryServerImpl) SendMessageById(_ context.Context, inreq *proto_sentry.SendMessageByIdRequest) (*proto_sentry.SentPeers, error) {
	peerID := string(gointerfaces.ConvertH512ToBytes(inreq.PeerId))
	x, ok := ss.GoodPeers.Load(peerID)
	if !ok {
		//TODO: enable after support peer to sentry mapping
		//return &proto_sentry.SentPeers{}, fmt.Errorf("peer not found: %s", peerID)
		return &proto_sentry.SentPeers{}, nil
	}
	peerInfo := x.(*PeerInfo)
	msgcode := eth.FromProto[ss.Protocol.Version][inreq.Data.Id]
	if msgcode != eth.GetBlockHeadersMsg &&
		msgcode != eth.BlockHeadersMsg &&
		msgcode != eth.BlockBodiesMsg &&
		msgcode != eth.GetReceiptsMsg &&
		msgcode != eth.ReceiptsMsg &&
		msgcode != eth.NewPooledTransactionHashesMsg &&
		msgcode != eth.PooledTransactionsMsg &&
		msgcode != eth.GetPooledTransactionsMsg {
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageById not implemented for message Id: %s", inreq.Data.Id)
	}

	if err := peerInfo.rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(inreq.Data.Data)), Payload: bytes.NewReader(inreq.Data.Data)}); err != nil {
		if x, ok := ss.GoodPeers.Load(peerID); ok {
			peerInfo := x.(*PeerInfo)
			if peerInfo != nil {
				peerInfo.Remove()
			}
		}
		ss.GoodPeers.Delete(peerID)
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageById to peer %s: %w", peerID, err)
	}
	return &proto_sentry.SentPeers{Peers: []*proto_types.H512{inreq.PeerId}}, nil
}

func (ss *SentryServerImpl) SendMessageToRandomPeers(ctx context.Context, req *proto_sentry.SendMessageToRandomPeersRequest) (*proto_sentry.SentPeers, error) {
	msgcode := eth.FromProto[ss.Protocol.Version][req.Data.Id]
	if msgcode != eth.NewBlockMsg &&
		msgcode != eth.NewBlockHashesMsg &&
		msgcode != eth.NewPooledTransactionHashesMsg {
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageToRandomPeers not implemented for message Id: %s", req.Data.Id)
	}

	amount := uint64(0)
	ss.GoodPeers.Range(func(key, value interface{}) bool {
		amount++
		return true
	})
	if req.MaxPeers < amount {
		amount = req.MaxPeers
	}

	// Send the block to a subset of our peers
	sendToAmount := int(math.Sqrt(float64(amount)))
	i := 0
	var innerErr error
	reply := &proto_sentry.SentPeers{Peers: []*proto_types.H512{}}
	ss.GoodPeers.Range(func(key, value interface{}) bool {
		peerID := key.(string)
		peerInfo, _ := value.(*PeerInfo)
		if peerInfo == nil {
			return true
		}
		if err := peerInfo.rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(req.Data.Data)), Payload: bytes.NewReader(req.Data.Data)}); err != nil {
			peerInfo.Remove()
			ss.GoodPeers.Delete(peerID)
			innerErr = err
			return true
		}
		reply.Peers = append(reply.Peers, gointerfaces.ConvertBytesToH512([]byte(peerID)))
		i++
		return i < sendToAmount
	})
	if innerErr != nil {
		return reply, fmt.Errorf("sendMessageToRandomPeers to peer %w", innerErr)
	}
	return reply, nil
}

func (ss *SentryServerImpl) SendMessageToAll(ctx context.Context, req *proto_sentry.OutboundMessageData) (*proto_sentry.SentPeers, error) {
	msgcode := eth.FromProto[ss.Protocol.Version][req.Id]
	if msgcode != eth.NewBlockMsg &&
		msgcode != eth.NewPooledTransactionHashesMsg && // to broadcast new local transactions
		msgcode != eth.NewBlockHashesMsg {
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageToAll not implemented for message Id: %s", req.Id)
	}

	var innerErr error
	reply := &proto_sentry.SentPeers{Peers: []*proto_types.H512{}}
	ss.GoodPeers.Range(func(key, value interface{}) bool {
		peerID := key.(string)
		peerInfo, _ := value.(*PeerInfo)
		if peerInfo == nil {
			return true
		}

		if err := peerInfo.rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(req.Data)), Payload: bytes.NewReader(req.Data)}); err != nil {
			peerInfo.Remove()
			ss.GoodPeers.Delete(peerID)
			innerErr = err
			return true
		}
		reply.Peers = append(reply.Peers, gointerfaces.ConvertBytesToH512([]byte(peerID)))
		return true
	})
	if innerErr != nil {
		return reply, fmt.Errorf("sendMessageToRandomPeers to peer %w", innerErr)
	}
	return reply, nil
}

func (ss *SentryServerImpl) HandShake(context.Context, *emptypb.Empty) (*proto_sentry.HandShakeReply, error) {
	reply := &proto_sentry.HandShakeReply{}
	switch ss.Protocol.Version {
	case eth.ETH66:
		reply.Protocol = proto_sentry.Protocol_ETH66
	}
	return reply, nil
}
func (ss *SentryServerImpl) SetStatus(_ context.Context, statusData *proto_sentry.StatusData) (*proto_sentry.SetStatusReply, error) {
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
			ss.Protocol.DialCandidates, err = setupDiscovery(ss.discoveryDNS)
			if err != nil {
				return nil, err
			}
		}

		ss.P2pServer, err = makeP2PServer(*ss.p2p, genesisHash, ss.Protocol)
		if err != nil {
			return reply, err
		}
		// Add protocol
		if err := ss.P2pServer.Start(); err != nil {
			return reply, fmt.Errorf("could not start server: %w", err)
		}
	}
	ss.P2pServer.LocalNode().Set(eth.CurrentENREntryFromForks(statusData.ForkData.Forks, genesisHash, statusData.MaxBlock))
	if ss.statusData == nil || statusData.MaxBlock != 0 {
		// Not overwrite statusData if the message contains zero MaxBlock (comes from standalone transaction pool)
		ss.statusData = statusData
	}
	return reply, nil
}

func (ss *SentryServerImpl) SimplePeerCount() (pc int) {
	ss.GoodPeers.Range(func(key, value interface{}) bool {
		peerInfo, _ := value.(*PeerInfo)
		if peerInfo == nil {
			return true
		}
		pc++
		return true
	})
	return pc
}

func (ss *SentryServerImpl) PeerCount(_ context.Context, req *proto_sentry.PeerCountRequest) (*proto_sentry.PeerCountReply, error) {
	return &proto_sentry.PeerCountReply{Count: uint64(ss.SimplePeerCount())}, nil
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

func (ss *SentryServerImpl) GetStatus() *proto_sentry.StatusData {
	ss.lock.RLock()
	defer ss.lock.RUnlock()
	return ss.statusData
}

func (ss *SentryServerImpl) send(msgID proto_sentry.MessageId, peerID string, b []byte) {
	ss.messageStreamsLock.RLock()
	defer ss.messageStreamsLock.RUnlock()
	req := &proto_sentry.InboundMessage{
		PeerId: gointerfaces.ConvertBytesToH512([]byte(peerID)),
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

func (ss *SentryServerImpl) hasSubscribers(msgID proto_sentry.MessageId) bool {
	ss.messageStreamsLock.RLock()
	defer ss.messageStreamsLock.RUnlock()
	return ss.messageStreams[msgID] != nil && len(ss.messageStreams[msgID]) > 0
	//	log.Error("Sending msg to core P2P failed", "msg", proto_sentry.MessageId_name[int32(streamMsg.msgId)], "error", err)
}

func (ss *SentryServerImpl) addMessagesStream(ids []proto_sentry.MessageId, ch chan *proto_sentry.InboundMessage) func() {
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
func (ss *SentryServerImpl) Messages(req *proto_sentry.MessagesRequest, server proto_sentry.Sentry_MessagesServer) error {
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
				log.Warn("Sending msg to core P2P failed", "msg", in.Id.String(), "error", err)
				return err
			}
		}
	}
}

// Close performs cleanup operations for the sentry
func (ss *SentryServerImpl) Close() {
	if ss.P2pServer != nil {
		ss.P2pServer.Stop()
	}
}

func (ss *SentryServerImpl) sendNewPeerToClients(peerID *proto_types.H512) {
	if err := ss.peersStreams.Broadcast(&proto_sentry.PeersReply{PeerId: peerID, Event: proto_sentry.PeersReply_Connect}); err != nil {
		log.Warn("Sending new peer notice to core P2P failed", "error", err)
	}
}

func (ss *SentryServerImpl) Peers(req *proto_sentry.PeersRequest, server proto_sentry.Sentry_PeersServer) error {
	clean := ss.peersStreams.Add(server)
	defer clean()
	select {
	case <-ss.ctx.Done():
		return nil
	case <-server.Context().Done():
		return nil
	}
}

// PeersStreams - it's safe to use this class as non-pointer
type PeersStreams struct {
	mu      sync.RWMutex
	id      uint
	streams map[uint]proto_sentry.Sentry_PeersServer
}

func NewPeersStreams() *PeersStreams {
	return &PeersStreams{}
}

func (s *PeersStreams) Add(stream proto_sentry.Sentry_PeersServer) (remove func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.streams == nil {
		s.streams = make(map[uint]proto_sentry.Sentry_PeersServer)
	}
	s.id++
	id := s.id
	s.streams[id] = stream
	return func() { s.remove(id) }
}

func (s *PeersStreams) doBroadcast(reply *proto_sentry.PeersReply) (ids []uint, errs []error) {
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

func (s *PeersStreams) Broadcast(reply *proto_sentry.PeersReply) (errs []error) {
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
