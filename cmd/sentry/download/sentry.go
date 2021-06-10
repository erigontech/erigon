package download

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"os/signal"
	"path"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/gointerfaces"
	proto_sentry "github.com/ledgerwatch/erigon/gointerfaces/sentry"
	proto_types "github.com/ledgerwatch/erigon/gointerfaces/types"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/metrics"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/dnsdisc"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/params"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
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
	ctx context.Context,
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
	case params.CalaverasGenesisHash:
		urls = params.CalaverasBootnodes
	case params.SokolGenesisHash:
		urls = params.SokolBootnodes
	}
	p2pConfig.BootstrapNodes = make([]*enode.Node, 0, len(urls))
	for _, url := range urls {
		if url != "" {
			node, err := enode.Parse(enode.ValidSchemes, url)
			if err != nil {
				log.Crit("Bootstrap URL invalid", "enode", url, "err", err)
				continue
			}
			p2pConfig.BootstrapNodes = append(p2pConfig.BootstrapNodes, node)
		}
	}

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
) error {
	if status == nil {
		return fmt.Errorf("could not get status message from core for peer %s connection", peerID)
	}

	// Send out own handshake in a new thread
	errc := make(chan error, 2)

	// Convert proto status data into the one required by devp2p
	genesisHash := gointerfaces.ConvertH256ToHash(status.ForkData.Genesis)
	go func() {
		s := &eth.StatusPacket{
			ProtocolVersion: uint32(version),
			NetworkID:       status.NetworkId,
			TD:              gointerfaces.ConvertH256ToUint256Int(status.TotalDifficulty).ToBig(),
			Head:            gointerfaces.ConvertH256ToHash(status.BestHash),
			Genesis:         genesisHash,
			ForkID:          forkid.NewIDFromForks(status.ForkData.Forks, genesisHash, status.MaxBlock),
		}
		errc <- p2p.Send(rw, eth.StatusMsg, s)
	}()
	var readStatus = func() error {
		forkFilter := forkid.NewFilterFromForks(status.ForkData.Forks, genesisHash, status.MaxBlock)
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
			return fmt.Errorf("decode message %v: %v", msg, err1)
		}
		msg.Discard()
		if reply.NetworkID != networkID {
			return fmt.Errorf("network id does not match: theirs %d, ours %d", reply.NetworkID, networkID)
		}
		if uint(reply.ProtocolVersion) < minVersion {
			return fmt.Errorf("version is less than allowed minimum: theirs %d, min %d", reply.ProtocolVersion, minVersion)
		}
		if reply.Genesis != genesisHash {
			return fmt.Errorf("genesis hash does not match: theirs %x, ours %x", reply.Genesis, genesisHash)
		}
		if err1 = forkFilter(reply.ForkID); err1 != nil {
			return fmt.Errorf("%v", err1)
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
		if peerPrinted {
			log.Info(fmt.Sprintf("Peer %s [%s] disconnected", peerID, peerInfo.peer.Fullname()))
		}
	}()
	for {
		if !peerPrinted {
			if time.Now().After(printTime) {
				log.Info(fmt.Sprintf("Peer %s [%s] stable", peerID, peerInfo.peer.Fullname()))
				peerPrinted = true
			}
		}
		var err error
		if err = common.Stopped(ctx.Done()); err != nil {
			return err
		}
		if peerInfo.Removed() {
			return fmt.Errorf("peer removed")
		}
		msg, err := rw.ReadMsg()
		if err != nil {
			return fmt.Errorf("reading message: %v", err)
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

func grpcSentryServer(ctx context.Context, sentryAddr string, ss *SentryServerImpl) error {
	// STARTING GRPC SERVER
	log.Info("Starting Sentry P2P server", "on", sentryAddr)
	listenConfig := net.ListenConfig{
		Control: func(network, address string, _ syscall.RawConn) error {
			log.Info("Sentry P2P received connection", "via", network, "from", address)
			return nil
		},
	}
	lis, err := listenConfig.Listen(ctx, "tcp", sentryAddr)
	if err != nil {
		return fmt.Errorf("could not create Sentry P2P listener: %w, addr=%s", err, sentryAddr)
	}
	var (
		streamInterceptors []grpc.StreamServerInterceptor
		unaryInterceptors  []grpc.UnaryServerInterceptor
	)
	streamInterceptors = append(streamInterceptors, grpc_recovery.StreamServerInterceptor())
	unaryInterceptors = append(unaryInterceptors, grpc_recovery.UnaryServerInterceptor())
	if metrics.Enabled {
		streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)
		unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	}

	var grpcServer *grpc.Server
	//cpus := uint32(runtime.GOMAXPROCS(-1))
	opts := []grpc.ServerOption{
		//grpc.NumStreamWorkers(cpus), // reduce amount of goroutines
		grpc.WriteBufferSize(1024), // reduce buffers to save mem
		grpc.ReadBufferSize(1024),
		grpc.MaxConcurrentStreams(100), // to force clients reduce concurrency level
		// Don't drop the connection, settings accordign to this comment on GitHub
		// https://github.com/grpc/grpc-go/issues/3171#issuecomment-552796779
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	}
	grpcServer = grpc.NewServer(opts...)

	proto_sentry.RegisterSentryServer(grpcServer, ss)
	if metrics.Enabled {
		grpc_prometheus.Register(grpcServer)
	}
	go func() {
		if err1 := grpcServer.Serve(lis); err1 != nil {
			log.Error("Sentry P2P server fail", "err", err1)
		}
	}()
	return nil
}

func NewSentryServer(ctx context.Context, dialCandidates enode.Iterator, readNodeInfo func() *eth.NodeInfo, cfg *p2p.Config, protocol uint) *SentryServerImpl {
	ss := &SentryServerImpl{
		ctx: ctx,
		p2p: cfg,
	}

	if protocol != eth.ETH65 && protocol != eth.ETH66 {
		panic(fmt.Errorf("unexpected p2p protocol: %d", protocol))
	}

	ss.Protocol = p2p.Protocol{
		Name:           eth.ProtocolName,
		Version:        protocol,
		Length:         17,
		DialCandidates: dialCandidates,
		Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) error {
			peerID := peer.ID().String()
			if _, ok := ss.Peers.Load(peerID); ok {
				log.Debug(fmt.Sprintf("[%s] Peer already has connection", peerID))
				return nil
			}
			log.Debug(fmt.Sprintf("[%s] Start with peer", peerID))
			if err := handShake(ctx, ss.GetStatus(), peerID, rw, protocol, protocol); err != nil {
				return fmt.Errorf("handshake to peer %s: %v", peerID, err)
			}
			log.Debug(fmt.Sprintf("[%s] Received status message OK", peerID), "name", peer.Name())
			peerInfo := &PeerInfo{
				peer: peer,
				rw:   rw,
			}
			ss.Peers.Store(peerID, peerInfo)
			if err := runPeer(
				ctx,
				peerID,
				protocol,
				rw,
				peerInfo,
				ss.send,
				ss.hasSubscribers,
			); err != nil {
				log.Debug(fmt.Sprintf("[%s] Error while running peer: %v", peerID, err))
			}
			ss.Peers.Delete(peerID)
			return nil
		},
		NodeInfo: func() interface{} {
			return readNodeInfo()
		},
		PeerInfo: func(id enode.ID) interface{} {
			p, ok := ss.Peers.Load(id.String())
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

	err := grpcSentryServer(ctx, sentryAddr, sentryServer)
	if err != nil {
		return err
	}
	sentryServer.discoveryDNS = discoveryDNS

	<-ctx.Done()
	return nil
}

type SentryServerImpl struct {
	proto_sentry.UnimplementedSentryServer
	ctx          context.Context
	Protocol     p2p.Protocol
	discoveryDNS []string
	Peers        sync.Map
	statusData   *proto_sentry.StatusData
	P2pServer    *p2p.Server
	TxSubscribed uint32 // Set to non-zero if downloader is subscribed to transaction messages
	lock         sync.RWMutex
	streams      map[proto_sentry.MessageId]*StreamsList
	p2p          *p2p.Config
}

func (ss *SentryServerImpl) PenalizePeer(_ context.Context, req *proto_sentry.PenalizePeerRequest) (*empty.Empty, error) {
	//log.Warn("Received penalty", "kind", req.GetPenalty().Descriptor().FullName, "from", fmt.Sprintf("%s", req.GetPeerId()))
	strId := string(gointerfaces.ConvertH512ToBytes(req.PeerId))
	if x, ok := ss.Peers.Load(strId); ok {
		peerInfo := x.(*PeerInfo)
		if peerInfo != nil {
			peerInfo.Remove()
		}
	}
	ss.Peers.Delete(strId)
	return &empty.Empty{}, nil
}

func (ss *SentryServerImpl) PeerMinBlock(_ context.Context, req *proto_sentry.PeerMinBlockRequest) (*empty.Empty, error) {
	peerID := string(gointerfaces.ConvertH512ToBytes(req.PeerId))
	x, _ := ss.Peers.Load(peerID)
	peerInfo, _ := x.(*PeerInfo)
	if peerInfo == nil {
		return &empty.Empty{}, nil
	}
	peerInfo.lock.Lock()
	defer peerInfo.lock.Unlock()
	if req.MinBlock > peerInfo.height {
		peerInfo.height = req.MinBlock
	}
	return &empty.Empty{}, nil
}

func (ss *SentryServerImpl) findPeer(minBlock uint64) (string, *PeerInfo, bool) {
	// Choose a peer that we can send this request to, with maximum number of permits
	var foundPeerID string
	var foundPeerInfo *PeerInfo
	var maxPermits int
	now := time.Now()
	ss.Peers.Range(func(key, value interface{}) bool {
		peerID := key.(string)
		x, _ := ss.Peers.Load(peerID)
		peerInfo, _ := x.(*PeerInfo)
		if peerInfo == nil {
			return true
		}
		if peerInfo.height >= minBlock {
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
		if x, ok := ss.Peers.Load(peerID); ok {
			peerInfo := x.(*PeerInfo)
			if peerInfo != nil {
				peerInfo.Remove()
			}
		}
		ss.Peers.Delete(peerID)
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageByMinBlock to peer %s: %v", peerID, err)
	}
	peerInfo.AddDeadline(time.Now().Add(30 * time.Second))
	return &proto_sentry.SentPeers{Peers: []*proto_types.H512{gointerfaces.ConvertBytesToH512([]byte(peerID))}}, nil
}

func (ss *SentryServerImpl) SendMessageById(_ context.Context, inreq *proto_sentry.SendMessageByIdRequest) (*proto_sentry.SentPeers, error) {
	peerID := string(gointerfaces.ConvertH512ToBytes(inreq.PeerId))
	x, ok := ss.Peers.Load(peerID)
	if !ok {
		return &proto_sentry.SentPeers{}, fmt.Errorf("peer not found: %s", peerID)
	}
	peerInfo := x.(*PeerInfo)
	msgcode := eth.FromProto[ss.Protocol.Version][inreq.Data.Id]
	if msgcode != eth.GetBlockHeadersMsg &&
		msgcode != eth.BlockHeadersMsg &&
		msgcode != eth.BlockBodiesMsg &&
		msgcode != eth.GetReceiptsMsg &&
		msgcode != eth.ReceiptsMsg &&
		msgcode != eth.PooledTransactionsMsg &&
		msgcode != eth.GetPooledTransactionsMsg {
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageById not implemented for message Id: %s", inreq.Data.Id)
	}

	if err := peerInfo.rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(inreq.Data.Data)), Payload: bytes.NewReader(inreq.Data.Data)}); err != nil {
		if x, ok := ss.Peers.Load(peerID); ok {
			peerInfo := x.(*PeerInfo)
			if peerInfo != nil {
				peerInfo.Remove()
			}
		}
		ss.Peers.Delete(peerID)
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageById to peer %s: %v", peerID, err)
	}
	return &proto_sentry.SentPeers{Peers: []*proto_types.H512{inreq.PeerId}}, nil
}

func (ss *SentryServerImpl) SendMessageToRandomPeers(ctx context.Context, req *proto_sentry.SendMessageToRandomPeersRequest) (*proto_sentry.SentPeers, error) {
	msgcode := eth.FromProto[ss.Protocol.Version][req.Data.Id]
	if msgcode != eth.NewBlockMsg && msgcode != eth.NewBlockHashesMsg {
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageToRandomPeers not implemented for message Id: %s", req.Data.Id)
	}

	amount := uint64(0)
	ss.Peers.Range(func(key, value interface{}) bool {
		amount++
		return true
	})
	if req.MaxPeers > amount {
		amount = req.MaxPeers
	}

	// Send the block to a subset of our peers
	sendToAmount := int(math.Sqrt(float64(amount)))
	i := 0
	var innerErr error
	reply := &proto_sentry.SentPeers{Peers: []*proto_types.H512{}}
	ss.Peers.Range(func(key, value interface{}) bool {
		peerID := key.(string)
		peerInfo, _ := value.(*PeerInfo)
		if peerInfo == nil {
			return true
		}
		if err := peerInfo.rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(req.Data.Data)), Payload: bytes.NewReader(req.Data.Data)}); err != nil {
			peerInfo.Remove()
			ss.Peers.Delete(peerID)
			innerErr = err
			return false
		}
		reply.Peers = append(reply.Peers, gointerfaces.ConvertBytesToH512([]byte(peerID)))
		i++
		return sendToAmount <= i
	})
	if innerErr != nil {
		return reply, fmt.Errorf("sendMessageToRandomPeers to peer %w", innerErr)
	}
	return reply, nil
}

func (ss *SentryServerImpl) SendMessageToAll(ctx context.Context, req *proto_sentry.OutboundMessageData) (*proto_sentry.SentPeers, error) {
	msgcode := eth.FromProto[ss.Protocol.Version][req.Id]
	if msgcode != eth.NewBlockMsg && msgcode != eth.NewBlockHashesMsg {
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageToRandomPeers not implemented for message Id: %s", req.Id)
	}

	var innerErr error
	reply := &proto_sentry.SentPeers{Peers: []*proto_types.H512{}}
	ss.Peers.Range(func(key, value interface{}) bool {
		peerID := key.(string)
		peerInfo, _ := value.(*PeerInfo)
		if peerInfo == nil {
			return true
		}
		if err := peerInfo.rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(req.Data)), Payload: bytes.NewReader(req.Data)}); err != nil {
			peerInfo.Remove()
			ss.Peers.Delete(peerID)
			innerErr = err
			return false
		}
		reply.Peers = append(reply.Peers, gointerfaces.ConvertBytesToH512([]byte(peerID)))
		return true
	})
	if innerErr != nil {
		return reply, fmt.Errorf("sendMessageToRandomPeers to peer %w", innerErr)
	}
	return reply, nil
}

func (ss *SentryServerImpl) SetStatus(_ context.Context, statusData *proto_sentry.StatusData) (*proto_sentry.SetStatusReply, error) {
	genesisHash := gointerfaces.ConvertH256ToHash(statusData.ForkData.Genesis)

	ss.lock.Lock()
	defer ss.lock.Unlock()
	reply := &proto_sentry.SetStatusReply{}
	switch ss.Protocol.Version {
	case eth.ETH66:
		reply.Protocol = proto_sentry.Protocol_ETH66
	case eth.ETH65:
		reply.Protocol = proto_sentry.Protocol_ETH65
	}

	init := ss.statusData == nil
	if init {
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

		ss.P2pServer, err = makeP2PServer(ss.ctx, *ss.p2p, genesisHash, ss.Protocol)
		if err != nil {
			return reply, err
		}
		// Add protocol
		if err := ss.P2pServer.Start(); err != nil {
			return reply, fmt.Errorf("could not start server: %w", err)
		}
	}
	genesisHash = gointerfaces.ConvertH256ToHash(statusData.ForkData.Genesis)
	ss.P2pServer.LocalNode().Set(eth.CurrentENREntryFromForks(statusData.ForkData.Forks, genesisHash, statusData.MaxBlock))
	ss.statusData = statusData
	return reply, nil
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
	ss.lock.RLock()
	defer ss.lock.RUnlock()
	errs := ss.streams[msgID].Broadcast(&proto_sentry.InboundMessage{
		PeerId: gointerfaces.ConvertBytesToH512([]byte(peerID)),
		Id:     msgID,
		Data:   b,
	})
	for _, err := range errs {
		log.Error("Sending msg to core P2P failed", "msg", proto_sentry.MessageId_name[int32(msgID)], "error", err)
	}
}

func (ss *SentryServerImpl) hasSubscribers(msgID proto_sentry.MessageId) bool {
	ss.lock.RLock()
	defer ss.lock.RUnlock()
	return ss.streams[msgID] != nil && ss.streams[msgID].Len() > 0
	//	log.Error("Sending msg to core P2P failed", "msg", proto_sentry.MessageId_name[int32(streamMsg.msgId)], "error", err)
}

func (ss *SentryServerImpl) addStream(ids []proto_sentry.MessageId, server proto_sentry.Sentry_MessagesServer) func() {
	ss.lock.Lock()
	defer ss.lock.Unlock()
	if ss.streams == nil {
		ss.streams = map[proto_sentry.MessageId]*StreamsList{}
	}

	cleanStack := make([]func(), len(ids))
	for i, id := range ids {
		m, ok := ss.streams[id]
		if !ok {
			m = NewStreamsList()
			ss.streams[id] = m
		}

		cleanStack[i] = m.Add(server)
	}
	return func() {
		for i := range cleanStack {
			cleanStack[i]()
		}
	}
}

func (ss *SentryServerImpl) Messages(req *proto_sentry.MessagesRequest, server proto_sentry.Sentry_MessagesServer) error {
	log.Info(fmt.Sprintf("[Messages] new subscriber to: %s\n", req.Ids))
	clean := ss.addStream(req.Ids, server)
	defer clean()
	select {
	case <-ss.ctx.Done():
		return nil
	case <-server.Context().Done():
		return nil
	}
}

// StreamsList - it's safe to use this class as non-pointer
type StreamsList struct {
	sync.Mutex
	id      uint
	streams map[uint]proto_sentry.Sentry_MessagesServer
}

func NewStreamsList() *StreamsList {
	return &StreamsList{}
}

func (s *StreamsList) Add(stream proto_sentry.Sentry_MessagesServer) (remove func()) {
	s.Lock()
	defer s.Unlock()
	if s.streams == nil {
		s.streams = make(map[uint]proto_sentry.Sentry_MessagesServer)
	}
	s.id++
	id := s.id
	s.streams[id] = stream
	return func() { s.remove(id) }
}

func (s *StreamsList) Broadcast(reply *proto_sentry.InboundMessage) (errs []error) {
	s.Lock()
	defer s.Unlock()
	for id, stream := range s.streams {
		err := stream.Send(reply)
		if err != nil {
			select {
			case <-stream.Context().Done():
				delete(s.streams, id)
			default:
			}
			errs = append(errs, err)
		}
	}
	return errs
}

func (s *StreamsList) Len() int {
	s.Lock()
	defer s.Unlock()
	return len(s.streams)
}

func (s *StreamsList) remove(id uint) {
	s.Lock()
	defer s.Unlock()
	_, ok := s.streams[id]
	if !ok { // double-unsubscribe support
		return
	}
	delete(s.streams, id)
}
