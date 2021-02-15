package download

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	proto_sentry "github.com/ledgerwatch/turbo-geth/cmd/headers/sentry"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/dnsdisc"
	"github.com/ledgerwatch/turbo-geth/p2p/enode"
	"github.com/ledgerwatch/turbo-geth/p2p/nat"
	"github.com/ledgerwatch/turbo-geth/p2p/netutil"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"
)

func nodeKey() *ecdsa.PrivateKey {
	keyfile := "nodekey"
	if key, err := crypto.LoadECDSA(keyfile); err == nil {
		return key
	}
	// No persistent key found, generate and store a new one.
	key, err := crypto.GenerateKey()
	if err != nil {
		log.Crit(fmt.Sprintf("Failed to generate node key: %v", err))
	}
	if err := crypto.SaveECDSA(keyfile, key); err != nil {
		log.Error(fmt.Sprintf("Failed to persist node key: %v", err))
	}
	return key
}

// SentryMsg declares ID fields necessary for communicating with the sentry
type SentryMsg struct {
	sentryId  int
	requestId int
}

// NewBlockFromSentry is a type of message sent from sentry to the downloader as a result of NewBlockMsg
type NewBlockFromSentry struct {
	SentryMsg
	eth.NewBlockData
}

type NewBlockHashFromSentry struct {
	SentryMsg
	eth.NewBlockHashesData
}

type BlockHeadersFromSentry struct {
	SentryMsg
	headers []*types.Header
}

type PenaltyMsg struct {
	SentryMsg
	penalty headerdownload.Penalty
}

func makeP2PServer(
	ctx context.Context,
	natSetting string,
	port int,
	peerHeightMap *sync.Map,
	peerTimeMap *sync.Map,
	peerRwMap *sync.Map,
	protocols []string,
	ss *SentryServerImpl,
) (*p2p.Server, error) {
	client := dnsdisc.NewClient(dnsdisc.Config{})

	dns := params.KnownDNSNetwork(params.MainnetGenesisHash, "all")
	dialCandidates, err := client.NewIterator(dns)
	if err != nil {
		return nil, fmt.Errorf("create discovery candidates: %v", err)
	}

	serverKey := nodeKey()
	p2pConfig := p2p.Config{}
	natif, err := nat.Parse(natSetting)
	if err != nil {
		return nil, fmt.Errorf("invalid nat option %s: %v", natSetting, err)
	}
	p2pConfig.NAT = natif
	p2pConfig.PrivateKey = serverKey
	p2pConfig.Name = "header downloader"
	p2pConfig.Logger = log.New()
	p2pConfig.MaxPeers = 100
	p2pConfig.Protocols = []p2p.Protocol{}
	p2pConfig.NodeDatabase = "downloader_nodes"
	p2pConfig.ListenAddr = fmt.Sprintf(":%d", port)
	pMap := map[string]p2p.Protocol{
		eth.ProtocolName: {
			Name:           eth.ProtocolName,
			Version:        eth.ProtocolVersions[0],
			Length:         eth.ProtocolLengths[eth.ProtocolVersions[0]],
			DialCandidates: dialCandidates,
			Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) error {
				peerID := peer.ID().String()
				log.Info(fmt.Sprintf("[%s] Start with peer", peerID))
				peerRwMap.Store(peerID, rw)
				if err := runPeer(
					ctx,
					peerHeightMap,
					peerTimeMap,
					peerRwMap,
					peer,
					rw,
					eth.ProtocolVersions[0], // version == eth65
					eth.ProtocolVersions[1], // minVersion == eth64
					ss,
				); err != nil {
					log.Info(fmt.Sprintf("[%s] Error while running peer: %v", peerID, err))
				}
				peerHeightMap.Delete(peerID)
				peerTimeMap.Delete(peerID)
				peerRwMap.Delete(peerID)
				return nil
			},
		},
	}

	for _, protocolName := range protocols {
		p2pConfig.Protocols = append(p2pConfig.Protocols, pMap[protocolName])
	}
	return &p2p.Server{Config: p2pConfig}, nil
}

func errResp(code int, format string, v ...interface{}) error {
	return fmt.Errorf("%v - %v", code, fmt.Sprintf(format, v...))
}

func runPeer(
	ctx context.Context,
	peerHeightMap *sync.Map,
	peerTimeMap *sync.Map,
	peerRwMap *sync.Map,
	peer *p2p.Peer,
	rw p2p.MsgReadWriter,
	version uint,
	minVersion uint,
	ss *SentryServerImpl,
) error {
	peerID := peer.ID().String()
	protoStatusData := ss.getStatus()
	if protoStatusData == nil {
		return fmt.Errorf("could not get status message from core for peer %s connection", peerID)
	}
	// Convert proto status data into the one required by devp2p
	genesisHash := common.BytesToHash(protoStatusData.ForkData.Genesis)
	statusData := &eth.StatusData{
		ProtocolVersion: uint32(version),
		NetworkID:       protoStatusData.NetworkId,
		TD:              new(big.Int).SetBytes(protoStatusData.TotalDifficulty),
		Head:            common.BytesToHash(protoStatusData.BestHash),
		Genesis:         genesisHash,
		ForkID:          forkid.NewIDFromForks(protoStatusData.ForkData.Forks, genesisHash),
	}
	forkFilter := forkid.NewFilterFromForks(protoStatusData.ForkData.Forks, genesisHash)
	networkID := protoStatusData.NetworkId
	if err := p2p.Send(rw, eth.StatusMsg, statusData); err != nil {
		return fmt.Errorf("handshake to peer %s: %v", peerID, err)
	}
	// Read handshake message
	msg, err := rw.ReadMsg()
	if err != nil {
		return err
	}

	if msg.Code != eth.StatusMsg {
		msg.Discard()
		return errResp(eth.ErrNoStatusMsg, "first msg has code %x (!= %x)", msg.Code, eth.StatusMsg)
	}
	if msg.Size > eth.ProtocolMaxMsgSize {
		msg.Discard()
		return errResp(eth.ErrMsgTooLarge, "message is too large %d, limit %d", msg.Size, eth.ProtocolMaxMsgSize)
	}
	// Decode the handshake and make sure everything matches
	var status eth.StatusData
	if err = msg.Decode(&status); err != nil {
		msg.Discard()
		return errResp(eth.ErrDecode, "decode message %v: %v", msg, err)
	}
	msg.Discard()
	if status.NetworkID != networkID {
		return errResp(eth.ErrNetworkIDMismatch, "network id does not match: theirs %d, ours %d", status.NetworkID, networkID)
	}
	if uint(status.ProtocolVersion) < minVersion {
		return errResp(eth.ErrProtocolVersionMismatch, "version is less than allowed minimum: theirs %d, min %d", status.ProtocolVersion, minVersion)
	}
	if status.Genesis != genesisHash {
		return errResp(eth.ErrGenesisMismatch, "genesis hash does not match: theirs %x, ours %x", status.Genesis, genesisHash)
	}
	if err = forkFilter(status.ForkID); err != nil {
		return errResp(eth.ErrForkIDRejected, "%v", err)
	}
	log.Info(fmt.Sprintf("[%s] Received status message OK", peerID), "name", peer.Name())

	for {
		if _, ok := peerRwMap.Load(peerID); !ok {
			return fmt.Errorf("peer has been penalized")
		}
		msg, err = rw.ReadMsg()
		if err != nil {
			return fmt.Errorf("reading message: %v", err)
		}
		if msg.Size > eth.ProtocolMaxMsgSize {
			msg.Discard()
			return errResp(eth.ErrMsgTooLarge, "message is too large %d, limit %d", msg.Size, eth.ProtocolMaxMsgSize)
		}
		switch msg.Code {
		case eth.StatusMsg:
			msg.Discard()
			// Status messages should never arrive after the handshake
			return errResp(eth.ErrExtraStatusMsg, "uncontrolled status message")
		case eth.GetBlockHeadersMsg:
			if err = ss.forwardUploadMessage(msg, peerID, "GetBlockHeadersMsg", proto_sentry.MessageId_GetBlockHeaders); err != nil {
				return err
			}
		case eth.BlockHeadersMsg:
			// Peer responded or sent message - reset the "back off" timer
			peerTimeMap.Store(peerID, time.Now().Unix())
			if err = ss.forwardMessage(msg, peerID, "BlockHeadersMsg", proto_sentry.MessageId_BlockHeaders); err != nil {
				return err
			}
		case eth.GetBlockBodiesMsg:
			if err = ss.forwardUploadMessage(msg, peerID, "GetBlockBodiesMsg", proto_sentry.MessageId_GetBlockBodies); err != nil {
				return err
			}
		case eth.BlockBodiesMsg:
			// Peer responded or sent message - reset the "back off" timer
			peerTimeMap.Store(peerID, time.Now().Unix())
			if err = ss.forwardMessage(msg, peerID, "BlockBodiesMsg", proto_sentry.MessageId_BlockBodies); err != nil {
				return err
			}
		case eth.GetNodeDataMsg:
			log.Info(fmt.Sprintf("[%s] GetNodeData", peerID))
		case eth.GetReceiptsMsg:
			log.Info(fmt.Sprintf("[%s] GetReceiptsMsg", peerID))
		case eth.ReceiptsMsg:
			log.Info(fmt.Sprintf("[%s] ReceiptsMsg", peerID))
		case eth.NewBlockHashesMsg:
			if err = ss.forwardMessage(msg, peerID, "NewBlockHashesMsg", proto_sentry.MessageId_NewBlockHashes); err != nil {
				return err
			}
		case eth.NewBlockMsg:
			if err = ss.forwardMessage(msg, peerID, "NewBlockMsg", proto_sentry.MessageId_NewBlock); err != nil {
				return err
			}
		case eth.NewPooledTransactionHashesMsg:
			var hashes []common.Hash
			if err := msg.Decode(&hashes); err != nil {
				return errResp(eth.ErrDecode, "decode NewPooledTransactionHashesMsg %v: %v", msg, err)
			}
			var hashesStr strings.Builder
			for _, hash := range hashes {
				if hashesStr.Len() > 0 {
					hashesStr.WriteString(",")
				}
				hashesStr.WriteString(fmt.Sprintf("%x-%x", hash[:4], hash[28:]))
			}
			//log.Info(fmt.Sprintf("[%s] NewPooledTransactionHashesMsg {%s}", peerID, hashesStr.String()))
		case eth.GetPooledTransactionsMsg:
			//log.Info(fmt.Sprintf("[%s] GetPooledTransactionsMsg", peerID)
		case eth.TransactionMsg:
			var txs []*types.Transaction
			if err := msg.Decode(&txs); err != nil {
				return errResp(eth.ErrDecode, "decode TransactionMsg %v: %v", msg, err)
			}
			var hashesStr strings.Builder
			for _, tx := range txs {
				if hashesStr.Len() > 0 {
					hashesStr.WriteString(",")
				}
				hash := tx.Hash()
				hashesStr.WriteString(fmt.Sprintf("%x-%x", hash[:4], hash[28:]))
			}
			//log.Info(fmt.Sprintf("[%s] TransactionMsg {%s}", peerID, hashesStr.String()))
		case eth.PooledTransactionsMsg:
			//log.Info(fmt.Sprintf("[%s] PooledTransactionsMsg", peerID)
		default:
			log.Error(fmt.Sprintf("[%s] Unknown message code: %d", peerID, msg.Code))
		}
		msg.Discard()
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

func grpcSentryServer(ctx context.Context, sentryAddr string) (*SentryServerImpl, error) {
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
		return nil, fmt.Errorf("could not create Sentry P2P listener: %w, addr=%s", err, sentryAddr)
	}
	var (
		streamInterceptors []grpc.StreamServerInterceptor
		unaryInterceptors  []grpc.UnaryServerInterceptor
	)
	if metrics.Enabled {
		streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)
		unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	}
	streamInterceptors = append(streamInterceptors, grpc_recovery.StreamServerInterceptor())
	unaryInterceptors = append(unaryInterceptors, grpc_recovery.UnaryServerInterceptor())
	var grpcServer *grpc.Server
	cpus := uint32(runtime.GOMAXPROCS(-1))
	opts := []grpc.ServerOption{
		grpc.NumStreamWorkers(cpus), // reduce amount of goroutines
		grpc.WriteBufferSize(1024),  // reduce buffers to save mem
		grpc.ReadBufferSize(1024),
		grpc.MaxConcurrentStreams(100), // to force clients reduce concurrency level
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time: 10 * time.Minute,
		}),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	}
	grpcServer = grpc.NewServer(opts...)
	sentryServer := &SentryServerImpl{}
	proto_sentry.RegisterSentryServer(grpcServer, sentryServer)
	if metrics.Enabled {
		grpc_prometheus.Register(grpcServer)
	}
	go func() {
		if err1 := grpcServer.Serve(lis); err1 != nil {
			log.Error("Sentry P2P server fail", "err", err1)
		}
	}()
	return sentryServer, nil
}

func p2pServer(ctx context.Context,
	sentryServer *SentryServerImpl,
	natSetting string, port int, staticPeers []string, discovery bool, netRestrict string,
) (*p2p.Server, error) {
	server, err := makeP2PServer(
		ctx,
		natSetting,
		port,
		&sentryServer.peerHeightMap,
		&sentryServer.peerTimeMap,
		&sentryServer.peerRwMap,
		[]string{eth.ProtocolName},
		sentryServer,
	)
	if err != nil {
		return nil, err
	}

	enodes := make([]*enode.Node, len(staticPeers))
	for i, e := range staticPeers {
		enodes[i] = enode.MustParse(e)
	}

	server.StaticNodes = enodes
	server.NoDiscovery = discovery

	if netRestrict != "" {
		server.NetRestrict = new(netutil.Netlist)
		server.NetRestrict.Add(netRestrict)
	}
	return server, nil
}

// Sentry creates and runs standalone sentry
func Sentry(natSetting string, port int, sentryAddr string, coreAddr string, staticPeers []string, discovery bool, netRestrict string) error {
	ctx := rootContext()

	sentryServer, err := grpcSentryServer(ctx, sentryAddr)
	if err != nil {
		return err
	}

	sentryServer.p2pServer, err = p2pServer(ctx, sentryServer, natSetting, port, staticPeers, discovery, netRestrict)
	if err != nil {
		return err
	}

	<-ctx.Done()
	return nil
}

type SentryServerImpl struct {
	proto_sentry.UnimplementedSentryServer
	peerHeightMap       sync.Map
	peerRwMap           sync.Map
	peerTimeMap         sync.Map
	statusData          *proto_sentry.StatusData
	p2pServer           *p2p.Server
	receiveServer       proto_sentry.Sentry_ReceiveMessagesServer
	receiveUploadServer proto_sentry.Sentry_ReceiveUploadMessagesServer
	lock                sync.RWMutex
}

func (ss *SentryServerImpl) forwardMessage(msg p2p.Msg, peerID string, msgName string, msgId proto_sentry.MessageId) error {
	b := make([]byte, msg.Size)
	if _, err := io.ReadFull(msg.Payload, b); err != nil {
		return fmt.Errorf("%s: reading msg into bytes: %v", peerID, err)
	}
	receiveServer := ss.getReceiveServer()
	if receiveServer == nil {
		log.Error("Sending msg to core, but receiver is nil", "msg", msgName)
	} else {
		outreq := proto_sentry.InboundMessage{
			PeerId: []byte(peerID),
			Id:     msgId,
			Data:   b,
		}
		if err := receiveServer.Send(&outreq); err != nil {
			log.Error("Sending msg to core P2P failed", "msg", msgName, "error", err)
		}
	}
	return nil
}

func (ss *SentryServerImpl) forwardUploadMessage(msg p2p.Msg, peerID string, msgName string, msgId proto_sentry.MessageId) error {
	b := make([]byte, msg.Size)
	if _, err := io.ReadFull(msg.Payload, b); err != nil {
		return fmt.Errorf("%s: reading msg into bytes: %v", peerID, err)
	}
	receiveUploadServer := ss.getReceiveUploadServer()
	if receiveUploadServer == nil {
		log.Error("Sending msg to core, but receiver is nil", "msg", msgName)
	} else {
		outreq := proto_sentry.InboundMessage{
			PeerId: []byte(peerID),
			Id:     msgId,
			Data:   b,
		}
		if err := receiveUploadServer.Send(&outreq); err != nil {
			log.Error("Sending msg to core P2P failed", "msg", msgName, "error", err)
		}
	}
	return nil
}

func (ss *SentryServerImpl) PenalizePeer(_ context.Context, req *proto_sentry.PenalizePeerRequest) (*empty.Empty, error) {
	//log.Warn("Received penalty", "kind", req.GetPenalty().Descriptor().FullName, "from", fmt.Sprintf("%s", req.GetPeerId()))
	ss.peerRwMap.Delete(string(req.PeerId))
	ss.peerTimeMap.Delete(string(req.PeerId))
	ss.peerHeightMap.Delete(string(req.PeerId))
	return &empty.Empty{}, nil
}

func (ss *SentryServerImpl) PeerMinBlock(_ context.Context, req *proto_sentry.PeerMinBlockRequest) (*empty.Empty, error) {
	peerID := string(req.PeerId)
	x, _ := ss.peerHeightMap.Load(peerID)
	highestBlock, _ := x.(uint64)
	if req.MinBlock > highestBlock {
		ss.peerHeightMap.Store(peerID, req.MinBlock)
	}
	return &empty.Empty{}, nil
}

func (ss *SentryServerImpl) findPeer(minBlock uint64) (string, bool) {
	// Choose a peer that we can send this request to
	var peerID string
	var found bool
	timeNow := time.Now().Unix()
	ss.peerHeightMap.Range(func(key, value interface{}) bool {
		valUint, _ := value.(uint64)
		if valUint >= minBlock {
			peerID = key.(string)
			timeRaw, _ := ss.peerTimeMap.Load(peerID)
			t, _ := timeRaw.(int64)
			// If request is large, we give 5 second pause to the peer before sending another request, unless it responded
			if t <= timeNow {
				found = true
				return false
			}
		}
		return true
	})
	return peerID, found
}

func (ss *SentryServerImpl) SendMessageByMinBlock(_ context.Context, inreq *proto_sentry.SendMessageByMinBlockRequest) (*proto_sentry.SentPeers, error) {
	peerID, found := ss.findPeer(inreq.MinBlock)
	if !found {
		return &proto_sentry.SentPeers{}, nil
	}
	rwRaw, _ := ss.peerRwMap.Load(peerID)
	rw, _ := rwRaw.(p2p.MsgReadWriter)
	if rw == nil {
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageByMinBlock find rw for peer %s", peerID)
	}
	var msgcode uint64
	switch inreq.Data.Id {
	case proto_sentry.MessageId_GetBlockHeaders:
		msgcode = eth.GetBlockHeadersMsg
	case proto_sentry.MessageId_GetBlockBodies:
		msgcode = eth.GetBlockBodiesMsg
	default:
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageByMinBlock not implemented for message Id: %s", inreq.Data.Id)
	}
	if err := rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(inreq.Data.Data)), Payload: bytes.NewReader(inreq.Data.Data)}); err != nil {
		ss.peerHeightMap.Delete(peerID)
		ss.peerTimeMap.Delete(peerID)
		ss.peerRwMap.Delete(peerID)
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageByMinBlock to peer %s: %v", peerID, err)
	}
	ss.peerTimeMap.Store(peerID, time.Now().Unix()+5)
	return &proto_sentry.SentPeers{Peers: [][]byte{[]byte(peerID)}}, nil
}

func (ss *SentryServerImpl) SendMessageById(_ context.Context, inreq *proto_sentry.SendMessageByIdRequest) (*proto_sentry.SentPeers, error) {
	peerID := string(inreq.PeerId)
	rwRaw, ok := ss.peerRwMap.Load(peerID)
	if !ok {
		return &proto_sentry.SentPeers{}, fmt.Errorf("peer not found: %x", inreq.PeerId)
	}
	rw, _ := rwRaw.(p2p.MsgReadWriter)
	var msgcode uint64
	switch inreq.Data.Id {
	case proto_sentry.MessageId_GetBlockHeaders:
		msgcode = eth.GetBlockHeadersMsg
	case proto_sentry.MessageId_BlockHeaders:
		msgcode = eth.BlockHeadersMsg
	case proto_sentry.MessageId_BlockBodies:
		msgcode = eth.BlockBodiesMsg
	default:
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageById not implemented for message Id: %s", inreq.Data.Id)
	}
	if err := rw.WriteMsg(p2p.Msg{Code: msgcode, Size: uint32(len(inreq.Data.Data)), Payload: bytes.NewReader(inreq.Data.Data)}); err != nil {
		ss.peerHeightMap.Delete(peerID)
		ss.peerTimeMap.Delete(peerID)
		ss.peerRwMap.Delete(peerID)
		return &proto_sentry.SentPeers{}, fmt.Errorf("sendMessageById to peer %s: %v", peerID, err)
	}
	return &proto_sentry.SentPeers{Peers: [][]byte{inreq.PeerId}}, nil
}

func (ss *SentryServerImpl) SendMessageToRandomPeers(context.Context, *proto_sentry.SendMessageToRandomPeersRequest) (*proto_sentry.SentPeers, error) {
	return nil, nil
}

func (ss *SentryServerImpl) SendMessageToAll(context.Context, *proto_sentry.OutboundMessageData) (*proto_sentry.SentPeers, error) {
	return nil, nil
}

func (ss *SentryServerImpl) SetStatus(_ context.Context, statusData *proto_sentry.StatusData) (*emptypb.Empty, error) {
	ss.lock.Lock()
	defer ss.lock.Unlock()
	init := ss.statusData == nil
	if init {
		// Add protocol
		if err := ss.p2pServer.Start(); err != nil {
			return &empty.Empty{}, fmt.Errorf("could not start server: %w", err)
		}
	}
	ss.statusData = statusData
	return &empty.Empty{}, nil
}

func (ss *SentryServerImpl) getStatus() *proto_sentry.StatusData {
	ss.lock.RLock()
	defer ss.lock.RUnlock()
	return ss.statusData
}

func (ss *SentryServerImpl) ReceiveMessages(_ *emptypb.Empty, server proto_sentry.Sentry_ReceiveMessagesServer) error {
	ss.lock.Lock()
	defer ss.lock.Unlock()
	ss.receiveServer = server
	return nil
}

func (ss *SentryServerImpl) ReceiveUploadMessages(_ *emptypb.Empty, server proto_sentry.Sentry_ReceiveUploadMessagesServer) error {
	ss.lock.Lock()
	defer ss.lock.Unlock()
	ss.receiveUploadServer = server
	return nil
}

func (ss *SentryServerImpl) getReceiveServer() proto_sentry.Sentry_ReceiveMessagesServer {
	ss.lock.RLock()
	defer ss.lock.RUnlock()
	return ss.receiveServer
}

func (ss *SentryServerImpl) getReceiveUploadServer() proto_sentry.Sentry_ReceiveMessagesServer {
	ss.lock.RLock()
	defer ss.lock.RUnlock()
	return ss.receiveUploadServer
}
