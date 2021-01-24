package download

import (
	"context"
	"crypto/ecdsa"
	"errors"
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

	"github.com/c2h5oh/datasize"
	"github.com/golang/protobuf/ptypes/empty"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	proto_core "github.com/ledgerwatch/turbo-geth/cmd/headers/core"
	proto_sentry "github.com/ledgerwatch/turbo-geth/cmd/headers/sentry"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/core"
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
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
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
	coreClient proto_core.ControlClient,
) (*p2p.Server, error) {
	client := dnsdisc.NewClient(dnsdisc.Config{})

	dns := params.KnownDNSNetwork(params.MainnetGenesisHash, "all")
	dialCandidates, err := client.NewIterator(dns)
	if err != nil {
		return nil, fmt.Errorf("create discovery candidates: %v", err)
	}

	genesis := core.DefaultGenesisBlock()
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
					peer,
					rw,
					eth.ProtocolVersions[0], // version == eth65
					eth.ProtocolVersions[1], // minVersion == eth64
					eth.DefaultConfig.NetworkID,
					genesis.Difficulty,
					params.MainnetGenesisHash,
					params.MainnetChainConfig,
					0, /* head */
					coreClient,
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
	peer *p2p.Peer,
	rw p2p.MsgReadWriter,
	version uint,
	minVersion uint,
	networkID uint64,
	td *big.Int,
	genesisHash common.Hash,
	chainConfig *params.ChainConfig,
	head uint64,
	coreClient proto_core.ControlClient,
) error {
	peerID := peer.ID().String()
	forkId := forkid.NewID(chainConfig, genesisHash, head)
	// Send handshake message
	if err := p2p.Send(rw, eth.StatusMsg, &eth.StatusData{
		ProtocolVersion: uint32(version),
		NetworkID:       networkID,
		TD:              td,
		Head:            genesisHash, // For now we always start unsyched
		Genesis:         genesisHash,
		ForkID:          forkId,
	}); err != nil {
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
	forkFilter := forkid.NewFilter(chainConfig, genesisHash, head)
	if err = forkFilter(status.ForkID); err != nil {
		return errResp(eth.ErrForkIDRejected, "%v", err)
	}
	log.Info(fmt.Sprintf("[%s] Received status message OK", peerID), "name", peer.Name())

	for {
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
			var query eth.GetBlockHeadersData
			if err = msg.Decode(&query); err != nil {
				return errResp(eth.ErrDecode, "decoding GetBlockHeadersMsg %v: %v", msg, err)
			}
			log.Info(fmt.Sprintf("[%s] GetBlockHeaderMsg{hash=%x, number=%d, amount=%d, skip=%d, reverse=%t}", peerID, query.Origin.Hash, query.Origin.Number, query.Amount, query.Skip, query.Reverse))
			var headers []*types.Header
			if err = p2p.Send(rw, eth.BlockHeadersMsg, headers); err != nil {
				return fmt.Errorf("send empty headers reply: %v", err)
			}
		case eth.BlockHeadersMsg:
			// Peer responded or sent message - reset the "back off" timer
			peerTimeMap.Store(peerID, time.Now().Unix())
			bytes := make([]byte, msg.Size)
			_, err = io.ReadFull(msg.Payload, bytes)
			if err != nil {
				return fmt.Errorf("%s: reading msg into bytes: %v", peerID, err)
			}
			var headers []*types.Header
			if err = rlp.DecodeBytes(bytes, &headers); err != nil {
				return errResp(eth.ErrDecode, "decoding BlockHeadersMsg %v: %v", msg, err)
			}
			/*
				var hashesStr strings.Builder
				for _, header := range headers {
					if hashesStr.Len() > 0 {
						hashesStr.WriteString(",")
					}
					hash := header.Hash()
					hashesStr.WriteString(fmt.Sprintf("%x-%x(%d)", hash[:4], hash[28:], header.Number.Uint64()))
				}
			*/
			log.Info(fmt.Sprintf("[%s] BlockHeadersMsg{%d hashes}", peerID, len(headers)))
			outreq := proto_core.InboundMessage{
				PeerId: []byte(peerID),
				Id:     proto_core.InboundMessageId_BlockHeaders,
				Data:   bytes,
			}
			if _, err = coreClient.ForwardInboundMessage(ctx, &outreq, &grpc.EmptyCallOption{}); err != nil {
				log.Error("Sending block headers to core P2P failed", "error", err)
			}
		case eth.GetBlockBodiesMsg:
			// Decode the retrieval message
			msgStream := rlp.NewStream(msg.Payload, uint64(msg.Size))
			if _, err = msgStream.List(); err != nil {
				return fmt.Errorf("getting list from RLP stream for GetBlockBodiesMsg: %v", err)
			}
			// Gather blocks until the fetch or network limits is reached
			var hash common.Hash
			var hashesStr strings.Builder
			for {
				// Retrieve the hash of the next block
				if err = msgStream.Decode(&hash); errors.Is(err, rlp.EOL) {
					break
				} else if err != nil {
					return errResp(eth.ErrDecode, "decode hash for GetBlockBodiesMsg %v: %v", msg, err)
				}
				if hashesStr.Len() > 0 {
					hashesStr.WriteString(",")
				}
				hashesStr.WriteString(fmt.Sprintf("%x-%x", hash[:4], hash[28:]))
			}
			log.Info(fmt.Sprintf("[%s] GetBlockBodiesMsg {%s}", peerID, hashesStr.String()))
		case eth.BlockBodiesMsg:
			// Peer responded or sent message - reset the "back off" timer
			peerTimeMap.Store(peerID, time.Now().Unix())
			//log.Info(fmt.Sprintf("[%s] BlockBodiesMsg", peerID))
			bytes := make([]byte, msg.Size)
			_, err = io.ReadFull(msg.Payload, bytes)
			if err != nil {
				return fmt.Errorf("%s: reading msg into bytes: %v", peerID, err)
			}
			outreq := proto_core.InboundMessage{
				PeerId: []byte(peerID),
				Id:     proto_core.InboundMessageId_BlockBodies,
				Data:   bytes,
			}
			if _, err = coreClient.ForwardInboundMessage(ctx, &outreq, &grpc.EmptyCallOption{}); err != nil {
				log.Error("Sending block bodies to core P2P failed", "error", err)
			}
		case eth.GetNodeDataMsg:
			log.Info(fmt.Sprintf("[%s] GetNodeData", peerID))
		case eth.GetReceiptsMsg:
			log.Info(fmt.Sprintf("[%s] GetReceiptsMsg", peerID))
		case eth.ReceiptsMsg:
			log.Info(fmt.Sprintf("[%s] ReceiptsMsg", peerID))
		case eth.NewBlockHashesMsg:
			bytes := make([]byte, msg.Size)
			_, err = io.ReadFull(msg.Payload, bytes)
			if err != nil {
				return fmt.Errorf("%s: reading msg into bytes: %v", peerID, err)
			}
			var announces eth.NewBlockHashesData
			if err = rlp.DecodeBytes(bytes, &announces); err != nil {
				return errResp(eth.ErrDecode, "decode NewBlockHashesData %v: %v", msg, err)
			}
			x, _ := peerHeightMap.Load(peerID)
			highestBlock, _ := x.(uint64)
			var numStr strings.Builder
			for _, announce := range announces {
				if numStr.Len() > 0 {
					numStr.WriteString(",")
				}
				numStr.WriteString(fmt.Sprintf("%d", announce.Number))
				if announce.Number > highestBlock {
					highestBlock = announce.Number
				}
			}
			peerHeightMap.Store(peerID, highestBlock)
			log.Info(fmt.Sprintf("[%s] NewBlockHashesMsg {%s}", peerID, numStr.String()))
			outreq := proto_core.InboundMessage{
				PeerId: []byte(peerID),
				Id:     proto_core.InboundMessageId_NewBlockHashes,
				Data:   bytes,
			}
			if _, err = coreClient.ForwardInboundMessage(ctx, &outreq, &grpc.EmptyCallOption{}); err != nil {
				log.Error("send block header announcement to core P2P failed", "error", err)
			}
		case eth.NewBlockMsg:
			bytes := make([]byte, msg.Size)
			_, err = io.ReadFull(msg.Payload, bytes)
			if err != nil {
				return fmt.Errorf("%s: reading msg into bytes: %v", peerID, err)
			}
			var request eth.NewBlockData
			if err = rlp.DecodeBytes(bytes, &request); err != nil {
				return errResp(eth.ErrDecode, "decode NewBlockMsg %v: %v", msg, err)
			}
			blockNum := request.Block.NumberU64()
			x, _ := peerHeightMap.Load(peerID)
			highestBlock, _ := x.(uint64)
			if blockNum > highestBlock {
				highestBlock = blockNum
				peerHeightMap.Store(peerID, highestBlock)
			}
			log.Info(fmt.Sprintf("[%s] NewBlockMsg{blockNumber: %d}", peerID, blockNum))
			outreq := proto_core.InboundMessage{
				PeerId: []byte(peerID),
				Id:     proto_core.InboundMessageId_NewBlock,
				Data:   bytes,
			}
			if _, err = coreClient.ForwardInboundMessage(ctx, &outreq, &grpc.EmptyCallOption{}); err != nil {
				log.Error("Sending new block to core P2P failed", "error", err)
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

func grpcControlClient(ctx context.Context, coreAddr string) (proto_core.ControlClient, error) {
	// CREATING GRPC CLIENT CONNECTION
	log.Info("Starting Control client", "connecting to core", coreAddr)
	var dialOpts []grpc.DialOption
	dialOpts = []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig, MinConnectTimeout: 10 * time.Minute}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(5 * datasize.MB))),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Timeout: 10 * time.Minute,
		}),
	}

	dialOpts = append(dialOpts, grpc.WithInsecure())

	conn, err := grpc.DialContext(ctx, coreAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating client connection to core P2P: %w", err)
	}
	return proto_core.NewControlClient(conn), nil
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
	coreClient proto_core.ControlClient,
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
		coreClient,
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

	coreClient, err1 := grpcControlClient(ctx, coreAddr)
	if err1 != nil {
		return err1
	}

	sentryServer, err2 := grpcSentryServer(ctx, sentryAddr)
	if err2 != nil {
		return err2
	}

	server, err3 := p2pServer(ctx, coreClient, sentryServer, natSetting, port, staticPeers, discovery, netRestrict)
	if err3 != nil {
		return err3
	}

	// Add protocol
	if err := server.Start(); err != nil {
		return fmt.Errorf("could not start server: %w", err)
	}

	<-ctx.Done()
	return nil
}

type SentryServerImpl struct {
	proto_sentry.UnimplementedSentryServer
	peerHeightMap sync.Map
	peerRwMap     sync.Map
	peerTimeMap   sync.Map
}

func (ss *SentryServerImpl) PenalizePeer(_ context.Context, req *proto_sentry.PenalizePeerRequest) (*empty.Empty, error) {
	log.Warn("Received penalty", "kind", req.GetPenalty().Descriptor().FullName, "from", fmt.Sprintf("%x", req.GetPeerId()))
	return nil, nil
}

func (ss *SentryServerImpl) findPeer(minBlock uint64) (string, bool) {
	// Choose a peer that we can send this request to
	var peerID string
	var found bool
	ss.peerHeightMap.Range(func(key, value interface{}) bool {
		valUint, _ := value.(uint64)
		if valUint >= minBlock {
			peerID = key.(string)
			timeRaw, _ := ss.peerTimeMap.Load(peerID)
			t, _ := timeRaw.(int64)
			// If request is large, we give 5 second pause to the peer before sending another request, unless it responded
			if t <= time.Now().Unix() {
				found = true
				return false
			}
		}
		return true
	})
	return peerID, found
}

func (ss *SentryServerImpl) getBlockHeaders(inreq *proto_sentry.SendMessageByMinBlockRequest) (*proto_sentry.SentPeers, error) {
	var req eth.GetBlockHeadersData
	if err := rlp.DecodeBytes(inreq.Data.Data, &req); err != nil {
		return &proto_sentry.SentPeers{}, fmt.Errorf("parse request: %v", err)
	}
	peerID, found := ss.findPeer(inreq.MinBlock)
	if !found {
		log.Debug("Could not find peer for request", "minBlock", inreq.MinBlock)
		return &proto_sentry.SentPeers{}, nil
	}
	log.Info(fmt.Sprintf("Sending req for hash %x, amount %d to peer %s\n", req.Origin.Hash, req.Amount, peerID))
	rwRaw, _ := ss.peerRwMap.Load(peerID)
	rw, _ := rwRaw.(p2p.MsgReadWriter)
	if rw == nil {
		return &proto_sentry.SentPeers{}, fmt.Errorf("find rw for peer %s", peerID)
	}
	if err := p2p.Send(rw, eth.GetBlockHeadersMsg, &req); err != nil {
		ss.peerHeightMap.Delete(peerID)
		ss.peerTimeMap.Delete(peerID)
		ss.peerRwMap.Delete(peerID)
		return &proto_sentry.SentPeers{}, fmt.Errorf("send to peer %s: %v", peerID, err)
	}
	ss.peerTimeMap.Store(peerID, time.Now().Unix()+5)
	return &proto_sentry.SentPeers{Peers: [][]byte{[]byte(peerID)}}, nil
}

func (ss *SentryServerImpl) getBlockBodies(inreq *proto_sentry.SendMessageByMinBlockRequest) (*proto_sentry.SentPeers, error) {
	var req []common.Hash
	if err := rlp.DecodeBytes(inreq.Data.Data, &req); err != nil {
		return &proto_sentry.SentPeers{}, fmt.Errorf("parse request: %v", err)
	}
	peerID, found := ss.findPeer(inreq.MinBlock)
	if !found {
		log.Debug("Could not find peer for request", "minBlock", inreq.MinBlock)
		return &proto_sentry.SentPeers{}, nil
	}
	//log.Info(fmt.Sprintf("Sending body req for %d bodies to peer %s\n", len(req), peerID))
	rwRaw, _ := ss.peerRwMap.Load(peerID)
	rw, _ := rwRaw.(p2p.MsgReadWriter)
	if rw == nil {
		return &proto_sentry.SentPeers{}, fmt.Errorf("find rw for peer %s", peerID)
	}
	if err := p2p.Send(rw, eth.GetBlockBodiesMsg, &req); err != nil {
		ss.peerHeightMap.Delete(peerID)
		ss.peerTimeMap.Delete(peerID)
		ss.peerRwMap.Delete(peerID)
		return &proto_sentry.SentPeers{}, fmt.Errorf("send to peer %s: %v", peerID, err)
	}
	ss.peerTimeMap.Store(peerID, time.Now().Unix()+5)
	return &proto_sentry.SentPeers{Peers: [][]byte{[]byte(peerID)}}, nil
}

func (ss *SentryServerImpl) SendMessageByMinBlock(_ context.Context, inreq *proto_sentry.SendMessageByMinBlockRequest) (*proto_sentry.SentPeers, error) {
	switch inreq.Data.Id {
	case proto_sentry.OutboundMessageId_GetBlockHeaders:
		return ss.getBlockHeaders(inreq)
	case proto_sentry.OutboundMessageId_GetBlockBodies:
		return ss.getBlockBodies(inreq)
	default:
		return &proto_sentry.SentPeers{}, fmt.Errorf("not implemented for message Id: %s", inreq.Data.Id)
	}
}

func (ss *SentryServerImpl) SendMessageById(context.Context, *proto_sentry.SendMessageByIdRequest) (*proto_sentry.SentPeers, error) {
	return nil, nil
}

func (ss *SentryServerImpl) SendMessageToRandomPeers(context.Context, *proto_sentry.SendMessageToRandomPeersRequest) (*proto_sentry.SentPeers, error) {
	return nil, nil
}

func (ss *SentryServerImpl) SendMessageToAll(context.Context, *proto_sentry.OutboundMessageData) (*proto_sentry.SentPeers, error) {
	return nil, nil
}
