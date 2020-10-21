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
	"github.com/ledgerwatch/turbo-geth/cmd/headers/proto"
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
	"github.com/ledgerwatch/turbo-geth/p2p/nat"
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
	newBlockCh chan NewBlockFromSentry,
	newBlockHashCh chan NewBlockHashFromSentry,
	headersCh chan BlockHeadersFromSentry,
	coreClient proto.ControlClient,
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
					newBlockCh,
					newBlockHashCh,
					headersCh,
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
	newBlockCh chan NewBlockFromSentry,
	newBlockHashCh chan NewBlockHashFromSentry,
	headersCh chan BlockHeadersFromSentry,
	coreClient proto.ControlClient,
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
		// Peer responded or sent message - reset the "back off" timer
		peerTimeMap.Store(peerID, time.Now().Unix())
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
			bytes := make([]byte, msg.Size)
			_, err = io.ReadFull(msg.Payload, bytes)
			if err != nil {
				return fmt.Errorf("%s: reading msg into bytes: %v", err)
			}
			var headers []*types.Header
			if err = rlp.DecodeBytes(bytes, &headers); err != nil {
				return errResp(eth.ErrDecode, "decoding BlockHeadersMsg %v: %v", msg, err)
			}
			var hashesStr strings.Builder
			for _, header := range headers {
				if hashesStr.Len() > 0 {
					hashesStr.WriteString(",")
				}
				hash := header.Hash()
				hashesStr.WriteString(fmt.Sprintf("%x-%x(%d)", hash[:4], hash[28:], header.Number.Uint64()))
			}
			log.Info(fmt.Sprintf("[%s] BlockHeadersMsg{%s}", peerID, hashesStr.String()))
			outreq := proto.InboundMessage{
				PeerId: []byte(peerID),
				Id:     proto.InboundMessageId_BlockHeaders,
				Data:   bytes,
			}
			_, err = coreClient.ForwardInboundMessage(ctx, &outreq, &grpc.EmptyCallOption{})
			if err != nil {
				return fmt.Errorf("send block headers to core P2P: %v", err)
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
			log.Info(fmt.Sprintf("[%s] BlockBodiesMsg", peerID))
		case eth.GetNodeDataMsg:
			log.Info(fmt.Sprintf("[%s] GetNodeData", peerID))
		case eth.GetReceiptsMsg:
			log.Info(fmt.Sprintf("[%s] GetReceiptsMsg", peerID))
		case eth.ReceiptsMsg:
			log.Info(fmt.Sprintf("[%s] ReceiptsMsg", peerID))
		case eth.NewBlockHashesMsg:
			var announces eth.NewBlockHashesData
			if err = msg.Decode(&announces); err != nil {
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
			newBlockHashCh <- NewBlockHashFromSentry{SentryMsg: SentryMsg{sentryId: 0, requestId: 0}, NewBlockHashesData: announces}
		case eth.NewBlockMsg:
			var request eth.NewBlockData
			if err = msg.Decode(&request); err != nil {
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
			newBlockCh <- NewBlockFromSentry{SentryMsg: SentryMsg{sentryId: 0, requestId: 0}, NewBlockData: request}
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

func Sentry(natSetting string, port int, sentryAddr string, coreAddr string) error {
	ctx := rootContext()
	// STARTING GRPC SERVER
	log.Info("Starting Sentry P2P server", "on", sentryAddr, "connecting to core", coreAddr)
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
	proto.RegisterSentryServer(grpcServer, sentryServer)
	if metrics.Enabled {
		grpc_prometheus.Register(grpcServer)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("Sentry P2P server fail", "err", err)
		}
	}()
	// CREATING GRPC CLIENT CONNECTION
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
		return fmt.Errorf("creating client connection to core P2P: %w", err)
	}
	coreClient := proto.NewControlClient(conn)

	newBlockCh := make(chan NewBlockFromSentry)
	newBlockHashCh := make(chan NewBlockHashFromSentry)
	reqHeadersCh := make(chan headerdownload.HeaderRequest)
	headersCh := make(chan BlockHeadersFromSentry)
	var peerHeightMap, peerRwMap, peerTimeMap sync.Map
	server, err := makeP2PServer(
		ctx,
		natSetting,
		port,
		&sentryServer.peerHeightMap,
		&sentryServer.peerTimeMap,
		&sentryServer.peerRwMap,
		[]string{eth.ProtocolName},
		newBlockCh,
		newBlockHashCh,
		headersCh,
		coreClient,
	)
	if err != nil {
		return err
	}
	// Add protocol
	if err = server.Start(); err != nil {
		return fmt.Errorf("could not start server: %w", err)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-reqHeadersCh:
				// Choose a peer that we can send this request to
				var peerID string
				var found bool
				peerHeightMap.Range(func(key, value interface{}) bool {
					valUint, _ := value.(uint64)
					if valUint >= req.Number {
						peerID = key.(string)
						timeRaw, _ := peerTimeMap.Load(peerID)
						t, _ := timeRaw.(int64)
						// If request is large, we give 5 second pause to the peer before sending another request, unless it responded
						if req.Length == 1 || t <= time.Now().Unix() {
							found = true
							return false
						}
					}
					return true
				})
				if !found {
					//log.Warn(fmt.Sprintf("Could not find suitable peer to send GetBlockHeadersData request for block %d", req.Number))
				} else {
					log.Info(fmt.Sprintf("Sending req for hash %x, blocknumber %d, length %d to peer %s\n", req.Hash, req.Number, req.Length, peerID))
					rwRaw, _ := peerRwMap.Load(peerID)
					rw, _ := rwRaw.(p2p.MsgReadWriter)
					if rw == nil {
						log.Error(fmt.Sprintf("Could not find rw for peer %s", peerID))
					} else {
						if err := p2p.Send(rw, eth.GetBlockHeadersMsg, &eth.GetBlockHeadersData{
							Amount:  uint64(req.Length),
							Reverse: true,
							Skip:    0,
							Origin:  eth.HashOrNumber{Hash: req.Hash},
						}); err != nil {
							log.Error(fmt.Sprintf("Failed to send to peer %s: %v", peerID, err))
						}
						peerTimeMap.Store(peerID, time.Now().Unix()+5)
					}
				}
			}
		}
	}()
	<-ctx.Done()
	return nil
}

type SentryServerImpl struct {
	proto.UnimplementedSentryServer
	peerHeightMap sync.Map
	peerRwMap     sync.Map
	peerTimeMap   sync.Map
}

func (ss *SentryServerImpl) PenalizePeer(_ context.Context, req *proto.PenalizePeerRequest) (*empty.Empty, error) {
	log.Warn("Received penalty", "kind", req.GetPenalty().Descriptor().FullName, "from", fmt.Sprintf("%x", req.GetPeerId()))
	return nil, nil
}

func (ss *SentryServerImpl) SendMessageByMinBlock(context.Context, *proto.SendMessageByMinBlockRequest) (*proto.SentPeers, error) {
	return nil, nil
}

func (ss *SentryServerImpl) SendMessageById(context.Context, *proto.SendMessageByIdRequest) (*proto.SentPeers, error) {
	return nil, nil
}

func (ss *SentryServerImpl) SendMessageToRandomPeers(context.Context, *proto.SendMessageToRandomPeersRequest) (*proto.SentPeers, error) {
	return nil, nil
}

func (ss *SentryServerImpl) SendMessageToAll(context.Context, *proto.OutboundMessageData) (*proto.SentPeers, error) {
	return nil, nil
}
