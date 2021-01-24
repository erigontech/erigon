package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"runtime"
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
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/stages"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

type chainReader struct {
	config *params.ChainConfig
}

func (cr chainReader) Config() *params.ChainConfig                             { return cr.config }
func (cr chainReader) CurrentHeader() *types.Header                            { panic("") }
func (cr chainReader) GetHeader(hash common.Hash, number uint64) *types.Header { panic("") }
func (cr chainReader) GetHeaderByNumber(number uint64) *types.Header           { panic("") }
func (cr chainReader) GetHeaderByHash(hash common.Hash) *types.Header          { panic("") }

//nolint:interfacer
func processSegment(lock *sync.Mutex, hd *headerdownload.HeaderDownload, segment *headerdownload.ChainSegment) {
	lock.Lock()
	defer lock.Unlock()
	log.Info(hd.AnchorState())
	log.Info("processSegment", "from", segment.Headers[0].Number.Uint64(), "to", segment.Headers[len(segment.Headers)-1].Number.Uint64())
	foundAnchor, start, anchorParent, invalidAnchors := hd.FindAnchors(segment)
	if len(invalidAnchors) > 0 {
		if _, err1 := hd.InvalidateAnchors(anchorParent, invalidAnchors); err1 != nil {
			log.Error("Invalidation of anchor failed", "error", err1)
		}
		log.Warn(fmt.Sprintf("Invalidated anchors %v for %x", invalidAnchors, anchorParent))
	}
	foundTip, end, penalty := hd.FindTip(segment, start) // We ignore penalty because we will check it as part of PoW check
	if penalty != headerdownload.NoPenalty {
		log.Error(fmt.Sprintf("FindTip penalty %d", penalty))
		return
	}
	currentTime := uint64(time.Now().Unix())
	var powDepth int
	if powDepth1, err1 := hd.VerifySeals(segment, foundAnchor, foundTip, start, end, currentTime); err1 == nil {
		powDepth = powDepth1
	} else {
		log.Error("VerifySeals", "error", err1)
		return
	}
	if err1 := hd.FlushBuffer(); err1 != nil {
		log.Error("Could not flush the buffer, will discard the data", "error", err1)
		return
	}
	// There are 4 cases
	if foundAnchor {
		if foundTip {
			// Connect
			if err1 := hd.Connect(segment, start, end, currentTime); err1 != nil {
				log.Error("Connect failed", "error", err1)
			} else {
				hd.AddSegmentToBuffer(segment, start, end)
				log.Info("Connected", "start", start, "end", end)
			}
		} else {
			// ExtendDown
			if err1 := hd.ExtendDown(segment, start, end, powDepth, currentTime); err1 != nil {
				log.Error("ExtendDown failed", "error", err1)
			} else {
				hd.AddSegmentToBuffer(segment, start, end)
				log.Info("Extended Down", "start", start, "end", end)
			}
		}
	} else if foundTip {
		if end == 0 {
			log.Info("No action needed, tip already exists")
		} else {
			// ExtendUp
			if err1 := hd.ExtendUp(segment, start, end, currentTime); err1 != nil {
				log.Error("ExtendUp failed", "error", err1)
			} else {
				hd.AddSegmentToBuffer(segment, start, end)
				log.Info("Extended Up", "start", start, "end", end)
			}
		}
	} else {
		// NewAnchor
		if err1 := hd.NewAnchor(segment, start, end, currentTime); err1 != nil {
			log.Error("NewAnchor failed", "error", err1)
		} else {
			hd.AddSegmentToBuffer(segment, start, end)
			log.Info("NewAnchor", "start", start, "end", end)
		}
	}
}

func grpcSentryClient(ctx context.Context, sentryAddr string) (proto_sentry.SentryClient, error) {
	log.Info("Starting Sentry client", "connecting to sentry", sentryAddr)
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

	conn, err := grpc.DialContext(ctx, sentryAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating client connection to sentry P2P: %w", err)
	}
	return proto_sentry.NewSentryClient(conn), nil
}

func grpcControlServer(ctx context.Context, coreAddr string, sentryClient proto_sentry.SentryClient, filesDir string, bufferSizeStr string, db ethdb.Database) (*ControlServerImpl, error) {
	log.Info("Starting Core P2P server", "on", coreAddr)

	listenConfig := net.ListenConfig{
		Control: func(network, address string, _ syscall.RawConn) error {
			log.Info("Core P2P received connection", "via", network, "from", address)
			return nil
		},
	}
	lis, err := listenConfig.Listen(ctx, "tcp", coreAddr)
	if err != nil {
		return nil, fmt.Errorf("could not create Core P2P listener: %w, addr=%s", err, coreAddr)
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

	var bufferSize datasize.ByteSize
	if err = bufferSize.UnmarshalText([]byte(bufferSizeStr)); err != nil {
		return nil, fmt.Errorf("parsing bufferSize %s: %w", bufferSizeStr, err)
	}
	var controlServer *ControlServerImpl

	if controlServer, err = NewControlServer(db, filesDir, int(bufferSize), sentryClient); err != nil {
		return nil, fmt.Errorf("create core P2P server: %w", err)
	}
	proto_core.RegisterControlServer(grpcServer, controlServer)
	if metrics.Enabled {
		grpc_prometheus.Register(grpcServer)
	}

	go func() {
		if err1 := grpcServer.Serve(lis); err1 != nil {
			log.Error("Core P2P server fail", "err", err1)
		}
	}()
	return controlServer, nil
}

// Download creates and starts standalone downloader
func Download(filesDir string, bufferSizeStr string, sentryAddr string, coreAddr string, db ethdb.Database, timeout int) error {
	ctx := rootContext()

	sentryClient, err1 := grpcSentryClient(ctx, sentryAddr)
	if err1 != nil {
		return err1
	}
	controlServer, err2 := grpcControlServer(ctx, coreAddr, sentryClient, filesDir, bufferSizeStr, db)
	if err2 != nil {
		return err2
	}
	go controlServer.headerLoop(ctx)
	//go controlServer.bodyLoop(ctx, db)

	if err := stages.StageLoop(ctx, db, controlServer.hd, controlServer.bd, controlServer.sendBodyRequest, func([]byte) {}, controlServer.requestWakeUpBodies, timeout); err != nil {
		log.Error("Stage loop failure", "error", err)
	}

	return nil
}

// Combined creates and starts sentry and downloader in the same process
func Combined(natSetting string, port int, staticPeers []string, discovery bool, netRestrict string, filesDir string, bufferSizeStr string, db ethdb.Database, timeout int) error {
	ctx := rootContext()

	coreClient := &ControlClientDirect{}
	sentryServer := &SentryServerImpl{}
	server, err1 := p2pServer(ctx, coreClient, sentryServer, natSetting, port, staticPeers, discovery, netRestrict)
	if err1 != nil {
		return err1
	}
	sentryClient := &SentryClientDirect{}
	sentryClient.SetServer(sentryServer)
	var bufferSize datasize.ByteSize
	if err := bufferSize.UnmarshalText([]byte(bufferSizeStr)); err != nil {
		return fmt.Errorf("parsing bufferSize %s: %w", bufferSizeStr, err)
	}
	controlServer, err2 := NewControlServer(db, filesDir, int(bufferSize), sentryClient)
	if err2 != nil {
		return fmt.Errorf("create core P2P server: %w", err2)
	}
	coreClient.SetServer(controlServer)
	if err := server.Start(); err != nil {
		return fmt.Errorf("could not start server: %w", err)
	}
	go controlServer.headerLoop(ctx)
	//go controlServer.bodyLoop(ctx, db)

	if err := stages.StageLoop(ctx, db, controlServer.hd, controlServer.bd, controlServer.sendBodyRequest, func([]byte) {}, controlServer.requestWakeUpBodies, timeout); err != nil {
		log.Error("Stage loop failure", "error", err)
	}
	return nil
}

type ControlServerImpl struct {
	proto_core.UnimplementedControlServer
	lock                 sync.Mutex
	hd                   *headerdownload.HeaderDownload
	bd                   *bodydownload.BodyDownload
	sentryClient         proto_sentry.SentryClient
	requestWakeUpHeaders chan struct{}
	requestWakeUpBodies  chan struct{}
	deliveredBodies      int
	undeliveredBodies    int
	unrequestedBodies    int
}

func NewControlServer(db ethdb.Database, filesDir string, bufferSize int, sentryClient proto_sentry.SentryClient) (*ControlServerImpl, error) {
	//config := eth.DefaultConfig.Ethash
	engine := ethash.New(ethash.Config{
		CachesInMem:      1,
		CachesLockMmap:   false,
		DatasetDir:       "ethash",
		DatasetsInMem:    1,
		DatasetsOnDisk:   0,
		DatasetsLockMmap: false,
	}, nil, false)
	cr := chainReader{config: params.MainnetChainConfig}
	calcDiffFunc := func(childTimestamp uint64, parentTime uint64, parentDifficulty, parentNumber *big.Int, parentHash, parentUncleHash common.Hash) *big.Int {
		return engine.CalcDifficulty(cr, childTimestamp, parentTime, parentDifficulty, parentNumber, parentHash, parentUncleHash)
	}
	verifySealFunc := func(header *types.Header) error {
		return engine.VerifySeal(cr, header)
	}
	hd := headerdownload.NewHeaderDownload(
		common.Hash{}, /* initialHash */
		filesDir,
		bufferSize, /* bufferLimit */
		16*1024,    /* tipLimit */
		1024,       /* initPowDepth */
		calcDiffFunc,
		verifySealFunc,
		3600, /* newAnchor future limit */
		3600, /* newAnchor past limit */
	)
	dbRecovered, err := hd.RecoverFromDb(db, uint64(time.Now().Unix()))
	if err != nil {
		log.Error("Recovery from DB failed", "error", err)
	}
	hardTips := headerdownload.InitHardCodedTips("hard-coded-headers.dat")
	filesRecovered, err1 := hd.RecoverFromFiles(uint64(time.Now().Unix()), hardTips)
	if err1 != nil {
		log.Error("Recovery from file failed, will start from scratch", "error", err1)
	}
	if !dbRecovered && !filesRecovered {
		hd.SetHardCodedTips(hardTips)
		// Insert hard-coded headers if present
		if _, err := os.Stat("hard-coded-headers.dat"); err == nil {
			if f, err1 := os.Open("hard-coded-headers.dat"); err1 == nil {
				var hBuffer [headerdownload.HeaderSerLength]byte
				i := 0
				for {
					var h types.Header
					if _, err2 := io.ReadFull(f, hBuffer[:]); err2 == nil {
						headerdownload.DeserialiseHeader(&h, hBuffer[:])
					} else if errors.Is(err2, io.EOF) {
						break
					} else {
						log.Error("Failed to read hard coded header", "i", i, "error", err2)
						break
					}
					if err2 := hd.HardCodedHeader(&h, uint64(time.Now().Unix())); err2 != nil {
						log.Error("Failed to insert hard coded header", "i", i, "block", h.Number.Uint64(), "error", err2)
					} else {
						hd.AddHeaderToBuffer(&h)
					}
					i++
				}
			}
		}
	}
	log.Info(hd.AnchorState())
	bd := bodydownload.NewBodyDownload(64 * 1024 /* outstandingLimit */)
	return &ControlServerImpl{hd: hd, bd: bd, sentryClient: sentryClient, requestWakeUpHeaders: make(chan struct{}), requestWakeUpBodies: make(chan struct{})}, nil
}

func (cs *ControlServerImpl) newBlockHashes(ctx context.Context, inreq *proto_core.InboundMessage) (*empty.Empty, error) {
	var request eth.NewBlockHashesData
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return nil, fmt.Errorf("decode NewBlockHashes: %v", err)
	}
	for _, announce := range request {
		if !cs.hd.HasTip(announce.Hash) {
			log.Info(fmt.Sprintf("Sending header request {hash: %x, height: %d, length: %d}", announce.Hash, announce.Number, 1))
			bytes, err := rlp.EncodeToBytes(&eth.GetBlockHeadersData{
				Amount:  1,
				Reverse: false,
				Skip:    0,
				Origin:  eth.HashOrNumber{Hash: announce.Hash},
			})
			if err != nil {
				return nil, fmt.Errorf("encode header request: %v", err)
			}
			outreq := proto_sentry.SendMessageByMinBlockRequest{
				MinBlock: announce.Number,
				Data: &proto_sentry.OutboundMessageData{
					Id:   proto_sentry.OutboundMessageId_GetBlockHeaders,
					Data: bytes,
				},
			}
			_, err = cs.sentryClient.SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
			if err != nil {
				return nil, fmt.Errorf("send header request: %v", err)
			}
		}
	}
	return &empty.Empty{}, nil
}

func (cs *ControlServerImpl) blockHeaders(ctx context.Context, inreq *proto_core.InboundMessage) (*empty.Empty, error) {
	var request []*types.Header
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return nil, fmt.Errorf("decode BlockHeaders: %v", err)
	}
	if segments, penalty, err := cs.hd.SplitIntoSegments(request); err == nil {
		if penalty == headerdownload.NoPenalty {
			for _, segment := range segments {
				processSegment(&cs.lock, cs.hd, segment)
			}
		} else {
			outreq := proto_sentry.PenalizePeerRequest{
				PeerId:  inreq.PeerId,
				Penalty: proto_sentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
			}
			if _, err1 := cs.sentryClient.PenalizePeer(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
				log.Error("Could not send penalty", "err", err1)
			}
		}
	} else {
		return nil, fmt.Errorf("singleHeaderAsSegment failed: %v", err)
	}
	log.Info("HeadersMsg processed")
	return &empty.Empty{}, nil
}

func (cs *ControlServerImpl) newBlock(ctx context.Context, inreq *proto_core.InboundMessage) (*empty.Empty, error) {
	var request eth.NewBlockData
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return nil, fmt.Errorf("decode NewBlockMsg: %v", err)
	}
	if segments, penalty, err := cs.hd.SingleHeaderAsSegment(request.Block.Header()); err == nil {
		if penalty == headerdownload.NoPenalty {
			processSegment(&cs.lock, cs.hd, segments[0]) // There is only one segment in this case
		} else {
			outreq := proto_sentry.PenalizePeerRequest{
				PeerId:  inreq.PeerId,
				Penalty: proto_sentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
			}
			if _, err1 := cs.sentryClient.PenalizePeer(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
				log.Error("Could not send penalty", "err", err1)
			}
		}
	} else {
		return nil, fmt.Errorf("singleHeaderAsSegment failed: %v", err)
	}
	log.Info(fmt.Sprintf("NewBlockMsg{blockNumber: %d}", request.Block.NumberU64()))
	return &empty.Empty{}, nil
}

func (cs *ControlServerImpl) blockBodies(inreq *proto_core.InboundMessage) (*empty.Empty, error) {
	var request eth.BlockBodiesData
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return nil, fmt.Errorf("decode BlockBodies: %v", err)
	}
	//var sb strings.Builder
	var undelivered int
	var delivered int
	for _, body := range request {
		if _, ok := cs.bd.DeliverBody(body); ok {
			//if sb.Len() > 0 {
			//	fmt.Fprintf(&sb, ",")
			//}
			//fmt.Fprintf(&sb, "%d", blockNum)
			delivered++
		} else {
			undelivered++
		}
	}
	//cs.bd.FeedDeliveries()
	cs.deliveredBodies += delivered
	cs.undeliveredBodies += undelivered
	if undelivered > 0 {
		log.Info(fmt.Sprintf("BlockBodies{delivered=%d, undelivered=%d, totalDelivered=%d, totalUndelivered=%d}", delivered, undelivered, cs.deliveredBodies, cs.undeliveredBodies))
	}
	return &empty.Empty{}, nil
}

func (cs *ControlServerImpl) ForwardInboundMessage(ctx context.Context, inreq *proto_core.InboundMessage) (*empty.Empty, error) {
	defer func() {
		select {
		case cs.requestWakeUpHeaders <- struct{}{}:
		default:
		}
		select {
		case cs.requestWakeUpBodies <- struct{}{}:
		default:
		}
	}()
	switch inreq.Id {
	case proto_core.InboundMessageId_NewBlockHashes:
		return cs.newBlockHashes(ctx, inreq)
	case proto_core.InboundMessageId_BlockHeaders:
		return cs.blockHeaders(ctx, inreq)
	case proto_core.InboundMessageId_NewBlock:
		return cs.newBlock(ctx, inreq)
	case proto_core.InboundMessageId_BlockBodies:
		return cs.blockBodies(inreq)
	default:
		return nil, fmt.Errorf("not implemented for message Id: %s", inreq.Id)
	}
}

func (cs *ControlServerImpl) GetStatus(context.Context, *empty.Empty) (*proto_core.StatusData, error) {
	return nil, nil
}

func (cs *ControlServerImpl) sendRequests(ctx context.Context, reqs []*headerdownload.HeaderRequest) {
	for _, req := range reqs {
		log.Debug(fmt.Sprintf("Sending header request {hash: %x, height: %d, length: %d}", req.Hash, req.Number, req.Length))
		bytes, err := rlp.EncodeToBytes(&eth.GetBlockHeadersData{
			Amount:  uint64(req.Length),
			Reverse: true,
			Skip:    0,
			Origin:  eth.HashOrNumber{Hash: req.Hash},
		})
		if err != nil {
			log.Error("Could not encode header request", "err", err)
			continue
		}
		outreq := proto_sentry.SendMessageByMinBlockRequest{
			MinBlock: req.Number,
			Data: &proto_sentry.OutboundMessageData{
				Id:   proto_sentry.OutboundMessageId_GetBlockHeaders,
				Data: bytes,
			},
		}
		_, err = cs.sentryClient.SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
		if err != nil {
			log.Error("Could not send header request", "err", err)
			continue
		}
	}
}

func (cs *ControlServerImpl) sendBodyRequest(ctx context.Context, req *bodydownload.BodyRequest) []byte {
	//log.Info(fmt.Sprintf("Sending body request for %v", req.BlockNums))
	var bytes []byte
	var err error
	bytes, err = rlp.EncodeToBytes(req.Hashes)
	if err != nil {
		log.Error("Could not encode block bodies request", "err", err)
		return nil
	}
	outreq := proto_sentry.SendMessageByMinBlockRequest{
		MinBlock: req.BlockNums[len(req.BlockNums)-1],
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.OutboundMessageId_GetBlockBodies,
			Data: bytes,
		},
	}
	sentPeers, err1 := cs.sentryClient.SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
	if err1 != nil {
		log.Error("Could not send block bodies request", "err", err1)
		return nil
	}
	if sentPeers == nil || len(sentPeers.Peers) == 0 {
		return nil
	}
	return sentPeers.Peers[0]
}

func (cs *ControlServerImpl) headerLoop(ctx context.Context) {
	for {
		reqs, timer := cs.hd.RequestMoreHeaders(uint64(time.Now().Unix()), 5 /*timeout */)
		cs.sendRequests(ctx, reqs)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			//log.Info("RequestQueueTimer (headers) ticked")
		case <-cs.requestWakeUpHeaders:
			//log.Info("headerLoop woken up by the incoming request")
		}
	}
}
