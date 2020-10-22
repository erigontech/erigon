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
	"github.com/ledgerwatch/turbo-geth/cmd/headers/proto"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
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

func processSegment(hd *headerdownload.HeaderDownload, segment *headerdownload.ChainSegment) {
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
	if start == 0 || end > 0 {
		hd.CheckInitiation(segment, params.MainnetGenesisHash)
	}
}

func Download(filesDir string, bufferSize int, sentryAddr string, coreAddr string) error {
	ctx := rootContext()
	log.Info("Starting Core P2P server", "on", coreAddr, "connecting to sentry", coreAddr)

	listenConfig := net.ListenConfig{
		Control: func(network, address string, _ syscall.RawConn) error {
			log.Info("Core P2P received connection", "via", network, "from", address)
			return nil
		},
	}
	lis, err := listenConfig.Listen(ctx, "tcp", coreAddr)
	if err != nil {
		return fmt.Errorf("could not create Core P2P listener: %w, addr=%s", err, coreAddr)
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
		return fmt.Errorf("creating client connection to sentry P2P: %w", err)
	}
	sentryClient := proto.NewSentryClient(conn)

	var controlServer *ControlServerImpl
	if controlServer, err = NewControlServer(filesDir, bufferSize, sentryClient); err != nil {
		return fmt.Errorf("create core P2P server: %w", err)
	}
	proto.RegisterControlServer(grpcServer, controlServer)
	if metrics.Enabled {
		grpc_prometheus.Register(grpcServer)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Error("Core P2P server fail", "err", err)
		}
	}()

	go controlServer.loop(ctx)

	<-ctx.Done()
	return nil
}

type ControlServerImpl struct {
	proto.UnimplementedControlServer
	hdLock       sync.Mutex
	hd           *headerdownload.HeaderDownload
	sentryClient proto.SentryClient
}

func NewControlServer(filesDir string, bufferLimit int, sentryClient proto.SentryClient) (*ControlServerImpl, error) {
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
		filesDir,
		bufferLimit, /* bufferLimit */
		16*1024,     /* tipLimit */
		1024,        /* initPowDepth */
		calcDiffFunc,
		verifySealFunc,
		3600, /* newAnchor future limit */
		3600, /* newAnchor past limit */
	)
	hd.InitHardCodedTips("hard-coded-headers.dat")
	if recovered, err := hd.RecoverFromFiles(uint64(time.Now().Unix())); err != nil || !recovered {
		if err != nil {
			log.Error("Recovery from file failed, will start from scratch", "error", err)
		}
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
	return &ControlServerImpl{hd: hd, sentryClient: sentryClient}, nil
}

func (cs *ControlServerImpl) newBlockHashes(ctx context.Context, inreq *proto.InboundMessage) (*empty.Empty, error) {
	cs.hdLock.Lock()
	defer cs.hdLock.Unlock()
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
			outreq := proto.SendMessageByMinBlockRequest{
				MinBlock: announce.Number,
				Data: &proto.OutboundMessageData{
					Id:   proto.OutboundMessageId_GetBlockHeaders,
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

func (cs *ControlServerImpl) blockHeaders(inreq *proto.InboundMessage) (*empty.Empty, error) {
	cs.hdLock.Lock()
	defer cs.hdLock.Unlock()
	var request []*types.Header
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return nil, fmt.Errorf("decode BlockHeaders: %v", err)
	}
	if segments, penalty, err := cs.hd.SplitIntoSegments(request); err == nil {
		if penalty == headerdownload.NoPenalty {
			for _, segment := range segments {
				processSegment(cs.hd, segment)
			}
		} else {
			//penaltyCh <- PenaltyMsg{SentryMsg: headersReq.SentryMsg, penalty: penalty}
		}
	} else {
		return nil, fmt.Errorf("SingleHeaderAsSegment failed: %v", err)
	}
	log.Info("HeadersMsg processed")
	return &empty.Empty{}, nil
}

func (cs *ControlServerImpl) newBlock(inreq *proto.InboundMessage) (*empty.Empty, error) {
	cs.hdLock.Lock()
	defer cs.hdLock.Unlock()
	var request eth.NewBlockData
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return nil, fmt.Errorf("decode NewBlockMsg: %v", err)
	}
	if segments, penalty, err := cs.hd.SingleHeaderAsSegment(request.Block.Header()); err == nil {
		if penalty == headerdownload.NoPenalty {
			processSegment(cs.hd, segments[0]) // There is only one segment in this case
		} else {
			// Send penalty back to the sentry
			//penaltyCh <- PenaltyMsg{SentryMsg: newBlockReq.SentryMsg, penalty: penalty}
		}
	} else {
		return nil, fmt.Errorf("singleHeaderAsSegment failed: %v", err)
	}
	log.Info(fmt.Sprintf("NewBlockMsg{blockNumber: %d}", request.Block.NumberU64()))
	return &empty.Empty{}, nil
}

func (cs *ControlServerImpl) ForwardInboundMessage(ctx context.Context, inreq *proto.InboundMessage) (*empty.Empty, error) {
	switch inreq.Id {
	case proto.InboundMessageId_NewBlockHashes:
		return cs.newBlockHashes(ctx, inreq)
	case proto.InboundMessageId_BlockHeaders:
		return cs.blockHeaders(inreq)
	case proto.InboundMessageId_NewBlock:
		return cs.newBlock(inreq)
	default:
		return nil, fmt.Errorf("not implemented for message Id: %s", inreq.Id)
	}
}

func (cs *ControlServerImpl) GetStatus(context.Context, *empty.Empty) (*proto.StatusData, error) {
	return nil, nil
}

func (cs *ControlServerImpl) loop(ctx context.Context) {
	var timer *time.Timer
	cs.hdLock.Lock()
	timer = cs.hd.RequestQueueTimer
	cs.hdLock.Unlock()
	for {
		select {
		case <-timer.C:
			fmt.Printf("RequestQueueTimer ticked\n")
		case <-ctx.Done():
			return
		}
		cs.hdLock.Lock()
		reqs := cs.hd.RequestMoreHeaders(uint64(time.Now().Unix()), 5 /*timeout */)
		timer = cs.hd.RequestQueueTimer
		cs.hdLock.Unlock()
		for _, req := range reqs {
			//log.Info(fmt.Sprintf("Sending header request {hash: %x, height: %d, length: %d}", req.Hash, req.Number, req.Length))
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
			outreq := proto.SendMessageByMinBlockRequest{
				MinBlock: req.Number,
				Data: &proto.OutboundMessageData{
					Id:   proto.OutboundMessageId_GetBlockHeaders,
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
}
