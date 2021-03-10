package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/golang/protobuf/ptypes/empty"
	proto_sentry "github.com/ledgerwatch/turbo-geth/cmd/headers/sentry"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth"
	"github.com/ledgerwatch/turbo-geth/eth/downloader"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/stages"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

const (
	softResponseLimit = 2 * 1024 * 1024 // Target maximum size of returned blocks, headers or node data.
	estHeaderRlpSize  = 500             // Approximate size of an RLP encoded block header
)

type chainReader struct {
	config *params.ChainConfig
}

func (cr chainReader) Config() *params.ChainConfig                             { return cr.config }
func (cr chainReader) CurrentHeader() *types.Header                            { panic("") }
func (cr chainReader) GetHeader(hash common.Hash, number uint64) *types.Header { panic("") }
func (cr chainReader) GetHeaderByNumber(number uint64) *types.Header           { panic("") }
func (cr chainReader) GetHeaderByHash(hash common.Hash) *types.Header          { panic("") }

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

// Download creates and starts standalone downloader
func Download(sentryAddr string, coreAddr string, db ethdb.Database, timeout, window int) error {
	ctx := rootContext()

	sentryClient, err := grpcSentryClient(ctx, sentryAddr)
	if err != nil {
		return err
	}

	controlServer, err1 := NewControlServer(db, sentryClient, window)
	if err1 != nil {
		return fmt.Errorf("create core P2P server: %w", err1)
	}

	// TODO: Make a reconnection loop
	statusMsg := &proto_sentry.StatusData{
		NetworkId:       controlServer.networkId,
		TotalDifficulty: controlServer.headTd.Bytes(),
		BestHash:        controlServer.headHash.Bytes(),
		ForkData: &proto_sentry.Forks{
			Genesis: controlServer.genesisHash.Bytes(),
			Forks:   controlServer.forks,
		},
	}
	callCtx, cancelFn := context.WithCancel(ctx)
	if _, err = sentryClient.SetStatus(callCtx, statusMsg, &grpc.EmptyCallOption{}); err != nil {
		cancelFn()
		return fmt.Errorf("setting initial status message: %w", err)
	}
	cancelFn()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			callCtx, cancelFn = context.WithCancel(ctx)
			receiveClient, err2 := sentryClient.ReceiveMessages(callCtx, &empty.Empty{}, &grpc.EmptyCallOption{})
			if err2 != nil {
				log.Error("Receive messages failed", "error", err2)
			} else {
				inreq, err := receiveClient.Recv()
				for ; err == nil; inreq, err = receiveClient.Recv() {
					if err1 := controlServer.handleInboundMessage(ctx, inreq); err1 != nil {
						log.Error("Handling incoming message", "error", err1)
					}
				}
				if err != nil && !errors.Is(err, io.EOF) {
					log.Error("Receive loop terminated", "error", err)
				}
			}
			cancelFn()
		}
	}()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			callCtx, cancelFn = context.WithCancel(ctx)
			receiveUploadClient, err3 := sentryClient.ReceiveUploadMessages(callCtx, &empty.Empty{}, &grpc.EmptyCallOption{})
			if err3 != nil {
				log.Error("Receive upload messages failed", "error", err3)
			} else {
				inreq, err := receiveUploadClient.Recv()
				for ; err == nil; inreq, err = receiveUploadClient.Recv() {
					if err1 := controlServer.handleInboundMessage(ctx, inreq); err1 != nil {
						log.Error("Handling incoming message", "error", err1)
					}
				}
				if err != nil && !errors.Is(err, io.EOF) {
					log.Error("Receive upload loop terminated", "error", err)
				}
			}
			cancelFn()
		}
	}()

	if err := stages.StageLoop(
		ctx,
		db,
		controlServer.hd,
		controlServer.bd,
		controlServer.sendHeaderRequest,
		controlServer.sendBodyRequest,
		controlServer.penalise,
		controlServer.updateHead,
		controlServer.requestWakeUpBodies,
		timeout,
	); err != nil {
		log.Error("Stage loop failure", "error", err)
	}

	return nil
}

// Combined creates and starts sentry and downloader in the same process
func Combined(natSetting string, port int, staticPeers []string, discovery bool, netRestrict string, db ethdb.Database, timeout, window int) error {
	ctx := rootContext()

	sentryServer := &SentryServerImpl{
		receiveCh:       make(chan StreamMsg, 1024),
		receiveUploadCh: make(chan StreamMsg, 1024),
	}
	var err error
	sentryServer.p2pServer, err = p2pServer(ctx, sentryServer, natSetting, port, staticPeers, discovery, netRestrict)
	if err != nil {
		return err
	}
	sentryClient := &SentryClientDirect{}
	sentryClient.SetServer(sentryServer)
	controlServer, err2 := NewControlServer(db, sentryClient, window)
	if err2 != nil {
		return fmt.Errorf("create core P2P server: %w", err2)
	}
	statusMsg := &proto_sentry.StatusData{
		NetworkId:       controlServer.networkId,
		TotalDifficulty: controlServer.headTd.Bytes(),
		BestHash:        controlServer.headHash.Bytes(),
		ForkData: &proto_sentry.Forks{
			Genesis: controlServer.genesisHash.Bytes(),
			Forks:   controlServer.forks,
		},
	}
	//nolint:govet
	callCtx, _ := context.WithCancel(ctx)
	if _, err = sentryClient.SetStatus(callCtx, statusMsg, &grpc.EmptyCallOption{}); err != nil {
		return fmt.Errorf("setting initial status message: %w", err)
	}
	//nolint:govet
	callCtx, _ = context.WithCancel(ctx)
	receiveClient, err2 := sentryClient.ReceiveMessages(callCtx, &empty.Empty{}, &grpc.EmptyCallOption{})
	if err2 != nil {
		return fmt.Errorf("receive messages failed: %w", err2)
	}
	go func() {
		inreq, err := receiveClient.Recv()
		for ; err == nil; inreq, err = receiveClient.Recv() {
			if err1 := controlServer.handleInboundMessage(ctx, inreq); err1 != nil {
				log.Error("Handling incoming message", "error", err1)
			}
		}
		if err != nil && !errors.Is(err, io.EOF) {
			log.Error("Receive loop terminated", "error", err)
		}
	}()
	//nolint:govet
	callCtx, _ = context.WithCancel(ctx)
	receiveUploadClient, err3 := sentryClient.ReceiveUploadMessages(callCtx, &empty.Empty{}, &grpc.EmptyCallOption{})
	if err3 != nil {
		return fmt.Errorf("receive upload messages failed: %w", err3)
	}
	go func() {
		inreq, err := receiveUploadClient.Recv()
		for ; err == nil; inreq, err = receiveUploadClient.Recv() {
			if err1 := controlServer.handleInboundMessage(ctx, inreq); err1 != nil {
				log.Error("Handling incoming message", "error", err1)
			}
		}
		if err != nil && !errors.Is(err, io.EOF) {
			log.Error("Receive loop terminated", "error", err)
		}
	}()

	if err := stages.StageLoop(
		ctx,
		db,
		controlServer.hd,
		controlServer.bd,
		controlServer.sendHeaderRequest,
		controlServer.sendBodyRequest,
		controlServer.penalise,
		controlServer.updateHead,
		controlServer.requestWakeUpBodies,
		timeout,
	); err != nil {
		log.Error("Stage loop failure", "error", err)
	}
	return nil
}

type ControlServerImpl struct {
	lock                 sync.RWMutex
	hd                   *headerdownload.HeaderDownload
	bd                   *bodydownload.BodyDownload
	sentryClient         proto_sentry.SentryClient
	requestWakeUpHeaders chan struct{}
	requestWakeUpBodies  chan struct{}
	headHeight           uint64
	headHash             common.Hash
	headTd               *big.Int
	chainConfig          *params.ChainConfig
	forks                []uint64
	genesisHash          common.Hash
	protocolVersion      uint32
	networkId            uint64
	db                   ethdb.Database
}

func NewControlServer(db ethdb.Database, sentryClient proto_sentry.SentryClient, window int) (*ControlServerImpl, error) {
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
	if _, _, _, err := core.SetupGenesisBlock(db, core.DefaultGenesisBlock(), false /* history */, false /* overwrite */); err != nil {
		return nil, fmt.Errorf("setup genesis block: %w", err)
	}
	hd := headerdownload.NewHeaderDownload(
		512,       /* anchorLimit */
		1024*1024, /* tipLimit */
		calcDiffFunc,
		verifySealFunc,
	)
	if err := hd.RecoverFromDb(db); err != nil {
		return nil, fmt.Errorf("recovery from DB failed: %w", err)
	}
	hardTips := headerdownload.InitHardCodedTips("mainnet")

	hd.SetHardCodedTips(hardTips)
	bd := bodydownload.NewBodyDownload(window /* outstandingLimit */)
	cs := &ControlServerImpl{hd: hd, bd: bd, sentryClient: sentryClient, requestWakeUpHeaders: make(chan struct{}, 1), requestWakeUpBodies: make(chan struct{}, 1), db: db}
	cs.chainConfig = params.MainnetChainConfig // Hard-coded, needs to be parametrized
	cs.forks = forkid.GatherForks(cs.chainConfig)
	cs.genesisHash = params.MainnetGenesisHash // Hard-coded, needs to be parametrized
	cs.protocolVersion = uint32(eth.ProtocolVersions[0])
	cs.networkId = eth.DefaultConfig.NetworkID // Hard-coded, needs to be parametrized
	var err error
	cs.headHeight, cs.headHash, cs.headTd, err = bd.UpdateFromDb(db)
	return cs, err
}

func (cs *ControlServerImpl) updateHead(ctx context.Context, height uint64, hash common.Hash, td *big.Int) {
	cs.lock.Lock()
	defer cs.lock.Unlock()
	cs.headHeight = height
	cs.headHash = hash
	cs.headTd = td
	statusMsg := &proto_sentry.StatusData{
		NetworkId:       cs.networkId,
		TotalDifficulty: cs.headTd.Bytes(),
		BestHash:        cs.headHash.Bytes(),
		ForkData: &proto_sentry.Forks{
			Genesis: cs.genesisHash.Bytes(),
			Forks:   cs.forks,
		},
	}
	//nolint:govet
	callCtx, _ := context.WithCancel(ctx)
	if _, err := cs.sentryClient.SetStatus(callCtx, statusMsg, &grpc.EmptyCallOption{}); err != nil {
		log.Error("Update status message for the sentry", "error", err)
	}
}

func (cs *ControlServerImpl) newBlockHashes(ctx context.Context, inreq *proto_sentry.InboundMessage) error {
	var request eth.NewBlockHashesData
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return fmt.Errorf("decode NewBlockHashes: %v", err)
	}
	for _, announce := range request {
		if !cs.hd.HasTip(announce.Hash) {
			//log.Info(fmt.Sprintf("Sending header request {hash: %x, height: %d, length: %d}", announce.Hash, announce.Number, 1))
			b, err := rlp.EncodeToBytes(&eth.GetBlockHeadersData{
				Amount:  1,
				Reverse: false,
				Skip:    0,
				Origin:  eth.HashOrNumber{Hash: announce.Hash},
			})
			if err != nil {
				return fmt.Errorf("encode header request: %v", err)
			}
			outreq := proto_sentry.SendMessageByIdRequest{
				PeerId: inreq.PeerId,
				Data: &proto_sentry.OutboundMessageData{
					Id:   proto_sentry.MessageId_GetBlockHeaders,
					Data: b,
				},
			}
			//nolint:govet
			callCtx, _ := context.WithCancel(ctx)
			_, err = cs.sentryClient.SendMessageById(callCtx, &outreq, &grpc.EmptyCallOption{})
			if err != nil {
				return fmt.Errorf("send header request: %v", err)
			}
		}
	}
	return nil
}

func (cs *ControlServerImpl) blockHeaders(ctx context.Context, inreq *proto_sentry.InboundMessage) error {
	rlpStream := rlp.NewStream(bytes.NewReader(inreq.Data), uint64(len(inreq.Data)))
	_, err := rlpStream.List()
	if err != nil {
		return fmt.Errorf("decode BlockHeaders: %w", err)
	}
	var headersRaw [][]byte
	var headerRaw []byte
	for headerRaw, err = rlpStream.Raw(); err == nil; headerRaw, err = rlpStream.Raw() {
		headersRaw = append(headersRaw, headerRaw)
	}
	if err != nil && !errors.Is(err, rlp.EOL) {
		return fmt.Errorf("decode BlockHeaders: %w", err)
	}
	headers := make([]*types.Header, len(headersRaw))
	var heighestBlock uint64
	var i int
	for i, headerRaw = range headersRaw {
		var h types.Header
		if err = rlp.DecodeBytes(headerRaw, &h); err != nil {
			return fmt.Errorf("decode BlockHeaders: %w", err)
		}
		headers[i] = &h
		if h.Number.Uint64() > heighestBlock {
			heighestBlock = h.Number.Uint64()
		}
	}
	if segments, penalty, err := cs.hd.SplitIntoSegments(headersRaw, headers); err == nil {
		if penalty == headerdownload.NoPenalty {
			for _, segment := range segments {
				cs.hd.ProcessSegment(segment)
			}
		} else {
			outreq := proto_sentry.PenalizePeerRequest{
				PeerId:  inreq.PeerId,
				Penalty: proto_sentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
			}
			//nolint:govet
			callCtx, _ := context.WithCancel(ctx)
			if _, err1 := cs.sentryClient.PenalizePeer(callCtx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
				log.Error("Could not send penalty", "err", err1)
			}
		}
	} else {
		return fmt.Errorf("singleHeaderAsSegment failed: %v", err)
	}
	outreq := proto_sentry.PeerMinBlockRequest{
		PeerId:   inreq.PeerId,
		MinBlock: heighestBlock,
	}
	//nolint:govet
	callCtx, _ := context.WithCancel(ctx)
	if _, err1 := cs.sentryClient.PeerMinBlock(callCtx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
		log.Error("Could not send min block for peer", "err", err1)
	}
	//log.Info("HeadersMsg processed")
	return nil
}

func (cs *ControlServerImpl) newBlock(ctx context.Context, inreq *proto_sentry.InboundMessage) error {
	// Extract header from the block
	rlpStream := rlp.NewStream(bytes.NewReader(inreq.Data), uint64(len(inreq.Data)))
	_, err := rlpStream.List() // Now stream is at the beginning of the block record
	if err != nil {
		return fmt.Errorf("decode NewBlockMsg: %w", err)
	}
	_, err = rlpStream.List() // Now stream is at the beginning of the header
	if err != nil {
		return fmt.Errorf("decode NewBlockMsg: %w", err)
	}
	var headerRaw []byte
	if headerRaw, err = rlpStream.Raw(); err != nil {
		return fmt.Errorf("decode NewBlockMsg: %w", err)
	}
	// Parse the entire request from scratch
	var request eth.NewBlockData
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return fmt.Errorf("decode NewBlockMsg: %v", err)
	}
	if segments, penalty, err := cs.hd.SingleHeaderAsSegment(headerRaw, request.Block.Header()); err == nil {
		if penalty == headerdownload.NoPenalty {
			cs.hd.ProcessSegment(segments[0]) // There is only one segment in this case
		} else {
			outreq := proto_sentry.PenalizePeerRequest{
				PeerId:  inreq.PeerId,
				Penalty: proto_sentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
			}
			//nolint:govet
			callCtx, _ := context.WithCancel(ctx)
			if _, err1 := cs.sentryClient.PenalizePeer(callCtx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
				log.Error("Could not send penalty", "err", err1)
			}
		}
	} else {
		return fmt.Errorf("singleHeaderAsSegment failed: %v", err)
	}
	outreq := proto_sentry.PeerMinBlockRequest{
		PeerId:   inreq.PeerId,
		MinBlock: request.Block.NumberU64(),
	}
	//nolint:govet
	callCtx, _ := context.WithCancel(ctx)
	if _, err1 := cs.sentryClient.PeerMinBlock(callCtx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
		log.Error("Could not send min block for peer", "err", err1)
	}
	log.Info(fmt.Sprintf("NewBlockMsg{blockNumber: %d} from [%s]", request.Block.NumberU64(), inreq.PeerId))
	return nil
}

func (cs *ControlServerImpl) blockBodies(inreq *proto_sentry.InboundMessage) error {
	var request []*types.Body
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return fmt.Errorf("decode BlockBodies: %v", err)
	}
	delivered, undelivered := cs.bd.DeliverBodies(request)
	// Approximate numbers
	cs.bd.DeliverySize(float64(len(inreq.Data))*float64(delivered)/float64(delivered+undelivered), float64(len(inreq.Data))*float64(undelivered)/float64(delivered+undelivered))
	return nil
}

func getAncestor(db ethdb.Database, hash common.Hash, number, ancestor uint64, maxNonCanonical *uint64) (common.Hash, uint64) {
	if ancestor > number {
		return common.Hash{}, 0
	}
	if ancestor == 1 {
		// in this case it is cheaper to just read the header
		if header := rawdb.ReadHeader(db, hash, number); header != nil {
			return header.ParentHash, number - 1
		}
		return common.Hash{}, 0
	}
	for ancestor != 0 {
		h, err := rawdb.ReadCanonicalHash(db, number)
		if err != nil {
			panic(err)
		}
		if h == hash {
			ancestorHash, err := rawdb.ReadCanonicalHash(db, number-ancestor)
			if err != nil {
				panic(err)
			}
			h, err := rawdb.ReadCanonicalHash(db, number)
			if err != nil {
				panic(err)
			}
			if h == hash {
				number -= ancestor
				return ancestorHash, number
			}
		}
		if *maxNonCanonical == 0 {
			return common.Hash{}, 0
		}
		*maxNonCanonical--
		ancestor--
		header := rawdb.ReadHeader(db, hash, number)
		if header == nil {
			return common.Hash{}, 0
		}
		hash = header.ParentHash
		number--
	}
	return hash, number
}

func queryHeaders(db ethdb.Database, query *eth.GetBlockHeadersData) ([]*types.Header, error) {
	hashMode := query.Origin.Hash != (common.Hash{})
	first := true
	maxNonCanonical := uint64(100)
	// Gather headers until the fetch or network limits is reached
	var (
		bytes   common.StorageSize
		headers []*types.Header
		unknown bool
	)
	for !unknown && len(headers) < int(query.Amount) && bytes < softResponseLimit && len(headers) < downloader.MaxHeaderFetch {
		// Retrieve the next header satisfying the query
		var origin *types.Header
		var err error
		if hashMode {
			if first {
				first = false
				if origin, err = rawdb.ReadHeaderByHash(db, query.Origin.Hash); err != nil {
					return nil, err
				}
				if origin != nil {
					query.Origin.Number = origin.Number.Uint64()
				}
			} else {
				origin = rawdb.ReadHeader(db, query.Origin.Hash, query.Origin.Number)
			}
		} else {
			origin = rawdb.ReadHeaderByNumber(db, query.Origin.Number)
		}
		if origin == nil {
			break
		}
		headers = append(headers, origin)
		bytes += estHeaderRlpSize

		// Advance to the next header of the query
		switch {
		case hashMode && query.Reverse:
			// Hash based traversal towards the genesis block
			ancestor := query.Skip + 1
			if ancestor == 0 {
				unknown = true
			} else {
				query.Origin.Hash, query.Origin.Number = getAncestor(db, query.Origin.Hash, query.Origin.Number, ancestor, &maxNonCanonical)
				unknown = (query.Origin.Hash == common.Hash{})
			}
		case hashMode && !query.Reverse:
			// Hash based traversal towards the leaf block
			var (
				current = origin.Number.Uint64()
				next    = current + query.Skip + 1
			)
			if next <= current {
				log.Warn("GetBlockHeaders skip overflow attack", "current", current, "skip", query.Skip, "next", next)
				unknown = true
			} else {
				if header := rawdb.ReadHeaderByNumber(db, next); header != nil {
					nextHash := header.Hash()
					expOldHash, _ := getAncestor(db, nextHash, next, query.Skip+1, &maxNonCanonical)
					if expOldHash == query.Origin.Hash {
						query.Origin.Hash, query.Origin.Number = nextHash, next
					} else {
						unknown = true
					}
				} else {
					unknown = true
				}
			}
		case query.Reverse:
			// Number based traversal towards the genesis block
			if query.Origin.Number >= query.Skip+1 {
				query.Origin.Number -= query.Skip + 1
			} else {
				unknown = true
			}

		case !query.Reverse:
			// Number based traversal towards the leaf block
			query.Origin.Number += query.Skip + 1
		}
	}
	return headers, nil
}

func (cs *ControlServerImpl) getBlockHeaders(ctx context.Context, inreq *proto_sentry.InboundMessage) error {
	var query eth.GetBlockHeadersData
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding GetBlockHeader: %v", err)
	}
	headers, err1 := queryHeaders(cs.db, &query)
	if err1 != nil {
		return fmt.Errorf("querying BlockHeaders: %w", err1)
	}
	b, err := rlp.EncodeToBytes(headers)
	if err != nil {
		return fmt.Errorf("encode header response: %v", err)
	}
	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: inreq.PeerId,
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_BlockHeaders,
			Data: b,
		},
	}
	//nolint:govet
	callCtx, _ := context.WithCancel(ctx)
	_, err = cs.sentryClient.SendMessageById(callCtx, &outreq, &grpc.EmptyCallOption{})
	if err != nil {
		return fmt.Errorf("send header response: %v", err)
	}
	//log.Info(fmt.Sprintf("[%s] GetBlockHeaderMsg{hash=%x, number=%d, amount=%d, skip=%d, reverse=%t, responseLen=%d}", inreq.PeerId, query.Origin.Hash, query.Origin.Number, query.Amount, query.Skip, query.Reverse, len(b)))
	return nil
}

func (cs *ControlServerImpl) getBlockBodies(ctx context.Context, inreq *proto_sentry.InboundMessage) error {
	// Decode the retrieval message
	msgStream := rlp.NewStream(bytes.NewReader(inreq.Data), uint64(len(inreq.Data)))
	if _, err := msgStream.List(); err != nil {
		return fmt.Errorf("getting list from RLP stream for GetBlockBodiesMsg: %v", err)
	}
	// Gather blocks until the fetch or network limits is reached
	var hash common.Hash
	var bytes int
	var bodies []rlp.RawValue
	var hashesStr strings.Builder
	for bytes < softResponseLimit && len(bodies) < downloader.MaxBlockFetch {
		// Retrieve the hash of the next block
		if err := msgStream.Decode(&hash); errors.Is(err, rlp.EOL) {
			break
		} else if err != nil {
			return errResp(eth.ErrDecode, "decode hash for GetBlockBodiesMsg: %v", err)
		}
		if hashesStr.Len() > 0 {
			hashesStr.WriteString(",")
		}
		hashesStr.WriteString(fmt.Sprintf("%x-%x", hash[:4], hash[28:]))
		// Retrieve the requested block body, stopping if enough was found
		number := rawdb.ReadHeaderNumber(cs.db, hash)
		if number == nil {
			continue
		}
		hashesStr.WriteString(fmt.Sprintf("(%d)", *number))
		data := rawdb.ReadBodyRLP(cs.db, hash, *number)
		if len(data) == 0 {
			continue
		}
		bodies = append(bodies, data)
		bytes += len(data)
	}
	b, err := rlp.EncodeToBytes(bodies)
	if err != nil {
		return fmt.Errorf("encode header response: %v", err)
	}
	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: inreq.PeerId,
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_BlockBodies,
			Data: b,
		},
	}
	//nolint:govet
	callCtx, _ := context.WithCancel(ctx)
	_, err = cs.sentryClient.SendMessageById(callCtx, &outreq, &grpc.EmptyCallOption{})
	if err != nil {
		return fmt.Errorf("send bodies response: %v", err)
	}
	//log.Info(fmt.Sprintf("[%s] GetBlockBodiesMsg {%s}, responseLen %d", inreq.PeerId, hashesStr.String(), len(bodies)))
	return nil
}

func (cs *ControlServerImpl) handleInboundMessage(ctx context.Context, inreq *proto_sentry.InboundMessage) error {
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
	case proto_sentry.MessageId_NewBlockHashes:
		return cs.newBlockHashes(ctx, inreq)
	case proto_sentry.MessageId_BlockHeaders:
		return cs.blockHeaders(ctx, inreq)
	case proto_sentry.MessageId_NewBlock:
		return cs.newBlock(ctx, inreq)
	case proto_sentry.MessageId_BlockBodies:
		return cs.blockBodies(inreq)
	case proto_sentry.MessageId_GetBlockHeaders:
		return cs.getBlockHeaders(ctx, inreq)
	case proto_sentry.MessageId_GetBlockBodies:
		return cs.getBlockBodies(ctx, inreq)
	default:
		return fmt.Errorf("not implemented for message Id: %s", inreq.Id)
	}
}

func (cs *ControlServerImpl) sendHeaderRequest(ctx context.Context, req *headerdownload.HeaderRequest) []byte {
	//log.Info(fmt.Sprintf("Sending header request {hash: %x, height: %d, length: %d}", req.Hash, req.Number, req.Length))
	reqData := &eth.GetBlockHeadersData{
		Amount:  req.Length,
		Reverse: req.Reverse,
		Skip:    req.Skip,
		Origin:  eth.HashOrNumber{Hash: req.Hash},
	}
	if req.Hash == (common.Hash{}) {
		reqData.Origin.Number = req.Number
	}
	bytes, err := rlp.EncodeToBytes(reqData)
	if err != nil {
		log.Error("Could not encode header request", "err", err)
		return nil
	}
	minBlock := req.Number
	if !req.Reverse {
		minBlock = req.Number + req.Length*req.Skip
	}
	outreq := proto_sentry.SendMessageByMinBlockRequest{
		MinBlock: minBlock,
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_GetBlockHeaders,
			Data: bytes,
		},
	}
	//nolint:govet
	callCtx, _ := context.WithCancel(ctx)
	sentPeers, err1 := cs.sentryClient.SendMessageByMinBlock(callCtx, &outreq, &grpc.EmptyCallOption{})
	if err1 != nil {
		log.Error("Could not send header request", "err", err1)
		return nil
	}
	if sentPeers == nil || len(sentPeers.Peers) == 0 {
		return nil
	}
	return sentPeers.Peers[0]
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
			Id:   proto_sentry.MessageId_GetBlockBodies,
			Data: bytes,
		},
	}
	//nolint:govet
	callCtx, _ := context.WithCancel(ctx)
	sentPeers, err1 := cs.sentryClient.SendMessageByMinBlock(callCtx, &outreq, &grpc.EmptyCallOption{})
	if err1 != nil {
		log.Error("Could not send block bodies request", "err", err1)
		return nil
	}
	if sentPeers == nil || len(sentPeers.Peers) == 0 {
		return nil
	}
	return sentPeers.Peers[0]
}

func (cs *ControlServerImpl) penalise(ctx context.Context, peer []byte) {
	penalizeReq := proto_sentry.PenalizePeerRequest{PeerId: peer, Penalty: proto_sentry.PenaltyKind_Kick}
	//nolint:govet
	callCtx, _ := context.WithCancel(ctx)
	if _, err := cs.sentryClient.PenalizePeer(callCtx, &penalizeReq, &grpc.EmptyCallOption{}); err != nil {
		log.Error("Could not penalise", "peer", peer, "error", err)
	}
}
