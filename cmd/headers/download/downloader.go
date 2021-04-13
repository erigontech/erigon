package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/eth/downloader"
	"github.com/ledgerwatch/turbo-geth/eth/ethconfig"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	proto_sentry "github.com/ledgerwatch/turbo-geth/gointerfaces/sentry"
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

// Methods of Core called by sentry

const (
	softResponseLimit = 2 * 1024 * 1024 // Target maximum size of returned blocks, headers or node data.
)

func grpcSentryClient(ctx context.Context, sentryAddr string) (proto_sentry.SentryClient, error) {
	// CREATING GRPC CLIENT CONNECTION
	var dialOpts []grpc.DialOption
	dialOpts = []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig, MinConnectTimeout: 10 * time.Minute}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(16 * datasize.MB))),
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
func Download(sentryAddrs []string, db ethdb.Database, timeout, window int, chain string) error {
	ctx := rootContext()

	log.Info("Starting Sentry client", "connecting to sentry", sentryAddrs)
	sentries := make([]proto_sentry.SentryClient, len(sentryAddrs))
	for i, addr := range sentryAddrs {
		sentry, err := grpcSentryClient(ctx, addr)
		if err != nil {
			return err
		}
		sentries[i] = sentry
	}

	controlServer, err1 := NewControlServer(db, sentries, window, chain)
	if err1 != nil {
		return fmt.Errorf("create core P2P server: %w", err1)
	}

	// TODO: Make a reconnection loop
	statusMsg := &proto_sentry.StatusData{
		NetworkId:       controlServer.networkId,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(controlServer.headTd),
		BestHash:        gointerfaces.ConvertHashToH256(controlServer.headHash),
		MaxBlock:        controlServer.headHeight,
		ForkData: &proto_sentry.Forks{
			Genesis: gointerfaces.ConvertHashToH256(controlServer.genesisHash),
			Forks:   controlServer.forks,
		},
	}

	for _, sentry := range sentries {
		if _, err := sentry.SetStatus(ctx, statusMsg, &grpc.EmptyCallOption{}); err != nil {
			return fmt.Errorf("setting initial status message: %w", err)
		}
	}

	for _, sentry := range sentries {
		go func(sentry proto_sentry.SentryClient) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				recvMessage(ctx, sentry, controlServer)
				// Wait before trying to reconnect to prevent log flooding
				time.Sleep(2 * time.Second)
			}
		}(sentry)
	}
	for _, sentry := range sentries {
		go func(sentry proto_sentry.SentryClient) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				recvUploadMessage(ctx, sentry, controlServer)
				// Wait before trying to reconnect to prevent log flooding
				time.Sleep(2 * time.Second)
			}
		}(sentry)
	}

	if err := stages.StageLoop(
		ctx,
		db,
		controlServer.hd,
		controlServer.bd,
		controlServer.chainConfig,
		controlServer.sendHeaderRequest,
		controlServer.sendBodyRequest,
		controlServer.penalise,
		controlServer.updateHead,
		controlServer,
		controlServer.requestWakeUpBodies,
		timeout,
	); err != nil {
		log.Error("Stage loop failure", "error", err)
	}

	return nil
}

func recvUploadMessage(ctx context.Context, sentry proto_sentry.SentryClient, controlServer *ControlServerImpl) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	receiveUploadClient, err3 := sentry.ReceiveUploadMessages(streamCtx, &empty.Empty{}, &grpc.EmptyCallOption{})
	if err3 != nil {
		log.Error("Receive upload messages failed", "error", err3)
		return
	}

	for req, err := receiveUploadClient.Recv(); ; req, err = receiveUploadClient.Recv() {
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Error("Receive upload loop terminated", "error", err)
				return
			}
			return
		}

		if err = controlServer.handleInboundMessage(ctx, req, sentry); err != nil {
			log.Error("Handling incoming message", "error", err)
		}
	}
}

func recvMessage(ctx context.Context, sentry proto_sentry.SentryClient, controlServer *ControlServerImpl) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	receiveClient, err2 := sentry.ReceiveMessages(streamCtx, &empty.Empty{}, &grpc.EmptyCallOption{})
	if err2 != nil {
		log.Error("Receive messages failed", "error", err2)
		return
	}

	for req, err := receiveClient.Recv(); ; req, err = receiveClient.Recv() {
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Error("Receive loop terminated", "error", err)
				return
			}
			return
		}

		if err = controlServer.handleInboundMessage(ctx, req, sentry); err != nil {
			log.Error("Handling incoming message", "error", err)
		}
	}
}

// Combined creates and starts sentry and downloader in the same process
func Combined(natSetting string, port int, staticPeers []string, discovery bool, netRestrict string, db ethdb.Database, timeout, window int, chain string) error {
	ctx := rootContext()

	sentryServer := &SentryServerImpl{
		ctx:             ctx,
		receiveCh:       make(chan StreamMsg, 1024),
		receiveUploadCh: make(chan StreamMsg, 1024),
	}
	sentry := &SentryClientDirect{}
	sentry.SetServer(sentryServer)
	controlServer, err := NewControlServer(db, []proto_sentry.SentryClient{sentry}, window, chain)
	if err != nil {
		return fmt.Errorf("create core P2P server: %w", err)
	}
	sentryServer.p2pServer, err = p2pServer(ctx, sentryServer, controlServer.genesisHash, natSetting, port, staticPeers, discovery, netRestrict)
	if err != nil {
		return err
	}
	statusMsg := &proto_sentry.StatusData{
		NetworkId:       controlServer.networkId,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(controlServer.headTd),
		BestHash:        gointerfaces.ConvertHashToH256(controlServer.headHash),
		MaxBlock:        controlServer.headHeight,
		ForkData: &proto_sentry.Forks{
			Genesis: gointerfaces.ConvertHashToH256(controlServer.genesisHash),
			Forks:   controlServer.forks,
		},
	}

	if _, err := sentry.SetStatus(ctx, statusMsg, &grpc.EmptyCallOption{}); err != nil {
		return fmt.Errorf("setting initial status message: %w", err)
	}

	go recvMessage(ctx, sentry, controlServer)
	go recvUploadMessage(ctx, sentry, controlServer)

	if err := stages.StageLoop(
		ctx,
		db,
		controlServer.hd,
		controlServer.bd,
		controlServer.chainConfig,
		controlServer.sendHeaderRequest,
		controlServer.sendBodyRequest,
		controlServer.penalise,
		controlServer.updateHead,
		controlServer,
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
	sentries             []proto_sentry.SentryClient
	requestWakeUpHeaders chan struct{}
	requestWakeUpBodies  chan struct{}
	headHeight           uint64
	headHash             common.Hash
	headTd               *uint256.Int
	chainConfig          *params.ChainConfig
	forks                []uint64
	genesisHash          common.Hash
	protocolVersion      uint32
	networkId            uint64
	db                   ethdb.Database
}

func NewControlServer(db ethdb.Database, sentries []proto_sentry.SentryClient, window int, chain string) (*ControlServerImpl, error) {
	ethashConfig := &ethash.Config{
		CachesInMem:      1,
		CachesLockMmap:   false,
		DatasetDir:       "ethash",
		DatasetsInMem:    1,
		DatasetsOnDisk:   0,
		DatasetsLockMmap: false,
	}
	var chainConfig *params.ChainConfig
	var err error
	var genesis *core.Genesis
	var genesisHash common.Hash
	var networkID uint64
	switch chain {
	case "mainnet":
		networkID = 1
		genesis = core.DefaultGenesisBlock()
		genesisHash = params.MainnetGenesisHash
	case "ropsten":
		networkID = 3
		genesis = core.DefaultRopstenGenesisBlock()
		genesisHash = params.RopstenGenesisHash
	case "goerli":
		networkID = 5
		genesis = core.DefaultGoerliGenesisBlock()
		genesisHash = params.GoerliGenesisHash
	default:
		return nil, fmt.Errorf("chain %s is not known", chain)
	}
	if chainConfig, _, err = core.SetupGenesisBlock(db, genesis, false /* history */, false /* overwrite */); err != nil {
		return nil, fmt.Errorf("setup genesis block: %w", err)
	}
	engine := ethconfig.CreateConsensusEngine(chainConfig, ethashConfig, nil, false, db)
	hd := headerdownload.NewHeaderDownload(
		512,       /* anchorLimit */
		1024*1024, /* tipLimit */
		engine,
	)
	if err = hd.RecoverFromDb(db); err != nil {
		return nil, fmt.Errorf("recovery from DB failed: %w", err)
	}
	preverifiedHashes, preverifiedHeight := headerdownload.InitPreverifiedHashes(chain)

	hd.SetPreverifiedHashes(preverifiedHashes, preverifiedHeight)
	bd := bodydownload.NewBodyDownload(window /* outstandingLimit */)
	cs := &ControlServerImpl{hd: hd, bd: bd, sentries: sentries, requestWakeUpHeaders: make(chan struct{}, 1), requestWakeUpBodies: make(chan struct{}, 1), db: db}
	cs.chainConfig = chainConfig
	cs.forks = forkid.GatherForks(cs.chainConfig)
	cs.genesisHash = genesisHash
	cs.protocolVersion = uint32(eth.ProtocolVersions[0])
	cs.networkId = networkID
	cs.headHeight, cs.headHash, cs.headTd, err = bd.UpdateFromDb(db)
	return cs, err
}

func (cs *ControlServerImpl) newBlockHashes(ctx context.Context, req *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var request eth.NewBlockHashesPacket
	if err := rlp.DecodeBytes(req.Data, &request); err != nil {
		return fmt.Errorf("decode NewBlockHashes: %v", err)
	}
	for _, announce := range request {
		if cs.hd.HasLink(announce.Hash) {
			continue
		}
		//log.Info(fmt.Sprintf("Sending header request {hash: %x, height: %d, length: %d}", announce.Hash, announce.Number, 1))
		b, err := rlp.EncodeToBytes(&eth.GetBlockHeadersPacket{
			Amount:  1,
			Reverse: false,
			Skip:    0,
			Origin:  eth.HashOrNumber{Hash: announce.Hash},
		})
		if err != nil {
			return fmt.Errorf("encode header request: %v", err)
		}
		outreq := proto_sentry.SendMessageByIdRequest{
			PeerId: req.PeerId,
			Data: &proto_sentry.OutboundMessageData{
				Id:   proto_sentry.MessageId_GetBlockHeaders,
				Data: b,
			},
		}

		if _, err = sentry.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{}); err != nil {
			return fmt.Errorf("send header request: %v", err)
		}
	}
	return nil
}

func (cs *ControlServerImpl) blockHeaders(ctx context.Context, req *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	rlpStream := rlp.NewStream(bytes.NewReader(req.Data), uint64(len(req.Data)))
	_, err := rlpStream.List()
	if err != nil {
		return fmt.Errorf("decode BlockHeaders: %w", err)
	}
	var headersRaw [][]byte
	var headerRaw []byte
	for headerRaw, err = rlpStream.Raw(); ; headerRaw, err = rlpStream.Raw() {
		if err != nil {
			if !errors.Is(err, rlp.EOL) {
				return fmt.Errorf("decode BlockHeaders: %w", err)
			}
			break
		}

		headersRaw = append(headersRaw, headerRaw)
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
				cs.hd.ProcessSegment(segment, false /* newBlock */)
			}
		} else {
			outreq := proto_sentry.PenalizePeerRequest{
				PeerId:  req.PeerId,
				Penalty: proto_sentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
			}
			if _, err1 := sentry.PenalizePeer(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
				log.Error("Could not send penalty", "err", err1)
			}
		}
	} else {
		return fmt.Errorf("singleHeaderAsSegment failed: %v", err)
	}
	outreq := proto_sentry.PeerMinBlockRequest{
		PeerId:   req.PeerId,
		MinBlock: heighestBlock,
	}
	if _, err1 := sentry.PeerMinBlock(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
		log.Error("Could not send min block for peer", "err", err1)
	}
	//log.Info("HeadersMsg processed")
	return nil
}

func (cs *ControlServerImpl) newBlock(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
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
	var request eth.NewBlockPacket
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return fmt.Errorf("decode NewBlockMsg: %v", err)
	}
	if segments, penalty, err := cs.hd.SingleHeaderAsSegment(headerRaw, request.Block.Header()); err == nil {
		if penalty == headerdownload.NoPenalty {
			cs.hd.ProcessSegment(segments[0], true /* newBlock */) // There is only one segment in this case
		} else {
			outreq := proto_sentry.PenalizePeerRequest{
				PeerId:  inreq.PeerId,
				Penalty: proto_sentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
			}
			for _, sentry := range cs.sentries {
				if _, err1 := sentry.PenalizePeer(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
					log.Error("Could not send penalty", "err", err1)
				}
			}
		}
	} else {
		return fmt.Errorf("singleHeaderAsSegment failed: %v", err)
	}
	cs.bd.AddToPrefetch(request.Block)
	outreq := proto_sentry.PeerMinBlockRequest{
		PeerId:   inreq.PeerId,
		MinBlock: request.Block.NumberU64(),
	}
	if _, err1 := sentry.PeerMinBlock(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
		log.Error("Could not send min block for peer", "err", err1)
	}
	log.Info(fmt.Sprintf("NewBlockMsg{blockNumber: %d} from [%s]", request.Block.NumberU64(), gointerfaces.ConvertH512ToBytes(inreq.PeerId)))
	return nil
}

func (cs *ControlServerImpl) blockBodies(inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var request []*types.Body
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return fmt.Errorf("decode BlockBodies: %v", err)
	}
	delivered, undelivered := cs.bd.DeliverBodies(request)
	total := delivered + undelivered
	if total > 0 {
		// Approximate numbers
		cs.bd.DeliverySize(float64(len(inreq.Data))*float64(delivered)/float64(delivered+undelivered), float64(len(inreq.Data))*float64(undelivered)/float64(delivered+undelivered))
	}
	return nil
}

func (cs *ControlServerImpl) getBlockHeaders(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var query eth.GetBlockHeadersPacket
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding GetBlockHeader: %v, data: %x", err, inreq.Data)
	}
	tx, err := cs.db.Begin(ctx, ethdb.RO)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	headers, err := eth.AnswerGetBlockHeadersQuery(tx, &query)
	if err != nil {
		return fmt.Errorf("querying BlockHeaders: %w", err)
	}
	tx.Rollback()

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
	_, err = sentry.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{})
	if err != nil {
		return fmt.Errorf("send header response: %v", err)
	}
	//log.Info(fmt.Sprintf("[%s] GetBlockHeaderMsg{hash=%x, number=%d, amount=%d, skip=%d, reverse=%t, responseLen=%d}", inreq.PeerId, query.Origin.Hash, query.Origin.Number, query.Amount, query.Skip, query.Reverse, len(b)))
	return nil
}

func (cs *ControlServerImpl) getBlockBodies(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
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
			return fmt.Errorf("decode hash for GetBlockBodiesMsg: %v", err)
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
	_, err = sentry.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{})
	if err != nil {
		return fmt.Errorf("send bodies response: %v", err)
	}
	//log.Info(fmt.Sprintf("[%s] GetBlockBodiesMsg {%s}, responseLen %d", inreq.PeerId, hashesStr.String(), len(bodies)))
	return nil
}

func (cs *ControlServerImpl) handleInboundMessage(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
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
		return cs.newBlockHashes(ctx, inreq, sentry)
	case proto_sentry.MessageId_BlockHeaders:
		return cs.blockHeaders(ctx, inreq, sentry)
	case proto_sentry.MessageId_NewBlock:
		return cs.newBlock(ctx, inreq, sentry)
	case proto_sentry.MessageId_BlockBodies:
		return cs.blockBodies(inreq, sentry)
	case proto_sentry.MessageId_GetBlockHeaders:
		return cs.getBlockHeaders(ctx, inreq, sentry)
	case proto_sentry.MessageId_GetBlockBodies:
		return cs.getBlockBodies(ctx, inreq, sentry)
	default:
		return fmt.Errorf("not implemented for message Id: %s", inreq.Id)
	}
}
