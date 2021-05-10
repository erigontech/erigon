package download

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/forkid"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/ethconfig"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
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

func GrpcSentryClient(ctx context.Context, sentryAddr string) (proto_sentry.SentryClient, error) {
	// creating grpc client connection
	var dialOpts []grpc.DialOption
	dialOpts = []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.DefaultConfig, MinConnectTimeout: 10 * time.Minute}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(16 * datasize.MB))),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{}),
	}

	dialOpts = append(dialOpts, grpc.WithInsecure())
	conn, err := grpc.DialContext(ctx, sentryAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating client connection to sentry P2P: %w", err)
	}
	return proto_sentry.NewSentryClient(conn), nil
}

// Download creates and starts standalone downloader
func Download(sentryAddrs []string, db ethdb.Database, timeout, window int, chain string, nodeName string, tmpdir string) error {
	ctx := rootContext()

	log.Info("Starting Sentry client", "connecting to sentry", sentryAddrs)
	sentries := make([]proto_sentry.SentryClient, len(sentryAddrs))
	for i, addr := range sentryAddrs {
		sentry, err := GrpcSentryClient(ctx, addr)
		if err != nil {
			return err
		}
		sentries[i] = sentry
	}

	chainConfig, genesisHash, engine, networkID := cfg(db, chain)
	controlServer, err1 := NewControlServer(db, nodeName, chainConfig, genesisHash, engine, networkID, sentries, window)
	if err1 != nil {
		return fmt.Errorf("create core P2P server: %w", err1)
	}

	// TODO: Make a reconnection loop
	statusMsg := makeStatusData(controlServer)

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
				RecvMessage(ctx, sentry, controlServer.HandleInboundMessage)
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
				RecvUploadMessage(ctx, sentry, controlServer.HandleInboundMessage)
				// Wait before trying to reconnect to prevent log flooding
				time.Sleep(2 * time.Second)
			}
		}(sentry)
	}

	sm, err := ethdb.GetStorageModeFromDB(db)
	if err != nil {
		return err
	}

	batchSize := 512 * datasize.MB
	sync, err := NewStagedSync(
		ctx,
		db.(ethdb.HasRwKV).RwKV(),
		sm,
		batchSize,
		timeout,
		controlServer,
		tmpdir,
		nil,
	)
	if err != nil {
		return err
	}

	if err := stages.StageLoop(
		ctx,
		db,
		sync,
		controlServer.hd,
		controlServer.chainConfig,
	); err != nil {
		log.Error("Stage loop failure", "error", err)
	}

	return nil
}

func RecvUploadMessage(ctx context.Context, sentry proto_sentry.SentryClient, handleInboundMessage func(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error) {
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
		if req == nil {
			return
		}

		if err = handleInboundMessage(ctx, req, sentry); err != nil {
			log.Error("RecvUploadMessage: Handling incoming message", "error", err)
		}
	}
}

func RecvMessage(ctx context.Context, sentry proto_sentry.SentryClient, handleInboundMessage func(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error) {
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
		if req == nil {
			return
		}

		if err = handleInboundMessage(ctx, req, sentry); err != nil {
			log.Error("RecvMessage: Handling incoming message", "error", err)
		}
	}
}

// Combined creates and starts sentry and downloader in the same process
func Combined(natSetting string, port int, staticPeers []string, discovery bool, netRestrict string, db ethdb.Database, timeout, window int, chain string, nodeName string, tmpdir string) error {
	ctx := rootContext()

	sentryServer := NewSentryServer(ctx)
	sentry := &SentryClientDirect{}
	sentry.SetServer(sentryServer)
	chainConfig, genesisHash, engine, networkID := cfg(db, chain)
	sentries := []proto_sentry.SentryClient{sentry}
	controlServer, err := NewControlServer(db, nodeName, chainConfig, genesisHash, engine, networkID, sentries, window)
	if err != nil {
		return fmt.Errorf("create core P2P server: %w", err)
	}

	var readNodeInfo = func() *eth.NodeInfo {
		var res *eth.NodeInfo
		_ = db.(ethdb.HasRwKV).RwKV().View(context.Background(), func(tx ethdb.Tx) error {
			res = eth.ReadNodeInfo(db, controlServer.chainConfig, controlServer.genesisHash, controlServer.networkId)
			return nil
		})
		return res
	}

	sentryServer.P2pServer, err = p2pServer(ctx, sentryServer.nodeName, readNodeInfo, sentryServer, natSetting, port, staticPeers, discovery, netRestrict, controlServer.genesisHash)
	if err != nil {
		return err
	}

	if err = SetSentryStatus(ctx, sentries, controlServer); err != nil {
		log.Error("failed to set sentry status", "error", err)
		return nil
	}
	sm, err := ethdb.GetStorageModeFromDB(db)
	if err != nil {
		return err
	}
	batchSize := 512 * datasize.MB
	sync, err := NewStagedSync(
		ctx,
		db.(ethdb.HasRwKV).RwKV(),
		sm,
		batchSize,
		timeout,
		controlServer,
		tmpdir,
		nil,
	)
	if err != nil {
		return err
	}

	go RecvMessage(ctx, sentry, controlServer.HandleInboundMessage)
	go RecvUploadMessage(ctx, sentry, controlServer.HandleInboundMessage)

	if err := stages.StageLoop(
		ctx,
		db,
		sync,
		controlServer.hd,
		controlServer.chainConfig,
	); err != nil {
		log.Error("Stage loop failure", "error", err)
	}
	return nil
}

func Loop(ctx context.Context, db ethdb.Database, sync *stagedsync.StagedSync, controlServer *ControlServerImpl) {
	if err := stages.StageLoop(
		ctx,
		db,
		sync,
		controlServer.hd,
		controlServer.chainConfig,
	); err != nil {
		log.Error("Stage loop failure", "error", err)
	}

}

func SetSentryStatus(ctx context.Context, sentries []proto_sentry.SentryClient, controlServer *ControlServerImpl) error {
	for i := range sentries {
		if _, err := sentries[i].SetStatus(ctx, makeStatusData(controlServer), &grpc.EmptyCallOption{}); err != nil {
			return err
		}
	}
	return nil
}

func NewStagedSync(
	ctx context.Context,
	db ethdb.RwKV,
	sm ethdb.StorageMode,
	batchSize datasize.ByteSize,
	bodyDownloadTimeout int,
	controlServer *ControlServerImpl,
	tmpdir string,
	txPool *core.TxPool,
) (*stagedsync.StagedSync, error) {
	var increment uint64
	if sm.Pruning {
		increment = params.FullImmutabilityThreshold
	}

	return stages.NewStagedSync(ctx, sm,
		stagedsync.StageHeadersCfg(
			db,
			controlServer.hd,
			*controlServer.chainConfig,
			controlServer.sendHeaderRequest,
			controlServer.PropagateNewBlockHashes,
			controlServer.penalize,
			controlServer.requestWakeUpBodies,
			batchSize,
			increment,
		),
		stagedsync.StageBodiesCfg(
			db,
			controlServer.bd,
			controlServer.sendBodyRequest,
			controlServer.penalise,
			controlServer.updateHead,
			controlServer,
			controlServer.requestWakeUpBodies,
			bodyDownloadTimeout,
			batchSize,
		),
		stagedsync.StageSendersCfg(db, controlServer.chainConfig),
		stagedsync.StageExecuteBlocksCfg(
			db,
			sm.Receipts,
			sm.CallTraces,
			batchSize,
			nil,
			nil,
			nil,
			nil,
			controlServer.chainConfig,
			controlServer.engine,
			&vm.Config{NoReceipts: !sm.Receipts},
			tmpdir,
		),
		stagedsync.StageHashStateCfg(db, tmpdir),
		stagedsync.StageTrieCfg(db, true, true, tmpdir),
		stagedsync.StageHistoryCfg(db, tmpdir),
		stagedsync.StageLogIndexCfg(db, tmpdir),
		stagedsync.StageCallTracesCfg(db, 0, batchSize, tmpdir, controlServer.chainConfig, controlServer.engine),
		stagedsync.StageTxLookupCfg(db, tmpdir),
		stagedsync.StageTxPoolCfg(db, txPool),
	), nil
}

type ControlServerImpl struct {
	lock                 sync.RWMutex
	hd                   *headerdownload.HeaderDownload
	bd                   *bodydownload.BodyDownload
	nodeName             string
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
	engine               consensus.Engine
}

func cfg(db ethdb.Database, chain string) (chainConfig *params.ChainConfig, genesisHash common.Hash, engine consensus.Engine, networkID uint64) {
	ethashConfig := &ethash.Config{
		CachesInMem:      1,
		CachesLockMmap:   false,
		DatasetDir:       "ethash",
		DatasetsInMem:    1,
		DatasetsOnDisk:   0,
		DatasetsLockMmap: false,
	}
	cliqueConfig := params.NewSnapshotConfig(10, 1024, 16384, false /* inMemory */, "clique", false /* mdbx */)
	var err error
	var genesis *core.Genesis
	var consensusConfig interface{}
	switch chain {
	case "mainnet":
		networkID = 1
		genesis = core.DefaultGenesisBlock()
		genesisHash = params.MainnetGenesisHash
		consensusConfig = ethashConfig
	case "ropsten":
		networkID = 3
		genesis = core.DefaultRopstenGenesisBlock()
		genesisHash = params.RopstenGenesisHash
		consensusConfig = ethashConfig
	case "goerli":
		networkID = 5
		genesis = core.DefaultGoerliGenesisBlock()
		genesisHash = params.GoerliGenesisHash
		consensusConfig = cliqueConfig
	default:
		panic(fmt.Errorf("chain %s is not known", chain))
	}
	if chainConfig, _, err = core.SetupGenesisBlock(db, genesis, false /* history */, false /* overwrite */); err != nil {
		panic(fmt.Errorf("setup genesis block: %w", err))
	}
	engine = ethconfig.CreateConsensusEngine(chainConfig, consensusConfig, nil, false)
	return chainConfig, genesisHash, engine, networkID
}

func NewControlServer(db ethdb.Database, nodeName string, chainConfig *params.ChainConfig, genesisHash common.Hash, engine consensus.Engine, networkID uint64, sentries []proto_sentry.SentryClient, window int) (*ControlServerImpl, error) {
	hd := headerdownload.NewHeaderDownload(
		512,       /* anchorLimit */
		1024*1024, /* linkLimit */
		engine,
	)
	if err := hd.RecoverFromDb(db); err != nil {
		return nil, fmt.Errorf("recovery from DB failed: %w", err)
	}
	preverifiedHashes, preverifiedHeight := headerdownload.InitPreverifiedHashes(chainConfig.ChainID)

	hd.SetPreverifiedHashes(preverifiedHashes, preverifiedHeight)
	bd := bodydownload.NewBodyDownload(window /* outstandingLimit */)

	cs := &ControlServerImpl{
		nodeName:             nodeName,
		hd:                   hd,
		bd:                   bd,
		sentries:             sentries,
		requestWakeUpHeaders: make(chan struct{}, 1),
		requestWakeUpBodies:  make(chan struct{}, 1),
		db:                   db,
		engine:               engine,
	}
	cs.chainConfig = chainConfig
	cs.forks = forkid.GatherForks(cs.chainConfig)
	cs.genesisHash = genesisHash
	cs.protocolVersion = uint32(eth.ProtocolVersions[0])
	cs.networkId = networkID
	var err error
	err = db.RwKV().Update(context.Background(), func(tx ethdb.RwTx) error {
		cs.headHeight, cs.headHash, cs.headTd, err = bd.UpdateFromDb(tx)
		return err
	})
	return cs, err
}

func (cs *ControlServerImpl) newBlockHashes(ctx context.Context, req *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var request eth.NewBlockHashesPacket
	if err := rlp.DecodeBytes(req.Data, &request); err != nil {
		return fmt.Errorf("decode NewBlockHashes: %v", err)
	}
	for _, announce := range request {
		cs.hd.SaveExternalAnnounce(announce.Hash)
		if cs.hd.HasLink(announce.Hash) {
			continue
		}
		//log.Info(fmt.Sprintf("Sending header request {hash: %x, height: %d, length: %d}", announce.Hash, announce.Number, 1))
		b, err := rlp.EncodeToBytes(&eth.GetBlockHeadersPacket66{
			RequestId: rand.Uint64(),
			GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
				Amount:  1,
				Reverse: false,
				Skip:    0,
				Origin:  eth.HashOrNumber{Hash: announce.Hash},
			},
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

func (cs *ControlServerImpl) blockHeaders(ctx context.Context, in *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	// Extract header from the block
	rlpStream := rlp.NewStream(bytes.NewReader(in.Data), uint64(len(in.Data)))
	if _, err := rlpStream.List(); err != nil { // Now stream is at the beginning of 66 object
		return fmt.Errorf("decode 1 BlockHeadersPacket66: %w", err)
	}
	if _, err := rlpStream.Uint(); err != nil { // Now stream is at the requestID field
		return fmt.Errorf("decode 2 BlockHeadersPacket66: %w", err)
	}
	if _, err := rlpStream.List(); err != nil { // Now stream is at the BlockHeadersPacket, which is list of headers
		return fmt.Errorf("decode 3 BlockHeadersPacket66: %w", err)
	}
	var headersRaw [][]byte
	for headerRaw, err := rlpStream.Raw(); ; headerRaw, err = rlpStream.Raw() {
		if err != nil {
			if !errors.Is(err, rlp.EOL) {
				return fmt.Errorf("decode 4 BlockHeadersPacket66: %w", err)
			}
			break
		}

		headersRaw = append(headersRaw, headerRaw)
	}

	// Parse the entire request from scratch
	var request eth.BlockHeadersPacket66
	if err := rlp.DecodeBytes(in.Data, &request); err != nil {
		return fmt.Errorf("decode 5 BlockHeadersPacket66: %v", err)
	}
	headers := request.BlockHeadersPacket
	var heighestBlock uint64
	for _, h := range headers {
		if h.Number.Uint64() > heighestBlock {
			heighestBlock = h.Number.Uint64()
		}
	}

	if segments, penalty, err := cs.hd.SplitIntoSegments(headersRaw, headers); err == nil {
		if penalty == headerdownload.NoPenalty {
			var canRequestMore bool
			for _, segment := range segments {
				requestMore := cs.hd.ProcessSegment(segment, false /* newBlock */, string(gointerfaces.ConvertH512ToBytes(in.PeerId)))
				canRequestMore = canRequestMore || requestMore
			}

			if canRequestMore {
				currentTime := uint64(time.Now().Unix())
				req, penalties := cs.hd.RequestMoreHeaders(currentTime)
				if req != nil {
					if peer := cs.sendHeaderRequest(ctx, req); peer != nil {
						cs.hd.SentRequest(req, currentTime, 5 /* timeout */)
						log.Debug("Sent request", "height", req.Number)
					}
				}
				cs.penalize(ctx, penalties)
			}
		} else {
			outreq := proto_sentry.PenalizePeerRequest{
				PeerId:  in.PeerId,
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
		PeerId:   in.PeerId,
		MinBlock: heighestBlock,
	}
	if _, err1 := sentry.PeerMinBlock(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
		log.Error("Could not send min block for peer", "err", err1)
	}
	return nil
}

// sending list of penalties to all sentries
func (cs *ControlServerImpl) penalize(ctx context.Context, penalties []headerdownload.PenaltyItem) {
	for i := range penalties {
		outreq := proto_sentry.PenalizePeerRequest{
			PeerId:  gointerfaces.ConvertBytesToH512([]byte(penalties[i].PeerID)),
			Penalty: proto_sentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
		}
		for i, ok, next := cs.randSentryIndex(); ok; i, ok = next() {
			if _, err1 := cs.sentries[i].PenalizePeer(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
				log.Error("Could not send penalty", "err", err1)
			}
		}
	}
}

func (cs *ControlServerImpl) newBlock(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	// Extract header from the block
	rlpStream := rlp.NewStream(bytes.NewReader(inreq.Data), uint64(len(inreq.Data)))
	_, err := rlpStream.List() // Now stream is at the beginning of the block record
	if err != nil {
		return fmt.Errorf("decode 1 NewBlockMsg: %w", err)
	}
	_, err = rlpStream.List() // Now stream is at the beginning of the header
	if err != nil {
		return fmt.Errorf("decode 2 NewBlockMsg: %w", err)
	}
	var headerRaw []byte
	if headerRaw, err = rlpStream.Raw(); err != nil {
		return fmt.Errorf("decode 3 NewBlockMsg: %w", err)
	}
	// Parse the entire request from scratch
	var request eth.NewBlockPacket
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return fmt.Errorf("decode 4 NewBlockMsg: %v", err)
	}
	if segments, penalty, err := cs.hd.SingleHeaderAsSegment(headerRaw, request.Block.Header()); err == nil {
		if penalty == headerdownload.NoPenalty {
			cs.hd.ProcessSegment(segments[0], true /* newBlock */, string(gointerfaces.ConvertH512ToBytes(inreq.PeerId))) // There is only one segment in this case
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
	log.Debug(fmt.Sprintf("NewBlockMsg{blockNumber: %d} from [%s]", request.Block.NumberU64(), gointerfaces.ConvertH512ToBytes(inreq.PeerId)))
	return nil
}

func (cs *ControlServerImpl) blockBodies(inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var request eth.BlockBodiesPacket66
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return fmt.Errorf("decode BlockBodiesPacket66: %v", err)
	}
	txs, uncles := request.BlockBodiesPacket.Unpack()
	delivered, undelivered := cs.bd.DeliverBodies(txs, uncles)
	total := delivered + undelivered
	if total > 0 {
		// Approximate numbers
		cs.bd.DeliverySize(float64(len(inreq.Data))*float64(delivered)/float64(delivered+undelivered), float64(len(inreq.Data))*float64(undelivered)/float64(delivered+undelivered))
	}
	return nil
}

func (cs *ControlServerImpl) receipts(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	return nil
}

func (cs *ControlServerImpl) getBlockHeaders(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var query eth.GetBlockHeadersPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding GetBlockHeader: %v, data: %x", err, inreq.Data)
	}

	tx, err := cs.db.Begin(ctx, ethdb.RO)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	headers, err := eth.AnswerGetBlockHeadersQuery(tx, query.GetBlockHeadersPacket)
	if err != nil {
		return fmt.Errorf("querying BlockHeaders: %w", err)
	}
	b, err := rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          query.RequestId,
		BlockHeadersPacket: headers,
	})
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
	var query eth.GetBlockBodiesPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding GetBlockHeader: %v, data: %x", err, inreq.Data)
	}
	tx, err := cs.db.(ethdb.HasRwKV).RwKV().BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	response := eth.AnswerGetBlockBodiesQuery(tx, query.GetBlockBodiesPacket)
	tx.Rollback()
	b, err := rlp.EncodeToBytes(&eth.BlockBodiesRLPPacket66{
		RequestId:            query.RequestId,
		BlockBodiesRLPPacket: response,
	})
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

func (cs *ControlServerImpl) getReceipts(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
	var query eth.GetReceiptsPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding GetBlockHeader: %v, data: %x", err, inreq.Data)
	}
	tx, err := cs.db.(ethdb.HasRwKV).RwKV().BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	receipts, err := eth.AnswerGetReceiptsQuery(tx, query.GetReceiptsPacket)
	if err != nil {
		return err
	}
	tx.Rollback()
	b, err := rlp.EncodeToBytes(&eth.ReceiptsRLPPacket66{
		RequestId:         query.RequestId,
		ReceiptsRLPPacket: receipts,
	})
	if err != nil {
		return fmt.Errorf("encode header response: %v", err)
	}
	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: inreq.PeerId,
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_Receipts,
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

func (cs *ControlServerImpl) HandleInboundMessage(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry proto_sentry.SentryClient) error {
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
	case proto_sentry.MessageId_Receipts:
		return cs.receipts(ctx, inreq, sentry)
	case proto_sentry.MessageId_GetReceipts:
		return cs.getReceipts(ctx, inreq, sentry)
	default:
		return fmt.Errorf("not implemented for message Id: %s", inreq.Id)
	}
}

func makeStatusData(s *ControlServerImpl) *proto_sentry.StatusData {
	return &proto_sentry.StatusData{
		NetworkId:       s.networkId,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(s.headTd),
		BestHash:        gointerfaces.ConvertHashToH256(s.headHash),
		MaxBlock:        s.headHeight,
		ForkData: &proto_sentry.Forks{
			Genesis: gointerfaces.ConvertHashToH256(s.genesisHash),
			Forks:   s.forks,
		},
	}
}
