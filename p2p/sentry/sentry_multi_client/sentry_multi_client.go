package sentry_multi_client

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentryproto"
	proto_types "github.com/ledgerwatch/erigon-lib/gointerfaces/typesproto"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/p2p/sentry"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

type (
	SentryMessageStream        grpc.ClientStream
	SentryMessageStreamFactory func(context.Context, direct.SentryClient) (SentryMessageStream, error)
	StatusDataFactory          func(context.Context) (*proto_sentry.StatusData, error)
	MessageFactory[T any]      func() T
	MessageHandler[T any]      func(context.Context, T, direct.SentryClient) error
)

// StartStreamLoops starts message processing loops for all sentries.
// The processing happens in several streams:
// RecvMessage - processing incoming headers/bodies
// RecvUploadMessage - sending bodies/receipts - may be heavy, it's ok to not process this messages enough fast, it's also ok to drop some of these messages if we can't process.
// RecvUploadHeadersMessage - sending headers - dedicated stream because headers propagation speed important for network health
// PeerEventsLoop - logging peer connect/disconnect events
func (cs *MultiClient) StartStreamLoops(ctx context.Context) {
	sentries := cs.Sentries()
	for i := range sentries {
		sentry := sentries[i]
		go cs.RecvMessageLoop(ctx, sentry, nil)
		go cs.RecvUploadMessageLoop(ctx, sentry, nil)
		go cs.RecvUploadHeadersMessageLoop(ctx, sentry, nil)
		go cs.PeerEventsLoop(ctx, sentry, nil)
	}
}

func (cs *MultiClient) RecvUploadMessageLoop(
	ctx context.Context,
	sentry direct.SentryClient,
	wg *sync.WaitGroup,
) {
	ids := []proto_sentry.MessageId{
		eth.ToProto[direct.ETH66][eth.GetBlockBodiesMsg],
		eth.ToProto[direct.ETH66][eth.GetReceiptsMsg],
	}
	streamFactory := func(streamCtx context.Context, sentry direct.SentryClient) (SentryMessageStream, error) {
		return sentry.Messages(streamCtx, &proto_sentry.MessagesRequest{Ids: ids}, grpc.WaitForReady(true))
	}

	SentryReconnectAndPumpStreamLoop(ctx, sentry, cs.makeStatusData, "RecvUploadMessage", streamFactory, MakeInboundMessage, cs.HandleInboundMessage, wg, cs.logger)
}

func (cs *MultiClient) RecvUploadHeadersMessageLoop(
	ctx context.Context,
	sentry direct.SentryClient,
	wg *sync.WaitGroup,
) {
	ids := []proto_sentry.MessageId{
		eth.ToProto[direct.ETH66][eth.GetBlockHeadersMsg],
	}
	streamFactory := func(streamCtx context.Context, sentry direct.SentryClient) (SentryMessageStream, error) {
		return sentry.Messages(streamCtx, &proto_sentry.MessagesRequest{Ids: ids}, grpc.WaitForReady(true))
	}

	SentryReconnectAndPumpStreamLoop(ctx, sentry, cs.makeStatusData, "RecvUploadHeadersMessage", streamFactory, MakeInboundMessage, cs.HandleInboundMessage, wg, cs.logger)
}

func (cs *MultiClient) RecvMessageLoop(
	ctx context.Context,
	sentry direct.SentryClient,
	wg *sync.WaitGroup,
) {
	ids := []proto_sentry.MessageId{
		eth.ToProto[direct.ETH66][eth.BlockHeadersMsg],
		eth.ToProto[direct.ETH66][eth.BlockBodiesMsg],
		eth.ToProto[direct.ETH66][eth.NewBlockHashesMsg],
		eth.ToProto[direct.ETH66][eth.NewBlockMsg],
	}
	streamFactory := func(streamCtx context.Context, sentry direct.SentryClient) (SentryMessageStream, error) {
		return sentry.Messages(streamCtx, &proto_sentry.MessagesRequest{Ids: ids}, grpc.WaitForReady(true))
	}

	SentryReconnectAndPumpStreamLoop(ctx, sentry, cs.makeStatusData, "RecvMessage", streamFactory, MakeInboundMessage, cs.HandleInboundMessage, wg, cs.logger)
}

func (cs *MultiClient) PeerEventsLoop(
	ctx context.Context,
	sentry direct.SentryClient,
	wg *sync.WaitGroup,
) {
	streamFactory := func(streamCtx context.Context, sentry direct.SentryClient) (SentryMessageStream, error) {
		return sentry.PeerEvents(streamCtx, &proto_sentry.PeerEventsRequest{}, grpc.WaitForReady(true))
	}
	messageFactory := func() *proto_sentry.PeerEvent {
		return new(proto_sentry.PeerEvent)
	}

	SentryReconnectAndPumpStreamLoop(ctx, sentry, cs.makeStatusData, "PeerEvents", streamFactory, messageFactory, cs.HandlePeerEvent, wg, cs.logger)
}

func SentryReconnectAndPumpStreamLoop[TMessage interface{}](
	ctx context.Context,
	sentryClient direct.SentryClient,
	statusDataFactory StatusDataFactory,
	streamName string,
	streamFactory SentryMessageStreamFactory,
	messageFactory MessageFactory[TMessage],
	handleInboundMessage MessageHandler[TMessage],
	wg *sync.WaitGroup,
	logger log.Logger,
) {
	for ctx.Err() == nil {
		if _, err := sentryClient.HandShake(ctx, &emptypb.Empty{}, grpc.WaitForReady(true)); err != nil {
			if errors.Is(err, context.Canceled) {
				continue
			}
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}
			logger.Warn("HandShake error, sentry not ready yet", "stream", streamName, "err", err)
			time.Sleep(time.Second)
			continue
		}

		statusData, err := statusDataFactory(ctx)

		if err != nil {
			if !errors.Is(err, sentry.ErrNoHead) {
				logger.Error("SentryReconnectAndPumpStreamLoop: statusDataFactory error", "stream", streamName, "err", err)
			}
			time.Sleep(time.Second)
			continue
		}

		if _, err := sentryClient.SetStatus(ctx, statusData); err != nil {
			if errors.Is(err, context.Canceled) {
				continue
			}
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}
			logger.Warn("Status error, sentry not ready yet", "stream", streamName, "err", err)
			time.Sleep(time.Second)
			continue
		}

		if err := pumpStreamLoop(ctx, sentryClient, streamName, streamFactory, messageFactory, handleInboundMessage, wg, logger); err != nil {
			if errors.Is(err, context.Canceled) {
				continue
			}
			if isPeerNotFoundErr(err) {
				continue
			}
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}
			logger.Warn("pumpStreamLoop failure", "stream", streamName, "err", err)
			continue
		}
	}
}

// pumpStreamLoop is normally run in a separate go-routine.
// It only exists until there are no more messages
// to be received (end of process, or interruption, or end of test).
// wg is used only in tests to avoid using waits, which is brittle. For non-test code wg == nil.
func pumpStreamLoop[TMessage interface{}](
	ctx context.Context,
	sentry direct.SentryClient,
	streamName string,
	streamFactory SentryMessageStreamFactory,
	messageFactory MessageFactory[TMessage],
	handleInboundMessage MessageHandler[TMessage],
	wg *sync.WaitGroup,
	logger log.Logger,
) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer sentry.MarkDisconnected()

	// need to read all messages from Sentry as fast as we can, then:
	// - can group them or process in batch
	// - can have slow processing
	reqs := make(chan TMessage, 256)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-reqs:
				if err := handleInboundMessage(ctx, req, sentry); err != nil {
					logger.Debug("Handling incoming message", "stream", streamName, "err", err)
				}
				if wg != nil {
					wg.Done()
				}
			}
		}
	}()

	stream, err := streamFactory(ctx, sentry)
	if err != nil {
		return err
	}

	for ctx.Err() == nil {
		req := messageFactory()
		err := stream.RecvMsg(req)
		if err != nil {
			return err
		}

		select {
		case reqs <- req:
		case <-ctx.Done():
		}
	}

	return ctx.Err()
}

// MultiClient - does handle request/response/subscriptions to multiple sentries
// each sentry may support same or different p2p protocol
type MultiClient struct {
	Hd                                *headerdownload.HeaderDownload
	Bd                                *bodydownload.BodyDownload
	IsMock                            bool
	sentries                          []direct.SentryClient
	ChainConfig                       *chain.Config
	db                                kv.RwDB
	Engine                            consensus.Engine
	blockReader                       services.FullBlockReader
	statusDataProvider                *sentry.StatusDataProvider
	logPeerInfo                       bool
	sendHeaderRequestsToMultiplePeers bool
	maxBlockBroadcastPeers            func(*types.Header) uint

	// disableBlockDownload is meant to be used temporarily for astrid until work to
	// decouple sentry multi client from header and body downloading logic is done
	disableBlockDownload bool

	logger log.Logger
}

func NewMultiClient(
	db kv.RwDB,
	chainConfig *chain.Config,
	engine consensus.Engine,
	sentries []direct.SentryClient,
	syncCfg ethconfig.Sync,
	blockReader services.FullBlockReader,
	blockBufferSize int,
	statusDataProvider *sentry.StatusDataProvider,
	logPeerInfo bool,
	maxBlockBroadcastPeers func(*types.Header) uint,
	disableBlockDownload bool,
	logger log.Logger,
) (*MultiClient, error) {
	// header downloader
	var hd *headerdownload.HeaderDownload
	if !disableBlockDownload {
		hd = headerdownload.NewHeaderDownload(
			512,       /* anchorLimit */
			1024*1024, /* linkLimit */
			engine,
			blockReader,
			logger,
		)
		if chainConfig.TerminalTotalDifficultyPassed {
			hd.SetPOSSync(true)
		}
		if err := hd.RecoverFromDb(db); err != nil {
			return nil, fmt.Errorf("recovery from DB failed: %w", err)
		}
	} else {
		hd = &headerdownload.HeaderDownload{}
	}

	// body downloader
	var bd *bodydownload.BodyDownload
	if !disableBlockDownload {
		bd = bodydownload.NewBodyDownload(engine, blockBufferSize, int(syncCfg.BodyCacheLimit), blockReader, logger)
		if err := db.View(context.Background(), func(tx kv.Tx) error {
			_, _, _, _, err := bd.UpdateFromDb(tx)
			return err
		}); err != nil {
			return nil, err
		}
	} else {
		bd = &bodydownload.BodyDownload{}
	}

	cs := &MultiClient{
		Hd:                                hd,
		Bd:                                bd,
		sentries:                          sentries,
		ChainConfig:                       chainConfig,
		db:                                db,
		Engine:                            engine,
		blockReader:                       blockReader,
		statusDataProvider:                statusDataProvider,
		logPeerInfo:                       logPeerInfo,
		sendHeaderRequestsToMultiplePeers: chainConfig.TerminalTotalDifficultyPassed,
		maxBlockBroadcastPeers:            maxBlockBroadcastPeers,
		disableBlockDownload:              disableBlockDownload,
		logger:                            logger,
	}

	return cs, nil
}

func (cs *MultiClient) Sentries() []direct.SentryClient { return cs.sentries }

func (cs *MultiClient) newBlockHashes66(ctx context.Context, req *proto_sentry.InboundMessage, sentry direct.SentryClient) error {
	if cs.disableBlockDownload {
		return nil
	}

	if cs.Hd.InitialCycle() && !cs.Hd.FetchingNew() {
		return nil
	}
	//cs.logger.Info(fmt.Sprintf("NewBlockHashes from [%s]", ConvertH256ToPeerID(req.PeerId)))
	var request eth.NewBlockHashesPacket
	if err := rlp.DecodeBytes(req.Data, &request); err != nil {
		return fmt.Errorf("decode NewBlockHashes66: %w", err)
	}
	for _, announce := range request {
		cs.Hd.SaveExternalAnnounce(announce.Hash)
		if cs.Hd.HasLink(announce.Hash) {
			continue
		}
		//cs.logger.Info(fmt.Sprintf("Sending header request {hash: %x, height: %d, length: %d}", announce.Hash, announce.Number, 1))
		b, err := rlp.EncodeToBytes(&eth.GetBlockHeadersPacket66{
			RequestId: rand.Uint64(), // nolint: gosec
			GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
				Amount:  1,
				Reverse: false,
				Skip:    0,
				Origin:  eth.HashOrNumber{Hash: announce.Hash},
			},
		})
		if err != nil {
			return fmt.Errorf("encode header request: %w", err)
		}
		outreq := proto_sentry.SendMessageByIdRequest{
			PeerId: req.PeerId,
			Data: &proto_sentry.OutboundMessageData{
				Id:   proto_sentry.MessageId_GET_BLOCK_HEADERS_66,
				Data: b,
			},
		}

		if _, err = sentry.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{}); err != nil {
			if isPeerNotFoundErr(err) {
				continue
			}
			return fmt.Errorf("send header request: %w", err)
		}
	}
	return nil
}

func (cs *MultiClient) blockHeaders66(ctx context.Context, in *proto_sentry.InboundMessage, sentry direct.SentryClient) error {
	// Parse the entire packet from scratch
	var pkt eth.BlockHeadersPacket66
	if err := rlp.DecodeBytes(in.Data, &pkt); err != nil {
		return fmt.Errorf("decode 1 BlockHeadersPacket66: %w", err)
	}

	// Prepare to extract raw headers from the block
	rlpStream := rlp.NewStream(bytes.NewReader(in.Data), uint64(len(in.Data)))
	if _, err := rlpStream.List(); err != nil { // Now stream is at the beginning of 66 object
		return fmt.Errorf("decode 1 BlockHeadersPacket66: %w", err)
	}
	if _, err := rlpStream.Uint(); err != nil { // Now stream is at the requestID field
		return fmt.Errorf("decode 2 BlockHeadersPacket66: %w", err)
	}
	// Now stream is at the BlockHeadersPacket, which is list of headers

	return cs.blockHeaders(ctx, pkt.BlockHeadersPacket, rlpStream, in.PeerId, sentry)
}

func (cs *MultiClient) blockHeaders(ctx context.Context, pkt eth.BlockHeadersPacket, rlpStream *rlp.Stream, peerID *proto_types.H512, sentryClient direct.SentryClient) error {
	if cs.disableBlockDownload {
		return nil
	}

	if len(pkt) == 0 {
		// No point processing empty response
		return nil
	}
	// Stream is at the BlockHeadersPacket, which is list of headers
	if _, err := rlpStream.List(); err != nil {
		return fmt.Errorf("decode 2 BlockHeadersPacket66: %w", err)
	}
	// Extract headers from the block
	//var blockNums []int
	var highestBlock uint64
	csHeaders := make([]headerdownload.ChainSegmentHeader, 0, len(pkt))
	for _, header := range pkt {
		headerRaw, err := rlpStream.Raw()
		if err != nil {
			return fmt.Errorf("decode 3 BlockHeadersPacket66: %w", err)
		}
		hRaw := append([]byte{}, headerRaw...)
		number := header.Number.Uint64()
		if number > highestBlock {
			highestBlock = number
		}
		csHeaders = append(csHeaders, headerdownload.ChainSegmentHeader{
			Header:    header,
			HeaderRaw: hRaw,
			Hash:      types.RawRlpHash(hRaw),
			Number:    number,
		})
		//blockNums = append(blockNums, int(number))
	}
	//sort.Ints(blockNums)
	//cs.logger.Debug("Delivered headers", "peer",  fmt.Sprintf("%x", ConvertH512ToPeerID(peerID))[:8], "blockNums", fmt.Sprintf("%d", blockNums))
	if cs.Hd.POSSync() {
		sort.Sort(headerdownload.HeadersReverseSort(csHeaders)) // Sorting by reverse order of block heights
		tx, err := cs.db.BeginRo(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		penalties, err := cs.Hd.ProcessHeadersPOS(csHeaders, tx, sentry.ConvertH512ToPeerID(peerID))
		if err != nil {
			return err
		}
		if len(penalties) > 0 {
			cs.Penalize(ctx, penalties)
		}
	} else {
		sort.Sort(headerdownload.HeadersSort(csHeaders)) // Sorting by order of block heights
		canRequestMore := cs.Hd.ProcessHeaders(csHeaders, false /* newBlock */, sentry.ConvertH512ToPeerID(peerID))

		if canRequestMore {
			currentTime := time.Now()
			req, penalties := cs.Hd.RequestMoreHeaders(currentTime)
			if req != nil {
				if peer, sentToPeer := cs.SendHeaderRequest(ctx, req); sentToPeer {
					cs.Hd.UpdateStats(req, false /* skeleton */, peer)
					cs.Hd.UpdateRetryTime(req, currentTime, 5*time.Second /* timeout */)
				}
			}
			if len(penalties) > 0 {
				cs.Penalize(ctx, penalties)
			}
		}
	}
	outreq := proto_sentry.PeerMinBlockRequest{
		PeerId:   peerID,
		MinBlock: highestBlock,
	}
	if _, err1 := sentryClient.PeerMinBlock(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
		cs.logger.Error("Could not send min block for peer", "err", err1)
	}
	return nil
}

func (cs *MultiClient) newBlock66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentryClient direct.SentryClient) error {
	if cs.disableBlockDownload {
		return nil
	}

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
	request := &eth.NewBlockPacket{}
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return fmt.Errorf("decode 4 NewBlockMsg: %w", err)
	}
	if err := request.SanityCheck(); err != nil {
		return fmt.Errorf("newBlock66: %w", err)
	}
	if err := request.Block.HashCheck(); err != nil {
		return fmt.Errorf("newBlock66: %w", err)
	}

	if segments, penalty, err := cs.Hd.SingleHeaderAsSegment(headerRaw, request.Block.Header(), true /* penalizePoSBlocks */); err == nil {
		if penalty == headerdownload.NoPenalty {
			propagate := !cs.ChainConfig.TerminalTotalDifficultyPassed
			// Do not propagate blocks who are post TTD
			firstPosSeen := cs.Hd.FirstPoSHeight()
			if firstPosSeen != nil && propagate {
				propagate = *firstPosSeen >= segments[0].Number
			}
			if !cs.IsMock && propagate {
				cs.PropagateNewBlockHashes(ctx, []headerdownload.Announce{
					{
						Number: segments[0].Number,
						Hash:   segments[0].Hash,
					},
				})
			}

			cs.Hd.ProcessHeaders(segments, true /* newBlock */, sentry.ConvertH512ToPeerID(inreq.PeerId)) // There is only one segment in this case
		} else {
			outreq := proto_sentry.PenalizePeerRequest{
				PeerId:  inreq.PeerId,
				Penalty: proto_sentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
			}
			for _, sentry := range cs.sentries {
				if !sentry.Ready() {
					continue
				}
				if _, err1 := sentry.PenalizePeer(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
					cs.logger.Error("Could not send penalty", "err", err1)
				}
			}
		}
	} else {
		return fmt.Errorf("singleHeaderAsSegment failed: %w", err)
	}
	cs.Bd.AddToPrefetch(request.Block.Header(), request.Block.RawBody())
	outreq := proto_sentry.PeerMinBlockRequest{
		PeerId:   inreq.PeerId,
		MinBlock: request.Block.NumberU64(),
	}
	if _, err1 := sentryClient.PeerMinBlock(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
		cs.logger.Error("Could not send min block for peer", "err", err1)
	}
	cs.logger.Trace(fmt.Sprintf("NewBlockMsg{blockNumber: %d} from [%s]", request.Block.NumberU64(), sentry.ConvertH512ToPeerID(inreq.PeerId)))
	return nil
}

func (cs *MultiClient) blockBodies66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentryClient direct.SentryClient) error {
	if cs.disableBlockDownload {
		return nil
	}

	var request eth.BlockRawBodiesPacket66
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return fmt.Errorf("decode BlockBodiesPacket66: %w", err)
	}
	txs, uncles, withdrawals := request.BlockRawBodiesPacket.Unpack()
	if len(txs) == 0 && len(uncles) == 0 && len(withdrawals) == 0 {
		// No point processing empty response
		return nil
	}
	cs.Bd.DeliverBodies(txs, uncles, withdrawals, uint64(len(inreq.Data)), sentry.ConvertH512ToPeerID(inreq.PeerId))
	return nil
}

func (cs *MultiClient) receipts66(_ context.Context, _ *proto_sentry.InboundMessage, _ direct.SentryClient) error {
	return nil
}

func (cs *MultiClient) getBlockHeaders66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry direct.SentryClient) error {
	var query eth.GetBlockHeadersPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding getBlockHeaders66: %w, data: %x", err, inreq.Data)
	}

	var headers []*types.Header
	if err := cs.db.View(ctx, func(tx kv.Tx) (err error) {
		headers, err = eth.AnswerGetBlockHeadersQuery(tx, query.GetBlockHeadersPacket, cs.blockReader)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return fmt.Errorf("querying BlockHeaders: %w", err)
	}

	// Even if we get empty headers list from db, we'll respond with that. Nodes
	// running on erigon 2.48 with --sentry.drop-useless-peers will kick us out
	// because of certain checks. But, nodes post that will not kick us out. This
	// is useful as currently with no response, we're anyways getting kicked due
	// to request timeout and EOF.

	b, err := rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          query.RequestId,
		BlockHeadersPacket: headers,
	})
	if err != nil {
		return fmt.Errorf("encode header response: %w", err)
	}
	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: inreq.PeerId,
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_BLOCK_HEADERS_66,
			Data: b,
		},
	}
	_, err = sentry.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{})
	if err != nil {
		if !isPeerNotFoundErr(err) {
			return fmt.Errorf("send header response 66: %w", err)
		}
		return fmt.Errorf("send header response 66: %w", err)
	}
	//cs.logger.Info(fmt.Sprintf("[%s] GetBlockHeaderMsg{hash=%x, number=%d, amount=%d, skip=%d, reverse=%t, responseLen=%d}", ConvertH512ToPeerID(inreq.PeerId), query.Origin.Hash, query.Origin.Number, query.Amount, query.Skip, query.Reverse, len(b)))
	return nil
}

func (cs *MultiClient) getBlockBodies66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry direct.SentryClient) error {
	var query eth.GetBlockBodiesPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding getBlockBodies66: %w, data: %x", err, inreq.Data)
	}
	tx, err := cs.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	response := eth.AnswerGetBlockBodiesQuery(tx, query.GetBlockBodiesPacket, cs.blockReader)
	tx.Rollback()
	b, err := rlp.EncodeToBytes(&eth.BlockBodiesRLPPacket66{
		RequestId:            query.RequestId,
		BlockBodiesRLPPacket: response,
	})
	if err != nil {
		return fmt.Errorf("encode header response: %w", err)
	}
	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: inreq.PeerId,
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_BLOCK_BODIES_66,
			Data: b,
		},
	}
	_, err = sentry.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{})
	if err != nil {
		if isPeerNotFoundErr(err) {
			return nil
		}
		return fmt.Errorf("send bodies response: %w", err)
	}
	//cs.logger.Info(fmt.Sprintf("[%s] GetBlockBodiesMsg responseLen %d", ConvertH512ToPeerID(inreq.PeerId), len(b)))
	return nil
}

func (cs *MultiClient) getReceipts66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry direct.SentryClient) error {
	return nil //TODO: https://github.com/ledgerwatch/erigon/issues/10320
	//var query eth.GetReceiptsPacket66
	//if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
	//	return fmt.Errorf("decoding getReceipts66: %w, data: %x", err, inreq.Data)
	//}
	//tx, err := cs.db.BeginRo(ctx)
	//if err != nil {
	//	return err
	//}
	//defer tx.Rollback()
	//receipts, err := eth.AnswerGetReceiptsQuery(cs.blockReader, tx, query.GetReceiptsPacket)
	//if err != nil {
	//	return err
	//}
	//tx.Rollback()
	//b, err := rlp.EncodeToBytes(&eth.ReceiptsRLPPacket66{
	//	RequestId:         query.RequestId,
	//	ReceiptsRLPPacket: receipts,
	//})
	//if err != nil {
	//	return fmt.Errorf("encode header response: %w", err)
	//}
	//outreq := proto_sentry.SendMessageByIdRequest{
	//	PeerId: inreq.PeerId,
	//	Data: &proto_sentry.OutboundMessageData{
	//		Id:   proto_sentry.MessageId_RECEIPTS_66,
	//		Data: b,
	//	},
	//}
	//_, err = sentry.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{})
	//if err != nil {
	//	if isPeerNotFoundErr(err) {
	//		return nil
	//	}
	//	return fmt.Errorf("send bodies response: %w", err)
	//}
	////cs.logger.Info(fmt.Sprintf("[%s] GetReceipts responseLen %d", ConvertH512ToPeerID(inreq.PeerId), len(b)))
	//return nil
}

func MakeInboundMessage() *proto_sentry.InboundMessage {
	return new(proto_sentry.InboundMessage)
}

func (cs *MultiClient) HandleInboundMessage(ctx context.Context, message *proto_sentry.InboundMessage, sentry direct.SentryClient) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, msgID=%s, trace: %s", rec, message.Id.String(), dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	err = cs.handleInboundMessage(ctx, message, sentry)

	if (err != nil) && rlp.IsInvalidRLPError(err) {
		cs.logger.Debug("Kick peer for invalid RLP", "err", err)
		penalizeRequest := proto_sentry.PenalizePeerRequest{
			PeerId:  message.PeerId,
			Penalty: proto_sentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
		}
		if _, err1 := sentry.PenalizePeer(ctx, &penalizeRequest, &grpc.EmptyCallOption{}); err1 != nil {
			cs.logger.Error("Could not send penalty", "err", err1)
		}
	}

	return err
}

func (cs *MultiClient) handleInboundMessage(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry direct.SentryClient) error {
	switch inreq.Id {
	// ========= eth 66 ==========

	case proto_sentry.MessageId_NEW_BLOCK_HASHES_66:
		return cs.newBlockHashes66(ctx, inreq, sentry)
	case proto_sentry.MessageId_BLOCK_HEADERS_66:
		return cs.blockHeaders66(ctx, inreq, sentry)
	case proto_sentry.MessageId_NEW_BLOCK_66:
		return cs.newBlock66(ctx, inreq, sentry)
	case proto_sentry.MessageId_BLOCK_BODIES_66:
		return cs.blockBodies66(ctx, inreq, sentry)
	case proto_sentry.MessageId_GET_BLOCK_HEADERS_66:
		return cs.getBlockHeaders66(ctx, inreq, sentry)
	case proto_sentry.MessageId_GET_BLOCK_BODIES_66:
		return cs.getBlockBodies66(ctx, inreq, sentry)
	case proto_sentry.MessageId_RECEIPTS_66:
		return cs.receipts66(ctx, inreq, sentry)
	case proto_sentry.MessageId_GET_RECEIPTS_66:
		return cs.getReceipts66(ctx, inreq, sentry)
	default:
		return fmt.Errorf("not implemented for message Id: %s", inreq.Id)
	}
}

func (cs *MultiClient) HandlePeerEvent(ctx context.Context, event *proto_sentry.PeerEvent, sentryClient direct.SentryClient) error {
	eventID := event.EventId.String()
	peerID := sentry.ConvertH512ToPeerID(event.PeerId)
	peerIDStr := hex.EncodeToString(peerID[:])

	if !cs.logPeerInfo {
		cs.logger.Trace("[p2p] Sentry peer did", "eventID", eventID, "peer", peerIDStr)
		return nil
	}

	var nodeURL string
	var clientID string
	var capabilities []string
	if event.EventId == proto_sentry.PeerEvent_Connect {
		reply, err := sentryClient.PeerById(ctx, &proto_sentry.PeerByIdRequest{PeerId: event.PeerId})
		if err != nil {
			cs.logger.Debug("sentry.PeerById failed", "err", err)
		}
		if (reply != nil) && (reply.Peer != nil) {
			nodeURL = reply.Peer.Enode
			clientID = reply.Peer.Name
			capabilities = reply.Peer.Caps
		}
	}

	cs.logger.Trace("[p2p] Sentry peer did", "eventID", eventID, "peer", peerIDStr,
		"nodeURL", nodeURL, "clientID", clientID, "capabilities", capabilities)
	return nil
}

func (cs *MultiClient) makeStatusData(ctx context.Context) (*proto_sentry.StatusData, error) {
	return cs.statusDataProvider.GetStatusData(ctx)
}

func GrpcClient(ctx context.Context, sentryAddr string) (*direct.SentryClientRemote, error) {
	// creating grpc client connection
	var dialOpts []grpc.DialOption

	backoffCfg := backoff.DefaultConfig
	backoffCfg.BaseDelay = 500 * time.Millisecond
	backoffCfg.MaxDelay = 10 * time.Second
	dialOpts = []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoffCfg, MinConnectTimeout: 10 * time.Minute}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(int(16 * datasize.MB))),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{}),
	}

	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.DialContext(ctx, sentryAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating client connection to sentry P2P: %w", err)
	}
	return direct.NewSentryClientRemote(proto_sentry.NewSentryClient(conn)), nil
}
