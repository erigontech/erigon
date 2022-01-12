package sentry

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
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	proto_types "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// 3 streams:
// RecvMessage - processing incoming headers/bodies
// RecvUploadHeadersMessage - sending headers - dedicated stream because headers propagation speed important for network health
// RecvUploadMessage - sending bodies/receipts - may be heavy, it's ok to not process this messages enough fast, it's also ok to drop some of this messages if can't process.

func RecvUploadMessageLoop(ctx context.Context,
	sentry direct.SentryClient,
	cs *ControlServerImpl,
	wg *sync.WaitGroup,
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if _, err := sentry.HandShake(ctx, &emptypb.Empty{}, grpc.WaitForReady(true)); err != nil {
			s, ok := status.FromError(err)
			doLog := !((ok && s.Code() == codes.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled))
			if doLog {
				log.Warn("[RecvUploadMessage] sentry not ready yet", "err", err)
			}
			time.Sleep(time.Second)
			continue
		}
		if err := SentrySetStatus(ctx, sentry, cs); err != nil {
			s, ok := status.FromError(err)
			doLog := !((ok && s.Code() == codes.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled))
			if doLog {
				log.Warn("[RecvUploadMessage] sentry not ready yet", "err", err)
			}
			time.Sleep(time.Second)
			continue
		}
		if err := RecvUploadMessage(ctx, sentry, cs.HandleInboundMessage, wg); err != nil {
			if isPeerNotFoundErr(err) {
				continue
			}
			s, ok := status.FromError(err)
			if (ok && s.Code() == codes.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				time.Sleep(time.Second)
				continue
			}
			log.Debug("[RecvUploadMessage]", "err", err)
			continue
		}
	}
}

func RecvUploadMessage(ctx context.Context,
	sentry direct.SentryClient,
	handleInboundMessage func(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry direct.SentryClient) error,
	wg *sync.WaitGroup,
) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := sentry.Messages(streamCtx, &proto_sentry.MessagesRequest{Ids: []proto_sentry.MessageId{
		eth.ToProto[eth.ETH66][eth.GetBlockBodiesMsg],
		eth.ToProto[eth.ETH66][eth.GetReceiptsMsg],
	}}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	var req *proto_sentry.InboundMessage
	for req, err = stream.Recv(); ; req, err = stream.Recv() {
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			return err
		}
		if req == nil {
			return
		}
		if err = handleInboundMessage(ctx, req, sentry); err != nil {
			log.Debug("[RecvUploadMessage]: Handling incoming message", "error", err)

		}
		if wg != nil {
			wg.Done()
		}

	}
}

func RecvUploadHeadersMessageLoop(ctx context.Context,
	sentry direct.SentryClient,
	cs *ControlServerImpl,
	wg *sync.WaitGroup,
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if _, err := sentry.HandShake(ctx, &emptypb.Empty{}, grpc.WaitForReady(true)); err != nil {
			s, ok := status.FromError(err)
			doLog := !((ok && s.Code() == codes.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled))
			if doLog {
				log.Warn("[RecvUploadMessage] sentry not ready yet", "err", err)
			}
			time.Sleep(time.Second)
			continue
		}
		if err := SentrySetStatus(ctx, sentry, cs); err != nil {
			s, ok := status.FromError(err)
			doLog := !((ok && s.Code() == codes.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled))
			if doLog {
				log.Warn("[RecvUploadMessage] sentry not ready yet", "err", err)
			}
			time.Sleep(time.Second)
			continue
		}
		if err := RecvUploadHeadersMessage(ctx, sentry, cs.HandleInboundMessage, wg); err != nil {
			if isPeerNotFoundErr(err) {
				continue
			}
			s, ok := status.FromError(err)
			if (ok && s.Code() == codes.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				time.Sleep(time.Second)
				continue
			}
			log.Warn("[RecvUploadMessage]", "err", err)
			continue
		}
	}
}

func RecvUploadHeadersMessage(ctx context.Context,
	sentry direct.SentryClient,
	handleInboundMessage func(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry direct.SentryClient) error,
	wg *sync.WaitGroup,
) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	stream, err := sentry.Messages(streamCtx, &proto_sentry.MessagesRequest{Ids: []proto_sentry.MessageId{
		eth.ToProto[eth.ETH66][eth.GetBlockHeadersMsg],
	}}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	var req *proto_sentry.InboundMessage
	for req, err = stream.Recv(); ; req, err = stream.Recv() {
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			return err
		}
		if req == nil {
			return
		}
		if err = handleInboundMessage(ctx, req, sentry); err != nil {
			log.Debug("[RecvUploadHeadersMessage] Handling incoming message", "error", err)
		}
		if wg != nil {
			wg.Done()
		}

	}
}

func RecvMessageLoop(ctx context.Context,
	sentry direct.SentryClient,
	cs *ControlServerImpl,
	wg *sync.WaitGroup,
) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if _, err := sentry.HandShake(ctx, &emptypb.Empty{}, grpc.WaitForReady(true)); err != nil {
			s, ok := status.FromError(err)
			if (ok && s.Code() == codes.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				time.Sleep(time.Second)
				continue
			}
			log.Warn("[RecvMessage] sentry not ready yet", "err", err)
			continue
		}
		if err := SentrySetStatus(ctx, sentry, cs); err != nil {
			s, ok := status.FromError(err)
			if (ok && s.Code() == codes.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				time.Sleep(time.Second)
				continue
			}
			log.Warn("[RecvMessage] sentry not ready yet", "err", err)
			continue
		}
		if err := RecvMessage(ctx, sentry, cs.HandleInboundMessage, wg); err != nil {
			if isPeerNotFoundErr(err) {
				continue
			}
			s, ok := status.FromError(err)
			if (ok && s.Code() == codes.Canceled) || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				time.Sleep(time.Second)
				continue
			}
			log.Warn("[RecvMessage]", "err", err)
			continue
		}
	}
}

// RecvMessage is normally run in a separate go-routine because it only exists when there a no more messages
// to be received (end of process, or interruption, or end of test)
// wg is used only in tests to avoid using waits, which is brittle. For non-test code wg == nil
func RecvMessage(
	ctx context.Context,
	sentry direct.SentryClient,
	handleInboundMessage func(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry direct.SentryClient) error,
	wg *sync.WaitGroup,
) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer sentry.MarkDisconnected()

	stream, err := sentry.Messages(streamCtx, &proto_sentry.MessagesRequest{Ids: []proto_sentry.MessageId{
		eth.ToProto[eth.ETH66][eth.BlockHeadersMsg],
		eth.ToProto[eth.ETH66][eth.BlockBodiesMsg],
		eth.ToProto[eth.ETH66][eth.NewBlockHashesMsg],
		eth.ToProto[eth.ETH66][eth.NewBlockMsg],
	}}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}

	var req *proto_sentry.InboundMessage
	for req, err = stream.Recv(); ; req, err = stream.Recv() {
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			return err
		}
		if req == nil {
			return
		}

		if err = handleInboundMessage(ctx, req, sentry); err != nil {
			if rlp.IsInvalidRLPError(err) {
				log.Debug("[RecvMessage] Kick peer for invalid RLP", "error", err)
				outreq := proto_sentry.PenalizePeerRequest{
					PeerId:  req.PeerId,
					Penalty: proto_sentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
				}
				if _, err1 := sentry.PenalizePeer(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
					log.Error("Could not send penalty", "err", err1)
				}
			} else {
				log.Warn("[RecvMessage] Handling incoming message", "error", err)
			}
		}

		if wg != nil {
			wg.Done()
		}
	}
}

func SentrySetStatus(ctx context.Context, sentry direct.SentryClient, controlServer *ControlServerImpl) error {
	_, err := sentry.SetStatus(ctx, makeStatusData(controlServer))
	return err
}

type ControlServerImpl struct {
	lock        sync.RWMutex
	Hd          *headerdownload.HeaderDownload
	Bd          *bodydownload.BodyDownload
	nodeName    string
	sentries    []direct.SentryClient
	headHeight  uint64
	headHash    common.Hash
	headTd      *uint256.Int
	ChainConfig *params.ChainConfig
	forks       []uint64
	genesisHash common.Hash
	networkId   uint64
	db          kv.RwDB
	Engine      consensus.Engine
	blockReader interfaces.HeaderAndCanonicalReader
}

func NewControlServer(db kv.RwDB, nodeName string, chainConfig *params.ChainConfig, genesisHash common.Hash, engine consensus.Engine, networkID uint64, sentries []direct.SentryClient, window int, blockReader interfaces.HeaderAndCanonicalReader) (*ControlServerImpl, error) {
	hd := headerdownload.NewHeaderDownload(
		512,       /* anchorLimit */
		1024*1024, /* linkLimit */
		engine,
	)

	if err := hd.RecoverFromDb(db); err != nil {
		return nil, fmt.Errorf("recovery from DB failed: %w", err)
	}
	preverifiedHashes, preverifiedHeight := headerdownload.InitPreverifiedHashes(chainConfig.ChainName)

	hd.SetPreverifiedHashes(preverifiedHashes, preverifiedHeight)
	bd := bodydownload.NewBodyDownload(window /* outstandingLimit */, engine)

	cs := &ControlServerImpl{
		nodeName:    nodeName,
		Hd:          hd,
		Bd:          bd,
		sentries:    sentries,
		db:          db,
		Engine:      engine,
		blockReader: blockReader,
	}
	cs.ChainConfig = chainConfig
	cs.forks = forkid.GatherForks(cs.ChainConfig)
	cs.genesisHash = genesisHash
	cs.networkId = networkID
	var err error
	err = db.View(context.Background(), func(tx kv.Tx) error {
		cs.headHeight, cs.headHash, cs.headTd, err = cs.Bd.UpdateFromDb(tx)
		return err
	})
	return cs, err
}

func (cs *ControlServerImpl) newBlockHashes66(ctx context.Context, req *proto_sentry.InboundMessage, sentry direct.SentryClient) error {
	if !cs.Hd.RequestChaining() && !cs.Hd.Fetching() {
		return nil
	}
	//log.Info(fmt.Sprintf("NewBlockHashes from [%s]", ConvertH256ToPeerID(req.PeerId)))
	var request eth.NewBlockHashesPacket
	if err := rlp.DecodeBytes(req.Data, &request); err != nil {
		return fmt.Errorf("decode NewBlockHashes66: %w", err)
	}
	for _, announce := range request {
		cs.Hd.SaveExternalAnnounce(announce.Hash)
		if cs.Hd.HasLink(announce.Hash) {
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

func (cs *ControlServerImpl) blockHeaders66(ctx context.Context, in *proto_sentry.InboundMessage, sentry direct.SentryClient) error {
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

func (cs *ControlServerImpl) blockHeaders(ctx context.Context, pkt eth.BlockHeadersPacket, rlpStream *rlp.Stream, peerID *proto_types.H256, sentry direct.SentryClient) error {
	// Stream is at the BlockHeadersPacket, which is list of headers
	if _, err := rlpStream.List(); err != nil {
		return fmt.Errorf("decode 2 BlockHeadersPacket66: %w", err)
	}
	// Extract headers from the block
	var highestBlock uint64
	csHeaders := make([]headerdownload.ChainSegmentHeader, 0, len(pkt))
	for _, header := range pkt {
		headerRaw, err := rlpStream.Raw()
		if err != nil {
			return fmt.Errorf("decode 3 BlockHeadersPacket66: %w", err)
		}
		number := header.Number.Uint64()
		if number > highestBlock {
			highestBlock = number
		}
		csHeaders = append(csHeaders, headerdownload.ChainSegmentHeader{
			Header:    header,
			HeaderRaw: headerRaw,
			Hash:      types.RawRlpHash(headerRaw),
			Number:    number,
		})
	}
	if segments, penaltyKind, err := cs.Hd.SplitIntoSegments(csHeaders); err == nil {
		if penaltyKind == headerdownload.NoPenalty {
			if cs.Hd.POSSync() {
				tx, err := cs.db.BeginRo(ctx)
				defer tx.Rollback()
				if err != nil {
					return err
				}
				for _, segment := range segments {
					if err := cs.Hd.ProcessSegmentPOS(segment, tx); err != nil {
						return err
					}
				}
			} else {
				var canRequestMore bool
				for _, segment := range segments {
					requestMore, penalties := cs.Hd.ProcessSegment(segment, false /* newBlock */, ConvertH256ToPeerID(peerID))
					canRequestMore = canRequestMore || requestMore
					if len(penalties) > 0 {
						cs.Penalize(ctx, penalties)
					}
				}

				if canRequestMore {
					currentTime := uint64(time.Now().Unix())
					req, penalties := cs.Hd.RequestMoreHeaders(currentTime)
					if req != nil {
						if _, sentToPeer := cs.SendHeaderRequest(ctx, req); sentToPeer {
							// If request was actually sent to a peer, we update retry time to be 5 seconds in the future
							cs.Hd.UpdateRetryTime(req, currentTime, 5 /* timeout */)
							log.Trace("Sent request", "height", req.Number)
						}
					}
					if len(penalties) > 0 {
						cs.Penalize(ctx, penalties)
					}
				}
			}
		} else {
			outreq := proto_sentry.PenalizePeerRequest{
				PeerId:  peerID,
				Penalty: proto_sentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
			}
			if _, err1 := sentry.PenalizePeer(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
				log.Error("Could not send penalty", "err", err1)
			}
		}
	} else {
		return fmt.Errorf("singleHeaderAsSegment failed: %w", err)
	}
	outreq := proto_sentry.PeerMinBlockRequest{
		PeerId:   peerID,
		MinBlock: highestBlock,
	}
	if _, err1 := sentry.PeerMinBlock(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
		log.Error("Could not send min block for peer", "err", err1)
	}
	return nil
}

func (cs *ControlServerImpl) newBlock66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry direct.SentryClient) error {
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

	if segments, penalty, err := cs.Hd.SingleHeaderAsSegment(headerRaw, request.Block.Header()); err == nil {
		if penalty == headerdownload.NoPenalty {
			cs.Hd.ProcessSegment(segments[0], true /* newBlock */, ConvertH256ToPeerID(inreq.PeerId)) // There is only one segment in this case
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
					log.Error("Could not send penalty", "err", err1)
				}
			}
		}
	} else {
		return fmt.Errorf("singleHeaderAsSegment failed: %w", err)
	}
	cs.Bd.AddToPrefetch(request.Block)
	outreq := proto_sentry.PeerMinBlockRequest{
		PeerId:   inreq.PeerId,
		MinBlock: request.Block.NumberU64(),
	}
	if _, err1 := sentry.PeerMinBlock(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
		log.Error("Could not send min block for peer", "err", err1)
	}
	log.Trace(fmt.Sprintf("NewBlockMsg{blockNumber: %d} from [%s]", request.Block.NumberU64(), ConvertH256ToPeerID(inreq.PeerId)))
	return nil
}

func (cs *ControlServerImpl) blockBodies66(inreq *proto_sentry.InboundMessage, sentry direct.SentryClient) error {
	var request eth.BlockRawBodiesPacket66
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return fmt.Errorf("decode BlockBodiesPacket66: %w", err)
	}
	txs, uncles := request.BlockRawBodiesPacket.Unpack()
	cs.Bd.DeliverBodies(txs, uncles, uint64(len(inreq.Data)), ConvertH256ToPeerID(inreq.PeerId))
	return nil
}

func (cs *ControlServerImpl) receipts66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry direct.SentryClient) error {
	return nil
}

func (cs *ControlServerImpl) getBlockHeaders66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry direct.SentryClient) error {
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
	//log.Info(fmt.Sprintf("[%s] GetBlockHeaderMsg{hash=%x, number=%d, amount=%d, skip=%d, reverse=%t, responseLen=%d}", ConvertH256ToPeerID(inreq.PeerId), query.Origin.Hash, query.Origin.Number, query.Amount, query.Skip, query.Reverse, len(b)))
	return nil
}

func (cs *ControlServerImpl) getBlockBodies66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry direct.SentryClient) error {
	var query eth.GetBlockBodiesPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding getBlockBodies66: %w, data: %x", err, inreq.Data)
	}
	tx, err := cs.db.BeginRo(ctx)
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
	//log.Info(fmt.Sprintf("[%s] GetBlockBodiesMsg responseLen %d", ConvertH256ToPeerID(inreq.PeerId), len(b)))
	return nil
}

func (cs *ControlServerImpl) getReceipts66(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry direct.SentryClient) error {
	var query eth.GetReceiptsPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding getReceipts66: %w, data: %x", err, inreq.Data)
	}
	tx, err := cs.db.BeginRo(ctx)
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
		return fmt.Errorf("encode header response: %w", err)
	}
	outreq := proto_sentry.SendMessageByIdRequest{
		PeerId: inreq.PeerId,
		Data: &proto_sentry.OutboundMessageData{
			Id:   proto_sentry.MessageId_RECEIPTS_66,
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
	//log.Info(fmt.Sprintf("[%s] GetReceipts responseLen %d", ConvertH256ToPeerID(inreq.PeerId), len(b)))
	return nil
}

func (cs *ControlServerImpl) HandleInboundMessage(ctx context.Context, inreq *proto_sentry.InboundMessage, sentry direct.SentryClient) error {
	switch inreq.Id {
	// ========= eth 66 ==========

	case proto_sentry.MessageId_NEW_BLOCK_HASHES_66:
		return cs.newBlockHashes66(ctx, inreq, sentry)
	case proto_sentry.MessageId_BLOCK_HEADERS_66:
		return cs.blockHeaders66(ctx, inreq, sentry)
	case proto_sentry.MessageId_NEW_BLOCK_66:
		return cs.newBlock66(ctx, inreq, sentry)
	case proto_sentry.MessageId_BLOCK_BODIES_66:
		return cs.blockBodies66(inreq, sentry)
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

	dialOpts = append(dialOpts, grpc.WithInsecure())
	conn, err := grpc.DialContext(ctx, sentryAddr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating client connection to sentry P2P: %w", err)
	}
	return direct.NewSentryClientRemote(proto_sentry.NewSentryClient(conn)), nil
}
