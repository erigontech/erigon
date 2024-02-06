package sentry

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	protosentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	prototypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

type sentryMessageStream grpc.ClientStream
type sentryMessageStreamFactory func(context.Context, direct.SentryClient) (sentryMessageStream, error)

// Chief of sentries.
//
// Provides a higher-level abstraction over sentry clients.
//   - Handles request/response/subscriptions to multiple sentries
//   - Each sentry may support same or different p2p protocol
//   - Provides a set of higher-level functionalities that abstract the
//     async nature of devp2p - useful for downloading data in a simpler fashion
type Chief struct {
	lock                              sync.RWMutex
	Hd                                *headerdownload.HeaderDownload
	Bd                                *bodydownload.BodyDownload
	IsMock                            bool
	nodeName                          string
	sentries                          []direct.SentryClient
	headHeight                        uint64
	headTime                          uint64
	headHash                          libcommon.Hash
	headTd                            *uint256.Int
	ChainConfig                       *chain.Config
	heightForks                       []uint64
	timeForks                         []uint64
	genesisHash                       libcommon.Hash
	networkId                         uint64
	db                                kv.RwDB
	Engine                            consensus.Engine
	blockReader                       services.FullBlockReader
	logPeerInfo                       bool
	sendHeaderRequestsToMultiplePeers bool
	maxBlockBroadcastPeers            func(*types.Header) uint
	historyV3                         bool
	logger                            log.Logger
}

func NewChief(
	db kv.RwDB,
	nodeName string,
	chainConfig *chain.Config,
	genesisHash libcommon.Hash,
	genesisTime uint64,
	engine consensus.Engine,
	networkID uint64,
	sentries []direct.SentryClient,
	syncCfg ethconfig.Sync,
	blockReader services.FullBlockReader,
	blockBufferSize int,
	logPeerInfo bool,
	maxBlockBroadcastPeers func(*types.Header) uint,
	logger log.Logger,
) (*Chief, error) {
	historyV3 := kvcfg.HistoryV3.FromDB(db)
	hd := headerdownload.NewHeaderDownload(
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

	bd := bodydownload.NewBodyDownload(engine, blockBufferSize, int(syncCfg.BodyCacheLimit), blockReader, logger)
	chief := &Chief{
		nodeName:                          nodeName,
		Hd:                                hd,
		Bd:                                bd,
		sentries:                          sentries,
		db:                                db,
		Engine:                            engine,
		blockReader:                       blockReader,
		logPeerInfo:                       logPeerInfo,
		historyV3:                         historyV3,
		sendHeaderRequestsToMultiplePeers: chainConfig.TerminalTotalDifficultyPassed,
		maxBlockBroadcastPeers:            maxBlockBroadcastPeers,
		logger:                            logger,
	}
	chief.ChainConfig = chainConfig
	chief.heightForks, chief.timeForks = forkid.GatherForks(chief.ChainConfig, genesisTime)
	chief.genesisHash = genesisHash
	chief.networkId = networkID
	var err error
	err = db.View(context.Background(), func(tx kv.Tx) error {
		chief.headHeight, chief.headTime, chief.headHash, chief.headTd, err = chief.Bd.UpdateFromDb(tx)
		return err
	})
	return chief, err
}

func (c *Chief) Sentries() []direct.SentryClient {
	return c.sentries
}

// StartStreamLoops starts message processing loops for all sentries.
// The processing happens in several streams:
// RecvMessage - processing incoming headers/bodies
// RecvUploadMessage - sending bodies/receipts - may be heavy, it's ok to not process this messages enough fast, it's also ok to drop some of these messages if we can't process.
// RecvUploadHeadersMessage - sending headers - dedicated stream because headers propagation speed important for network health
// PeerEventsLoop - logging peer connect/disconnect events
func (c *Chief) StartStreamLoops(ctx context.Context) {
	sentries := c.Sentries()
	for i := range sentries {
		sentry := sentries[i]
		go c.RecvMessageLoop(ctx, sentry, nil)
		go c.RecvUploadMessageLoop(ctx, sentry, nil)
		go c.RecvUploadHeadersMessageLoop(ctx, sentry, nil)
		go c.PeerEventsLoop(ctx, sentry, nil)
	}
}

func (c *Chief) RecvUploadMessageLoop(ctx context.Context, sentry direct.SentryClient, wg *sync.WaitGroup) {
	ids := []protosentry.MessageId{
		eth.ToProto[direct.ETH66][eth.GetBlockBodiesMsg],
		eth.ToProto[direct.ETH66][eth.GetReceiptsMsg],
	}

	streamFactory := func(streamCtx context.Context, sentry direct.SentryClient) (sentryMessageStream, error) {
		return sentry.Messages(streamCtx, &protosentry.MessagesRequest{Ids: ids}, grpc.WaitForReady(true))
	}

	sentryReconnectAndPumpStreamLoop(
		ctx,
		sentry,
		c.makeStatusData,
		"RecvUploadMessage",
		streamFactory,
		makeInboundMessage,
		c.HandleInboundMessage,
		wg,
		c.logger,
	)
}

func (c *Chief) RecvUploadHeadersMessageLoop(ctx context.Context, sentry direct.SentryClient, wg *sync.WaitGroup) {
	ids := []protosentry.MessageId{
		eth.ToProto[direct.ETH66][eth.GetBlockHeadersMsg],
	}

	streamFactory := func(streamCtx context.Context, sentry direct.SentryClient) (sentryMessageStream, error) {
		return sentry.Messages(streamCtx, &protosentry.MessagesRequest{Ids: ids}, grpc.WaitForReady(true))
	}

	sentryReconnectAndPumpStreamLoop(
		ctx,
		sentry,
		c.makeStatusData,
		"RecvUploadHeadersMessage",
		streamFactory,
		makeInboundMessage,
		c.HandleInboundMessage,
		wg,
		c.logger,
	)
}

func (c *Chief) RecvMessageLoop(ctx context.Context, sentry direct.SentryClient, wg *sync.WaitGroup) {
	ids := []protosentry.MessageId{
		eth.ToProto[direct.ETH66][eth.BlockHeadersMsg],
		eth.ToProto[direct.ETH66][eth.BlockBodiesMsg],
		eth.ToProto[direct.ETH66][eth.NewBlockHashesMsg],
		eth.ToProto[direct.ETH66][eth.NewBlockMsg],
	}

	streamFactory := func(streamCtx context.Context, sentry direct.SentryClient) (sentryMessageStream, error) {
		return sentry.Messages(streamCtx, &protosentry.MessagesRequest{Ids: ids}, grpc.WaitForReady(true))
	}

	sentryReconnectAndPumpStreamLoop(
		ctx,
		sentry,
		c.makeStatusData,
		"RecvMessage",
		streamFactory,
		makeInboundMessage,
		c.HandleInboundMessage,
		wg,
		c.logger,
	)
}

func (c *Chief) PeerEventsLoop(ctx context.Context, sentry direct.SentryClient, wg *sync.WaitGroup) {
	streamFactory := func(streamCtx context.Context, sentry direct.SentryClient) (sentryMessageStream, error) {
		return sentry.PeerEvents(streamCtx, &protosentry.PeerEventsRequest{}, grpc.WaitForReady(true))
	}

	messageFactory := func() *protosentry.PeerEvent {
		return new(protosentry.PeerEvent)
	}

	sentryReconnectAndPumpStreamLoop(
		ctx,
		sentry,
		c.makeStatusData,
		"PeerEvents",
		streamFactory,
		messageFactory,
		c.HandlePeerEvent,
		wg,
		c.logger,
	)
}

func (c *Chief) UpdateHead(ctx context.Context, height, time uint64, hash libcommon.Hash, td *uint256.Int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.headHeight = height
	c.headTime = time
	c.headHash = hash
	c.headTd = td
	statusMsg := c.makeStatusData()
	for _, sentry := range c.sentries {
		if !sentry.Ready() {
			continue
		}

		if _, err := sentry.SetStatus(ctx, statusMsg, &grpc.EmptyCallOption{}); err != nil {
			c.logger.Error("Update status message for the sentry", "err", err)
		}
	}
}

func (c *Chief) SendBodyRequest(ctx context.Context, req *bodydownload.BodyRequest) (peerID [64]byte, ok bool) {
	// if sentry not found peers to send such message, try next one. stop if found.
	for i, ok, next := c.randSentryIndex(); ok; i, ok = next() {
		if !c.sentries[i].Ready() {
			continue
		}

		//log.Info(fmt.Sprintf("Sending body request for %v", req.BlockNums))
		var bytes []byte
		var err error
		bytes, err = rlp.EncodeToBytes(&eth.GetBlockBodiesPacket66{
			RequestId:            rand.Uint64(), // nolint: gosec
			GetBlockBodiesPacket: req.Hashes,
		})
		if err != nil {
			c.logger.Error("Could not encode block bodies request", "err", err)
			return [64]byte{}, false
		}
		outreq := protosentry.SendMessageByMinBlockRequest{
			MinBlock: req.BlockNums[len(req.BlockNums)-1],
			Data: &protosentry.OutboundMessageData{
				Id:   protosentry.MessageId_GET_BLOCK_BODIES_66,
				Data: bytes,
			},
			MaxPeers: 1,
		}

		sentPeers, err1 := c.sentries[i].SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
		if err1 != nil {
			c.logger.Error("Could not send block bodies request", "err", err1)
			return [64]byte{}, false
		}
		if sentPeers == nil || len(sentPeers.Peers) == 0 {
			continue
		}
		return ConvertH512ToPeerID(sentPeers.Peers[0]), true
	}
	return [64]byte{}, false
}

func (c *Chief) SendHeaderRequest(ctx context.Context, req *headerdownload.HeaderRequest) (peerID [64]byte, ok bool) {
	// if sentry not found peers to send such message, try next one. stop if found.
	for i, ok, next := c.randSentryIndex(); ok; i, ok = next() {
		if !c.sentries[i].Ready() {
			continue
		}
		reqData := &eth.GetBlockHeadersPacket66{
			RequestId: rand.Uint64(), // nolint: gosec
			GetBlockHeadersPacket: &eth.GetBlockHeadersPacket{
				Amount:  req.Length,
				Reverse: req.Reverse,
				Skip:    req.Skip,
				Origin:  eth.HashOrNumber{Hash: req.Hash},
			},
		}
		if req.Hash == (libcommon.Hash{}) {
			reqData.Origin.Number = req.Number
		}
		bytes, err := rlp.EncodeToBytes(reqData)
		if err != nil {
			c.logger.Error("Could not encode header request", "err", err)
			return [64]byte{}, false
		}
		minBlock := req.Number

		outreq := protosentry.SendMessageByMinBlockRequest{
			MinBlock: minBlock,
			Data: &protosentry.OutboundMessageData{
				Id:   protosentry.MessageId_GET_BLOCK_HEADERS_66,
				Data: bytes,
			},
			MaxPeers: 5,
		}
		sentPeers, err1 := c.sentries[i].SendMessageByMinBlock(ctx, &outreq, &grpc.EmptyCallOption{})
		if err1 != nil {
			c.logger.Error("Could not send header request", "err", err1)
			return [64]byte{}, false
		}
		if sentPeers == nil || len(sentPeers.Peers) == 0 {
			continue
		}
		return ConvertH512ToPeerID(sentPeers.Peers[0]), true
	}
	return [64]byte{}, false
}

// Penalize sends a list of penalties to all sentries
func (c *Chief) Penalize(ctx context.Context, penalties []headerdownload.PenaltyItem) {
	for i := range penalties {
		outreq := protosentry.PenalizePeerRequest{
			PeerId:  gointerfaces.ConvertHashToH512(penalties[i].PeerID),
			Penalty: protosentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
		}
		for i, ok, next := c.randSentryIndex(); ok; i, ok = next() {
			if !c.sentries[i].Ready() {
				continue
			}

			if _, err1 := c.sentries[i].PenalizePeer(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
				c.logger.Error("Could not send penalty", "err", err1)
			}
		}
	}
}

func (c *Chief) PropagateNewBlockHashes(ctx context.Context, announces []headerdownload.Announce) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	typedRequest := make(eth.NewBlockHashesPacket, len(announces))
	for i := range announces {
		typedRequest[i].Hash = announces[i].Hash
		typedRequest[i].Number = announces[i].Number
	}

	data, err := rlp.EncodeToBytes(&typedRequest)
	if err != nil {
		log.Error("propagateNewBlockHashes", "err", err)
		return
	}

	req66 := protosentry.OutboundMessageData{
		Id:   protosentry.MessageId_NEW_BLOCK_HASHES_66,
		Data: data,
	}

	for _, sentry := range c.sentries {
		if !sentry.Ready() {
			continue
		}

		_, err = sentry.SendMessageToAll(ctx, &req66, &grpc.EmptyCallOption{})
		if err != nil {
			log.Error("propagateNewBlockHashes", "err", err)
		}
	}
}

func (c *Chief) BroadcastNewBlock(ctx context.Context, header *types.Header, body *types.RawBody, td *big.Int) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	block, err := types.RawBlock{Header: header, Body: body}.AsBlock()

	if err != nil {
		log.Error("broadcastNewBlock", "err", err)
	}

	data, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: block,
		TD:    td,
	})

	if err != nil {
		log.Error("broadcastNewBlock", "err", err)
		return
	}

	req66 := protosentry.SendMessageToRandomPeersRequest{
		MaxPeers: uint64(c.maxBlockBroadcastPeers(header)),
		Data: &protosentry.OutboundMessageData{
			Id:   protosentry.MessageId_NEW_BLOCK_66,
			Data: data,
		},
	}

	for _, sentry := range c.sentries {
		if !sentry.Ready() {
			continue
		}

		_, err = sentry.SendMessageToRandomPeers(ctx, &req66, &grpc.EmptyCallOption{})
		if err != nil {
			if isPeerNotFoundErr(err) || networkTemporaryErr(err) {
				log.Debug("broadcastNewBlock", "err", err)
				continue
			}
			log.Error("broadcastNewBlock", "err", err)
		}
	}
}

func (c *Chief) HandlePeerEvent(ctx context.Context, event *protosentry.PeerEvent, sentryClient direct.SentryClient) error {
	eventID := event.EventId.String()
	peerID := ConvertH512ToPeerID(event.PeerId)
	peerIDStr := hex.EncodeToString(peerID[:])

	if !c.logPeerInfo {
		c.logger.Trace("[p2p] Sentry peer did", "eventID", eventID, "peer", peerIDStr)
		return nil
	}

	var nodeURL string
	var clientID string
	var capabilities []string
	if event.EventId == protosentry.PeerEvent_Connect {
		reply, err := sentryClient.PeerById(ctx, &protosentry.PeerByIdRequest{PeerId: event.PeerId})
		if err != nil {
			c.logger.Debug("sentry.PeerById failed", "err", err)
		}
		if (reply != nil) && (reply.Peer != nil) {
			nodeURL = reply.Peer.Enode
			clientID = reply.Peer.Name
			capabilities = reply.Peer.Caps
		}
	}

	c.logger.Trace(
		"[p2p] Sentry peer did",
		"eventID", eventID,
		"peer", peerIDStr,
		"nodeURL", nodeURL,
		"clientID", clientID,
		"capabilities", capabilities,
	)

	return nil
}

func (c *Chief) HandleInboundMessage(ctx context.Context, message *protosentry.InboundMessage, sentry direct.SentryClient) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, msgID=%s, trace: %s", rec, message.Id.String(), dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	err = c.handleInboundMessage(ctx, message, sentry)

	if (err != nil) && rlp.IsInvalidRLPError(err) {
		c.logger.Debug("Kick peer for invalid RLP", "err", err)
		penalizeRequest := protosentry.PenalizePeerRequest{
			PeerId:  message.PeerId,
			Penalty: protosentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
		}
		if _, err1 := sentry.PenalizePeer(ctx, &penalizeRequest, &grpc.EmptyCallOption{}); err1 != nil {
			c.logger.Error("Could not send penalty", "err", err1)
		}
	}

	return err
}

func (c *Chief) handleInboundMessage(ctx context.Context, inreq *protosentry.InboundMessage, sentry direct.SentryClient) error {
	switch inreq.Id {
	// ========= eth 66 ==========

	case protosentry.MessageId_NEW_BLOCK_HASHES_66:
		return c.newBlockHashes66(ctx, inreq, sentry)
	case protosentry.MessageId_BLOCK_HEADERS_66:
		return c.blockHeaders66(ctx, inreq, sentry)
	case protosentry.MessageId_NEW_BLOCK_66:
		return c.newBlock66(ctx, inreq, sentry)
	case protosentry.MessageId_BLOCK_BODIES_66:
		return c.blockBodies66(ctx, inreq, sentry)
	case protosentry.MessageId_GET_BLOCK_HEADERS_66:
		return c.getBlockHeaders66(ctx, inreq, sentry)
	case protosentry.MessageId_GET_BLOCK_BODIES_66:
		return c.getBlockBodies66(ctx, inreq, sentry)
	case protosentry.MessageId_RECEIPTS_66:
		return c.receipts66(ctx, inreq, sentry)
	case protosentry.MessageId_GET_RECEIPTS_66:
		return c.getReceipts66(ctx, inreq, sentry)
	default:
		return fmt.Errorf("not implemented for message Id: %s", inreq.Id)
	}
}

func (c *Chief) newBlockHashes66(ctx context.Context, req *protosentry.InboundMessage, sentryClient direct.SentryClient) error {
	if c.Hd.InitialCycle() && !c.Hd.FetchingNew() {
		return nil
	}
	var request eth.NewBlockHashesPacket
	if err := rlp.DecodeBytes(req.Data, &request); err != nil {
		return fmt.Errorf("decode NewBlockHashes66: %w", err)
	}
	for _, announce := range request {
		c.Hd.SaveExternalAnnounce(announce.Hash)
		if c.Hd.HasLink(announce.Hash) {
			continue
		}
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
		outreq := protosentry.SendMessageByIdRequest{
			PeerId: req.PeerId,
			Data: &protosentry.OutboundMessageData{
				Id:   protosentry.MessageId_GET_BLOCK_HEADERS_66,
				Data: b,
			},
		}

		if _, err = sentryClient.SendMessageById(ctx, &outreq, &grpc.EmptyCallOption{}); err != nil {
			if isPeerNotFoundErr(err) {
				continue
			}
			return fmt.Errorf("send header request: %w", err)
		}
	}
	return nil
}

func (c *Chief) blockHeaders66(ctx context.Context, in *protosentry.InboundMessage, sentryClient direct.SentryClient) error {
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
	return c.blockHeaders(ctx, pkt.BlockHeadersPacket, rlpStream, in.PeerId, sentryClient)
}

func (c *Chief) blockHeaders(ctx context.Context, pkt eth.BlockHeadersPacket, rlpStream *rlp.Stream, peerID *prototypes.H512, sentryClient direct.SentryClient) error {
	if len(pkt) == 0 {
		// No point processing empty response
		return nil
	}
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
	}
	if c.Hd.POSSync() {
		sort.Sort(headerdownload.HeadersReverseSort(csHeaders)) // Sorting by reverse order of block heights
		tx, err := c.db.BeginRo(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		penalties, err := c.Hd.ProcessHeadersPOS(csHeaders, tx, ConvertH512ToPeerID(peerID))
		if err != nil {
			return err
		}
		if len(penalties) > 0 {
			c.Penalize(ctx, penalties)
		}
	} else {
		sort.Sort(headerdownload.HeadersSort(csHeaders)) // Sorting by order of block heights
		canRequestMore := c.Hd.ProcessHeaders(csHeaders, false /* newBlock */, ConvertH512ToPeerID(peerID))

		if canRequestMore {
			currentTime := time.Now()
			req, penalties := c.Hd.RequestMoreHeaders(currentTime)
			if req != nil {
				if peer, sentToPeer := c.SendHeaderRequest(ctx, req); sentToPeer {
					c.Hd.UpdateStats(req, false /* skeleton */, peer)
					c.Hd.UpdateRetryTime(req, currentTime, 5*time.Second /* timeout */)
				}
			}
			if len(penalties) > 0 {
				c.Penalize(ctx, penalties)
			}
		}
	}
	outreq := protosentry.PeerMinBlockRequest{
		PeerId:   peerID,
		MinBlock: highestBlock,
	}
	if _, err1 := sentryClient.PeerMinBlock(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
		c.logger.Error("Could not send min block for peer", "err", err1)
	}
	return nil
}

func (c *Chief) newBlock66(ctx context.Context, inreq *protosentry.InboundMessage, sentryClient direct.SentryClient) error {
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

	if segments, penalty, err := c.Hd.SingleHeaderAsSegment(headerRaw, request.Block.Header(), true /* penalizePoSBlocks */); err == nil {
		if penalty == headerdownload.NoPenalty {
			propagate := !c.ChainConfig.TerminalTotalDifficultyPassed
			// Do not propagate blocks who are post TTD
			firstPosSeen := c.Hd.FirstPoSHeight()
			if firstPosSeen != nil && propagate {
				propagate = *firstPosSeen >= segments[0].Number
			}
			if !c.IsMock && propagate {
				c.PropagateNewBlockHashes(ctx, []headerdownload.Announce{
					{
						Number: segments[0].Number,
						Hash:   segments[0].Hash,
					},
				})
			}

			c.Hd.ProcessHeaders(segments, true /* newBlock */, ConvertH512ToPeerID(inreq.PeerId)) // There is only one segment in this case
		} else {
			outreq := protosentry.PenalizePeerRequest{
				PeerId:  inreq.PeerId,
				Penalty: protosentry.PenaltyKind_Kick, // TODO: Extend penalty kinds
			}
			for _, sentry := range c.sentries {
				if !sentry.Ready() {
					continue
				}
				if _, err1 := sentry.PenalizePeer(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
					c.logger.Error("Could not send penalty", "err", err1)
				}
			}
		}
	} else {
		return fmt.Errorf("singleHeaderAsSegment failed: %w", err)
	}
	c.Bd.AddToPrefetch(request.Block.Header(), request.Block.RawBody())
	outreq := protosentry.PeerMinBlockRequest{
		PeerId:   inreq.PeerId,
		MinBlock: request.Block.NumberU64(),
	}
	if _, err1 := sentryClient.PeerMinBlock(ctx, &outreq, &grpc.EmptyCallOption{}); err1 != nil {
		c.logger.Error("Could not send min block for peer", "err", err1)
	}
	c.logger.Trace(fmt.Sprintf("NewBlockMsg{blockNumber: %d} from [%s]", request.Block.NumberU64(), ConvertH512ToPeerID(inreq.PeerId)))
	return nil
}

func (c *Chief) blockBodies66(_ context.Context, inreq *protosentry.InboundMessage, _ direct.SentryClient) error {
	var request eth.BlockRawBodiesPacket66
	if err := rlp.DecodeBytes(inreq.Data, &request); err != nil {
		return fmt.Errorf("decode BlockBodiesPacket66: %w", err)
	}
	txs, uncles, withdrawals := request.BlockRawBodiesPacket.Unpack()
	if len(txs) == 0 && len(uncles) == 0 && len(withdrawals) == 0 {
		// No point processing empty response
		return nil
	}
	c.Bd.DeliverBodies(txs, uncles, withdrawals, uint64(len(inreq.Data)), ConvertH512ToPeerID(inreq.PeerId))
	return nil
}

func (c *Chief) receipts66(_ context.Context, _ *protosentry.InboundMessage, _ direct.SentryClient) error {
	return nil
}

func (c *Chief) getBlockHeaders66(ctx context.Context, inreq *protosentry.InboundMessage, sentry direct.SentryClient) error {
	var query eth.GetBlockHeadersPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding getBlockHeaders66: %w, data: %x", err, inreq.Data)
	}

	var headers []*types.Header
	if err := c.db.View(ctx, func(tx kv.Tx) (err error) {
		headers, err = eth.AnswerGetBlockHeadersQuery(tx, query.GetBlockHeadersPacket, c.blockReader)
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
	outreq := protosentry.SendMessageByIdRequest{
		PeerId: inreq.PeerId,
		Data: &protosentry.OutboundMessageData{
			Id:   protosentry.MessageId_BLOCK_HEADERS_66,
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
	return nil
}

func (c *Chief) getBlockBodies66(ctx context.Context, inreq *protosentry.InboundMessage, sentry direct.SentryClient) error {
	var query eth.GetBlockBodiesPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding getBlockBodies66: %w, data: %x", err, inreq.Data)
	}
	tx, err := c.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	response := eth.AnswerGetBlockBodiesQuery(tx, query.GetBlockBodiesPacket, c.blockReader)
	tx.Rollback()
	b, err := rlp.EncodeToBytes(&eth.BlockBodiesRLPPacket66{
		RequestId:            query.RequestId,
		BlockBodiesRLPPacket: response,
	})
	if err != nil {
		return fmt.Errorf("encode header response: %w", err)
	}
	outreq := protosentry.SendMessageByIdRequest{
		PeerId: inreq.PeerId,
		Data: &protosentry.OutboundMessageData{
			Id:   protosentry.MessageId_BLOCK_BODIES_66,
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
	return nil
}

func (c *Chief) getReceipts66(ctx context.Context, inreq *protosentry.InboundMessage, sentry direct.SentryClient) error {
	if c.historyV3 { // historyV3 doesn't store receipts in DB
		return nil
	}

	var query eth.GetReceiptsPacket66
	if err := rlp.DecodeBytes(inreq.Data, &query); err != nil {
		return fmt.Errorf("decoding getReceipts66: %w, data: %x", err, inreq.Data)
	}
	tx, err := c.db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	receipts, err := eth.AnswerGetReceiptsQuery(c.blockReader, tx, query.GetReceiptsPacket)
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
	outreq := protosentry.SendMessageByIdRequest{
		PeerId: inreq.PeerId,
		Data: &protosentry.OutboundMessageData{
			Id:   protosentry.MessageId_RECEIPTS_66,
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
	return nil
}

func makeInboundMessage() *protosentry.InboundMessage {
	return new(protosentry.InboundMessage)
}

func (c *Chief) makeStatusData() *protosentry.StatusData {
	s := c
	return &protosentry.StatusData{
		NetworkId:       s.networkId,
		TotalDifficulty: gointerfaces.ConvertUint256IntToH256(s.headTd),
		BestHash:        gointerfaces.ConvertHashToH256(s.headHash),
		MaxBlockHeight:  s.headHeight,
		MaxBlockTime:    s.headTime,
		ForkData: &protosentry.Forks{
			Genesis:     gointerfaces.ConvertHashToH256(s.genesisHash),
			HeightForks: s.heightForks,
			TimeForks:   s.timeForks,
		},
	}
}

func (c *Chief) randSentryIndex() (int, bool, func() (int, bool)) {
	var i int
	if len(c.sentries) > 1 {
		i = rand.Intn(len(c.sentries) - 1) // nolint: gosec
	}
	to := i
	return i, true, func() (int, bool) {
		i = (i + 1) % len(c.sentries)
		return i, i != to
	}
}

func networkTemporaryErr(err error) bool {
	return errors.Is(err, syscall.EPIPE) || errors.Is(err, p2p.ErrShuttingDown)
}

func isPeerNotFoundErr(err error) bool {
	return strings.Contains(err.Error(), "peer not found")
}

func sentryReconnectAndPumpStreamLoop[TMessage interface{}](
	ctx context.Context,
	sentry direct.SentryClient,
	statusDataFactory func() *protosentry.StatusData,
	streamName string,
	streamFactory sentryMessageStreamFactory,
	messageFactory func() TMessage,
	handleInboundMessage func(context.Context, TMessage, direct.SentryClient) error,
	wg *sync.WaitGroup,
	logger log.Logger,
) {
	for ctx.Err() == nil {
		if _, err := sentry.HandShake(ctx, &emptypb.Empty{}, grpc.WaitForReady(true)); err != nil {
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

		if _, err := sentry.SetStatus(ctx, statusDataFactory()); err != nil {
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

		if err := pumpStreamLoop(ctx, sentry, streamName, streamFactory, messageFactory, handleInboundMessage, wg, logger); err != nil {
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
	streamFactory sentryMessageStreamFactory,
	messageFactory func() TMessage,
	handleInboundMessage func(context.Context, TMessage, direct.SentryClient) error,
	wg *sync.WaitGroup,
	logger log.Logger,
) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer sentry.MarkDisconnected()

	// need to read all messages from Sentry as fast as we can, then:
	// - can group them or process in batch
	// - can have slow processing
	reqs := make(chan TMessage, 256)
	defer close(reqs)

	go func() {
		for req := range reqs {
			if err := handleInboundMessage(ctx, req, sentry); err != nil {
				logger.Debug("Handling incoming message", "stream", streamName, "err", err)
			}
			if wg != nil {
				wg.Done()
			}
		}
	}()

	stream, err := streamFactory(streamCtx, sentry)
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
