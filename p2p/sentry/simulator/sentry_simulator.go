package simulator

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"

	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	isentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/cmd/snapshots/sync"
	coresnaptype "github.com/ledgerwatch/erigon/core/snaptype"
	coretypes "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/discover/v4wire"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/sentry"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
)

type server struct {
	isentry.UnimplementedSentryServer
	ctx              context.Context
	peers            map[[64]byte]*p2p.Peer
	messageReceivers map[isentry.MessageId][]isentry.Sentry_MessagesServer
	logger           log.Logger
	knownSnapshots   *freezeblocks.RoSnapshots
	activeSnapshots  *freezeblocks.RoSnapshots
	blockReader      *freezeblocks.BlockReader
	downloader       *sync.TorrentClient
	chain            string
}

func newPeer(name string, caps []p2p.Cap) (*p2p.Peer, error) {
	key, err := crypto.GenerateKey()

	if err != nil {
		return nil, err
	}

	return p2p.NewPeer(enode.PubkeyToIDV4(&key.PublicKey), v4wire.EncodePubkey(&key.PublicKey), name, caps, true), nil
}

func NewSentry(ctx context.Context, chain string, snapshotLocation string, peerCount int, logger log.Logger) (isentry.SentryServer, error) {
	peers := map[[64]byte]*p2p.Peer{}

	for i := 0; i < peerCount; i++ {
		peer, err := newPeer(fmt.Sprint("peer-", i), nil)

		if err != nil {
			return nil, err
		}
		peers[peer.Pubkey()] = peer
	}

	cfg := snapcfg.KnownCfg(chain)
	torrentDir := filepath.Join(snapshotLocation, "torrents", chain)

	knownSnapshots := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{
		Enabled:      true,
		Produce:      false,
		NoDownloader: true,
	}, "", 0, logger)

	files := make([]string, 0, len(cfg.Preverified))

	for _, item := range cfg.Preverified {
		files = append(files, item.Name)
	}

	knownSnapshots.InitSegments(files)

	//s.knownSnapshots.ReopenList([]string{ent2.Name()}, false)
	activeSnapshots := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{
		Enabled:      true,
		Produce:      false,
		NoDownloader: true,
	}, torrentDir, 0, logger)

	if err := activeSnapshots.ReopenFolder(); err != nil {
		return nil, err
	}

	config := sync.NewDefaultTorrentClientConfig(chain, snapshotLocation, logger)
	downloader, err := sync.NewTorrentClient(config)

	if err != nil {
		return nil, err
	}

	s := &server{
		ctx:              ctx,
		peers:            peers,
		messageReceivers: map[isentry.MessageId][]isentry.Sentry_MessagesServer{},
		knownSnapshots:   knownSnapshots,
		activeSnapshots:  activeSnapshots,
		blockReader:      freezeblocks.NewBlockReader(activeSnapshots, nil),
		logger:           logger,
		downloader:       downloader,
		chain:            chain,
	}

	go func() {
		<-ctx.Done()
		s.Close()
	}()

	return s, nil
}

func (s *server) Close() {
	_ = s.downloader.Close()
	s.activeSnapshots.Close()
}

func (s *server) NodeInfo(context.Context, *emptypb.Empty) (*types.NodeInfoReply, error) {
	return nil, fmt.Errorf("TODO")
}

func (s *server) PeerById(ctx context.Context, in *isentry.PeerByIdRequest) (*isentry.PeerByIdReply, error) {
	peerId := sentry.ConvertH512ToPeerID(in.PeerId)

	peer, ok := s.peers[peerId]

	if !ok {
		return nil, fmt.Errorf("unknown peer")
	}

	info := peer.Info()

	return &isentry.PeerByIdReply{
		Peer: &types.PeerInfo{
			Id:             info.ID,
			Name:           info.Name,
			Enode:          info.Enode,
			Enr:            info.ENR,
			Caps:           info.Caps,
			ConnLocalAddr:  info.Network.LocalAddress,
			ConnRemoteAddr: info.Network.RemoteAddress,
			ConnIsInbound:  info.Network.Inbound,
			ConnIsTrusted:  info.Network.Trusted,
			ConnIsStatic:   info.Network.Static,
		},
	}, nil
}

func (s *server) PeerCount(context.Context, *isentry.PeerCountRequest) (*isentry.PeerCountReply, error) {
	return &isentry.PeerCountReply{Count: uint64(len(s.peers))}, nil
}

func (s *server) PeerEvents(*isentry.PeerEventsRequest, isentry.Sentry_PeerEventsServer) error {
	return fmt.Errorf("TODO")
}

func (s *server) PeerMinBlock(context.Context, *isentry.PeerMinBlockRequest) (*emptypb.Empty, error) {
	return nil, fmt.Errorf("TODO")
}

func (s *server) Peers(context.Context, *emptypb.Empty) (*isentry.PeersReply, error) {
	reply := &isentry.PeersReply{}

	for _, peer := range s.peers {
		info := peer.Info()

		reply.Peers = append(reply.Peers,
			&types.PeerInfo{
				Id:             info.ID,
				Name:           info.Name,
				Enode:          info.Enode,
				Enr:            info.ENR,
				Caps:           info.Caps,
				ConnLocalAddr:  info.Network.LocalAddress,
				ConnRemoteAddr: info.Network.RemoteAddress,
				ConnIsInbound:  info.Network.Inbound,
				ConnIsTrusted:  info.Network.Trusted,
				ConnIsStatic:   info.Network.Static,
			})
	}

	return reply, nil
}

func (s *server) SendMessageById(ctx context.Context, in *isentry.SendMessageByIdRequest) (*isentry.SentPeers, error) {
	peerId := sentry.ConvertH512ToPeerID(in.PeerId)

	if err := s.sendMessageById(ctx, peerId, in.Data); err != nil {
		return nil, err
	}

	return &isentry.SentPeers{
		Peers: []*types.H512{in.PeerId},
	}, nil
}

func (s *server) sendMessageById(ctx context.Context, peerId [64]byte, messageData *isentry.OutboundMessageData) error {
	peer, ok := s.peers[peerId]

	if !ok {
		return fmt.Errorf("unknown peer")
	}

	switch messageData.Id {
	case isentry.MessageId_GET_BLOCK_HEADERS_65:
		packet := &eth.GetBlockHeadersPacket{}
		if err := rlp.DecodeBytes(messageData.Data, packet); err != nil {
			return fmt.Errorf("failed to decode packet: %w", err)
		}

		go s.processGetBlockHeaders(ctx, peer, 0, packet)

	case isentry.MessageId_GET_BLOCK_HEADERS_66:
		packet := &eth.GetBlockHeadersPacket66{}
		if err := rlp.DecodeBytes(messageData.Data, packet); err != nil {
			return fmt.Errorf("failed to decode packet: %w", err)
		}

		go s.processGetBlockHeaders(ctx, peer, packet.RequestId, packet.GetBlockHeadersPacket)

	default:
		return fmt.Errorf("unhandled message id: %s", messageData.Id)
	}

	return nil
}

func (s *server) SendMessageByMinBlock(ctx context.Context, request *isentry.SendMessageByMinBlockRequest) (*isentry.SentPeers, error) {
	return s.UnimplementedSentryServer.SendMessageByMinBlock(ctx, request)
}

func (s *server) SendMessageToAll(ctx context.Context, data *isentry.OutboundMessageData) (*isentry.SentPeers, error) {
	sentPeers := &isentry.SentPeers{}

	for _, peer := range s.peers {
		peerKey := peer.Pubkey()

		if err := s.sendMessageById(ctx, peerKey, data); err != nil {
			return sentPeers, err
		}

		sentPeers.Peers = append(sentPeers.Peers, gointerfaces.ConvertBytesToH512(peerKey[:]))
	}

	return sentPeers, nil
}

func (s *server) SendMessageToRandomPeers(ctx context.Context, request *isentry.SendMessageToRandomPeersRequest) (*isentry.SentPeers, error) {
	sentPeers := &isentry.SentPeers{}

	var i uint64

	for _, peer := range s.peers {
		peerKey := peer.Pubkey()

		if err := s.sendMessageById(ctx, peerKey, request.Data); err != nil {
			return sentPeers, err
		}

		sentPeers.Peers = append(sentPeers.Peers, gointerfaces.ConvertBytesToH512(peerKey[:]))

		i++

		if i == request.MaxPeers {
			break
		}
	}

	return sentPeers, nil

}

func (s *server) Messages(request *isentry.MessagesRequest, receiver isentry.Sentry_MessagesServer) error {
	for _, messageId := range request.Ids {
		receivers := s.messageReceivers[messageId]
		s.messageReceivers[messageId] = append(receivers, receiver)
	}

	<-s.ctx.Done()

	return nil
}

func (s *server) processGetBlockHeaders(ctx context.Context, peer *p2p.Peer, requestId uint64, request *eth.GetBlockHeadersPacket) {
	r65 := s.messageReceivers[isentry.MessageId_BLOCK_HEADERS_65]
	r66 := s.messageReceivers[isentry.MessageId_BLOCK_HEADERS_66]

	if len(r65)+len(r66) > 0 {

		peerKey := peer.Pubkey()
		peerId := gointerfaces.ConvertBytesToH512(peerKey[:])

		headers, err := s.getHeaders(ctx, request.Origin, request.Amount, request.Skip, request.Reverse)

		if err != nil {
			s.logger.Warn("Can't get headers", "error", err)
			return
		}

		if len(r65) > 0 {
			var data bytes.Buffer

			err := rlp.Encode(&data, headers)

			if err != nil {
				s.logger.Warn("Can't encode headers", "error", err)
				return
			}

			for _, receiver := range r65 {
				receiver.Send(&isentry.InboundMessage{
					Id:     isentry.MessageId_BLOCK_HEADERS_65,
					Data:   data.Bytes(),
					PeerId: peerId,
				})
			}
		}

		if len(r66) > 0 {
			var data bytes.Buffer

			err := rlp.Encode(&data, &eth.BlockHeadersPacket66{
				RequestId:          requestId,
				BlockHeadersPacket: headers,
			})

			if err != nil {
				fmt.Printf("Error (move to logger): %s", err)
				return
			}

			for _, receiver := range r66 {
				receiver.Send(&isentry.InboundMessage{
					Id:     isentry.MessageId_BLOCK_HEADERS_66,
					Data:   data.Bytes(),
					PeerId: peerId,
				})
			}
		}
	}
}

func (s *server) getHeaders(ctx context.Context, origin eth.HashOrNumber, amount uint64, skip uint64, reverse bool) (eth.BlockHeadersPacket, error) {

	var headers eth.BlockHeadersPacket

	var next uint64

	nextBlockNum := func(blockNum uint64) uint64 {
		inc := uint64(1)

		if skip != 0 {
			inc = skip
		}

		if reverse {
			return blockNum - inc
		} else {
			return blockNum + inc
		}
	}

	if origin.Hash != (common.Hash{}) {
		header, err := s.getHeaderByHash(ctx, origin.Hash)

		if err != nil {
			return nil, err
		}

		headers = append(headers, header)

		next = nextBlockNum(header.Number.Uint64())
	} else {
		header, err := s.getHeader(ctx, origin.Number)

		if err != nil {
			return nil, err
		}

		headers = append(headers, header)

		next = nextBlockNum(header.Number.Uint64())
	}

	for len(headers) < int(amount) {
		header, err := s.getHeader(ctx, next)

		if err != nil {
			return nil, err
		}

		headers = append(headers, header)

		next = nextBlockNum(header.Number.Uint64())
	}

	return headers, nil
}

func (s *server) getHeader(ctx context.Context, blockNum uint64) (*coretypes.Header, error) {
	header, err := s.blockReader.Header(ctx, nil, common.Hash{}, blockNum)

	if err != nil {
		return nil, err
	}

	if header == nil {
		view := s.knownSnapshots.View()
		defer view.Close()

		if seg, ok := view.HeadersSegment(blockNum); ok {
			if err := s.downloadHeaders(ctx, seg); err != nil {
				return nil, err
			}
		}

		s.activeSnapshots.ReopenSegments([]snaptype.Type{coresnaptype.Headers}, true)

		header, err = s.blockReader.Header(ctx, nil, common.Hash{}, blockNum)

		if err != nil {
			return nil, err
		}
	}

	return header, nil
}

func (s *server) getHeaderByHash(ctx context.Context, hash common.Hash) (*coretypes.Header, error) {
	return s.blockReader.HeaderByHash(ctx, nil, hash)
}

func (s *server) downloadHeaders(ctx context.Context, header *freezeblocks.Segment) error {
	fileName := snaptype.SegmentFileName(0, header.From(), header.To(), coresnaptype.Enums.Headers)
	session := sync.NewTorrentSession(s.downloader, s.chain)

	s.logger.Info("Downloading", "file", fileName)

	err := session.Download(ctx, fileName)

	if err != nil {
		return fmt.Errorf("can't download %s: %w", fileName, err)
	}

	s.logger.Info("Indexing", "file", fileName)

	info, _, _ := snaptype.ParseFileName(session.LocalFsRoot(), fileName)

	return coresnaptype.Headers.BuildIndexes(ctx, info, nil, session.LocalFsRoot(), nil, log.LvlDebug, s.logger)
}
