package simulator

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"

	"github.com/ledgerwatch/erigon-lib/chain/snapcfg"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/downloader/snaptype"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	sentry_if "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	core_types "github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/discover/v4wire"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/sentry"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"
)

type server struct {
	sentry_if.UnimplementedSentryServer
	ctx              context.Context
	peers            map[[64]byte]*p2p.Peer
	messageReceivers map[sentry_if.MessageId][]sentry_if.Sentry_MessagesServer
	logger           log.Logger
	knownSnapshots   *freezeblocks.RoSnapshots
	activeSnapshots  *freezeblocks.RoSnapshots
	blockReader      *freezeblocks.BlockReader
	downloader       *TorrentClient
}

func newPeer(name string, caps []p2p.Cap) (*p2p.Peer, error) {
	key, err := crypto.GenerateKey()

	if err != nil {
		return nil, err
	}

	return p2p.NewPeer(enode.PubkeyToIDV4(&key.PublicKey), v4wire.EncodePubkey(&key.PublicKey), name, caps, true), nil
}

func NewSentry(ctx context.Context, chain string, snapshotLocation string, peerCount int, logger log.Logger) (sentry_if.SentryServer, error) {
	peers := map[[64]byte]*p2p.Peer{}

	for i := 0; i < peerCount; i++ {
		peer, err := newPeer(fmt.Sprint("peer-", i), nil)

		if err != nil {
			return nil, err
		}
		peers[peer.Pubkey()] = peer
	}

	cfg := snapcfg.KnownCfg(chain, 0)

	knownSnapshots := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{
		Enabled:      true,
		Produce:      false,
		NoDownloader: true,
	}, "", cfg.Version, logger)

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
	}, snapshotLocation, cfg.Version, logger)

	if err := activeSnapshots.ReopenFolder(); err != nil {
		return nil, err
	}

	downloader, err := NewTorrentClient(ctx, chain, snapshotLocation, logger)

	if err != nil {
		return nil, err
	}

	s := &server{
		ctx:              ctx,
		peers:            peers,
		messageReceivers: map[sentry_if.MessageId][]sentry_if.Sentry_MessagesServer{},
		knownSnapshots:   knownSnapshots,
		activeSnapshots:  activeSnapshots,
		blockReader:      freezeblocks.NewBlockReader(activeSnapshots, nil),
		logger:           logger,
		downloader:       downloader,
	}

	go func() {
		<-ctx.Done()
		s.Close()
	}()

	return s, nil
}

func (s *server) Close() {
	s.downloader.Close()
	if closer, ok := s.downloader.cfg.DefaultStorage.(interface{ Close() error }); ok {
		closer.Close()
	}
	s.activeSnapshots.Close()
}

func (s *server) NodeInfo(context.Context, *emptypb.Empty) (*types.NodeInfoReply, error) {
	return nil, fmt.Errorf("TODO")
}

func (s *server) PeerById(ctx context.Context, in *sentry_if.PeerByIdRequest) (*sentry_if.PeerByIdReply, error) {
	peerId := sentry.ConvertH512ToPeerID(in.PeerId)

	peer, ok := s.peers[peerId]

	if !ok {
		return nil, fmt.Errorf("unknown peer")
	}

	info := peer.Info()

	return &sentry_if.PeerByIdReply{
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

func (s *server) PeerCount(context.Context, *sentry_if.PeerCountRequest) (*sentry_if.PeerCountReply, error) {
	return &sentry_if.PeerCountReply{Count: uint64(len(s.peers))}, nil
}

func (s *server) PeerEvents(*sentry_if.PeerEventsRequest, sentry_if.Sentry_PeerEventsServer) error {
	return fmt.Errorf("TODO")
}

func (s *server) PeerMinBlock(context.Context, *sentry_if.PeerMinBlockRequest) (*emptypb.Empty, error) {
	return nil, fmt.Errorf("TODO")
}

func (s *server) Peers(context.Context, *emptypb.Empty) (*sentry_if.PeersReply, error) {
	reply := &sentry_if.PeersReply{}

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

func (s *server) SendMessageById(ctx context.Context, in *sentry_if.SendMessageByIdRequest) (*sentry_if.SentPeers, error) {
	peerId := sentry.ConvertH512ToPeerID(in.PeerId)

	if err := s.sendMessageById(ctx, peerId, in.Data); err != nil {
		return nil, err
	}

	return &sentry_if.SentPeers{
		Peers: []*types.H512{in.PeerId},
	}, nil
}

func (s *server) sendMessageById(ctx context.Context, peerId [64]byte, messageData *sentry_if.OutboundMessageData) error {
	peer, ok := s.peers[peerId]

	if !ok {
		return fmt.Errorf("unknown peer")
	}

	switch messageData.Id {
	case sentry_if.MessageId_GET_BLOCK_HEADERS_65:
		packet := &eth.GetBlockHeadersPacket{}
		if err := rlp.DecodeBytes(messageData.Data, packet); err != nil {
			return fmt.Errorf("failed to decode packet: %w", err)
		}

		go s.processGetBlockHeaders(ctx, peer, 0, packet)

	case sentry_if.MessageId_GET_BLOCK_HEADERS_66:
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

func (s *server) SendMessageByMinBlock(ctx context.Context, request *sentry_if.SendMessageByMinBlockRequest) (*sentry_if.SentPeers, error) {
	return s.UnimplementedSentryServer.SendMessageByMinBlock(ctx, request)
}

func (s *server) SendMessageToAll(ctx context.Context, data *sentry_if.OutboundMessageData) (*sentry_if.SentPeers, error) {
	sentPeers := &sentry_if.SentPeers{}

	for _, peer := range s.peers {
		peerKey := peer.Pubkey()

		if err := s.sendMessageById(ctx, peerKey, data); err != nil {
			return sentPeers, err
		}

		sentPeers.Peers = append(sentPeers.Peers, gointerfaces.ConvertBytesToH512(peerKey[:]))
	}

	return sentPeers, nil
}

func (s *server) SendMessageToRandomPeers(ctx context.Context, request *sentry_if.SendMessageToRandomPeersRequest) (*sentry_if.SentPeers, error) {
	sentPeers := &sentry_if.SentPeers{}

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

func (s *server) Messages(request *sentry_if.MessagesRequest, receiver sentry_if.Sentry_MessagesServer) error {
	for _, messageId := range request.Ids {
		receivers := s.messageReceivers[messageId]
		s.messageReceivers[messageId] = append(receivers, receiver)
	}

	<-s.ctx.Done()

	return nil
}

func (s *server) processGetBlockHeaders(ctx context.Context, peer *p2p.Peer, requestId uint64, request *eth.GetBlockHeadersPacket) {
	r65 := s.messageReceivers[sentry_if.MessageId_BLOCK_HEADERS_65]
	r66 := s.messageReceivers[sentry_if.MessageId_BLOCK_HEADERS_66]

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
				receiver.Send(&sentry_if.InboundMessage{
					Id:     sentry_if.MessageId_BLOCK_HEADERS_65,
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
				receiver.Send(&sentry_if.InboundMessage{
					Id:     sentry_if.MessageId_BLOCK_HEADERS_66,
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

func (s *server) getHeader(ctx context.Context, blockNum uint64) (*core_types.Header, error) {
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

		s.activeSnapshots.ReopenSegments([]snaptype.Type{snaptype.Headers})

		header, err = s.blockReader.Header(ctx, nil, common.Hash{}, blockNum)

		if err != nil {
			return nil, err
		}
	}

	return header, nil
}

func (s *server) getHeaderByHash(ctx context.Context, hash common.Hash) (*core_types.Header, error) {
	return s.blockReader.HeaderByHash(ctx, nil, hash)
}

func (s *server) downloadHeaders(ctx context.Context, header *freezeblocks.HeaderSegment) error {
	fileName := snaptype.SegmentFileName(s.knownSnapshots.Version(), header.From(), header.To(), snaptype.Headers)

	s.logger.Info(fmt.Sprintf("Downloading %s", fileName))

	err := s.downloader.Download(ctx, fileName)

	if err != nil {
		return fmt.Errorf("can't download %s: %w", fileName, err)
	}

	s.logger.Info(fmt.Sprintf("Indexing %s", fileName))

	return freezeblocks.HeadersIdx(ctx,
		filepath.Join(s.downloader.LocalFsRoot(), fileName), s.knownSnapshots.Version(), header.From(), s.downloader.LocalFsRoot(), nil, log.LvlDebug, s.logger)
}
