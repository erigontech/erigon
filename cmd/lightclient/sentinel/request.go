package sentinel

import (
	"time"

	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/handlers"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto/p2p"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto/ssz_snappy"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func (s *Sentinel) pingRequest() {
	pingPacket := &p2p.Ping{
		Id: uint64(1),
	}

	_, peerInfo, err := connectToRandomPeer(s)
	if err != nil {
		log.Warn("[Req] failed to ping request", "err", err)
		return
	}

	stream, err := s.host.NewStream(s.ctx, peerInfo.ID, protocol.ID(handlers.ProtocolPrefix+"/ping/1/ssz_snappy"))
	if err != nil {
		log.Warn("[Req] failed to create stream to send ping request", "err", err)
		return
	}
	sc := ssz_snappy.NewStreamCodec(stream)
	defer sc.Close()

	n, err := sc.WritePacket(pingPacket)
	if err != nil {
		log.Warn("[Req] failed to write ping request packet", "err", err)
		s.peers.Penalize(peerInfo.ID)
		return
	}

	if n != 8 {
		log.Warn("[Req] wrong ping packet size")
		s.peers.Penalize(peerInfo.ID)
		return
	}
	time.Sleep(100 * time.Millisecond)
	sc.CloseWriter()
	log.Info("[Req] sent ping request", "peer", peerInfo.ID)

	code, err := sc.ReadByte()
	if err != nil {
		log.Warn("[Resp] failed to read byte", "err", err)
		s.peers.Penalize(peerInfo.ID)
		return
	}

	switch code {
	case 0:
		responsePing := &p2p.Ping{}
		protoCtx, err := sc.Decode(responsePing)
		if err != nil {
			log.Warn("fail ping success", "err", err, "got", string(protoCtx.Raw))
			s.peers.Penalize(peerInfo.ID)
			return
		}
		log.Info("[Resp] ping success", "peer", peerInfo.ID, "code", code, "pong", responsePing.Id)
	case 1, 2, 3:
		errMessage := &proto.ErrorMessage{}
		protoCtx, err := sc.Decode(errMessage)
		if err != nil {
			log.Warn("fail decode ping error", "err", err, "got", string(protoCtx.Raw))
			s.peers.Penalize(peerInfo.ID)
			return
		}
		log.Info("[Resp] ping error ", "peer", peerInfo.ID, "code", code, "msg", string(errMessage.Message))
	default:
		log.Info("[Resp] ping unknown code", "peer", peerInfo.ID, "code", code)
	}
}

func (s *Sentinel) metadataRequest() {
	_, peerInfo, err := connectToRandomPeer(s)
	if err != nil {
		log.Warn("[Req] failed to metadata request", "err", err)
		return
	}

	stream, err := s.host.NewStream(s.ctx, peerInfo.ID, protocol.ID(handlers.ProtocolPrefix+"/metadata/1/ssz_snappy"))
	if err != nil {
		log.Warn("[Req] failed to create stream to send metadata request", "err", err)
		return
	}
	sc := ssz_snappy.NewStreamCodec(stream)
	defer sc.Close()

	log.Info("[Req] sent metadata request", "peer", peerInfo.ID)
	sc.CloseWriter()

	code, err := sc.ReadByte()
	if err != nil {
		log.Warn("[Resp] failed to read byte", "err", err)
		s.peers.Penalize(peerInfo.ID)
		return
	}

	switch code {
	case 0:
		responseMetadata := &p2p.MetadataV0{}
		protoCtx, err := sc.Decode(responseMetadata)
		if err != nil {
			log.Warn("fail metadata success", "err", err, "got", string(protoCtx.Raw))
			s.peers.Penalize(peerInfo.ID)
			return
		}
		log.Info("[Resp] metadata success", "peer", peerInfo.ID, "code", code, "seq_number", responseMetadata.SeqNumber)
	case 1, 2, 3:
		errMessage := &proto.ErrorMessage{}
		protoCtx, err := sc.Decode(errMessage)
		if err != nil {
			log.Warn("fail decode metadata error", "err", err, "got", string(protoCtx.Raw))
			s.peers.Penalize(peerInfo.ID)
			return
		}
		log.Info("[Resp] metadata error ", "peer", peerInfo.ID, "code", code, "msg", string(errMessage.Message))
	default:
		log.Info("[Resp] metadata unknown code", "peer", peerInfo.ID, "code", code)
	}
}
