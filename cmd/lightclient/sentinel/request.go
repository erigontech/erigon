package sentinel

import (
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

	stream, err := s.host.NewStream(s.ctx, peerInfo.ID, protocol.ID(ProtocolPrefix+"/ping/1/ssz_snappy"))
	if err != nil {
		log.Warn("[Req] failed to create stream to send ping request", "err", err)
		return
	}
	defer stream.Close()

	sc := ssz_snappy.NewStreamCodec(stream)

	n, err := sc.WritePacket(pingPacket)
	if err != nil {
		log.Warn("[Req] failed to write ping request packet", "err", err)
		return
	}

	if n != 8 {
		log.Warn("[Req] wrong ping packet size")
		return
	}
	log.Info("[Req] sent ping request", "peer", peerInfo.ID)

	code, err := sc.ReadByte()
	if err != nil {
		log.Warn("[Resp] failed to read byte", "err", err)
		return
	}

	switch code {
	case 0:
		responsePing := &p2p.Ping{}
		pctx, err := sc.Decode(responsePing)
		if err != nil {
			log.Warn("fail ping success", "err", err, "got", string(pctx.Raw))
			return
		}
		log.Info("[Resp] ping success", "peer", peerInfo.ID, "code", code, "pong", responsePing.Id)
	case 1, 2, 3:
		errMessage := &proto.ErrorMessage{}
		pctx, err := sc.Decode(errMessage)
		if err != nil {
			log.Warn("fail decode ping error", "err", err, "got", string(pctx.Raw))
			return
		}
		log.Info("[Resp] ping error ", "peer", peerInfo.ID, "code", code, "msg", string(errMessage.Message))
	default:
		log.Info("[Resp] ping unknown code", "peer", peerInfo.ID, "code", code)
	}
}
