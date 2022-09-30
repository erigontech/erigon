package sentinel

import (
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
		log.Warn("Failed to ping request", "err", err)
		return
	}

	stream, err := s.host.NewStream(s.ctx, peerInfo.ID, protocol.ID(ProtocolPrefix+"/ping/1/ssz_snappy"))
	if err != nil {
		log.Warn("failed to create stream to send ping request", "err", err)
		return
	}
	defer stream.Close()

	sc := ssz_snappy.NewStreamCodec(stream)

	n, err := sc.WritePacket(pingPacket)
	if err != nil {
		log.Warn("failed to write ping request packet", "err", err)
		return
	}

	if n != 8 {
		log.Warn("wrong ping packet size")
		return
	}
	log.Info("[Req] sent ping request", "peer", peerInfo.ID)
}
