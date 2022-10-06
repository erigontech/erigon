package handlers

import (
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication/p2p"
	"github.com/ledgerwatch/erigon/cmd/lightclient/utils"
	"github.com/ledgerwatch/log/v3"
)

// These below are the handlers for heartbeat functions

func (c *ConsensusHandlers) goodbyeHandler(ctx *communication.StreamContext, dat *p2p.Goodbye) error {
	//log.Info("[Lightclient] Received", "goodbye", dat.Reason)
	defer c.peers.DisconnectPeer(ctx.Stream.Conn().RemotePeer())
	_, err := ctx.Codec.WritePacket(dat)
	if err != nil {
		return err
	}
	return nil
}

// type safe handlers which all have access to the original stream & decompressed data
// ping handler
func pingHandler(ctx *communication.StreamContext, dat *p2p.Ping) error {
	// since packets are just structs, they can be resent with no issue
	_, err := ctx.Codec.WritePacket(dat)
	if err != nil {
		return err
	}
	return nil
}


func (c *ConsensusHandlers) metadataHandlerV1(ctx *proto.StreamContext, dat *proto.EmptyPacket) error {
	_, err := ctx.Codec.WritePacket(c.metadataV1)
	if err != nil {
		return err
	}

	if err := ctx.Codec.CloseWriter(); err != nil {
		return err
	}

	return nil
}

func statusHandler(ctx *communication.StreamContext, dat *p2p.Status) error {
	log.Debug("[ReqResp] Status",
		"epoch", dat.FinalizedEpoch,
		"final root", utils.BytesToHex(dat.FinalizedRoot),
		"head root", utils.BytesToHex(dat.HeadRoot),
		"head slot", dat.HeadSlot,
		"fork digest", utils.BytesToHex(dat.ForkDigest),
	)
	return nil
}
