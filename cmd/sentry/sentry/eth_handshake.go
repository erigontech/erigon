package sentry

import (
	"fmt"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/core/forkid"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/p2p"
)

func readAndValidatePeerStatusMessage(
	rw p2p.MsgReadWriter,
	status *proto_sentry.StatusData,
	version uint,
	minVersion uint,
) (*eth.StatusPacket, error) {
	msg, err := rw.ReadMsg()
	if err != nil {
		return nil, err
	}

	reply, err := tryDecodeStatusMessage(&msg)
	msg.Discard()
	if err != nil {
		return nil, err
	}

	err = checkPeerStatusCompatibility(reply, status, version, minVersion)
	return reply, err
}

func tryDecodeStatusMessage(msg *p2p.Msg) (*eth.StatusPacket, error) {
	if msg.Code != eth.StatusMsg {
		return nil, fmt.Errorf("first msg has code %x (!= %x)", msg.Code, eth.StatusMsg)
	}

	if msg.Size > eth.ProtocolMaxMsgSize {
		return nil, fmt.Errorf("message is too large %d, limit %d", msg.Size, eth.ProtocolMaxMsgSize)
	}

	var reply eth.StatusPacket
	if err := msg.Decode(&reply); err != nil {
		return nil, fmt.Errorf("decode message %v: %w", msg, err)
	}

	return &reply, nil
}

var NetworkIdMissmatchErr = fmt.Errorf("network id does not match")

func checkPeerStatusCompatibility(
	reply *eth.StatusPacket,
	status *proto_sentry.StatusData,
	version uint,
	minVersion uint,
) error {
	networkID := status.NetworkId
	if reply.NetworkID != networkID {
		return fmt.Errorf("%w: theirs %d, ours %d", NetworkIdMissmatchErr, reply.NetworkID, networkID)
	}

	if uint(reply.ProtocolVersion) > version {
		return fmt.Errorf("version is more than what this senty supports: theirs %d, max %d", reply.ProtocolVersion, version)
	}
	if uint(reply.ProtocolVersion) < minVersion {
		return fmt.Errorf("version is less than allowed minimum: theirs %d, min %d", reply.ProtocolVersion, minVersion)
	}

	genesisHash := gointerfaces.ConvertH256ToHash(status.ForkData.Genesis)
	if reply.Genesis != genesisHash {
		return fmt.Errorf("genesis hash does not match: theirs %x, ours %x", reply.Genesis, genesisHash)
	}

	forkFilter := forkid.NewFilterFromForks(status.ForkData.HeightForks, status.ForkData.TimeForks, genesisHash, status.MaxBlockHeight, status.MaxBlockTime)
	return forkFilter(reply.ForkID)
}
