package rpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/c2h5oh/datasize"
	"github.com/golang/snappy"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/zap/buffer"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/common"
)

const maxMessageLength = 18 * datasize.MB

// BeaconRpcP2P represents a beacon chain RPC client.
type BeaconRpcP2P struct {
	// ctx is the context for the RPC client.
	ctx context.Context
	// sentinel is a client for sending and receiving messages to and from a beacon chain node.
	sentinel sentinel.SentinelClient
	// beaconConfig is the configuration for the beacon chain.
	beaconConfig *clparams.BeaconChainConfig
	// genesisConfig is the configuration for the genesis block of the beacon chain.
	genesisConfig *clparams.GenesisConfig
}

// NewBeaconRpcP2P creates a new BeaconRpcP2P struct and returns a pointer to it.
// It takes a context, a sentinel.Sent
func NewBeaconRpcP2P(ctx context.Context, sentinel sentinel.SentinelClient, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig) *BeaconRpcP2P {
	return &BeaconRpcP2P{
		ctx:           ctx,
		sentinel:      sentinel,
		beaconConfig:  beaconConfig,
		genesisConfig: genesisConfig,
	}
}

func (b *BeaconRpcP2P) sendBlocksRequest(topic string, reqData []byte, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
	// Prepare output slice.
	responsePacket := []*cltypes.SignedBeaconBlock{}

	message, err := b.sentinel.SendRequest(b.ctx, &sentinel.RequestData{
		Data:  reqData,
		Topic: topic,
	})
	if err != nil {
		return nil, "", err
	}
	if message.Error {
		log.Debug("received range req error", "err", string(message.Data))
		return nil, message.Peer.Pid, nil
	}

	r := bytes.NewReader(message.Data)
	for i := 0; i < int(count); i++ {
		forkDigest := make([]byte, 4)
		if _, err := r.Read(forkDigest); err != nil {
			if err == io.EOF {
				break
			}
			return nil, message.Peer.Pid, err
		}

		// Read varint for length of message.
		encodedLn, _, err := ssz_snappy.ReadUvarint(r)
		if err != nil {
			return nil, message.Peer.Pid, fmt.Errorf("unable to read varint from message prefix: %v", err)
		}
		// Sanity check for message size.
		if encodedLn > uint64(maxMessageLength) {
			return nil, message.Peer.Pid, fmt.Errorf("received message too big")
		}

		// Read bytes using snappy into a new raw buffer of side encodedLn.
		raw := make([]byte, encodedLn)
		sr := snappy.NewReader(r)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			if err != nil {
				return nil, message.Peer.Pid, fmt.Errorf("read error: %w", err)
			}
			bytesRead += n
		}
		// Fork digests
		respForkDigest := binary.BigEndian.Uint32(forkDigest)
		if respForkDigest == 0 {
			return nil, message.Peer.Pid, fmt.Errorf("null fork digest")
		}

		version, err := fork.ForkDigestVersion(utils.Uint32ToBytes4(respForkDigest), b.beaconConfig, b.genesisConfig.GenesisValidatorRoot)
		if err != nil {
			return nil, message.Peer.Pid, err
		}
		responseChunk := &cltypes.SignedBeaconBlock{}

		if err = responseChunk.DecodeSSZ(raw, int(version)); err != nil {
			return nil, message.Peer.Pid, err
		}
		responsePacket = append(responsePacket, responseChunk)
		// TODO(issues/5884): figure out why there is this extra byte.
		r.ReadByte()
	}

	return responsePacket, message.Peer.Pid, nil
}

// SendBeaconBlocksByRangeReq retrieves blocks range from beacon chain.
func (b *BeaconRpcP2P) SendBeaconBlocksByRangeReq(start, count uint64) ([]*cltypes.SignedBeaconBlock, string, error) {
	req := &cltypes.BeaconBlocksByRangeRequest{
		StartSlot: start,
		Count:     count,
		Step:      1, // deprecated, and must be set to 1.
	}
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, req); err != nil {
		return nil, "", err
	}

	data := common.CopyBytes(buffer.Bytes())
	return b.sendBlocksRequest(communication.BeaconBlocksByRangeProtocolV2, data, count)
}

// SendBeaconBlocksByRootReq retrieves blocks by root from beacon chain.
func (b *BeaconRpcP2P) SendBeaconBlocksByRootReq(roots [][32]byte) ([]*cltypes.SignedBeaconBlock, string, error) {
	var req cltypes.BeaconBlocksByRootRequest = roots
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, &req); err != nil {
		return nil, "", err
	}
	data := common.CopyBytes(buffer.Bytes())
	return b.sendBlocksRequest(communication.BeaconBlocksByRootProtocolV2, data, uint64(len(roots)))
}

// Peers retrieves peer count.
func (b *BeaconRpcP2P) Peers() (uint64, error) {
	amount, err := b.sentinel.GetPeers(b.ctx, &sentinel.EmptyMessage{})
	if err != nil {
		return 0, err
	}
	return amount.Amount, nil
}

func (b *BeaconRpcP2P) SetStatus(finalizedRoot libcommon.Hash, finalizedEpoch uint64, headRoot libcommon.Hash, headSlot uint64) error {
	forkDigest, err := fork.ComputeForkDigest(b.beaconConfig, b.genesisConfig)
	if err != nil {
		return err
	}
	_, err = b.sentinel.SetStatus(b.ctx, &sentinel.Status{
		ForkDigest:     utils.Bytes4ToUint32(forkDigest),
		FinalizedRoot:  gointerfaces.ConvertHashToH256(finalizedRoot),
		FinalizedEpoch: finalizedEpoch,
		HeadRoot:       gointerfaces.ConvertHashToH256(headRoot),
		HeadSlot:       headSlot,
	})
	return err
}

func (b *BeaconRpcP2P) PropagateBlock(block *cltypes.SignedBeaconBlock) error {
	encoded, err := block.EncodeSSZ(nil)
	if err != nil {
		return err
	}
	_, err = b.sentinel.PublishGossip(b.ctx, &sentinel.GossipData{
		Data: encoded,
		Type: sentinel.GossipType_BeaconBlockGossipType,
	})
	return err
}

func (b *BeaconRpcP2P) BanPeer(pid string) {
	b.sentinel.BanPeer(b.ctx, &sentinel.Peer{Pid: pid})
}
