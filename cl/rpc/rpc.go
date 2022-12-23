package rpc

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/snappy"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/zap/buffer"
)

type BeaconRpcP2P struct {
	ctx           context.Context
	sentinel      sentinel.SentinelClient
	beaconConfig  *clparams.BeaconChainConfig
	genesisConfig *clparams.GenesisConfig
}

func NewBeaconRpcP2P(ctx context.Context, sentinel sentinel.SentinelClient, beaconConfig *clparams.BeaconChainConfig, genesisConfig *clparams.GenesisConfig) *BeaconRpcP2P {
	return &BeaconRpcP2P{
		ctx:           ctx,
		sentinel:      sentinel,
		beaconConfig:  beaconConfig,
		genesisConfig: genesisConfig,
	}
}

func (b *BeaconRpcP2P) SendLightClientFinaltyUpdateReqV1() (*cltypes.LightClientFinalityUpdate, error) {
	responsePacket := &cltypes.LightClientFinalityUpdate{}

	message, err := b.sentinel.SendRequest(b.ctx, &sentinel.RequestData{
		Topic: communication.LightClientFinalityUpdateV1,
	})
	if err != nil {
		return nil, err
	}
	if message.Error {
		log.Warn("received error", "err", string(message.Data))
		return nil, nil
	}

	if err := ssz_snappy.DecodeAndRead(bytes.NewReader(message.Data), responsePacket); err != nil {
		return nil, fmt.Errorf("unable to decode packet: %v", err)
	}
	return responsePacket, nil
}

func (b *BeaconRpcP2P) SendLightClientOptimisticUpdateReqV1() (*cltypes.LightClientOptimisticUpdate, error) {
	responsePacket := &cltypes.LightClientOptimisticUpdate{}

	message, err := b.sentinel.SendRequest(b.ctx, &sentinel.RequestData{
		Topic: communication.LightClientOptimisticUpdateV1,
	})
	if err != nil {
		return nil, err
	}
	if message.Error {
		log.Warn("received error", "err", string(message.Data))
		return nil, nil
	}

	if err := ssz_snappy.DecodeAndRead(bytes.NewReader(message.Data), responsePacket); err != nil {
		return nil, fmt.Errorf("unable to decode packet: %v", err)
	}
	return responsePacket, nil
}

func (b *BeaconRpcP2P) SendLightClientBootstrapReqV1(root common.Hash) (*cltypes.LightClientBootstrap, error) {
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, &cltypes.SingleRoot{Root: root}); err != nil {
		return nil, err
	}
	responsePacket := &cltypes.LightClientBootstrap{}
	data := common.CopyBytes(buffer.Bytes())
	message, err := b.sentinel.SendRequest(b.ctx, &sentinel.RequestData{
		Data:  data,
		Topic: communication.LightClientBootstrapV1,
	})
	if err != nil {
		return nil, err
	}
	if message.Error {
		log.Warn("received error", "err", string(message.Data))
		return nil, nil
	}
	if err := ssz_snappy.DecodeAndRead(bytes.NewReader(message.Data), responsePacket); err != nil {
		return nil, fmt.Errorf("unable to decode packet: %v", err)
	}
	return responsePacket, nil
}

func (b *BeaconRpcP2P) SendLightClientUpdatesReqV1(period uint64) (*cltypes.LightClientUpdate, error) {
	// This is approximately one day worth of data, we dont need to receive more than 1.
	req := &cltypes.LightClientUpdatesByRangeRequest{
		Period: period,
		Count:  1,
	}
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, req); err != nil {
		return nil, err
	}

	responsePacket := []cltypes.ObjectSSZ{&cltypes.LightClientUpdate{}}

	data := common.CopyBytes(buffer.Bytes())
	message, err := b.sentinel.SendRequest(b.ctx, &sentinel.RequestData{
		Data:  data,
		Topic: communication.LightClientUpdatesByRangeV1,
	})
	if err != nil {
		return nil, err
	}
	if message.Error {
		log.Warn("received error", "err", string(message.Data))
		return nil, nil
	}
	if err := ssz_snappy.DecodeListSSZ(message.Data, 1, responsePacket); err != nil {
		return nil, fmt.Errorf("unable to decode packet: %v", err)
	}
	return responsePacket[0].(*cltypes.LightClientUpdate), nil
}

func (b *BeaconRpcP2P) sendBlocksRequest(topic string, reqData []byte, count uint64) ([]cltypes.ObjectSSZ, error) {
	// Prepare output slice.
	responsePacket := []cltypes.ObjectSSZ{}

	message, err := b.sentinel.SendRequest(b.ctx, &sentinel.RequestData{
		Data:  reqData,
		Topic: topic,
	})
	if err != nil {
		return nil, err
	}
	if message.Error {
		log.Debug("received range req error", "err", string(message.Data))
		return nil, nil
	}

	r := bytes.NewReader(message.Data)
	for i := 0; i < int(count); i++ {
		forkDigest := make([]byte, 4)
		if _, err := r.Read(forkDigest); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Read varint for length of message.
		encodedLn, _, err := ssz_snappy.ReadUvarint(r)
		if err != nil {
			return nil, fmt.Errorf("unable to read varint from message prefix: %v", err)
		}

		// Read bytes using snappy into a new raw buffer of side encodedLn.
		raw := make([]byte, encodedLn)
		sr := snappy.NewReader(r)
		bytesRead := 0
		for bytesRead < int(encodedLn) {
			n, err := sr.Read(raw[bytesRead:])
			if err != nil {
				return nil, fmt.Errorf("read error: %w", err)
			}
			bytesRead += n
		}
		// Fork digests
		respForkDigest := binary.BigEndian.Uint32(forkDigest)
		if respForkDigest == 0 {
			return nil, fmt.Errorf("null fork digest")
		}
		var phase0ForkDigest, altairForkDigest, bellatrixForkDigest [4]byte
		if b.beaconConfig.GenesisForkVersion != nil {
			phase0ForkDigest, err = fork.ComputeForkDigestForVersion(
				utils.BytesToBytes4(b.beaconConfig.GenesisForkVersion),
				b.genesisConfig.GenesisValidatorRoot,
			)
			if err != nil {
				return nil, err
			}
		}

		if b.beaconConfig.AltairForkVersion != nil {
			altairForkDigest, err = fork.ComputeForkDigestForVersion(
				utils.BytesToBytes4(b.beaconConfig.AltairForkVersion),
				b.genesisConfig.GenesisValidatorRoot,
			)
			if err != nil {
				return nil, err
			}
		}

		if b.beaconConfig.BellatrixForkVersion != nil {
			bellatrixForkDigest, err = fork.ComputeForkDigestForVersion(
				utils.BytesToBytes4(b.beaconConfig.BellatrixForkVersion),
				b.genesisConfig.GenesisValidatorRoot,
			)
			if err != nil {
				return nil, err
			}
		}

		switch respForkDigest {
		case utils.Bytes4ToUint32(phase0ForkDigest):
			responsePacket = append(responsePacket, &cltypes.SignedBeaconBlockPhase0{})
		case utils.Bytes4ToUint32(altairForkDigest):
			responsePacket = append(responsePacket, &cltypes.SignedBeaconBlockAltair{})
		case utils.Bytes4ToUint32(bellatrixForkDigest):
			responsePacket = append(responsePacket, &cltypes.SignedBeaconBlockBellatrix{})
		default:
			return nil, fmt.Errorf("received invalid fork digest")
		}

		if err := responsePacket[i].UnmarshalSSZ(raw); err != nil {
			return nil, fmt.Errorf("unmarshalling: %w", err)
		}

		// TODO(issues/5884): figure out why there is this extra byte.
		r.ReadByte()
	}
	return responsePacket, nil
}

func (b *BeaconRpcP2P) SendBeaconBlocksByRangeReq(start, count uint64) ([]cltypes.ObjectSSZ, error) {
	req := &cltypes.BeaconBlocksByRangeRequest{
		StartSlot: start,
		Count:     count,
		Step:      1, // deprecated, and must be set to 1.
	}
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, req); err != nil {
		return nil, err
	}

	data := common.CopyBytes(buffer.Bytes())
	return b.sendBlocksRequest(communication.BeaconBlocksByRangeProtocolV2, data, count)
}

func (b *BeaconRpcP2P) SendBeaconBlocksByRootReq(roots [][32]byte) ([]cltypes.ObjectSSZ, error) {
	var req cltypes.BeaconBlocksByRootRequest = roots
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, &req); err != nil {
		return nil, err
	}
	data := common.CopyBytes(buffer.Bytes())
	return b.sendBlocksRequest(communication.BeaconBlocksByRootProtocolV2, data, uint64(len(roots)))
}

func (b *BeaconRpcP2P) Peers() (uint64, error) {
	amount, err := b.sentinel.GetPeers(b.ctx, &sentinel.EmptyMessage{})
	if err != nil {
		return 0, err
	}
	return amount.Amount, nil
}

func (b *BeaconRpcP2P) SetStatus(finalizedRoot common.Hash, finalizedEpoch uint64, headRoot common.Hash, headSlot uint64) error {
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
