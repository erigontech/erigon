package rpc

import (
	"bytes"
	"context"
	"fmt"

	ssz "github.com/ferranbt/fastssz"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/rpc/consensusrpc"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/handlers"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/zap/buffer"
)

func DecodeGossipData(data *consensusrpc.GossipData) (ssz.Unmarshaler, error) {
	switch data.Type {
	case consensusrpc.GossipType_BeaconBlockGossipType:
		pkt := &cltypes.SignedBeaconBlockBellatrix{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	case consensusrpc.GossipType_AggregateAndProofGossipType:
		pkt := &cltypes.SignedAggregateAndProof{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	case consensusrpc.GossipType_VoluntaryExitGossipType:
		pkt := &cltypes.SignedVoluntaryExit{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	case consensusrpc.GossipType_ProposerSlashingGossipType:
		pkt := &cltypes.ProposerSlashing{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	case consensusrpc.GossipType_AttesterSlashingGossipType:
		pkt := &cltypes.AttesterSlashing{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	case consensusrpc.GossipType_LightClientOptimisticUpdateGossipType:
		pkt := &cltypes.LightClientOptimisticUpdate{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	case consensusrpc.GossipType_LightClientFinalityUpdateGossipType:
		pkt := &cltypes.LightClientFinalityUpdate{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	default:
		return nil, fmt.Errorf("invalid gossip type: %d", data.Type)
	}
}

func SendLightClientFinaltyUpdateReqV1(ctx context.Context, client consensusrpc.SentinelClient) (*cltypes.LightClientFinalityUpdate, error) {
	responsePacket := &cltypes.LightClientFinalityUpdate{}

	message, err := client.SendRequest(ctx, &consensusrpc.RequestData{
		Topic: handlers.LightClientFinalityUpdateV1,
	})
	if err != nil {
		return nil, err
	}
	if message.Error {
		log.Warn("received error", "err", string(message.Data))
		return nil, nil
	}

	err = ssz_snappy.DecodeAndRead(bytes.NewReader(message.Data), responsePacket)
	return responsePacket, err
}

func SendLightClientOptimisticUpdateReqV1(ctx context.Context, client consensusrpc.SentinelClient) (*cltypes.LightClientOptimisticUpdate, error) {
	responsePacket := &cltypes.LightClientOptimisticUpdate{}

	message, err := client.SendRequest(ctx, &consensusrpc.RequestData{
		Topic: handlers.LightClientOptimisticUpdateV1,
	})
	if err != nil {
		return nil, err
	}
	if message.Error {
		log.Warn("received error", "err", string(message.Data))
		return nil, nil
	}

	err = ssz_snappy.DecodeAndRead(bytes.NewReader(message.Data), responsePacket)
	return responsePacket, err
}

func SendLightClientBootstrapReqV1(ctx context.Context, req *cltypes.SingleRoot, client consensusrpc.SentinelClient) (*cltypes.LightClientBootstrap, error) {
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, req); err != nil {
		return nil, err
	}
	responsePacket := &cltypes.LightClientBootstrap{}
	data := common.CopyBytes(buffer.Bytes())
	message, err := client.SendRequest(ctx, &consensusrpc.RequestData{
		Data:  data,
		Topic: handlers.LightClientBootstrapV1,
	})
	if err != nil {
		return nil, err
	}
	if message.Error {
		log.Warn("received error", "err", string(message.Data))
		return nil, nil
	}
	err = ssz_snappy.DecodeAndRead(bytes.NewReader(message.Data), responsePacket)
	return responsePacket, err
}

func SendLightClientUpdatesReqV1(ctx context.Context, period uint64, client consensusrpc.SentinelClient) (*cltypes.LightClientUpdate, error) {
	// This is approximately one day worth of data, we dont need to receive more than 1.
	req := &cltypes.LightClientUpdatesByRangeRequest{
		Period: period,
		Count:  1,
	}
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, req); err != nil {
		return nil, err
	}

	data := common.CopyBytes(buffer.Bytes())
	message, err := client.SendRequest(ctx, &consensusrpc.RequestData{
		Data:  data,
		Topic: handlers.LightClientUpdatesByRangeV1,
	})
	if err != nil {
		return nil, err
	}
	if message.Error {
		log.Warn("received error", "err", string(message.Data))
		return nil, nil
	}
	//err = ssz_snappy.DecodeAndRead(bytes.NewReader(message.Data), responsePacket)
	return ssz_snappy.DecodeLightClientUpdate(message.Data)
}
