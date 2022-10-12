package rpc

import (
	"bytes"
	"context"
	"fmt"

	ssz "github.com/ferranbt/fastssz"
	"github.com/ledgerwatch/erigon/cmd/lightclient/cltypes"
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/handlers"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/zap/buffer"
)

func DecodeGossipData(data *lightrpc.GossipData) (ssz.Unmarshaler, error) {
	switch data.Type {
	case lightrpc.GossipType_BeaconBlockGossipType:
		pkt := &cltypes.SignedBeaconBlockBellatrix{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	case lightrpc.GossipType_LightClientOptimisticUpdateGossipType:
		pkt := &cltypes.LightClientOptimisticUpdate{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	case lightrpc.GossipType_LightClientFinalityUpdateGossipType:
		pkt := &cltypes.LightClientFinalityUpdate{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	default:
		return nil, fmt.Errorf("invalid gossip type: %d", data.Type)
	}
}

func SendPingReqV1(ctx context.Context, req *cltypes.Ping, client lightrpc.SentinelClient) (*cltypes.Ping, error) {
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, req); err != nil {
		return nil, err
	}
	responsePacket := &cltypes.Ping{}
	data := common.CopyBytes(buffer.Bytes())
	message, err := client.SendRequest(ctx, &lightrpc.RequestData{
		Data:  data,
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

func SendMetadataReqV1(ctx context.Context, client lightrpc.SentinelClient) (*cltypes.MetadataV1, error) {
	responsePacket := &cltypes.MetadataV1{}

	message, err := client.SendRequest(ctx, &lightrpc.RequestData{
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

func SendLightClientFinaltyUpdateReqV1(ctx context.Context, client lightrpc.SentinelClient) (*cltypes.LightClientFinalityUpdate, error) {
	responsePacket := &cltypes.LightClientFinalityUpdate{}

	message, err := client.SendRequest(ctx, &lightrpc.RequestData{
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

func SendLightClientOptimisticUpdateReqV1(ctx context.Context, client lightrpc.SentinelClient) (*cltypes.LightClientOptimisticUpdate, error) {
	responsePacket := &cltypes.LightClientOptimisticUpdate{}

	message, err := client.SendRequest(ctx, &lightrpc.RequestData{
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
