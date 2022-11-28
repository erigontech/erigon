package rpc

import (
	"bytes"
	"context"
	"fmt"

	ssz "github.com/ferranbt/fastssz"

	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/handlers"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
	"go.uber.org/zap/buffer"
)

func DecodeGossipData(data *sentinel.GossipData) (ssz.Unmarshaler, error) {
	switch data.Type {
	case sentinel.GossipType_BeaconBlockGossipType:
		pkt := &cltypes.SignedBeaconBlockBellatrix{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	case sentinel.GossipType_AggregateAndProofGossipType:
		pkt := &cltypes.SignedAggregateAndProof{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	case sentinel.GossipType_VoluntaryExitGossipType:
		pkt := &cltypes.SignedVoluntaryExit{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	case sentinel.GossipType_ProposerSlashingGossipType:
		pkt := &cltypes.ProposerSlashing{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	case sentinel.GossipType_AttesterSlashingGossipType:
		pkt := &cltypes.AttesterSlashing{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	case sentinel.GossipType_LightClientOptimisticUpdateGossipType:
		pkt := &cltypes.LightClientOptimisticUpdate{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	case sentinel.GossipType_LightClientFinalityUpdateGossipType:
		pkt := &cltypes.LightClientFinalityUpdate{}
		err := pkt.UnmarshalSSZ(data.Data)
		return pkt, err
	default:
		return nil, fmt.Errorf("invalid gossip type: %d", data.Type)
	}
}

func SendLightClientFinaltyUpdateReqV1(ctx context.Context, client sentinel.SentinelClient) (*cltypes.LightClientFinalityUpdate, error) {
	responsePacket := &cltypes.LightClientFinalityUpdate{}

	message, err := client.SendRequest(ctx, &sentinel.RequestData{
		Topic: handlers.LightClientFinalityUpdateV1,
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

func SendLightClientOptimisticUpdateReqV1(ctx context.Context, client sentinel.SentinelClient) (*cltypes.LightClientOptimisticUpdate, error) {
	responsePacket := &cltypes.LightClientOptimisticUpdate{}

	message, err := client.SendRequest(ctx, &sentinel.RequestData{
		Topic: handlers.LightClientOptimisticUpdateV1,
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

func SendLightClientBootstrapReqV1(ctx context.Context, req *cltypes.SingleRoot, client sentinel.SentinelClient) (*cltypes.LightClientBootstrap, error) {
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, req); err != nil {
		return nil, err
	}
	responsePacket := &cltypes.LightClientBootstrap{}
	data := common.CopyBytes(buffer.Bytes())
	message, err := client.SendRequest(ctx, &sentinel.RequestData{
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
	if err := ssz_snappy.DecodeAndRead(bytes.NewReader(message.Data), responsePacket); err != nil {
		return nil, fmt.Errorf("unable to decode packet: %v", err)
	}
	return responsePacket, nil
}

func SendLightClientUpdatesReqV1(ctx context.Context, period uint64, client sentinel.SentinelClient) (*cltypes.LightClientUpdate, error) {
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
	message, err := client.SendRequest(ctx, &sentinel.RequestData{
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
	if err := ssz_snappy.DecodeListSSZ(message.Data, 1, responsePacket); err != nil {
		return nil, fmt.Errorf("unable to decode packet: %v", err)
	}
	return responsePacket[0].(*cltypes.LightClientUpdate), nil
}

type blocksRequestOpts struct {
	topic   string
	count   int
	client  sentinel.SentinelClient
	reqData []byte
}

func sendBlocksRequest(ctx context.Context, opts blocksRequestOpts) ([]cltypes.ObjectSSZ, error) {
	// Prepare output slice.
	responsePacket := []cltypes.ObjectSSZ{}
	for i := 0; i < opts.count; i++ {
		responsePacket = append(responsePacket, &cltypes.SignedBeaconBlockBellatrix{})
	}

	message, err := opts.client.SendRequest(ctx, &sentinel.RequestData{
		Data:  opts.reqData,
		Topic: opts.topic,
	})
	if err != nil {
		return nil, err
	}
	if message.Error {
		log.Trace("received error", "err", string(message.Data))
		return nil, nil
	}
	if err := ssz_snappy.DecodeListSSZBeaconBlock(message.Data, uint64(opts.count), responsePacket); err != nil {
		return nil, fmt.Errorf("unable to decode packet: %v", err)
	}
	return responsePacket, nil
}

func SendBeaconBlocksByRangeReq(ctx context.Context, start, count uint64, client sentinel.SentinelClient) ([]cltypes.ObjectSSZ, error) {
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
	return sendBlocksRequest(ctx, blocksRequestOpts{
		topic:   handlers.BeaconBlocksByRangeProtocolV2,
		count:   int(count),
		client:  client,
		reqData: data,
	})
}

func SendBeaconBlocksByRootReq(ctx context.Context, roots [][32]byte, client sentinel.SentinelClient) ([]cltypes.ObjectSSZ, error) {
	var req cltypes.BeaconBlocksByRootRequest = roots
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, &req); err != nil {
		return nil, err
	}
	data := common.CopyBytes(buffer.Bytes())
	return sendBlocksRequest(ctx, blocksRequestOpts{
		topic:   handlers.BeaconBlocksByRootProtocolV2,
		count:   len(roots),
		client:  client,
		reqData: data,
	})
}

func SendStatusReq(ctx context.Context, ourStatus *cltypes.Status, client sentinel.SentinelClient) (*cltypes.Status, error) {
	var buffer buffer.Buffer
	if err := ssz_snappy.EncodeAndWrite(&buffer, ourStatus); err != nil {
		return nil, err
	}
	responsePacket := &cltypes.Status{}

	data := common.CopyBytes(buffer.Bytes())

	message, err := client.SendRequest(ctx, &sentinel.RequestData{
		Data:  data,
		Topic: handlers.StatusProtocolV1,
	})
	if err != nil {
		return nil, err
	}
	if message.Error {
		log.Warn("received error", "err", string(message.Data))
		return nil, nil
	}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(bytes.NewReader(message.Data), responsePacket); err != nil {
		return nil, fmt.Errorf("unable to decode packet: %v", err)
	}
	return responsePacket, nil
}
