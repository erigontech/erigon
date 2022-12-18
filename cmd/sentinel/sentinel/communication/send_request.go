package communication

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var NoRequestHandlers = map[string]bool{
	MetadataProtocolV1:          true,
	MetadataProtocolV2:          true,
	LightClientFinalityUpdateV1: true,
}

func SendRequestRawToPeer(ctx context.Context, host host.Host, data []byte, topic string, peerId peer.ID) ([]byte, bool, error) {
	reqRetryTimer := time.NewTimer(clparams.ReqTimeout)
	defer reqRetryTimer.Stop()

	respRetryTicker := time.NewTimer(100 * time.Millisecond)
	defer respRetryTicker.Stop()

	stream, err := writeRequestRaw(host, ctx, data, peerId, topic)
	if err != nil {
		return nil, false, err
	}
	defer stream.Close()

	log.Trace("[Sentinel Req] sent request", "topic", topic, "peer", peerId)

	respRetryTimer := time.NewTimer(clparams.RespTimeout)
	defer respRetryTimer.Stop()

	resp, foundErrRequest, err := verifyResponse(stream, peerId)

Loop:
	for err != nil {
		select {
		case <-ctx.Done():
			log.Warn("[Sentinel Resp] sentinel has been shutdown")
			break Loop
		case <-respRetryTimer.C:
			log.Trace("[Sentinel Resp] timeout", "topic", topic, "peer", peerId)
			break Loop
		case <-respRetryTicker.C:
			resp, foundErrRequest, err = verifyResponse(stream, peerId)
			if err == network.ErrReset {
				break Loop
			}
		}
	}

	return resp, foundErrRequest, err
}

func writeRequestRaw(host host.Host, ctx context.Context, data []byte, peerId peer.ID, topic string) (network.Stream, error) {
	stream, err := host.NewStream(ctx, peerId, protocol.ID(topic))
	if err != nil {
		return nil, fmt.Errorf("failed to begin stream, err=%s", err)
	}

	if _, ok := NoRequestHandlers[topic]; !ok {
		if _, err := stream.Write(data); err != nil {
			return nil, err
		}
	}

	return stream, stream.CloseWrite()
}

func verifyResponse(stream network.Stream, peerId peer.ID) ([]byte, bool, error) {
	code := make([]byte, 1)
	_, err := stream.Read(code)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read code byte peer=%s, err=%s", peerId, err)
	}

	message, err := io.ReadAll(stream)
	if err != nil {
		return nil, false, err
	}

	return common.CopyBytes(message), code[0] != 0, nil
}
