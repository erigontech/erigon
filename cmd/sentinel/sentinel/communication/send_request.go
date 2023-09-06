package communication

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

var NoRequestHandlers = map[string]bool{
	MetadataProtocolV1: true,
	MetadataProtocolV2: true,
}

type response struct {
	data []byte
	code byte
	err  error
}

func SendRequestRawToPeer(ctx context.Context, host host.Host, data []byte, topic string, peerId peer.ID) ([]byte, byte, error) {
	nctx, cn := context.WithTimeout(ctx, 5*time.Second)
	defer cn()
	stream, err := writeRequestRaw(host, nctx, data, peerId, topic)
	if err != nil {
		return nil, 189, err
	}
	defer stream.Close()

	retryVerifyTicker := time.NewTicker(10 * time.Millisecond)
	defer retryVerifyTicker.Stop()

	res := verifyResponse(stream, peerId)
	for res.err != nil && res.err == network.ErrReset {
		select {
		case <-retryVerifyTicker.C:
			res = verifyResponse(stream, peerId)
		case <-nctx.Done():
			stream.Reset()
			return nil, 0, nctx.Err()
		}
	}

	return res.data, res.code, res.err

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

func verifyResponse(stream network.Stream, peerId peer.ID) (resp response) {
	code := make([]byte, 1)
	_, resp.err = stream.Read(code)
	if resp.err != nil {
		return
	}
	resp.code = code[0]
	resp.data, resp.err = io.ReadAll(stream)
	if resp.err != nil {
		return
	}
	return
}
