package handlers

import (
	"reflect"
	"strings"

	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication"
	"github.com/ledgerwatch/log/v3"
	"github.com/libp2p/go-libp2p/core/network"
)

// curryStreamHandler converts a func(ctx *communication.StreamContext, dat communication.Packet) error to func(network.Stream)
// this allows us to write encoding non specific type safe handler without performance overhead
func curryStreamHandler[T communication.Packet](newcodec func(network.Stream) communication.StreamCodec, fn func(ctx *communication.StreamContext, v T) error) func(network.Stream) {
	return func(s network.Stream) {
		defer s.Close()
		sd := newcodec(s)
		var t T
		val := t.Clone().(T)
		ctx, err := sd.Decode(val)
		if err != nil {
			// the stream reset error is ignored, because
			if !strings.Contains(err.Error(), "stream reset") {
				log.Debug("fail to decode packet", "err", err, "path", ctx.Protocol, "pkt", reflect.TypeOf(val))
			}
			return
		}
		err = fn(ctx, val)
		if err != nil {
			log.Debug("failed handling packet", "err", err, "path", ctx.Protocol, "pkt", reflect.TypeOf(val))
			return
		}
		log.Trace("[ReqResp] Req->Host", "from", ctx.Stream.ID(), "endpoint", ctx.Protocol, "msg", val)
	}
}
