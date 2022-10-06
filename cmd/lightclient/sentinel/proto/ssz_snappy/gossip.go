package ssz_snappy

import (
	"context"
	"sync"

	ssz "github.com/ferranbt/fastssz"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	"github.com/ledgerwatch/erigon/cmd/lightclient/utils"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type GossipCodec struct {
	sub *pubsub.Subscription
	top *pubsub.Topic
	mu  sync.Mutex
}

func NewGossipCodec(
	sub *pubsub.Subscription,
	top *pubsub.Topic,
) proto.GossipCodec {
	return &GossipCodec{
		sub: sub,
		top: top,
	}
}

// decode into packet p, then return the packet context
func (d *GossipCodec) Decode(ctx context.Context, p proto.Packet) (sctx *proto.GossipContext, err error) {
	sctx, err = d.readPacket(ctx, p)
	return
}

func (d *GossipCodec) WritePacket(ctx context.Context, p proto.Packet) error {
	if val, ok := p.(ssz.Marshaler); ok {
		ans, err := utils.EncodeSSZSnappy(val)
		if err != nil {
			return err
		}

		return d.top.Publish(ctx, ans)
	}
	return nil
}
func (d *GossipCodec) readPacket(ctx context.Context, p proto.Packet) (*proto.GossipContext, error) {
	c := &proto.GossipContext{
		Packet: p,
		Codec:  d,
	}
	// read the next message
	msg, err := d.sub.Next(ctx)
	if err != nil {
		return nil, err
	}
	c.Topic = d.top
	c.Msg = msg
	if p == nil {
		return c, nil
	}

	if val, ok := p.(ssz.Unmarshaler); ok {
		//TODO: we can use a bufpool here and improve performance? we can possibly pick up used packet write buffers. (or get them from other running components)
		if err := utils.DecodeSSZSnappy(val, msg.Data); err != nil {
			return nil, err
		}
	}
	return c, nil
}
