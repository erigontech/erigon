package ssz_snappy

import (
	"context"
	"fmt"
	"sync"

	"gfx.cafe/util/go/bytepool"

	ssz "github.com/ferranbt/fastssz"
	"github.com/golang/snappy"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
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
		bts := bytepool.Get(val.SizeSSZ())
		defer bytepool.Put(bts)
		enc, err := val.MarshalSSZTo(bts[:0])
		if err != nil {
			return err
		}
		cmp := bytepool.Get(val.SizeSSZ())
		defer bytepool.Put(cmp)
		ans := snappy.Encode(cmp, enc)
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
		return c, err
	}
	c.Topic = d.top
	c.Msg = msg
	if p != nil {
		//TODO: we can use a bufpool here and improve performance. we can possibly pick up used packet write buffers.
		c.Raw, err = snappy.Decode(nil, msg.Data)
		if err != nil {
			return c, fmt.Errorf("readPacket: %w", err)
		}
		if val, ok := p.(ssz.Unmarshaler); ok {
			err = val.UnmarshalSSZ(c.Raw)
			if err != nil {
				return c, fmt.Errorf("unmarshalPacket: %w", err)
			}
		}
	}
	return c, nil
}
