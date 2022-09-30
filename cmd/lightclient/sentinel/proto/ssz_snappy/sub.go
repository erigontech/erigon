package ssz_snappy

import (
	"context"
	"fmt"
	"sync"

	ssz "github.com/ferranbt/fastssz"
	"github.com/golang/snappy"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type SubCodec struct {
	sub *pubsub.Subscription
	mu  sync.Mutex
}

func NewSubCodec(
	sub *pubsub.Subscription,
) proto.SubCodec {
	return &SubCodec{
		sub: sub,
	}
}

// decode into packet p, then return the packet context
func (d *SubCodec) Decode(ctx context.Context, p proto.Packet) (sctx *proto.SubContext, err error) {
	sctx, err = d.readPacket(ctx, p)
	return
}

func (d *SubCodec) readPacket(ctx context.Context, p proto.Packet) (*proto.SubContext, error) {
	c := &proto.SubContext{
		Packet: p,
		Codec:  d,
	}
	// read the next message
	msg, err := d.sub.Next(ctx)
	if err != nil {
		return c, err
	}
	c.Topic = msg.GetTopic()
	c.Msg = msg
	if p != nil {
		//TODO: we can use a bufpool here and improve performance. we can pick up used packet write buffers.
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
