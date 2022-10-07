package ssz_snappy

import (
	"context"

	ssz "github.com/ferranbt/fastssz"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication"
	"github.com/ledgerwatch/erigon/cmd/lightclient/utils"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
)

type GossipCodec struct {
	sub *pubsub.Subscription
	top *pubsub.Topic
}

func NewGossipCodec(
	sub *pubsub.Subscription,
	top *pubsub.Topic,
) communication.GossipCodec {
	return &GossipCodec{
		sub: sub,
		top: top,
	}
}

// decode into packet p, then return the packet context
func (d *GossipCodec) Decode(ctx context.Context, p communication.Packet) (sctx *communication.GossipContext, err error) {
	msg, err := d.sub.Next(ctx)
	if err != nil {
		return nil, err
	}
	sctx, err = d.readPacket(msg, p)
	return
}

func (d *GossipCodec) WritePacket(ctx context.Context, p communication.Packet) error {
	if val, ok := p.(ssz.Marshaler); ok {
		ans, err := utils.EncodeSSZSnappy(val)
		if err != nil {
			return err
		}

		return d.top.Publish(ctx, ans)
	}
	return nil
}

func (d *GossipCodec) readPacket(msg *pubsub.Message, p communication.Packet) (*communication.GossipContext, error) {
	// read the next message
	c := &communication.GossipContext{
		Packet: p,
		Codec:  d,
	}
	c.Topic = d.top
	c.Msg = msg
	if p == nil {
		return c, nil
	}

	return c, d.decodeData(p, msg.Data)
}

func (d *GossipCodec) decodeData(p communication.Packet, data []byte) error {
	var val ssz.Unmarshaler
	var ok bool
	if val, ok = p.(ssz.Unmarshaler); !ok {
		return nil
	}
	return utils.DecodeSSZSnappy(val, data)
}
