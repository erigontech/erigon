package snappy_ssz

import (
	"bytes"
	"context"
	"fmt"
	"io"
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
		s := bytes.NewBuffer(msg.Data)
		sr := snappy.NewReader(s)

		ln, _, err := proto.ReadUvarint(s)
		if err != nil {
			return c, err
		}
		//TODO: we should probably take this from a buffer pool.
		c.Raw = make([]byte, ln)
		_, err = io.ReadFull(sr, c.Raw)
		if err != nil {
			return c, fmt.Errorf("readPacket: %w", err)
		}
		if val, ok := p.(ssz.Unmarshaler); ok {
			err = val.UnmarshalSSZ(c.Raw)
			if err != nil {
				return c, fmt.Errorf("readPacket: %w", err)
			}
		}
	}
	return c, nil
}
