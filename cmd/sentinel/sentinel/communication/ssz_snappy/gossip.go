/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package ssz_snappy

import (
	"context"

	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/communication"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	ssz "github.com/prysmaticlabs/fastssz"
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
