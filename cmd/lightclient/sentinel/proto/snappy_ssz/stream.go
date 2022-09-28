package snappy_ssz

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

	"gfx.cafe/util/go/bufpool"
	ssz "github.com/ferranbt/fastssz"
	"github.com/golang/snappy"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	"github.com/libp2p/go-libp2p/core/network"
)

type StreamCodec struct {
	s  network.Stream
	sr *snappy.Reader
	sw *snappy.Writer

	mu sync.Mutex
}

func NewStreamCodec(
	s network.Stream,
) proto.StreamCodec {
	return &StreamCodec{
		s:  s,
		sr: snappy.NewReader(s),
		sw: snappy.NewWriter(s),
	}
}

// write packet to stream. will add correct header + compression
// will error if packet does not implement ssz.Marshaler interface
func (d *StreamCodec) WritePacket(pkt proto.Packet) (n int, err error) {
	if val, ok := pkt.(ssz.Marshaler); ok {
		sz := val.SizeSSZ()
		bp := bufpool.Get(sz)
		defer bufpool.Put(bp)
		p := bp.Bytes()
		p = append(p, make([]byte, 10)...)
		vin := binary.PutVarint(p, int64(sz))
		_, err = val.MarshalSSZTo(p[vin:])
		if err != nil {
			return 0, fmt.Errorf("marshal ssz: %w", err)
		}
		n, err := d.sw.Write(p)
		if err != nil {
			return 0, err
		}
		return n, nil
	}
	return 0, fmt.Errorf("packet %s does not implement ssz.Marshaler", reflect.TypeOf(pkt))
}

// write raw bytes to stream
func (d *StreamCodec) Write(payload []byte) (n int, err error) {
	return d.s.Write(payload)
}

// decode into packet p, then return the packet context
func (d *StreamCodec) Decode(p proto.Packet) (ctx *proto.StreamContext, err error) {
	ctx, err = d.readPacket(p)
	return
}

func (d *StreamCodec) readPacket(p proto.Packet) (ctx *proto.StreamContext, err error) {
	c := &proto.StreamContext{
		Packet:   p,
		Stream:   d.s,
		Codec:    d,
		Protocol: d.s.Protocol(),
	}
	if val, ok := p.(ssz.Unmarshaler); ok {
		ln, _, err := proto.ReadUvarint(d.s)
		if err != nil {
			return c, err
		}
		c.Raw = make([]byte, ln)
		_, err = io.ReadFull(d.sr, c.Raw)
		if err != nil {
			return c, fmt.Errorf("readPacket: %w", err)
		}
		err = val.UnmarshalSSZ(c.Raw)
		if err != nil {
			return c, fmt.Errorf("readPacket: %w", err)
		}
	}
	return c, nil
}
