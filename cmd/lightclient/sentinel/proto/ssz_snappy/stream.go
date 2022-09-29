package ssz_snappy

import (
	"encoding/binary"
	"fmt"
	"io"
	"reflect"
	"sync"

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
		p := make([]byte, 10+sz)
		vin := binary.PutVarint(p, int64(sz))
		enc, err := val.MarshalSSZ()
		if err != nil {
			return 0, fmt.Errorf("marshal ssz: %w", err)
		}
		n, err := d.sw.Write(append(p[:vin], enc...))
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
