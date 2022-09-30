package ssz_snappy

import (
	"fmt"
	"io"
	"sync"

	ssz "github.com/ferranbt/fastssz"
	"github.com/golang/snappy"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/proto"
	"github.com/libp2p/go-libp2p/core/network"
)

type StreamCodec struct {
	s  network.Stream
	sr *snappy.Reader

	mu sync.Mutex
}

func NewStreamCodec(
	s network.Stream,
) proto.StreamCodec {
	return &StreamCodec{
		s:  s,
		sr: snappy.NewReader(s),
	}
}

func (d *StreamCodec) Close() error {
	if err := d.s.Close(); err != nil {
		return err
	}
	return nil
}

// write packet to stream. will add correct header + compression
// will error if packet does not implement ssz.Marshaler interface
func (d *StreamCodec) WritePacket(pkt proto.Packet) (n int, err error) {
	p, sw, err := EncodePacket(pkt, d.s)
	if err != nil {
		return 0, fmt.Errorf("Failed to write packet err=%s", err)
	}

	n, err = sw.Write(p)
	if err != nil {
		return 0, err
	}
	err = sw.Flush()
	if err != nil {
		return 0, err
	}
	return n, nil
}

// write raw bytes to stream
func (d *StreamCodec) Write(payload []byte) (n int, err error) {
	return d.s.Write(payload)
}

// read raw bytes to stream
func (d *StreamCodec) Read(b []byte) (n int, err error) {
	return d.s.Read(b)
}

// read raw bytes to stream
func (d *StreamCodec) ReadByte() (b byte, err error) {
	o := [1]byte{}
	_, err = io.ReadFull(d.s, o[:])
	if err != nil {
		return
	}
	return o[0], nil
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
