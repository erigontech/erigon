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

type Codec struct {
	s  network.Stream
	sr *snappy.Reader
	sw *snappy.Writer

	mu sync.Mutex
}

func NewCodec(
	s network.Stream,
) *Codec {
	return &Codec{
		s:  s,
		sr: snappy.NewReader(s),
		sw: snappy.NewWriter(s),
	}
}

func (d *Codec) WritePacket(p proto.Packet) (n int, err error) {
	if val, ok := p.(ssz.Marshaler); ok {
		sz := val.SizeSSZ()
		bp := bufpool.Get(sz)
		defer bufpool.Put(bp)
		p := bp.Bytes()
		p, err = val.MarshalSSZTo(p)
		if err != nil {
			return 0, fmt.Errorf("marshal ssz: %w", err)
		}
		vi := binary.AppendVarint(p, int64(sz))
		n0, err := d.s.Write(vi)
		if err != nil {
			return 0, err
		}
		n1, err := d.sw.Write(p)
		if err != nil {
			return n0, err
		}
		return n0 + n1, nil
	}
	return 0, fmt.Errorf("packet %s does not implement ssz.Marshaler", reflect.TypeOf(p))
}

func (d *Codec) Write(payload []byte) (n int, err error) {
	return d.s.Write(payload)
}

func (d *Codec) Decode(p proto.Packet) (ctx *proto.Context, err error) {
	ctx, err = d.readPacket(p)
	return
}

func (d *Codec) readPacket(p proto.Packet) (ctx *proto.Context, err error) {
	c := &proto.Context{
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
