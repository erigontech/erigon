package ssz_snappy

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"

	ssz "github.com/ferranbt/fastssz"
	"github.com/golang/snappy"
	"github.com/ledgerwatch/erigon/cmd/lightclient/clparams"
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication"
	"github.com/libp2p/go-libp2p/core/network"
)

type StreamCodec struct {
	s  network.Stream
	sr *snappy.Reader

	mu sync.Mutex
}

func NewStreamCodec(
	s network.Stream,
) communication.StreamCodec {
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

func (d *StreamCodec) CloseWriter() error {
	if err := d.s.CloseWrite(); err != nil {
		return err
	}
	return nil
}

func (d *StreamCodec) CloseReader() error {
	if err := d.s.CloseRead(); err != nil {
		return err
	}
	return nil
}

// write packet to stream. will add correct header + compression
// will error if packet does not implement ssz.Marshaler interface
func (d *StreamCodec) WritePacket(pkt communication.Packet) (n int, err error) {
	// if its a metadata request we dont write anything
	if reflect.TypeOf(pkt) == reflect.TypeOf(&lightrpc.MetadataV1{}) || reflect.TypeOf(pkt) == reflect.TypeOf(&lightrpc.MetadataV2{}) {
		return 0, nil
	}

	p, sw, err := encodePacket(pkt, d.s)
	if err != nil {
		return 0, fmt.Errorf("failed to write packet err=%s", err)
	}

	n, err = sw.Write(p)
	if err != nil {
		return 0, err
	}

	if err := sw.Flush(); err != nil {
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
func (d *StreamCodec) Decode(p communication.Packet) (ctx *communication.StreamContext, err error) {
	ctx, err = d.readPacket(p)
	return
}

func (d *StreamCodec) readPacket(p communication.Packet) (ctx *communication.StreamContext, err error) {
	c := &communication.StreamContext{
		Packet:   p,
		Stream:   d.s,
		Codec:    d,
		Protocol: d.s.Protocol(),
	}
	if val, ok := p.(ssz.Unmarshaler); ok {
		ln, err := readUvarint(d.s)
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

func encodePacket(pkt communication.Packet, stream network.Stream) ([]byte, *snappy.Writer, error) {
	if val, ok := pkt.(ssz.Marshaler); ok {
		wr := bufio.NewWriter(stream)
		sw := snappy.NewWriter(wr)
		p := make([]byte, 10)

		vin := binary.PutVarint(p, int64(val.SizeSSZ()))

		enc, err := val.MarshalSSZ()
		if err != nil {
			return nil, nil, fmt.Errorf("marshal ssz: %w", err)
		}

		if len(enc) > int(clparams.MaxChunkSize) {
			return nil, nil, fmt.Errorf("chunk size too big")
		}

		_, err = wr.Write(p[:vin])
		if err != nil {
			return nil, nil, fmt.Errorf("write varint: %w", err)
		}

		return enc, sw, nil
	}

	return nil, nil, fmt.Errorf("packet %s does not implement ssz.Marshaler", reflect.TypeOf(pkt))
}

func readUvarint(r io.Reader) (uint64, error) {
	var x uint64
	var s uint
	bs := [1]byte{}
	for i := 0; i < 10; i++ {
		_, err := r.Read(bs[:])
		if err != nil {
			return x, err
		}
		b := bs[0]
		if b < 0x80 {
			if i == 10-1 && b > 1 {
				return x, errors.New("readUvarint: overflow")
			}
			return x | uint64(b)<<s, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return x, errors.New("readUvarint: overflow")
}
