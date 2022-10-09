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
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"

	ssz "github.com/ferranbt/fastssz"
	"github.com/golang/snappy"
	"github.com/ledgerwatch/erigon/cmd/lightclient/rpc/lightrpc"
	"github.com/ledgerwatch/erigon/cmd/lightclient/sentinel/communication"
	"github.com/libp2p/go-libp2p/core/network"
)

type StreamCodec struct {
	s  network.Stream
	sr *snappy.Reader
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
func (d *StreamCodec) WritePacket(pkt communication.Packet, prefix ...byte) (n int, err error) {
	// if its a metadata request we dont write anything
	if reflect.TypeOf(pkt) == reflect.TypeOf(&lightrpc.MetadataV1{}) || reflect.TypeOf(pkt) == reflect.TypeOf(&lightrpc.MetadataV2{}) {
		return 0, nil
	}
	val, ok := pkt.(ssz.Marshaler)
	if !ok {
		return 0, nil
	}
	lengthBuf := make([]byte, 10)
	vin := binary.PutUvarint(lengthBuf, uint64(val.SizeSSZ()))
	wr := bufio.NewWriterSize(d.s, 10+val.SizeSSZ())
	defer wr.Flush()
	wr.Write(prefix) // write prefix first (done for responses)
	wr.Write(lengthBuf[:vin])
	sw := snappy.NewBufferedWriter(wr)
	defer sw.Flush()
	xs := make([]byte, 0, val.SizeSSZ())
	enc, err := val.MarshalSSZTo(xs)
	if err != nil {
		return 0, err
	}
	return sw.Write(enc)
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
