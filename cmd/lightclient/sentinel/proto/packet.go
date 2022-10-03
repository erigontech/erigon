package proto

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p-core/protocol"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/network"
)

// packet simply needs to implement clone so that it can be instantiated within the generic
type Packet interface {
	Clone() Packet
}

// context includes the original stream, the raw decompressed bytes, the codec the context was generated from, and the protocol ID
type StreamContext struct {
	Packet   Packet
	Protocol protocol.ID
	Codec    StreamCodec
	Stream   network.Stream

	Raw []byte
}

func (c *StreamContext) String() string {
	return fmt.Sprintf("peer %s | packet %s | len %d", c.Stream.ID(), c.Protocol, c.Packet)
}

type SubContext struct {
	// the packet
	Packet Packet
	// the topic of the message
	Topic string
	// the actual message
	Msg *pubsub.Message

	// the codec used to decode the message
	Codec SubCodec

	// the decompressed message in the native encoding of msg
	Raw []byte
}

// PacketCodec describes a wire format.
type StreamCodec interface {
	Close() error
	CloseWriter() error
	CloseReader() error

	Write(payload []byte) (n int, err error)
	WritePacket(pck Packet) (n int, err error)
	Decode(Packet) (ctx *StreamContext, err error)

	Read(payload []byte) (n int, err error)
	ReadByte() (b byte, err error)
}

// SubCodec describes a wire format for pubsub messages
type SubCodec interface {
	Decode(context.Context, Packet) (*SubContext, error)
}

func (c *SubContext) String() string {
	return fmt.Sprintf("peer %s | topic %s | len %d", c.Msg.ReceivedFrom, c.Topic, c.Packet)
}

// the empty packet doesn't implement any serialization, so it means to skip.
type EmptyPacket struct{}

func (e *EmptyPacket) Clone() Packet {
	return &EmptyPacket{}
}

// the error message skips decoding but does do the decompression.
type ErrorMessage struct {
	Message []byte `json:"message"`
}

func (typ *ErrorMessage) Clone() Packet {
	return &ErrorMessage{}
}

func (typ *ErrorMessage) UnmarshalSSZ(buf []byte) error {
	typ.Message = buf
	return nil
}
