package proto

import (
	"fmt"

	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p/core/network"
)

// packet simply needs to implement clone so that it can be instantiated within the generic
type Packet interface {
	Clone() Packet
}

// context includes the original stream, the raw decompressed bytes, the codec the context was generated from, and the protocol ID
type Context struct {
	Packet   Packet
	Protocol protocol.ID
	Codec    PacketCodec
	Stream   network.Stream

	Raw []byte
}

// PacketCodec describes a wire format.
type PacketCodec interface {
	WritePacket(pck Packet) (n int, err error)
	Write(payload []byte) (n int, err error)

	Decode(Packet) (ctx *Context, err error)
}

func (c *Context) String() string {
	return fmt.Sprintf("peer %s | packet %s | len %d", c.Stream.ID(), c.Protocol, c.Packet)
}

// the empty packet doesn't implement any serialization, so it means to skip.
type EmptyPacket struct{}

func (e *EmptyPacket) Clone() Packet {
	return &EmptyPacket{}
}
