package proto

import (
	"fmt"

	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/libp2p/go-libp2p/core/network"
)

type Packet interface {
	Clone() Packet
}

type EmptyPacket struct{}

func (e *EmptyPacket) Clone() Packet {
	return &EmptyPacket{}
}

type Context struct {
	Packet   Packet
	Protocol protocol.ID
	Codec    PacketCodec
	Stream   network.Stream

	Raw []byte
}

type PacketCodec interface {
	WritePacket(pck Packet) (n int, err error)
	Decode(Packet) (ctx *Context, err error)

	Write(payload []byte) (n int, err error)
}

func (c *Context) String() string {
	return fmt.Sprintf("peer %s | packet %s | len %d", c.Stream.ID(), c.Stream.Protocol(), c.Packet)
}
