package types

import (
	"encoding/binary"
	"fmt"
)

const HeaderSize = 29

type StreamType uint64

type HeaderEntry struct {
	PacketType   uint8      // 1:Header
	HeadLength   uint32     // 29
	StreamType   StreamType // 1:Sequencer
	TotalLength  uint64     // Total bytes used in the file
	TotalEntries uint64     // Total number of data entries (entry type 2)
}

// Decode/convert from binary bytes slice to a header entry type
func DecodeHeaderEntry(b []byte) (*HeaderEntry, error) {
	if len(b) != HeaderSize {
		return &HeaderEntry{}, fmt.Errorf("invalid header entry binary size. Expected: %d, got: %d", HeaderSize, len(b))
	}
	return &HeaderEntry{
		PacketType:   b[0],
		HeadLength:   binary.BigEndian.Uint32(b[1:5]),
		StreamType:   StreamType(binary.BigEndian.Uint64(b[5:13])),
		TotalLength:  binary.BigEndian.Uint64(b[13:21]),
		TotalEntries: binary.BigEndian.Uint64(b[21:29]),
	}, nil
}
