package types

import (
	"encoding/binary"
	"fmt"
)

const HeaderSize = 38
const HeaderSizePreEtrog = 29

type StreamType uint64

type HeaderEntry struct {
	PacketType   uint8  // 1:Header
	HeadLength   uint32 // 38 oe 29
	Version      uint8
	SystemId     uint64
	StreamType   StreamType // 1:Sequencer
	TotalLength  uint64     // Total bytes used in the file
	TotalEntries uint64     // Total number of data entries (entry type 2)
}

// Decode/convert from binary bytes slice to a header entry type
func DecodeHeaderEntryPreEtrog(b []byte) (*HeaderEntry, error) {
	return &HeaderEntry{
		PacketType:   b[0],
		HeadLength:   binary.BigEndian.Uint32(b[1:5]),
		StreamType:   StreamType(binary.BigEndian.Uint64(b[5:13])),
		TotalLength:  binary.BigEndian.Uint64(b[13:21]),
		TotalEntries: binary.BigEndian.Uint64(b[21:29]),
	}, nil
}

// Decode/convert from binary bytes slice to a header entry type
func DecodeHeaderEntry(b []byte) (*HeaderEntry, error) {
	if len(b) != HeaderSize {
		if len(b) == HeaderSizePreEtrog {
			return DecodeHeaderEntryPreEtrog(b)
		}
		return &HeaderEntry{}, fmt.Errorf("invalid header entry binary size. Expected: %d, got: %d", HeaderSize, len(b))
	}
	return &HeaderEntry{
		PacketType:   b[0],
		HeadLength:   binary.BigEndian.Uint32(b[1:5]),
		Version:      b[5],
		SystemId:     binary.BigEndian.Uint64(b[6:14]),
		StreamType:   StreamType(binary.BigEndian.Uint64(b[14:22])),
		TotalLength:  binary.BigEndian.Uint64(b[22:30]),
		TotalEntries: binary.BigEndian.Uint64(b[30:38]),
	}, nil
}
