package types

import (
	"encoding/binary"
	"fmt"
)

type EntryType uint32

const (
	FileEntryMinSize  uint32    = 17 // 1+4+4+8
	BookmarkEntryType EntryType = 176
)

type FileEntry struct {
	PacketType uint8     // 2:Data entry, 0:Padding, (1:Header)
	Length     uint32    // Length of the entry
	EntryType  EntryType // e.g. 1:L2 block, 2:L2 tx, 176: bookmark
	EntryNum   uint64    // Entry number (sequential starting with 0)
	Data       []byte
}

func (f *FileEntry) IsBlockStart() bool {
	return f.EntryType == EntryTypeStartL2Block
}

func (f *FileEntry) IsTx() bool {
	return f.EntryType == EntryTypeL2Tx
}

func (f *FileEntry) IsBlockEnd() bool {
	return f.EntryType == EntryTypeEndL2Block
}

func (f *FileEntry) IsBookmark() bool {
	return f.EntryType == BookmarkEntryType
}

func (f *FileEntry) IsGerUpdate() bool {
	return f.EntryType == EntryTypeGerUpdate
}

// Decode/convert from binary bytes slice to FileEntry type
func DecodeFileEntry(b []byte) (*FileEntry, error) {
	if uint32(len(b)) < FileEntryMinSize {
		return &FileEntry{}, fmt.Errorf("invalid FileEntry binary size. Expected: >=%d, got: %d", FileEntryMinSize, len(b))
	}

	length := binary.BigEndian.Uint32(b[1:5])
	if length != uint32(len(b)) {
		return &FileEntry{}, fmt.Errorf("invalid FileEntry binary size. Expected: %d, got: %d", length, len(b))
	}

	data := b[17:]
	if uint32(len(data)) != length-FileEntryMinSize {
		return &FileEntry{}, fmt.Errorf("invalid FileEntry.data binary size. Expected: %d, got: %d", length-FileEntryMinSize, len(data))
	}

	return &FileEntry{
		PacketType: b[0],
		Length:     length,
		EntryType:  EntryType(binary.BigEndian.Uint32(b[5:9])),
		EntryNum:   binary.BigEndian.Uint64(b[9:17]),
		Data:       data,
	}, nil
}
