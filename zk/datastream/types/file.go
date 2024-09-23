package types

import (
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
)

const (
	FileEntryMinSize uint32 = 17 // 1+4+4+8
)

type FileEntry struct {
	PacketType uint8     // 2:Data entry, 0:Padding, (1:Header)
	Length     uint32    // Length of the entry
	EntryType  EntryType // e.g. 1:L2 block, 2:L2 tx, 176: bookmark
	EntryNum   uint64    // Entry number (sequential starting with 0)
	Data       []byte
}

func (f *FileEntry) IsBookmark() bool {
	return f.EntryType == BookmarkEntryType
}

// PROTO TYPES

func (f *FileEntry) IsBookmarkBatch() bool {
	return uint32(f.EntryType) == uint32(datastream.BookmarkType_BOOKMARK_TYPE_BATCH)
}

func (f *FileEntry) IsBookmarkBlock() bool {
	return uint32(f.EntryType) == uint32(datastream.BookmarkType_BOOKMARK_TYPE_L2_BLOCK)
}

func (f *FileEntry) IsL2BlockEnd() bool {
	return uint32(f.EntryType) == uint32(datastream.EntryType_ENTRY_TYPE_L2_BLOCK_END)
}
func (f *FileEntry) IsL2Block() bool {
	return uint32(f.EntryType) == uint32(datastream.EntryType_ENTRY_TYPE_L2_BLOCK)
}

func (f *FileEntry) IsL2Tx() bool {
	return uint32(f.EntryType) == uint32(datastream.EntryType_ENTRY_TYPE_TRANSACTION)
}

func (f *FileEntry) IsBatchStart() bool {
	return uint32(f.EntryType) == uint32(datastream.EntryType_ENTRY_TYPE_BATCH_START)
}

func (f *FileEntry) IsBatchEnd() bool {
	return uint32(f.EntryType) == uint32(datastream.EntryType_ENTRY_TYPE_BATCH_END)
}

func (f *FileEntry) IsUpdateGer() bool {
	return uint32(f.EntryType) == uint32(datastream.EntryType_ENTRY_TYPE_UPDATE_GER)
}

// End PROTO TYPES

func (f *FileEntry) IsGerUpdate() bool {
	return f.EntryType == EntryTypeGerUpdate
}

// Encode encodes file entry to the binary format
func (f *FileEntry) Encode() []byte {
	be := make([]byte, 1)
	be[0] = f.PacketType
	be = binary.BigEndian.AppendUint32(be, f.Length)
	be = binary.BigEndian.AppendUint32(be, uint32(f.EntryType))
	be = binary.BigEndian.AppendUint64(be, f.EntryNum)
	be = append(be, f.Data...) //nolint:makezero
	return be
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
