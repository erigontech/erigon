package types

import (
	"encoding/binary"
)

const (
	BookmarkTypeBlock byte = 0
	BookmarkTypeBatch byte = 1
	EntryTypeBookmark      = EntryType(176)
)

type Bookmark struct {
	Type byte
	From uint64
}

func (b *Bookmark) EntryType() EntryType {
	return EntryTypeBookmark
}

func (b *Bookmark) Bytes(bigEndian bool) []byte {
	if bigEndian {
		return b.EncodeBigEndian()
	}
	return b.Encode()
}

func NewL2BlockBookmark(fromBlock uint64) *Bookmark {
	return &Bookmark{
		Type: BookmarkTypeBlock,
		From: fromBlock,
	}
}

func NewL2BatchBookmark(fromBatch uint64) *Bookmark {
	return &Bookmark{
		Type: BookmarkTypeBatch,
		From: fromBatch,
	}
}

func (b *Bookmark) Encode() []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, b.Type)
	bytes = binary.LittleEndian.AppendUint64(bytes, b.From)
	return bytes
}

func (b *Bookmark) EncodeBigEndian() []byte {
	bytes := make([]byte, 0)
	bytes = append(bytes, b.Type)
	bytes = binary.BigEndian.AppendUint64(bytes, b.From)
	return bytes
}

func (b *Bookmark) IsTypeBlock() bool {
	return b.Type == BookmarkTypeBlock
}

func (b *Bookmark) IsTypeBatch() bool {
	return b.Type == BookmarkTypeBatch
}

func DecodeBookmark(data []byte) *Bookmark {
	return &Bookmark{
		Type: data[0],
		From: binary.LittleEndian.Uint64(data[1:]),
	}
}

func DecodeBookmarkBigEndian(data []byte) *Bookmark {
	return &Bookmark{
		Type: data[0],
		From: binary.BigEndian.Uint64(data[1:]),
	}
}
