package types

import (
	"encoding/binary"
)

const (
	BookmarkTypeStart byte = 0
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
		Type: BookmarkTypeStart,
		From: fromBlock,
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
