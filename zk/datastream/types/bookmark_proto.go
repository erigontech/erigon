package types

import (
	"github.com/ledgerwatch/erigon/zk/datastream/proto/github.com/0xPolygonHermez/zkevm-node/state/datastream"
	"google.golang.org/protobuf/proto"
)

type BookmarkProto struct {
	*datastream.BookMark
}

func NewBookmarkProto(value uint64, bookmarkType datastream.BookmarkType) *BookmarkProto {
	return &BookmarkProto{
		BookMark: &datastream.BookMark{
			Type:  bookmarkType,
			Value: value,
		},
	}
}

func (b *BookmarkProto) Marshal() ([]byte, error) {
	return proto.Marshal(b.BookMark)
}

func (b *BookmarkProto) Type() EntryType {
	return BookmarkEntryType
}

func (b *BookmarkProto) BookmarkType() datastream.BookmarkType {
	return b.BookMark.GetType()
}

func (b *BookmarkProto) UnmarshalBookmark(data []byte) error {
	bookmark := &datastream.BookMark{}
	if err := proto.Unmarshal(data, bookmark); err != nil {
		return err
	}

	b.BookMark = bookmark
	return nil
}

func UnmarshalBookmark(data []byte) (*BookmarkProto, error) {
	bookmark := &datastream.BookMark{}
	if err := proto.Unmarshal(data, bookmark); err != nil {
		return nil, err
	}

	return &BookmarkProto{
		BookMark: bookmark,
	}, nil
}
