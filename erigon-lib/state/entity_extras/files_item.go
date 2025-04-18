package entity_extras

import (
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
)

type FilesItem interface {
	Segment() *seg.Decompressor
	AccessorIndex() *recsplit.Index
	BtIndex() *BtIndex
	ExistenceFilter() *ExistenceFilter
}
