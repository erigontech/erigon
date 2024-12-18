package appendables

import (
	"context"
	"iter"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/log/v3"
)

type IndexInputDataQuery iter.Seq2[[]byte, uint64]

type IndexKeyFactory interface {
	// IndexInputDataQuery elements passed here to create key for index
	Make(value []byte, index uint64) []byte
}

type IndexDescriptor struct {
	IndexPath  string
	BaseDataId uint64
	KeyFactory IndexKeyFactory
}

type IndexBuilder interface {
	Build(ctx context.Context, descriptor IndexDescriptor, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) error
	GetIdentifier() string // unique identifier for this index
	GetInputDataQuery(step uint64) IndexInputDataQuery
}

type AccessorIndexBuilder interface {
	IndexBuilder
	SetAccessorArgs(*AccessorArgs)
}

type AccessorArgs struct {
	enums              bool
	lessFalsePositives bool
	salt               bool
	nofsync            bool
}

func NewAccessorArgs(enums, lessFalsePositives, salt, nofsync bool) *AccessorArgs {
	return &AccessorArgs{
		enums:              enums,
		lessFalsePositives: lessFalsePositives,
		salt:               salt,
		nofsync:            nofsync,
	}
}
