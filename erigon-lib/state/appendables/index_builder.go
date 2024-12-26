package appendables

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
)

type IndexInputDataQuery interface {
	GetStream(ctx context.Context) stream.Trio[[]byte, uint64, uint64] // (word/value, index, offset)
	GetBaseDataId() uint64
	GetCount() uint64
}

type IndexKeyFactory interface {
	// IndexInputDataQuery elements passed here to create key for index
	Make(value []byte, index uint64) []byte
}

type IndexBuilder[IndexType any] interface {
	Build(ctx context.Context, stepFrom, stepTo uint64, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (IndexType, error)
	// GetIdentifier() string // unique identifier for this index
	// GetInputDataQuery(stepKeyFrom, stepKeyTo uint64) IndexInputDataQuery
	// GetIndexKeyFactory() IndexKeyFactory
	// GetIndexPath(stepKeyFrom, stepKeyTo uint64) string // can be removed if not in Build()
}

type AccessorIndexBuilder interface {
	IndexBuilder[*recsplit.Index]
	SetAccessorArgs(*AccessorArgs)
}

type AccessorArgs struct {
	enums              bool
	lessFalsePositives bool
	salt               uint32
	nofsync            bool
}

func NewAccessorArgs(enums, lessFalsePositives, nofsync bool, salt uint32) *AccessorArgs {
	return &AccessorArgs{
		enums:              enums,
		lessFalsePositives: lessFalsePositives,
		salt:               salt,
		nofsync:            nofsync,
	}
}

// simple accessor index

type SimpleAccessorBuilder struct {
	args        *AccessorArgs
	idxBaseName string
	enum        ApEnum
	kf          IndexKeyFactory
}

func NewSimpleAccessorBuilder(args *AccessorArgs, enum ApEnum, idxBaseName string) *SimpleAccessorBuilder {
	return &SimpleAccessorBuilder{
		args:        args,
		idxBaseName: idxBaseName,
		enum:        enum,
		//version:
	}
}

func (s *SimpleAccessorBuilder) SetAccessorArgs(args *AccessorArgs) {
	s.args = args
}

func (s *SimpleAccessorBuilder) GetInputDataQuery(stepKeyFrom, stepKeyTo uint64) *DecompressorIndexInputDataQuery {
	// just segname?

	sgname := AppeSegName(s.enum, 1, stepKeyFrom, stepKeyTo)
	decomp, _ := seg.NewDecompressor(sgname)
	return &DecompressorIndexInputDataQuery{decomp: decomp, from: stepKeyFrom}
}

func (s *SimpleAccessorBuilder) SetIndexKeyFactory(factory IndexKeyFactory) {
	s.kf = factory
}

func (s *SimpleAccessorBuilder) Build(ctx context.Context, stepFrom, stepTo uint64, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (*recsplit.Index, error) {
	iidq := s.GetInputDataQuery(stepFrom, stepTo)
	idxFile := AppeIdxName(1, stepFrom, stepTo, s.idxBaseName)

	// enums              bool
	// lessFalsePositives bool
	// salt               bool
	// nofsync            bool

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:           int(iidq.GetCount()),
		Enums:              true,
		BucketSize:         2000,
		IndexFile:          idxFile,
		Salt:               &s.args.salt,
		NoFsync:            s.args.nofsync,
		LessFalsePositives: s.args.lessFalsePositives,
		BaseDataID:         iidq.GetBaseDataId(),
	}, logger)
	if err != nil {
		return nil, err
	}

	defer iidq.decomp.EnableReadAhead().DisableReadAhead()

	for {
		stream := iidq.GetStream(ctx)
		for stream.HasNext() {
			word, index, offset, err := stream.Next()
			key := s.kf.Make(word, index)
			if err = rs.AddKey(key, offset); err != nil {
				return nil, err
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
		}
		if err = rs.Build(ctx); err != nil {
			// collision handling
			if errors.Is(err, recsplit.ErrCollision) {
				rs.ResetNextSalt()
				continue
				// return etc.
			}
			return nil, err
			// return etc.
		}

		break
	}

	return recsplit.OpenIndex(idxFile)

}

type DecompressorIndexInputDataQuery struct {
	decomp *seg.Decompressor
	from   uint64
}

func (d *DecompressorIndexInputDataQuery) GetStream(ctx context.Context) stream.Trio[[]byte, uint64, uint64] {
	// open seg if not yet
	return &seg_stream{ctx: ctx, g: d.decomp.MakeGetter(), word: make([]byte, 0, 4096)}
}

func (d *DecompressorIndexInputDataQuery) GetBaseDataId() uint64 {
	return d.from
}

func (d *DecompressorIndexInputDataQuery) GetCount() uint64 {
	return uint64(d.decomp.Count())
}

type seg_stream struct {
	g         *seg.Getter
	i, offset uint64
	ctx       context.Context
	word      []byte
}

func (s *seg_stream) Next() ([]byte, uint64, uint64, error) {
	// check if ctx is done...
	if s.g.HasNext() {
		word, nextPos := s.g.Next(s.word[:0])
		defer func() {
			s.offset = nextPos
			s.i++
		}()
		return word, s.i, s.offset, nil
	}
	return nil, 0, 0, io.EOF
}

func (s *seg_stream) HasNext() bool {
	return s.g.HasNext()
}

func (s *seg_stream) Close() {
	s.g = nil
}

type SimpleIndexKeyFactory struct {
	num []byte
}

func NewNoopIndexKeyFactory() IndexKeyFactory {
	return &SimpleIndexKeyFactory{num: make([]byte, binary.MaxVarintLen64)}
}

func (n *SimpleIndexKeyFactory) Make(value []byte, index uint64) []byte {
	nm := binary.PutUvarint(n.num, index)
	return n.num[:nm]
}
