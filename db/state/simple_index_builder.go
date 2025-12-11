package state

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv/stream"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/version"
)

// interfaces defined here are not required to be implemented in
// forkables. These are just helpers when SimpleAccessorBuilder is used. Also can be used to provide some structure
// to build more custom indexes.
type IndexInputDataQuery interface {
	GetStream(ctx context.Context) stream.Trio[[]byte, uint64, uint64] // (word/value, index, offset)
	GetBaseDataId() uint64
	GetCount() uint64
	Close()
}

type IndexKeyFactory interface {
	// IndexInputDataQuery elements passed here to create key for index
	// `value` is snapshot element;
	// `index` is the corresponding sequence number in the file.
	Refresh()
	Make(value []byte, index uint64) []byte
	Close()
}

type AccessorArgs struct {
	Enums              bool
	LessFalsePositives bool
	Nofsync            bool

	BucketSize int
	LeafSize   uint16

	// other config options for recsplit
}

func NewAccessorArgs(enums, lessFalsePositives bool) *AccessorArgs {
	return &AccessorArgs{
		Enums:              enums,
		LessFalsePositives: lessFalsePositives,
		BucketSize:         recsplit.DefaultBucketSize,
		LeafSize:           recsplit.DefaultLeafSize,
	}
}

// simple accessor index
// goes through all (value, index) in segment file
// creates a recsplit index with
// index.key = kf(value, index)
// and index.value = offset
type SimpleAccessorBuilder struct {
	args     *AccessorArgs
	indexPos uint64
	id       ForkableId
	parser   SnapNameSchema
	kf       IndexKeyFactory
	fetcher  FirstEntityNumFetcher
	logger   log.Logger
}

type FirstEntityNumFetcher = func(from, to RootNum, seg *seg.Decompressor) Num

var _ AccessorIndexBuilder = (*SimpleAccessorBuilder)(nil)

func NewSimpleAccessorBuilder(args *AccessorArgs, id ForkableId, logger log.Logger, options ...AccessorBuilderOptions) *SimpleAccessorBuilder {
	b := &SimpleAccessorBuilder{
		args:   args,
		id:     id,
		parser: Registry.SnapshotConfig(id).Schema,
		logger: logger,
	}

	for _, opt := range options {
		opt(b)
	}

	if b.kf == nil {
		b.kf = NewSimpleIndexKeyFactory() //&SimpleIndexKeyFactory{num: make([]byte, binary.MaxVarintLen64)}
	}

	if b.fetcher == nil {
		// assume rootnum and num is same
		logger.Debug("using default first entity num fetcher for %s", id)
		b.fetcher = func(from, to RootNum, seg *seg.Decompressor) Num {
			return Num(from)
		}
	}

	return b
}

type AccessorBuilderOptions func(*SimpleAccessorBuilder)

func WithIndexPos(indexPos uint64) AccessorBuilderOptions {
	return func(s *SimpleAccessorBuilder) {
		if int(s.indexPos) >= len(Registry.IndexFileTag(s.id)) {
			panic("indexPos greater than indexFileTag length")
		}
		s.indexPos = indexPos
	}
}

func WithIndexKeyFactory(factory IndexKeyFactory) AccessorBuilderOptions {
	return func(s *SimpleAccessorBuilder) {
		s.kf = factory
	}
}

func (s *SimpleAccessorBuilder) SetAccessorArgs(args *AccessorArgs) {
	s.args = args
}

// TODO: this is supposed to go away once we start storing first entity num in the snapshot
func (s *SimpleAccessorBuilder) SetFirstEntityNumFetcher(fetcher FirstEntityNumFetcher) {
	s.fetcher = fetcher
}

func (s *SimpleAccessorBuilder) GetInputDataQuery(from, to RootNum) *DecompressorIndexInputDataQuery {
	sgname := s.parser.DataFile(version.V1_0, from, to)
	decomp, _ := seg.NewDecompressor(sgname)
	return &DecompressorIndexInputDataQuery{decomp: decomp, baseDataId: uint64(s.fetcher(from, to, decomp))}
}

func (s *SimpleAccessorBuilder) SetIndexKeyFactory(factory IndexKeyFactory) {
	s.kf = factory
}

func (s *SimpleAccessorBuilder) AllowsOrdinalLookupByNum() bool {
	return s.args.Enums
}

func (s *SimpleAccessorBuilder) Build(ctx context.Context, from, to RootNum, p *background.Progress) (i *recsplit.Index, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%s: at=%d-%d, %v, %s", Registry.IndexFileTag(s.id)[s.indexPos], from, to, rec, dbg.Stack())
		}
	}()
	iidq := s.GetInputDataQuery(from, to)
	defer iidq.Close()
	idxFile := s.parser.AccessorIdxFile(version.V1_0, from, to, s.indexPos)

	keyCount := iidq.GetCount()
	if p != nil {
		p.Name.Store(&idxFile)
		p.Total.Store(keyCount)
	}
	salt, err := Registry.Salt(s.id)
	if err != nil {
		return nil, err
	}

	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:           int(keyCount),
		Enums:              s.args.Enums,
		BucketSize:         s.args.BucketSize,
		LeafSize:           s.args.LeafSize,
		IndexFile:          idxFile,
		Salt:               &salt,
		NoFsync:            s.args.Nofsync,
		TmpDir:             Registry.Dirs(s.id).Tmp,
		LessFalsePositives: s.args.LessFalsePositives,
		BaseDataID:         iidq.GetBaseDataId(),
	}, s.logger)
	if err != nil {
		return nil, err
	}
	defer rs.Close()

	s.kf.Refresh()
	defer s.kf.Close()

	defer iidq.decomp.MadvSequential().DisableReadAhead()

	for {
		stream := iidq.GetStream(ctx)
		defer stream.Close()
		for stream.HasNext() {
			word, index, offset, err := stream.Next()
			if err != nil {
				return nil, err
			}
			if p != nil {
				p.Processed.Add(1)
			}
			key := s.kf.Make(word, index)
			if err = rs.AddKey(key, offset); err != nil {
				return nil, err
			}
			select {
			case <-ctx.Done():
				stream.Close()
				return nil, ctx.Err()
			default:
			}
		}
		stream.Close()
		if err = rs.Build(ctx); err != nil {
			p.Processed.CompareAndSwap(p.Processed.Load(), 0)
			// collision handling
			if errors.Is(err, recsplit.ErrCollision) {
				rs.ResetNextSalt()
				continue
			}
			return nil, err
		}

		break
	}

	return recsplit.OpenIndex(idxFile)

}

type DecompressorIndexInputDataQuery struct {
	decomp     *seg.Decompressor
	baseDataId uint64
}

// return trio: word, index, offset,
func (d *DecompressorIndexInputDataQuery) GetStream(ctx context.Context) stream.Trio[[]byte, uint64, uint64] {
	// open seg if not yet
	return &seg_stream{ctx: ctx, g: d.decomp.MakeGetter(), word: make([]byte, 0, 4096)}
}

func (d *DecompressorIndexInputDataQuery) GetBaseDataId() uint64 {
	// discuss: adding base data id to snapshotfile?
	// or might need to add callback to get first basedataid...
	return d.baseDataId
	//return d.from
}

func (d *DecompressorIndexInputDataQuery) GetCount() uint64 {
	return uint64(d.decomp.Count())
}

func (d *DecompressorIndexInputDataQuery) Close() {
	d.decomp.Close()
	d.decomp = nil
}

type seg_stream struct {
	g         *seg.Getter
	i, offset uint64
	ctx       context.Context
	word      []byte
}

func (s *seg_stream) Next() (word []byte, index uint64, offset uint64, err error) {
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
	if s.g == nil {
		return
	}
	s.g = nil
}

// index key factory "manufacturing" index keys only
type SimpleIndexKeyFactory struct {
	num []byte
}

func NewSimpleIndexKeyFactory() *SimpleIndexKeyFactory {
	return &SimpleIndexKeyFactory{num: make([]byte, binary.MaxVarintLen64)}
}

func (n *SimpleIndexKeyFactory) Refresh() {}

func (n *SimpleIndexKeyFactory) Make(_ []byte, index uint64) []byte {
	if n.num == nil {
		panic("index key factory closed or not initialized properly")
	}

	// everywhere except heimdall indexes, which use BigIndian format
	nm := binary.PutUvarint(n.num, index)
	return n.num[:nm]
}

func (n *SimpleIndexKeyFactory) Close() {
	//n.num = nil
}
