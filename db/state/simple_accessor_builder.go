package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"path/filepath"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
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

	// data file settings
	ValuesOnCompressedPage int
	StepSize               int

	// other config options for recsplit
}

func NewAccessorArgs(enums, lessFalsePositives bool, valuesOnCompressedPage int, stepSize uint64) *AccessorArgs {
	return &AccessorArgs{
		Enums:                  enums,
		LessFalsePositives:     lessFalsePositives,
		BucketSize:             recsplit.DefaultBucketSize,
		LeafSize:               recsplit.DefaultLeafSize,
		ValuesOnCompressedPage: valuesOnCompressedPage,
		StepSize:               int(stepSize),
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
	id       kv.ForkableId
	parser   SnapNameSchema
	kf       IndexKeyFactory
	fetcher  FirstEntityNumFetcher

	tmpDir string
	logger log.Logger
}

type FirstEntityNumFetcher = func(from, to RootNum, seg *seg.Decompressor) Num

var _ AccessorIndexBuilder = (*SimpleAccessorBuilder)(nil)

func NewSimpleAccessorBuilder(args *AccessorArgs, id kv.ForkableId, tmpDir string, logger log.Logger, options ...AccessorBuilderOptions) *SimpleAccessorBuilder {
	b := &SimpleAccessorBuilder{
		args:   args,
		id:     id,
		parser: Registry.SnapshotConfig(id).Schema,
		tmpDir: tmpDir,
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
		logger.Debug("using default first entity num fetcher", "id", id)
		b.fetcher = func(from, to RootNum, seg *seg.Decompressor) Num {
			return Num(from)
		}
	}

	if b.indexPos >= uint64(len(b.parser.IndexTags())) {
		panic("indexPos greater than indexFileTag length")
	}

	return b
}

type AccessorBuilderOptions func(*SimpleAccessorBuilder)

func WithIndexPos(indexPos uint64) AccessorBuilderOptions {
	return func(s *SimpleAccessorBuilder) {
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

func (s *SimpleAccessorBuilder) GetInputDataQuery(decomp *seg.Decompressor, compressionUsed bool) (*DecompressorIndexInputDataQuery, error) {
	//sgname := s.parser.DataFile(version.V1_0, from, to)
	//decomp, _ := seg.NewDecompressorWithMetadata(sgname, true)
	reader := seg.NewPagedReader(decomp.MakeGetter(), s.args.ValuesOnCompressedPage, compressionUsed)
	return NewDecompressorIndexInputDataQuery(reader)
}

func (s *SimpleAccessorBuilder) isCompressionUsed(from, to RootNum) bool {
	return uint64(to-from) > uint64(s.args.StepSize)
}

func (s *SimpleAccessorBuilder) SetIndexKeyFactory(factory IndexKeyFactory) {
	s.kf = factory
}

func (s *SimpleAccessorBuilder) Build(ctx context.Context, decomp *seg.Decompressor, from, to RootNum, p *background.Progress) (i *recsplit.Index, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%s: at=%d-%d, %v, %s", s.parser.IndexTags()[s.indexPos], from, to, rec, dbg.Stack())
		}
	}()

	iidq, err := s.GetInputDataQuery(decomp, s.isCompressionUsed(from, to))
	if err != nil {
		return nil, err
	}
	defer iidq.Close()
	meta := iidq.Metadata()
	idxFile := s.parser.AccessorIdxFile(version.V1_0, from, to, s.indexPos)

	keyCount := iidq.Count()
	if p != nil {
		baseFileName := filepath.Base(idxFile)
		p.Name.Store(&baseFileName)
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
		TmpDir:             s.tmpDir,
		LessFalsePositives: s.args.LessFalsePositives,
		BaseDataID:         uint64(meta.First),
	}, s.logger)
	if err != nil {
		return nil, err
	}
	defer rs.Close()
	rs.LogLvl(log.LvlTrace)

	s.kf.Refresh()
	defer s.kf.Close()

	defer iidq.reader.MadvNormal().DisableReadAhead()
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
			// collision handling
			if rs.Collision() {
				p.Processed.Store(0)
				s.logger.Debug("found collision, trying again", "file", filepath.Base(idxFile), "salt", rs.Salt(), "err", err)
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
	reader *seg.PagedReader
	m      NumMetadata
}

func NewDecompressorIndexInputDataQuery(reader *seg.PagedReader) (*DecompressorIndexInputDataQuery, error) {
	d := &DecompressorIndexInputDataQuery{reader: reader}
	d.m = NumMetadata{}
	if err := d.m.Unmarshal(reader.GetMetadata()); err != nil {
		return nil, err
	}
	return d, nil
}

// return trio: word, index, offset,
func (d *DecompressorIndexInputDataQuery) GetStream(ctx context.Context) stream.Trio[[]byte, uint64, uint64] {
	// open seg if not yet
	pds := &pagedSegDataStream{
		ctx:    ctx,
		reader: d.reader,
		word:   make([]byte, 0, 4096),
		meta:   d.m,
		i:      d.m.First.Uint64(),
	}
	pds.pageSize = uint64(d.reader.PageSize())
	return pds
}

func (d *DecompressorIndexInputDataQuery) Count() uint64 {
	return uint64(d.reader.Count())
}

func (d *DecompressorIndexInputDataQuery) Metadata() NumMetadata {
	return d.m
}

func (d *DecompressorIndexInputDataQuery) Close() {
	d.reader = nil
}

// this can handle case where key is stored (pageSize > 1)
// or when keys are sequential uints
// case where neither key is stored, nor keys are sequential
// require a ef file. Which will be done separately.
type pagedSegDataStream struct {
	reader    *seg.PagedReader
	meta      NumMetadata
	i, offset uint64
	pageSize  uint64
	ctx       context.Context
	word      []byte
}

func (s *pagedSegDataStream) Next() (word []byte, index uint64, offset uint64, err error) {
	// check if ctx is done...
	var pageOffset uint64
	var k []byte
	if s.reader.HasNext() {
		//for
		k, word, s.word, pageOffset = s.reader.Next2(s.word[:0])
		defer func() {
			s.i++
			s.offset = pageOffset
			for s.reader.HasNextOnPage() {
				s.reader.Skip()
				s.i++
			}
		}()

		if s.pageSize <= 1 {
			index = s.i
		} else {
			// keys are present
			s.offset = pageOffset
			index = binary.BigEndian.Uint64(k)
		}
		return word, index, s.offset, nil
	}
	return nil, 0, 0, io.EOF
}

func (s *pagedSegDataStream) HasNext() bool {
	return s.reader.HasNext()
}

func (s *pagedSegDataStream) Close() {
	if s.reader == nil {
		return
	}
	s.reader = nil
}

// index key factory "manufacturing" index key uint64 -> big endian bytes
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

	binary.BigEndian.PutUint64(n.num, index)
	return n.num[:8]
}

func (n *SimpleIndexKeyFactory) Close() {
	//n.num = nil
}
