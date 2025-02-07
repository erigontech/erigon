package appendableutils

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/kv/stream"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon-lib/seg"
	"github.com/erigontech/erigon-lib/state"
)

// interfaces defined here are not required to be implemented by
// appendables. These are just helpers when SimpleAccessorBuilder is used. Also can be used to provide some structure
// to build more custom indexes.

type IndexInputDataQuery interface {
	GetStream(ctx context.Context) stream.Trio[[]byte, uint64, uint64] // (word/value, index, offset)
	GetBaseDataId() uint64
	GetCount() uint64
}

type IndexKeyFactory interface {
	// IndexInputDataQuery elements passed here to create key for index
	Make(value []byte, index uint64) []byte
}

type AccessorArgs struct {
	enums              bool
	lessFalsePositives bool
	salt               uint32
	nofsync            bool

	// other config options for recsplit
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
// goes through all (value, index) in segment file
// creates a recsplit index with
// index.key = kf(value, index)
// and index.value = offset
type SimpleAccessorBuilder struct {
	args     *AccessorArgs
	indexPos uint64
	id       AppendableId
	kf       IndexKeyFactory
}

func NewSimpleAccessorBuilder(args *AccessorArgs, id AppendableId) *SimpleAccessorBuilder {
	return &SimpleAccessorBuilder{
		args: args,
		id:   id,
		kf:   simpleIndexKeyFactoryInstance,
	}
}

func (s *SimpleAccessorBuilder) SetIndexPos(indexPos uint64) {
	if int(s.indexPos) >= len(s.id.IndexPrefix()) {
		panic("indexPos greater than indexPrefix length")
	}
	s.indexPos = indexPos
}

func (s *SimpleAccessorBuilder) SetAccessorArgs(args *AccessorArgs) {
	s.args = args
}

func (s *SimpleAccessorBuilder) GetInputDataQuery(from, to state.RootNum) *DecompressorIndexInputDataQuery {
	// just segname?
	sgname := SegName(s.id, snaptype.Version(1), from, to)
	decomp, _ := seg.NewDecompressor(sgname)
	return &DecompressorIndexInputDataQuery{decomp: decomp}
}

func (s *SimpleAccessorBuilder) SetIndexKeyFactory(factory IndexKeyFactory) {
	s.kf = factory
}

func (s *SimpleAccessorBuilder) AllowsOrdinalLookupByNum() bool {
	return s.args.enums
}

func (s *SimpleAccessorBuilder) Build(ctx context.Context, from, to state.RootNum, tmpDir string, p *background.Progress, lvl log.Lvl, logger log.Logger) (*recsplit.Index, error) {
	iidq := s.GetInputDataQuery(from, to)
	idxFile := IdxName(s.id, snaptype.Version(1), from, to, s.indexPos)
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
			if err != nil {
				return nil, err
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
	decomp *seg.Decompressor
}

// return trio: word, index, offset,
func (d *DecompressorIndexInputDataQuery) GetStream(ctx context.Context) stream.Trio[[]byte, uint64, uint64] {
	// open seg if not yet
	return &seg_stream{ctx: ctx, g: d.decomp.MakeGetter(), word: make([]byte, 0, 4096)}
}

func (d *DecompressorIndexInputDataQuery) GetBaseDataId() uint64 {
	// discuss: adding base data id to snapshotfile?
	// or might need to add callback to get first basedataid...
	return 0
	//return d.from
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
	s.g = nil
}

// index key factory "manufacturing" index keys only
var simpleIndexKeyFactoryInstance = &SimpleIndexKeyFactory{num: make([]byte, binary.MaxVarintLen64)}

type SimpleIndexKeyFactory struct {
	num []byte
}

func (n *SimpleIndexKeyFactory) Make(_ []byte, index uint64) []byte {
	// everywhere except heimdall indexes, which use BigIndian format
	nm := binary.PutUvarint(n.num, index)
	return n.num[:nm]
}
