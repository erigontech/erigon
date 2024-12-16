package multiencseq

import (
	"encoding/binary"

	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
)

// Encode sequences up to this length using simple encoding.
//
// The choice of this constant is tightly coupled with the encoding type; we used
// the least significant bits of the encoding byte type to signal simple encoding +
// sequence size.
//
// In this case, we use the range [0b10000000, 0b10001111] to say the sequence must contain
// (N & 0b00001111) + 1 elements, i.e., 0 means 1 element, 1 means 2 elements, etc...
const SIMPLE_SEQUENCE_MAX_THRESHOLD = 16

// A SequenceBuilder is used to build serialized number sequences.
//
// It follows the following pattern:
//
// - New builder: NewBuilder()
// - Add offsets: AddOffset()
// - Build: Build()
// - Serialize: AppendBytes()
//
// It contains decision logic to choose the best encoding for the given sequence.
//
// This is the "writer" counterpart of SequenceReader.
type SequenceBuilder struct {
	baseNum  uint64
	ef       *eliasfano32.EliasFano
	optimize bool
}

// Creates a new builder. The builder is not meant to be reused. The construction
// parameters may or may not be used during the build process depending on the
// encoding being used.
//
// The encoding being used depends on the parameters themselves and the characteristics
// of the number sequence.
//
// baseNum: this is used to calculate the deltas on simple encoding and on "rebased elias fano"
// count: this is the number of elements in the sequence, used in case of elias fano
// max: this is maximum value in the sequence, used in case of elias fano
// optimize: if false, the builder will always output plain elias fano; it is "legacy mode",
// and is used to be backwards compatible with E3 default format. If true, it will output
// forward compatible, optimized multiencoding sequences.
func NewBuilder(baseNum, count, max uint64, optimize bool) *SequenceBuilder {
	return &SequenceBuilder{
		baseNum:  baseNum,
		ef:       eliasfano32.NewEliasFano(count, max),
		optimize: optimize,
	}
}

func (b *SequenceBuilder) AddOffset(offset uint64) {
	b.ef.AddOffset(offset)
}

func (b *SequenceBuilder) Build() {
	b.ef.Build()
}

func (b *SequenceBuilder) AppendBytes(buf []byte) []byte {
	if !b.optimize {
		return b.ef.AppendBytes(buf)
	}

	if b.ef.Count() <= SIMPLE_SEQUENCE_MAX_THRESHOLD {
		return b.simpleEncoding(buf)
	}

	return b.rebasedEliasFano(buf)
}

func (b *SequenceBuilder) simpleEncoding(buf []byte) []byte {
	// Simple encoding type + size: [0x80, 0x8F]
	count := b.ef.Count()
	enc := byte(count-1) & byte(0b00001111)
	enc |= byte(SimpleEncoding)
	buf = append(buf, enc)

	// Encode elems
	var bn [4]byte
	for it := b.ef.Iterator(); it.HasNext(); {
		n, err := it.Next()
		if err != nil {
			// TODO: err
			panic(err)
		}
		n -= b.baseNum

		binary.BigEndian.PutUint32(bn[:], uint32(n))
		buf = append(buf, bn[:]...)
	}

	return buf
}

func (b *SequenceBuilder) rebasedEliasFano(buf []byte) []byte {
	// Reserved encoding type 0x90 == rebased elias fano
	buf = append(buf, byte(RebasedEliasFano))

	// Rebased ef
	rbef := eliasfano32.NewEliasFano(b.ef.Count(), b.ef.Max()-b.baseNum)
	for it := b.ef.Iterator(); it.HasNext(); {
		n, err := it.Next()
		if err != nil {
			panic(err)
		}

		rbef.AddOffset(n - b.baseNum)
	}
	rbef.Build()
	return rbef.AppendBytes(buf)
}
