package multiencseq

import (
	"encoding/binary"

	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
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
	baseNum    uint64
	smallBuf   [SIMPLE_SEQUENCE_MAX_THRESHOLD]uint32 // rebased values for simple encoding (count <= 16)
	smallCount uint8
	rebasedEf  *eliasfano32.EliasFano // direct rebased EF for large sequences (count > 16)
	it1        SequenceIterator
	it2        SequenceIterator
}

// Creates a new builder. The builder is not meant to be reused. The construction
// parameters may or may not be used during the build process depending on the
// encoding being used.
//
// The encoding being used depends on the parameters themselves and the characteristics
// of the number sequence.
//
// While non-optimized "legacy mode" is supported (for now) on SequenceReader to be backwards
// compatible with old files, this writer ONLY writes optimized multiencoding sequences.
//
// baseNum: this is used to calculate the deltas on simple encoding and on "rebased elias fano"
// count: this is the number of elements in the sequence, used in case of elias fano
// maxOffset: this is maximum value in the sequence, used in case of elias fano
func NewBuilder(baseNum, count, maxOffset uint64) *SequenceBuilder {
	if count > SIMPLE_SEQUENCE_MAX_THRESHOLD {
		// For large sequences, target rebased EF directly. AddOffset subtracts baseNum
		// on the fly, so AppendBytes can serialize without a second pass.
		return &SequenceBuilder{
			baseNum:   baseNum,
			rebasedEf: eliasfano32.NewEliasFano(count, maxOffset-baseNum),
		}
	}
	return &SequenceBuilder{baseNum: baseNum}
}

// Reset reinitializes the builder for a new sequence, reusing the existing object
// and its internal EliasFano allocation where possible.
// Same parameter semantics as NewBuilder.
func (b *SequenceBuilder) Reset(baseNum, count, maxOffset uint64) {
	b.baseNum = baseNum
	b.smallCount = 0
	if count > SIMPLE_SEQUENCE_MAX_THRESHOLD {
		if b.rebasedEf != nil {
			b.rebasedEf.ResetForWrite(count, maxOffset-baseNum)
		} else {
			b.rebasedEf = eliasfano32.NewEliasFano(count, maxOffset-baseNum)
		}
	} else {
		b.rebasedEf = nil
	}
}

func (b *SequenceBuilder) AddOffset(offset uint64) {
	if b.rebasedEf != nil {
		b.rebasedEf.AddOffset(offset - b.baseNum)
		return
	}
	b.smallBuf[b.smallCount] = uint32(offset - b.baseNum)
	b.smallCount++
}

func (b *SequenceBuilder) Build() {
	if b.rebasedEf != nil {
		b.rebasedEf.Build()
	}
}

func (b *SequenceBuilder) AppendBytes(buf []byte) []byte {
	if b.rebasedEf != nil {
		buf = append(buf, byte(RebasedEliasFano))
		return b.rebasedEf.AppendBytes(buf)
	}
	return b.simpleEncoding(buf)
}

func (b *SequenceBuilder) simpleEncoding(buf []byte) []byte {
	// Simple encoding type + size: [0x80, 0x8F]
	enc := (b.smallCount-1)&0x0F | byte(SimpleEncoding)
	buf = append(buf, enc)

	for _, v := range b.smallBuf[:b.smallCount] {
		buf = binary.BigEndian.AppendUint32(buf, v)
	}

	return buf
}

// Merge merges s1 and s2 into this builder, resetting it first.
// s1 and s2 must be pre-sorted with s1.Max() <= s2.Min().
// Call AppendBytes on the builder to serialize.
func (b *SequenceBuilder) Merge(s1, s2 *SequenceReader, outBaseNum uint64) error {
	b.Reset(outBaseNum, s1.Count()+s2.Count(), s2.Max())
	b.it1.Reset(s1, 0)
	b.it2.Reset(s2, 0)
	for b.it1.HasNext() {
		v, err := b.it1.Next()
		if err != nil {
			return err
		}
		b.AddOffset(v)
	}
	for b.it2.HasNext() {
		v, err := b.it2.Next()
		if err != nil {
			return err
		}
		b.AddOffset(v)
	}
	b.Build()
	return nil
}
