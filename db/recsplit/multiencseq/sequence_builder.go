package multiencseq

import (
	"encoding/binary"
	"fmt"

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
	useEf      bool                   // true when current sequence uses EF encoding (count > 16)
	rebasedEf  *eliasfano32.EliasFano // kept alive across resets to amortize allocations
	it1        SequenceIterator
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
			useEf:     true,
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
		b.useEf = true
		if b.rebasedEf != nil {
			b.rebasedEf.ResetForWrite(count, maxOffset-baseNum)
		} else {
			b.rebasedEf = eliasfano32.NewEliasFano(count, maxOffset-baseNum)
		}
	} else {
		b.useEf = false
		// Keep rebasedEf alive so its backing array can be reused on the next large sequence.
	}
}

func (b *SequenceBuilder) AddOffset(offset uint64) {
	if b.useEf {
		b.rebasedEf.AddOffset(offset - b.baseNum)
		return
	}
	b.smallBuf[b.smallCount] = uint32(offset - b.baseNum)
	b.smallCount++
}

func (b *SequenceBuilder) Build() {
	if b.useEf {
		b.rebasedEf.Build()
	}
}

func (b *SequenceBuilder) AppendBytes(buf []byte) []byte {
	if b.useEf {
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

// MergeSorted merges N sorted, non-overlapping sequences into b in a single pass.
//
// seqs[i] is the raw encoded bytes and baseNums[i] is the base number for the i-th sequence.
// Sequences must be in ascending order: seqs[i].Max() <= seqs[i+1].Min().
// seqReader is caller-supplied for reuse (avoids heap allocation).
// Panics if the ordering invariant is violated. Calls b.Build() automatically.
func (b *SequenceBuilder) MergeSorted(seqReader *SequenceReader, outBaseNum uint64, baseNums []uint64, seqs [][]byte) error {
	// First pass: compute total count and max offset to size the builder correctly.
	var totalCount, maxOff uint64
	for i, data := range seqs {
		seqReader.Reset(baseNums[i], data)
		totalCount += seqReader.Count()
		if seqReader.Max() > maxOff {
			maxOff = seqReader.Max()
		}
	}
	b.Reset(outBaseNum, totalCount, maxOff)

	// Second pass: add values, asserting each sequence starts after the previous ends.
	var prevMax uint64
	for i, data := range seqs {
		seqReader.Reset(baseNums[i], data)
		if i > 0 && prevMax > seqReader.Min() {
			panic(fmt.Sprintf("MergeSorted: sequences out of order: prevMax=%d > currentMin=%d", prevMax, seqReader.Min()))
		}
		prevMax = seqReader.Max()
		b.it1.Reset(seqReader, 0)
		for b.it1.HasNext() {
			v, err := b.it1.Next()
			if err != nil {
				return err
			}
			b.AddOffset(v)
		}
	}
	b.Build()
	return nil
}

// Merge merges s1 and s2 into this builder, resetting it first.
// s1 and s2 must be pre-sorted with s1.Max() <= s2.Min().
// Call AppendBytes on the builder to serialize.
func (b *SequenceBuilder) Merge(s1, s2 *SequenceReader, outBaseNum uint64) error {
	s1.assertSorted()
	s2.assertSorted()
	maxOffset := max(s1.Max(), s2.Max())
	if s1.Max() > s2.Min() {
		panic(fmt.Sprintf("Merge precondition violated: s1.Max()=%d > s2.Min()=%d", s1.Max(), s2.Min()))
	}
	b.Reset(outBaseNum, s1.Count()+s2.Count(), maxOffset)
	b.it1.Reset(s1, 0)
	for b.it1.HasNext() {
		v, err := b.it1.Next()
		if err != nil {
			return err
		}
		b.AddOffset(v)
	}
	b.it1.Reset(s2, 0)
	for b.it1.HasNext() {
		v, err := b.it1.Next()
		if err != nil {
			return err
		}
		b.AddOffset(v)
	}
	b.Build()
	return nil
}
