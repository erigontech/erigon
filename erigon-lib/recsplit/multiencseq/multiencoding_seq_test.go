package multiencseq

import (
	"testing"

	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
	"github.com/erigontech/erigon-lib/recsplit/simpleseq"
	"github.com/stretchr/testify/require"
)

func TestMultiEncSeq(t *testing.T) {

	t.Run("plain elias fano", func(t *testing.T) {
		ef := eliasfano32.NewEliasFano(3, 1027)
		ef.AddOffset(1000)
		ef.AddOffset(1015)
		ef.AddOffset(1027)
		ef.Build()

		b := make([]byte, 0)
		b = ef.AppendBytes(b)

		s := ReadMultiEncSeq(1000, b)
		require.Equal(t, PlainEliasFano, s.currentEnc)
		require.Equal(t, uint64(1000), s.Min())
		require.Equal(t, uint64(1027), s.Max())
		require.Equal(t, uint64(3), s.Count())

		require.Equal(t, uint64(1000), s.Get(0))
		require.Equal(t, uint64(1015), s.Get(1))
		require.Equal(t, uint64(1027), s.Get(2))
	})

	t.Run("delta encoding", func(t *testing.T) {
		seq := simpleseq.NewSimpleSequence(1000, 3)
		seq.AddOffset(1000)
		seq.AddOffset(1015)
		seq.AddOffset(1027)
		seq.Build()

		b := make([]byte, 0)
		b = append(b, 0b10000000)
		b = seq.AppendBytes(b)

		s := ReadMultiEncSeq(1000, b)
		require.Equal(t, SimpleEncoding, s.currentEnc)
		require.Equal(t, uint64(1000), s.Min())
		require.Equal(t, uint64(1027), s.Max())
		require.Equal(t, uint64(3), s.Count())

		require.Equal(t, uint64(1000), s.Get(0))
		require.Equal(t, uint64(1015), s.Get(1))
		require.Equal(t, uint64(1027), s.Get(2))
	})
}
