package multiencseq

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/recsplit/simpleseq"
)

func TestMultiEncSeq(t *testing.T) {

	t.Run("plain elias fano", func(t *testing.T) {
		b := make([]byte, 0)

		// append serialized elias fano
		ef := eliasfano32.NewEliasFano(3, 1027)
		ef.AddOffset(1000)
		ef.AddOffset(1015)
		ef.AddOffset(1027)
		ef.Build()
		b = ef.AppendBytes(b)

		// check deserialization
		s := ReadMultiEncSeq(1000, b)
		require.Equal(t, PlainEliasFano, s.EncodingType())
		requireSequenceChecks(t, s)
		requireRawDataChecks(t, b)
	})

	t.Run("simple encoding", func(t *testing.T) {
		b := make([]byte, 0)

		// type: simple encoding, count: 3
		b = append(b, 0b10000010)

		// append serialized simple sequence
		seq := simpleseq.NewSimpleSequence(1000, 3)
		seq.AddOffset(1000)
		seq.AddOffset(1015)
		seq.AddOffset(1027)
		b = seq.AppendBytes(b)

		// check deserialization
		s := ReadMultiEncSeq(1000, b)
		require.Equal(t, SimpleEncoding, s.EncodingType())
		requireSequenceChecks(t, s)
		requireRawDataChecks(t, b)
	})

	t.Run("rebased elias fano", func(t *testing.T) {
		b := make([]byte, 0)

		// type: rebased elias fano
		b = append(b, 0b10010000)

		// append serialized elias fano (rebased -1000)
		ef := eliasfano32.NewEliasFano(3, 27)
		ef.AddOffset(0)
		ef.AddOffset(15)
		ef.AddOffset(27)
		ef.Build()
		b = ef.AppendBytes(b)

		// check deserialization
		s := ReadMultiEncSeq(1000, b)
		require.Equal(t, RebasedEliasFano, s.EncodingType())
		requireSequenceChecks(t, s)
		requireRawDataChecks(t, b)
	})

	t.Run("reset", func(t *testing.T) {
		b := make([]byte, 0)

		// type: simple encoding, count: 3
		b = append(b, 0b10000010)

		// append serialized simple sequence
		seq := simpleseq.NewSimpleSequence(1000, 3)
		seq.AddOffset(1000)
		seq.AddOffset(1015)
		seq.AddOffset(1027)
		b = seq.AppendBytes(b)

		// check deserialization through reset
		var s SequenceReader
		s.Reset(1000, b)
		require.Equal(t, SimpleEncoding, s.EncodingType())
		requireSequenceChecks(t, &s)
		requireRawDataChecks(t, b)

		// RESET
		b = make([]byte, 0)

		// type: rebased elias fano
		b = append(b, 0b10010000)

		// append serialized elias fano (rebased -1000)
		ef := eliasfano32.NewEliasFano(3, 27)
		ef.AddOffset(0)
		ef.AddOffset(15)
		ef.AddOffset(27)
		ef.Build()
		b = ef.AppendBytes(b)

		// check deserialization
		s.Reset(1000, b)
		require.Equal(t, RebasedEliasFano, s.EncodingType())
		requireSequenceChecks(t, &s)
		requireRawDataChecks(t, b)
	})
}

func requireSequenceChecks(t *testing.T, s *SequenceReader) {
	t.Helper()

	require.Equal(t, uint64(1000), s.Min())
	require.Equal(t, uint64(1027), s.Max())
	require.Equal(t, uint64(3), s.Count())

	require.Equal(t, uint64(1000), s.Get(0))
	require.Equal(t, uint64(1015), s.Get(1))
	require.Equal(t, uint64(1027), s.Get(2))

	// check iterator
	it := s.Iterator(0)
	require.True(t, it.HasNext())
	n, err := it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1000), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1015), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1027), n)

	require.False(t, it.HasNext())

	// check iterator + seek
	it = s.Iterator(1014)
	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1015), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1027), n)

	require.False(t, it.HasNext())

	// check iterator + seek before base num
	it = s.Iterator(999)
	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1000), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1015), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1027), n)

	require.False(t, it.HasNext())

	// check reverse iterator
	it = s.ReverseIterator(2000)
	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1027), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1015), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1000), n)

	require.False(t, it.HasNext())

	// check reverse iterator + seek
	it = s.ReverseIterator(1016)
	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1015), n)

	require.True(t, it.HasNext())
	n, err = it.Next()
	require.NoError(t, err)
	require.Equal(t, uint64(1000), n)

	require.False(t, it.HasNext())

	// check reverse iterator + seek before base num
	it = s.ReverseIterator(999)
	require.False(t, it.HasNext())
}

func requireRawDataChecks(t *testing.T, b []byte) {
	t.Helper()

	// check fast count
	require.Equal(t, uint64(3), Count(1000, b))

	// check search
	n, found := Seek(1000, b, 1014)
	require.True(t, found)
	require.Equal(t, uint64(1015), n)

	n, found = Seek(1000, b, 1015)
	require.True(t, found)
	require.Equal(t, uint64(1015), n)

	_, found = Seek(1000, b, 1028)
	require.False(t, found)

	// check search before base num
	n, found = Seek(1000, b, 999)
	require.True(t, found)
	require.Equal(t, uint64(1000), n)
}
