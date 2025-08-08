package multiencseq

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
)

func TestMultiEncodingSeqBuilder(t *testing.T) {

	t.Run("singleton sequence", func(t *testing.T) {
		builder := NewBuilder(1000, 1, 1005)
		builder.AddOffset(1005)
		builder.Build()

		b := make([]byte, 0)
		b = builder.AppendBytes(b)
		require.Equal(t, hexutil.MustDecodeHex("0x8000000005"), b)
	})

	t.Run("short sequences must use simple encoding", func(t *testing.T) {
		builder := NewBuilder(1000, 16, 1035)
		builder.AddOffset(1005)
		builder.AddOffset(1007)
		builder.AddOffset(1009)
		builder.AddOffset(1011)
		builder.AddOffset(1013)
		builder.AddOffset(1015)
		builder.AddOffset(1017)
		builder.AddOffset(1019)
		builder.AddOffset(1021)
		builder.AddOffset(1023)
		builder.AddOffset(1025)
		builder.AddOffset(1027)
		builder.AddOffset(1029)
		builder.AddOffset(1031)
		builder.AddOffset(1033)
		builder.AddOffset(1035)
		builder.Build()

		b := make([]byte, 0)
		b = builder.AppendBytes(b)
		require.Equal(t, hexutil.MustDecodeHex(
			"0x8F"+
				"00000005"+
				"00000007"+
				"00000009"+
				"0000000B"+
				"0000000D"+
				"0000000F"+
				"00000011"+
				"00000013"+
				"00000015"+
				"00000017"+
				"00000019"+
				"0000001B"+
				"0000001D"+
				"0000001F"+
				"00000021"+
				"00000023"), b)
	})

	t.Run("large sequences must use rebased elias fano", func(t *testing.T) {
		builder := NewBuilder(1000, 17, 1035)
		builder.AddOffset(1005)
		builder.AddOffset(1007)
		builder.AddOffset(1009)
		builder.AddOffset(1011)
		builder.AddOffset(1013)
		builder.AddOffset(1015)
		builder.AddOffset(1017)
		builder.AddOffset(1019)
		builder.AddOffset(1021)
		builder.AddOffset(1023)
		builder.AddOffset(1025)
		builder.AddOffset(1027)
		builder.AddOffset(1029)
		builder.AddOffset(1031)
		builder.AddOffset(1033)
		builder.AddOffset(1035)
		builder.AddOffset(1037)
		builder.Build()

		b := make([]byte, 0)
		b = builder.AppendBytes(b)
		require.Equal(t, b[0], byte(0x90), "encoding type is not 0x90")

		ef, _ := eliasfano32.ReadEliasFano(b[1:])
		require.Equal(t, uint64(17), ef.Count())
		curr := uint64(5)
		for it := ef.Iterator(); it.HasNext(); {
			n, err := it.Next()

			require.NoError(t, err)
			require.Equal(t, curr, n)

			curr += 2
		}
	})
}
