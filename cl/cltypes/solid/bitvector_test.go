package solid

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBitvectorSetGetBits(t *testing.T) {
	require := require.New(t)

	v := NewBitVector(10)

	v.SetBitAt(2, true)
	v.SetBitAt(4, true)
	v.SetBitAt(9, true)
	v.SetBitAt(10, false)

	require.True(v.GetBitAt(2), "BitVector SetBitAt did not set the bit correctly")
	require.True(v.GetBitAt(4), "BitVector SetBitAt did not set the bit correctly")
	require.True(v.GetBitAt(9), "BitVector SetBitAt did not set the bit correctly")

	require.False(v.GetBitAt(3), "BitVector SetBitAt did not set the bit correctly")
	require.False(v.GetBitAt(10), "BitVector SetBitAt did not set the bit correctly")
}

func TestBitvectorGetOnIndices(t *testing.T) {
	require := require.New(t)

	v := NewBitVector(10)

	v.SetBitAt(2, true)
	v.SetBitAt(4, true)
	v.SetBitAt(9, true)

	indices := v.GetOnIndices()

	require.Equal([]int{2, 4, 9}, indices, "BitVector GetOnIndices did not return the expected indices")
}

func TestBitvectorCopy(t *testing.T) {
	require := require.New(t)

	v := NewBitVector(10)

	v.SetBitAt(2, true)
	v.SetBitAt(4, true)
	v.SetBitAt(9, true)

	c := v.Copy()

	require.Equal(v.BitLen(), c.BitLen(), "BitVector Copy did not copy the bit length correctly")
	require.Equal(v.BitCap(), c.BitCap(), "BitVector Copy did not copy the bit cap correctly")

	for i := 0; i < v.BitLen(); i++ {
		require.Equal(v.GetBitAt(i), c.GetBitAt(i), "BitVector Copy did not copy the bits correctly")
	}
}

func TestBitvectorEncodingSizeSSZ(t *testing.T) {
	require := require.New(t)

	v := NewBitVector(10)

	v.SetBitAt(2, true)
	v.SetBitAt(4, true)
	v.SetBitAt(9, true)

	size := v.EncodingSizeSSZ()

	require.Equal(2, size, "BitVector EncodingSizeSSZ did not return the expected size")
}

func TestBitvectorEncodeSSZ(t *testing.T) {
	require := require.New(t)

	v := NewBitVector(24)

	v.SetBitAt(2, true)
	v.SetBitAt(4, true)
	v.SetBitAt(9, true)

	var (
		buf []byte = make([]byte, 0, 1)
		err error
	)
	buf, err = v.EncodeSSZ(buf)
	require.NoError(err, "BitVector EncodeSSZ failed")
	require.Equal([]byte{byte(0b00010100), byte(0b00000010), 0}, buf, "BitVector EncodeSSZ did not encode the bits correctly")

	// try more zero padding
	v = NewBitVector(33)
	v.SetBitAt(9, true)
	v.SetBitAt(2, true)
	v.SetBitAt(4, true)
	buf, err = v.EncodeSSZ(buf) // intentionally reusing the buffer
	require.NoError(err, "BitVector EncodeSSZ failed")
	require.Equal([]byte{byte(0b00010100), byte(0b00000010), 0, 0, 0}, buf, "BitVector EncodeSSZ did not encode the bits correctly")
}

func TestBitvectorDecodeSSZ(t *testing.T) {
	require := require.New(t)

	v := NewBitVector(24)
	buf := []byte{byte(0b00010100), byte(0b00000010), 0}
	err := v.DecodeSSZ(buf, 0)
	require.NoError(err, "BitVector DecodeSSZ failed")
	indices := v.GetOnIndices()
	require.Equal([]int{2, 4, 9}, indices, "BitVector DecodeSSZ did not decode the bits correctly")
}

func TestBitvectorJson(t *testing.T) {
	require := require.New(t)

	// marshal
	v := NewBitVector(20)
	v.SetBitAt(2, true)
	v.SetBitAt(4, true)
	v.SetBitAt(9, true)

	j, err := v.MarshalJSON()
	require.NoError(err, "BitVector MarshalJSON failed")
	require.Equal(`"0x140200"`, string(j), "BitVector MarshalJSON did not return the expected JSON")

	// unmarshal
	text := []byte(`"0x140200"`)
	v = NewBitVector(20) // Use same size as original vector
	err = v.UnmarshalJSON(text)
	require.NoError(err, "BitVector UnmarshalJSON failed")
	indices := v.GetOnIndices()
	require.Equal([]int{2, 4, 9}, indices, "BitVector UnmarshalJSON did not return the expected indices")
}

func TestBitvectorUnion(t *testing.T) {
	require := require.New(t)

	v1 := NewBitVector(10)
	v2 := NewBitVector(10)

	v1.SetBitAt(2, true)
	v1.SetBitAt(4, true)
	v1.SetBitAt(9, true)

	v2.SetBitAt(1, true)
	v2.SetBitAt(4, true)
	v2.SetBitAt(9, true)
	v2.SetBitAt(3, true)

	u, err := v1.Union(v2)
	require.NoError(err, "BitVector Union failed")
	indices := u.GetOnIndices()
	require.Equal([]int{1, 2, 3, 4, 9}, indices, "BitVector Union did not return the expected indices")
}
