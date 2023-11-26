package base_encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test64(t *testing.T) {
	number := uint64(9992)

	out := Encode64ToBytes4(number)
	require.Equal(t, Decode64FromBytes4(out), number)

	out = EncodeCompactUint64(number)
	require.Equal(t, DecodeCompactUint64(out), number)
}

func TestDiff64(t *testing.T) {
	old := make([]byte, 800000)
	new := make([]byte, 800008)
	inc := 1
	for i := 0; i < 80; i++ {
		if i%9 == 0 {
			inc++
		}
		old[i] = byte(i)
		new[i] = byte(i + inc)
	}

	var b bytes.Buffer

	err := ComputeCompressedSerializedUint64ListDiff(&b, old, new)
	require.NoError(t, err)

	out := b.Bytes()
	new2, err := ApplyCompressedSerializedUint64ListDiff(old, nil, out)
	require.NoError(t, err)

	require.Equal(t, new, new2)
}

func TestDiff64Effective(t *testing.T) {
	sizeOld := 800
	sizeNew := 816
	old := make([]byte, sizeOld*121)
	new := make([]byte, sizeNew*121)
	previous := make([]byte, sizeOld*8)
	expected := make([]byte, sizeNew*8)
	for i := 0; i < sizeNew; i++ {
		validatorOffset := i * 121
		newNum := i + 32
		oldNum := i + 12
		binary.BigEndian.PutUint64(expected[i*8:], uint64(newNum))
		binary.BigEndian.PutUint64(new[validatorOffset+80:], uint64(newNum))
		if i < len(old)/121 {
			binary.BigEndian.PutUint64(previous[i*8:], uint64(oldNum))
			binary.BigEndian.PutUint64(old[validatorOffset+80:], uint64(oldNum))
		}
	}

	var b bytes.Buffer

	err := ComputeCompressedSerializedEffectiveBalancesDiff(&b, old, new)
	require.NoError(t, err)

	out := b.Bytes()
	new2, err := ApplyCompressedSerializedUint64ListDiff(previous, nil, out)
	require.NoError(t, err)
	fmt.Println(previous)

	require.Equal(t, new2, expected)
}

func TestDiffBytes(t *testing.T) {
	// Setup two lists of bytes 2000 bytes long
	old := make([]byte, 2000)
	new := make([]byte, 2000)
	for i := 0; i < 2000; i++ {
		old[i] = byte(i)
		new[i] = byte(i + 1)
	}
	new = append(new, 2)

	var b bytes.Buffer

	err := ComputeCompressedSerializedByteListDiff(&b, old, new)
	out := b.Bytes()

	new2, err := ApplyCompressedSerializedByteListDiff(old, nil, out)
	require.NoError(t, err)
	require.Equal(t, new, new2)
}

func BenchmarkComputeCompressedSerializedByteListDiff(b *testing.B) {
	old := make([]byte, 8000000)
	new := make([]byte, 8000000)
	for i := 0; i < 800000; i++ {
		old[i] = byte(i)
		new[i] = byte(i + 1)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ComputeCompressedSerializedByteListDiff(ioutil.Discard, old, new)
	}
}
