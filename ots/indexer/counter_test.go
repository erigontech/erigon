package indexer

import (
	"bytes"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
)

func checkCounter(t *testing.T, result []byte, expected string) {
	if !bytes.Equal(result, hexutility.MustDecodeHex(expected)) {
		t.Errorf("got %s expected %s", hexutility.Encode(result), expected)
	}
}

func TestOptimizedCounterSerializerMin(t *testing.T) {
	r := OptimizedCounterSerializer(1)
	expected := "0x00"
	checkCounter(t, r, expected)
}

func TestOptimizedCounterSerializerMax(t *testing.T) {
	r := OptimizedCounterSerializer(256)
	expected := "0xff"
	checkCounter(t, r, expected)
}

func TestRegularCounterSerializer(t *testing.T) {
	r := RegularCounterSerializer(257, hexutility.MustDecodeHex("0x1234567812345678"))
	expected := "0x00000000000001011234567812345678"
	checkCounter(t, r, expected)
}

func TestLastCounterSerializerMin(t *testing.T) {
	r := LastCounterSerializer(0)
	expected := "0x0000000000000000ffffffffffffffff"
	checkCounter(t, r, expected)
}

func TestLastCounterSerializerMax(t *testing.T) {
	r := LastCounterSerializer(^uint64(0))
	expected := "0xffffffffffffffffffffffffffffffff"
	checkCounter(t, r, expected)
}
