package common

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/erigontech/erigon/common/hexutil"
)

type textAppendMarshaler interface {
	MarshalText() ([]byte, error)
	AppendText(dst []byte) ([]byte, error)
}

// AppendText must be byte-identical to MarshalText (only the destination differs).
func checkByteIdentical(t *testing.T, name string, v textAppendMarshaler) {
	t.Helper()
	mt, err := v.MarshalText()
	if err != nil {
		t.Fatalf("%s MarshalText: %v", name, err)
	}
	const pfx = "PFX"
	at, err := v.AppendText([]byte(pfx))
	if err != nil {
		t.Fatalf("%s AppendText: %v", name, err)
	}
	if want := append([]byte(pfx), mt...); !bytes.Equal(at, want) {
		t.Fatalf("%s: AppendText=%q, want %q", name, at, want)
	}
}

func TestAppendTextByteIdentical(t *testing.T) {
	checkByteIdentical(t, "hexutil.Bytes/empty", hexutil.Bytes{})
	checkByteIdentical(t, "hexutil.Bytes", hexutil.Bytes{0x00, 0x1a, 0xff, 0xcd})
	checkByteIdentical(t, "hexutil.Uint64/0", hexutil.Uint64(0))
	checkByteIdentical(t, "hexutil.Uint64", hexutil.Uint64(0x1a2b3c4d5e))
	checkByteIdentical(t, "hexutil.Uint", hexutil.Uint(0xabcd))
	checkByteIdentical(t, "hexutil.Big/0", hexutil.Big(*big.NewInt(0)))
	checkByteIdentical(t, "hexutil.Big", hexutil.Big(*new(big.Int).SetBytes([]byte{0x12, 0x34, 0x56, 0x78, 0x9a})))
	checkByteIdentical(t, "hexutil.Big/neg", hexutil.Big(*big.NewInt(-0x123456789)))
	checkByteIdentical(t, "Hash", Hash{0x00, 0x11, 0xaa, 0xff})
	checkByteIdentical(t, "Address", Address{0x00, 0x1a, 0xff})
	checkByteIdentical(t, "Bytes4", Bytes4{0x00, 0xab, 0xcd, 0xef})
	checkByteIdentical(t, "Bytes48", Bytes48{})
	checkByteIdentical(t, "Bytes64", Bytes64{})
	checkByteIdentical(t, "Bytes96", Bytes96{})
	checkByteIdentical(t, "UnprefixedHash", UnprefixedHash{0x00, 0x1a, 0xff})
	checkByteIdentical(t, "UnprefixedAddress", UnprefixedAddress{0x00, 0x1a, 0xff})
}
