package rlphacks

import (
	"io"

	"github.com/ledgerwatch/turbo-geth/rlp"
)

type RlpSerializableBytes []byte

func (b RlpSerializableBytes) ToDoubleRLP(w io.Writer) error {
	return encodeBytesAsRlpToWriter(b, w, generateByteArrayLenDouble, 8)
}

func (b RlpSerializableBytes) RawBytes() []byte {
	return b
}

func (b RlpSerializableBytes) DoubleRLPLen() int {
	if len(b) < 1 {
		return 0
	}
	return generateRlpPrefixLenDouble(len(b), b[0]) + len(b)
}

type RlpEncodedBytes []byte

func (b RlpEncodedBytes) ToDoubleRLP(w io.Writer) error {
	return encodeBytesAsRlpToWriter(b, w, generateByteArrayLen, 4)
}

func (b RlpEncodedBytes) RawBytes() []byte {
	return b
}

func (b RlpEncodedBytes) DoubleRLPLen() int {
	return generateRlpPrefixLen(len(b)) + len(b)
}

func encodeBytesAsRlpToWriter(source []byte, w io.Writer, prefixGenFunc func([]byte, int, int) int, prefixBufferSize uint) error {
	// > 1 byte, write a prefix or prefixes first
	if len(source) > 1 || (len(source) == 1 && source[0] >= rlp.EmptyStringCode) {
		prefix := make([]byte, prefixBufferSize)
		prefixLen := prefixGenFunc(prefix, 0, len(source))

		if _, err := w.Write(prefix[:prefixLen]); err != nil {
			return err
		}
	}

	_, err := w.Write(source)
	return err
}

func EncodeByteArrayAsRlp(raw []byte, w io.Writer) (int, error) {
	err := encodeBytesAsRlpToWriter(raw, w, generateByteArrayLen, 4)
	if err != nil {
		return 0, err
	}
	return generateRlpPrefixLen(len(raw)) + len(raw), nil
}
