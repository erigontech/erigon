package trie

import (
	"bytes"
	"fmt"
	"io"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func putIntermediateCache(db ethdb.Putter, prefix []byte, subtrieHash []byte) error {
	if len(prefix) == 0 {
		return nil
	}
	v := make([]byte, len(subtrieHash))
	copy(v, subtrieHash)

	k := &bytes.Buffer{}
	if err := CompressNibbles(prefix, k); err != nil {
		return err
	}

	if err := db.Put(dbutils.IntermediateTrieHashesBucket, k.Bytes(), v); err != nil {
		return fmt.Errorf("could not put IntermediateTrieHashesBucket, %w", err)
	}
	return nil
}

// CompressNibbles - supports only even number of nibbles
// HI_NIBBLE(b) = (b >> 4) & 0x0F
// LO_NIBBLE(b) = b & 0x0F
func CompressNibbles(nibbles []byte, out io.ByteWriter) error {
	if len(nibbles) < 1 {
		return nil
	}

	targetLen := len(nibbles)/2 + len(nibbles)%2 + 1

	// store amount_of_nibbles-1 in low_nibble of first byte
	if err := out.WriteByte(byte(len(nibbles) - 1)); err != nil {
		return err
	}

	var b byte

	nibbleIndex := 0
	for i := 1; i < targetLen; i++ {
		b = (nibbles[nibbleIndex] << 4) & 0xF0
		nibbleIndex++
		if nibbleIndex < len(nibbles) {
			b |= nibbles[nibbleIndex] & 0x0F
			nibbleIndex++
		}

		if err := out.WriteByte(b); err != nil {
			return err
		}

	}

	return nil
}

// DecompressNibbles - supports only even number of nibbles
//
// HI_NIBBLE(b) = (b >> 4) & 0x0F
// LO_NIBBLE(b) = b & 0x0F
func DecompressNibbles(in []byte, out io.ByteWriter) error {
	if len(in) < 1 {
		return nil
	}

	var nibblesAmount int = (len(in) - 1) * 2

	nibbleIndex := 0
	for i := 1; i < len(in); i++ {
		if err := out.WriteByte((in[i] >> 4) & 0x0F); err != nil {
			return err
		}

		nibbleIndex++
		if nibbleIndex < nibblesAmount {
			if err := out.WriteByte(in[i] & 0x0F); err != nil {
				return err
			}

			nibbleIndex++
		}
	}

	return nil
}
