package trie

import (
	"bytes"
	"errors"
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
	if len(nibbles)%2 != 0 {
		return errors.New("this method supports only arrays of even nibbles")
	}

	var b byte
	for i := 0; i < len(nibbles); i += 2 {
		b = (nibbles[i] << 4) & 0xF0
		b |= nibbles[i+1] & 0x0F

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
	for i := 0; i < len(in); i++ {
		//fmt.Printf("%x\n", (in[i]>>4)&0x0F)
		if err := out.WriteByte((in[i] >> 4) & 0x0F); err != nil {
			return err
		}
		//fmt.Printf("%x\n", in[i]&0x0F)
		if err := out.WriteByte(in[i] & 0x0F); err != nil {
			return err
		}
	}

	return nil
}
