package state

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/trie"
)

func putIntermediateCache(db ethdb.Putter, prefix []byte, subtrieHash []byte) error {
	if len(prefix) == 0 {
		return nil
	}
	v := make([]byte, len(subtrieHash))
	copy(v, subtrieHash)

	k := &bytes.Buffer{}

	if err := trie.CompressNibbles(prefix, k); err != nil {
		return err
	}

	if err := db.Put(dbutils.IntermediateTrieHashesBucket, k.Bytes(), v); err != nil {
		return fmt.Errorf("could not put IntermediateTrieHashesBucket, %w", err)
	}
	return nil
}

func delIntermediateCache(db ethdb.Deleter, prefix []byte) error {
	if len(prefix) == 0 {
		return nil
	}

	k := &bytes.Buffer{}
	if err := trie.CompressNibbles(prefix, k); err != nil {
		return err
	}

	if err := db.Delete(dbutils.IntermediateTrieHashesBucket, k.Bytes()); err != nil {
		return fmt.Errorf("could not put IntermediateTrieHashesBucket, %w", err)
	}

	return nil
}
