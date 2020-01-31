package state

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/trie"
)

func putIntermediateCache(db ethdb.Putter, prefix []byte, subtrieHash []byte) error {
	v := make([]byte, len(subtrieHash))
	copy(v, subtrieHash)

	buf := pool.GetBuffer(64)
	buf.Reset()
	defer pool.PutBuffer(buf)

	if err := trie.CompressNibbles(prefix, &buf.B); err != nil {
		return err
	}

	k := make([]byte, buf.Len())
	copy(k, buf.Bytes())

	if err := db.Put(dbutils.IntermediateTrieCacheBucket, k, v); err != nil {
		return fmt.Errorf("could not put IntermediateTrieCacheBucket, %w", err)
	}
	return nil
}

func delIntermediateCache(db ethdb.Deleter, prefix []byte) error {
	buf := pool.GetBuffer(64)
	buf.Reset()
	defer pool.PutBuffer(buf)

	if err := trie.CompressNibbles(prefix, &buf.B); err != nil {
		return err
	}

	if err := db.Delete(dbutils.IntermediateTrieCacheBucket, buf.Bytes()); err != nil {
		return fmt.Errorf("could not put IntermediateTrieCacheBucket, %w", err)
	}

	return nil
}
