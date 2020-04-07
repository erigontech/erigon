package state

import (
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/trie"
)

const keyBufferSize = 64

type IntermediateHashes struct {
	trie.NoopObserver // make sure that we don't need to subscribe to unnecessary methods
	putter            ethdb.Putter
	deleter           ethdb.Deleter
}

func NewIntermediateHashes(putter ethdb.Putter, deleter ethdb.Deleter) *IntermediateHashes {
	return &IntermediateHashes{putter: putter, deleter: deleter}
}

func (ih *IntermediateHashes) WillUnloadBranchNode(prefixAsNibbles []byte, nodeHash common.Hash) {
	// only put to bucket prefixes with even number of nibbles
	if len(prefixAsNibbles) == 0 || len(prefixAsNibbles)%2 == 1 {
		return
	}

	key := pool.GetBuffer(keyBufferSize)
	trie.CompressNibbles(prefixAsNibbles, &key.B)

	if err := ih.putter.Put(dbutils.IntermediateTrieHashBucket, common.CopyBytes(key.B), common.CopyBytes(nodeHash[:])); err != nil {
		log.Warn("could not put intermediate trie hash", "err", err)
	}
}

func (ih *IntermediateHashes) BranchNodeLoaded(prefixAsNibbles []byte) {
	// only put to bucket prefixes with even number of nibbles
	if len(prefixAsNibbles) == 0 || len(prefixAsNibbles)%2 == 1 {
		return
	}

	key := pool.GetBuffer(keyBufferSize)
	trie.CompressNibbles(prefixAsNibbles, &key.B)

	if err := ih.deleter.Delete(dbutils.IntermediateTrieHashBucket, key.B); err != nil {
		log.Warn("could not delete intermediate trie hash", "err", err)
		return
	}
}
