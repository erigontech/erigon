package state

import (
	"encoding/binary"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"github.com/ledgerwatch/turbo-geth/trie"
)

var (
	InsertCounter = metrics.NewRegisteredCounter("db/ih/insert", nil)
	DeleteCounter = metrics.NewRegisteredCounter("db/ih/delete", nil)
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

func (ih *IntermediateHashes) WillUnloadBranchNode(prefixAsNibbles []byte, nodeHash common.Hash, incarnation uint64, witnessLen uint64) {
	// only put to bucket prefixes with even number of nibbles
	if len(prefixAsNibbles) == 0 || len(prefixAsNibbles)%2 == 1 {
		return
	}

	InsertCounter.Inc(1)

	buf := pool.GetBuffer(keyBufferSize)
	defer pool.PutBuffer(buf)
	trie.CompressNibbles(prefixAsNibbles, &buf.B)

	var key []byte
	if len(buf.B) >= common.HashLength {
		key = dbutils.GenerateCompositeStoragePrefix(buf.B[:common.HashLength], incarnation, buf.B[common.HashLength:])
	} else {
		key = common.CopyBytes(buf.B)
	}

	if !debug.IsTrackWitnessSizeEnabled() {
		if err := ih.putter.Put(dbutils.IntermediateTrieHashBucket, key, common.CopyBytes(nodeHash[:])); err != nil {
			log.Warn("could not put intermediate trie hash", "err", err)
		}
	} else {
		lenBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(lenBytes, witnessLen)

		if err := ih.putter.Put(dbutils.IntermediateTrieHashBucket, key, common.CopyBytes(nodeHash[:])); err != nil {
			log.Warn("could not put intermediate trie hash", "err", err)
		}
		//fmt.Printf("Put:%x\n", key)
		if err := ih.putter.Put(dbutils.IntermediateWitnessLenBucket, common.CopyBytes(key), lenBytes); err != nil {
			log.Warn("could not put intermediate trie data len", "err", err)
		}
	}
}

func (ih *IntermediateHashes) BranchNodeLoaded(prefixAsNibbles []byte, incarnation uint64) {
	// only put to bucket prefixes with even number of nibbles
	if len(prefixAsNibbles) == 0 || len(prefixAsNibbles)%2 == 1 {
		return
	}
	DeleteCounter.Inc(1)

	buf := pool.GetBuffer(keyBufferSize)
	defer pool.PutBuffer(buf)
	trie.CompressNibbles(prefixAsNibbles, &buf.B)

	var key []byte
	if len(buf.B) >= common.HashLength {
		key = dbutils.GenerateCompositeStoragePrefix(buf.B[:common.HashLength], incarnation, buf.B[common.HashLength:])
	} else {
		key = common.CopyBytes(buf.B)
	}

	if err := ih.deleter.Delete(dbutils.IntermediateTrieHashBucket, key); err != nil {
		log.Warn("could not delete intermediate trie hash", "err", err)
	}
	if debug.IsTrackWitnessSizeEnabled() {
		//fmt.Printf("Del:%x\n", key)
		if err := ih.deleter.Delete(dbutils.IntermediateWitnessLenBucket, common.CopyBytes(key)); err != nil {
			log.Warn("could not delete intermediate trie hash", "err", err)
		}
	}
}
