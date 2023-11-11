package verkletrie

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/anacrolix/sync"
	"github.com/gballet/go-verkle"
	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
)

func int256ToVerkleFormat(x *uint256.Int, buffer []byte) {
	bbytes := x.ToBig().Bytes()
	if len(bbytes) > 0 {
		for i, b := range bbytes {
			buffer[len(bbytes)-i-1] = b
		}
	}
}

func flushVerkleNode(db kv.RwTx, node verkle.VerkleNode, logInterval *time.Ticker, key []byte, logger log.Logger) error {
	var err error
	totalInserted := 0
	node.(*verkle.InternalNode).Flush(func(node verkle.VerkleNode) {
		if err != nil {
			return
		}

		err = rawdb.WriteVerkleNode(db, node)
		if err != nil {
			return
		}
		totalInserted++
		select {
		case <-logInterval.C:
			logger.Info("Flushing Verkle nodes", "inserted", totalInserted, "key", common.Bytes2Hex(key))
		default:
		}
	})
	return err
}

func collectVerkleNode(collector *etl.Collector, node verkle.VerkleNode, logInterval *time.Ticker, key []byte, logger log.Logger) error {
	var err error
	totalInserted := 0
	node.(*verkle.InternalNode).Flush(func(node verkle.VerkleNode) {
		if err != nil {
			return
		}
		var encodedNode []byte

		rootHash := node.Commitment().Bytes()
		encodedNode, err = node.Serialize()
		if err != nil {
			return
		}
		err = collector.Collect(rootHash[:], encodedNode)
		totalInserted++
		select {
		case <-logInterval.C:
			logger.Info("Flushing Verkle nodes", "inserted", totalInserted, "key", common.Bytes2Hex(key))
		default:
		}
	})
	return err
}

type VerkleTreeWriter struct {
	db        kv.RwTx
	collector *etl.Collector
	mu        sync.Mutex
	tmpdir    string
	logger    log.Logger
}

func NewVerkleTreeWriter(db kv.RwTx, tmpdir string, logger log.Logger) *VerkleTreeWriter {
	return &VerkleTreeWriter{
		db:        db,
		collector: etl.NewCollector("verkleTreeWriterLogPrefix", tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize*8), logger),
		tmpdir:    tmpdir,
		logger:    logger,
	}
}

func (v *VerkleTreeWriter) UpdateAccount(versionKey []byte, codeSize uint64, isContract bool, acc accounts.Account) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	var codeHashKey, nonceKey, balanceKey, codeSizeKey, nonce, balance, cs [32]byte
	copy(codeHashKey[:], versionKey[:31])
	copy(nonceKey[:], versionKey[:31])
	copy(balanceKey[:], versionKey[:31])
	copy(codeSizeKey[:], versionKey[:31])
	codeHashKey[31] = vtree.CodeKeccakLeafKey
	nonceKey[31] = vtree.NonceLeafKey
	balanceKey[31] = vtree.BalanceLeafKey
	codeSizeKey[31] = vtree.CodeSizeLeafKey
	// Process values
	int256ToVerkleFormat(&acc.Balance, balance[:])
	binary.LittleEndian.PutUint64(nonce[:], acc.Nonce)

	// Insert in the tree
	if err := v.collector.Collect(versionKey, []byte{0}); err != nil {
		return err
	}

	if err := v.collector.Collect(nonceKey[:], nonce[:]); err != nil {
		return err
	}
	if err := v.collector.Collect(balanceKey[:], balance[:]); err != nil {
		return err
	}
	if isContract {
		binary.LittleEndian.PutUint64(cs[:], codeSize)
		if err := v.collector.Collect(codeHashKey[:], acc.CodeHash[:]); err != nil {
			return err
		}
		if err := v.collector.Collect(codeSizeKey[:], cs[:]); err != nil {
			return err
		}
	}
	return nil
}

func (v *VerkleTreeWriter) DeleteAccount(versionKey []byte, isContract bool) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	var codeHashKey, nonceKey, balanceKey, codeSizeKey [32]byte
	copy(codeHashKey[:], versionKey[:31])
	copy(nonceKey[:], versionKey[:31])
	copy(balanceKey[:], versionKey[:31])
	copy(codeSizeKey[:], versionKey[:31])
	codeHashKey[31] = vtree.CodeKeccakLeafKey
	nonceKey[31] = vtree.NonceLeafKey
	balanceKey[31] = vtree.BalanceLeafKey
	codeSizeKey[31] = vtree.CodeSizeLeafKey
	// Insert in the tree
	if err := v.collector.Collect(versionKey, []byte{0}); err != nil {
		return err
	}

	if err := v.collector.Collect(nonceKey[:], []byte{0}); err != nil {
		return err
	}
	if err := v.collector.Collect(balanceKey[:], []byte{0}); err != nil {
		return err
	}
	if isContract {
		if err := v.collector.Collect(codeHashKey[:], []byte{0}); err != nil {
			return err
		}
		if err := v.collector.Collect(codeSizeKey[:], []byte{0}); err != nil {
			return err
		}
	}
	return nil
}

func (v *VerkleTreeWriter) Insert(key, value []byte) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.collector.Collect(key, value)
}

func (v *VerkleTreeWriter) WriteContractCodeChunks(codeKeys [][]byte, chunks [][]byte) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	for i, codeKey := range codeKeys {
		if err := v.collector.Collect(codeKey, chunks[i]); err != nil {
			return err
		}
	}
	return nil
}

func (v *VerkleTreeWriter) CommitVerkleTreeFromScratch() (libcommon.Hash, error) {
	if err := v.db.ClearBucket(kv.VerkleTrie); err != nil {
		return libcommon.Hash{}, err
	}

	verkleCollector := etl.NewCollector(kv.VerkleTrie, v.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), v.logger)
	defer verkleCollector.Close()

	root := verkle.New()

	logInterval := time.NewTicker(30 * time.Second)
	if err := v.collector.Load(v.db, kv.VerkleTrie, func(k []byte, val []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(val) == 0 {
			return next(k, nil, nil)
		}
		if err := root.InsertOrdered(libcommon.CopyBytes(k), libcommon.CopyBytes(val), func(node verkle.VerkleNode) {
			rootHash := node.Commitment().Bytes()
			encodedNode, err := node.Serialize()
			if err != nil {
				panic(err)
			}
			if err := verkleCollector.Collect(rootHash[:], encodedNode); err != nil {
				panic(err)
			}
			select {
			case <-logInterval.C:
				v.logger.Info("[Verkle] Assembling Verkle Tree", "key", common.Bytes2Hex(k))
			default:
			}
		}); err != nil {
			return err
		}
		return next(k, nil, nil)
	}, etl.TransformArgs{Quit: context.Background().Done()}); err != nil {
		return libcommon.Hash{}, err
	}

	// Flush the rest all at once
	if err := collectVerkleNode(v.collector, root, logInterval, nil, v.logger); err != nil {
		return libcommon.Hash{}, err
	}

	v.logger.Info("Started Verkle Tree Flushing")
	return root.Commitment().Bytes(), verkleCollector.Load(v.db, kv.VerkleTrie, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"key", common.Bytes2Hex(k)}
		}})
}

func (v *VerkleTreeWriter) CommitVerkleTree(root libcommon.Hash) (libcommon.Hash, error) {
	resolverFunc := func(root []byte) ([]byte, error) {
		return v.db.GetOne(kv.VerkleTrie, root)
	}

	var rootNode verkle.VerkleNode
	var err error
	if root != (libcommon.Hash{}) {
		rootNode, err = rawdb.ReadVerkleNode(v.db, root)
		if err != nil {
			return libcommon.Hash{}, err
		}
	} else {
		return v.CommitVerkleTreeFromScratch() // TODO(Giulio2002): ETL is buggy, go fix it >:(.
	}

	verkleCollector := etl.NewCollector(kv.VerkleTrie, v.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize), v.logger)
	defer verkleCollector.Close()

	insertionBeforeFlushing := 2_000_000 // 2M node to flush at a time
	insertions := 0
	logInterval := time.NewTicker(30 * time.Second)
	if err := v.collector.Load(v.db, kv.VerkleTrie, func(key []byte, value []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(value) > 0 {
			if err := rootNode.Insert(libcommon.CopyBytes(key), libcommon.CopyBytes(value), resolverFunc); err != nil {
				return err
			}
			insertions++
		}
		if insertions > insertionBeforeFlushing {
			if err := flushVerkleNode(v.db, rootNode, logInterval, key, v.logger); err != nil {
				return err
			}
			insertions = 0
		}
		return next(key, nil, nil)
	}, etl.TransformArgs{Quit: context.Background().Done()}); err != nil {
		return libcommon.Hash{}, err
	}
	commitment := rootNode.Commitment().Bytes()
	return libcommon.BytesToHash(commitment[:]), flushVerkleNode(v.db, rootNode, logInterval, nil, v.logger)
}

func (v *VerkleTreeWriter) Close() {
	v.collector.Close()
}
