package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/gballet/go-verkle"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
	"github.com/ledgerwatch/log/v3"
)

func getPedersenAccountKey(tx kv.RwTx, address []byte) ([]byte, error) {
	treeKey, err := tx.GetOne(PedersenHashedAccountsLookup, address)
	if err != nil {
		return nil, err
	}

	if len(treeKey) > 0 {
		return treeKey, nil
	}
	treeKey = vtree.GetTreeKeyVersion(address)
	return treeKey, tx.Put(PedersenHashedStorageLookup, address, treeKey)

}

func getPedersenStorageKey(tx kv.RwTx, address []byte, storageKey []byte) ([]byte, error) {
	treeKey, err := tx.GetOne(PedersenHashedStorageLookup, append(address, storageKey...))
	if err != nil {
		return nil, err
	}

	if len(treeKey) > 0 {
		return treeKey, nil
	}
	treeKey = vtree.GetTreeKeyStorageSlot(address, new(uint256.Int).SetBytes(storageKey))
	return treeKey, tx.Put(PedersenHashedStorageLookup, append(address, storageKey...), treeKey)
}

func getPedersenCodeKey(tx kv.RwTx, address []byte, index *uint256.Int) ([]byte, error) {
	lookupKey := make([]byte, 24)
	copy(lookupKey, address)
	binary.BigEndian.PutUint32(lookupKey[20:], uint32(index.Uint64()))
	treeKey, err := tx.GetOne(PedersenHashedCodeLookup, lookupKey)
	if err != nil {
		return nil, err
	}

	if len(treeKey) > 0 {
		return treeKey, nil
	}
	treeKey = vtree.GetTreeKeyCodeChunk(address, index)
	return treeKey, tx.Put(PedersenHashedStorageLookup, lookupKey, treeKey)
}

func updateAccount(tx kv.Tx, vTx kv.RwTx, currentNode verkle.VerkleNode, address, encodedAccount []byte) (allocs uint64, err error) {
	resolverFunc := func(root []byte) ([]byte, error) {
		return vTx.GetOne(VerkleTrie, root)
	}

	versionKey, err := getPedersenAccountKey(vTx, address)
	if err != nil {
		return 0, err
	}
	var codeHashKey, nonceKey, balanceKey, codeSizeKey [32]byte
	copy(codeHashKey[:], versionKey[:31])
	copy(nonceKey[:], versionKey[:31])
	copy(balanceKey[:], versionKey[:31])
	copy(codeSizeKey[:], versionKey[:31])
	codeHashKey[31] = vtree.CodeKeccakLeafKey
	nonceKey[31] = vtree.NonceLeafKey
	balanceKey[31] = vtree.BalanceLeafKey
	codeSizeKey[31] = vtree.CodeSizeLeafKey

	var acc accounts.Account
	if err := acc.DecodeForStorage(encodedAccount); err != nil {
		return 0, err
	}
	var nonce, balance, codeSize [32]byte
	// Compute balance value
	bbytes := acc.Balance.ToBig().Bytes()
	if len(bbytes) > 0 {
		for i, b := range bbytes {
			balance[len(bbytes)-i-1] = b
		}
	}
	// compute nonce value
	binary.LittleEndian.PutUint64(nonce[:], acc.Nonce)
	codeLen := uint64(0)
	if !acc.IsEmptyCodeHash() {
		code, err := tx.GetOne(kv.Code, acc.CodeHash[:])
		if err != nil {
			return 0, err
		}
		codeLen = uint64(len(code))
		version, err := currentNode.Get(versionKey, resolverFunc)
		if err != nil {
			return 0, err
		}
		if len(version) == 0 {
			// Create the new code inside the verkle tree
			chunkedCode := vtree.ChunkifyCode(code)
			for i := 0; i < len(chunkedCode); i += 32 {
				allocs += 64
				chunk := chunkedCode[i : i+32]
				index := uint256.NewInt(uint64(i) / 32)
				treeKey, err := getPedersenCodeKey(vTx, address, index)
				if err != nil {
					return 0, err
				}
				if err := currentNode.Insert(treeKey, chunk, resolverFunc); err != nil {
					return 0, err
				}
			}
		}
	}
	// Compute code size value
	binary.LittleEndian.PutUint64(codeSize[:], codeLen)
	allocs += 33 + 64*4
	if err := currentNode.Insert(versionKey, []byte{0}, resolverFunc); err != nil {
		return 0, err
	}
	if err := currentNode.Insert(nonceKey[:], nonce[:], resolverFunc); err != nil {
		return 0, err
	}
	if err := currentNode.Insert(codeHashKey[:], acc.CodeHash[:], resolverFunc); err != nil {
		return 0, err
	}
	if err := currentNode.Insert(balanceKey[:], balance[:], resolverFunc); err != nil {
		return 0, err
	}
	if err := currentNode.Insert(codeSizeKey[:], codeSize[:], resolverFunc); err != nil {
		return 0, err
	}
	return allocs, nil
}

func deleteAccount(tx kv.Tx, vTx kv.RwTx, currentNode verkle.VerkleNode, address []byte) (allocs uint64, err error) {
	resolverFunc := func(root []byte) ([]byte, error) {
		return vTx.GetOne(VerkleTrie, root)
	}

	versionKey, err := getPedersenAccountKey(vTx, address)
	if err != nil {
		return 0, err
	}
	var codeHashKey, nonceKey, balanceKey, codeSizeKey [32]byte
	copy(codeHashKey[:], versionKey[:31])
	copy(nonceKey[:], versionKey[:31])
	copy(balanceKey[:], versionKey[:31])
	copy(codeSizeKey[:], versionKey[:31])
	codeHashKey[31] = vtree.CodeKeccakLeafKey
	nonceKey[31] = vtree.NonceLeafKey
	balanceKey[31] = vtree.BalanceLeafKey
	codeSizeKey[31] = vtree.CodeSizeLeafKey

	// We need to wipe out all references in the tree to that address
	allocs += 330 + 640*4
	if err := currentNode.Insert(versionKey, nil, resolverFunc); err != nil {
		return 0, err
	}
	if err := currentNode.Insert(nonceKey[:], nil, resolverFunc); err != nil {
		return 0, err
	}
	if err := currentNode.Insert(codeHashKey[:], nil, resolverFunc); err != nil {
		return 0, err
	}
	if err := currentNode.Insert(balanceKey[:], nil, resolverFunc); err != nil {
		return 0, err
	}
	if err := currentNode.Insert(codeSizeKey[:], nil, resolverFunc); err != nil {
		return 0, err
	}
	// Delete also code and storage slots that are connected to that account (iterating over lookups is simpe)
	storageLookupCursor, err := vTx.Cursor(PedersenHashedStorageLookup)
	if err != nil {
		return 0, err
	}
	defer storageLookupCursor.Close()
	codeLookupCursor, err := vTx.Cursor(PedersenHashedCodeLookup)
	if err != nil {
		return 0, err
	}
	defer codeLookupCursor.Close()

	for k, treeKey, err := storageLookupCursor.Seek(address); len(k) >= 20 && bytes.Equal(k[:20], address); k, treeKey, err = storageLookupCursor.Next() {
		if err != nil {
			return 0, err
		}
		allocs += 640
		if err := currentNode.Insert(treeKey, nil, resolverFunc); err != nil {
			return 0, err
		}
	}

	for k, treeKey, err := codeLookupCursor.Seek(address); len(k) >= 20 && bytes.Equal(k[:20], address); k, treeKey, err = codeLookupCursor.Next() {
		if err != nil {
			return 0, err
		}
		allocs += 640
		if err := currentNode.Insert(treeKey, nil, resolverFunc); err != nil {
			return 0, err
		}
	}

	return allocs, nil
}

func IncrementVerkleTree(cfg optionsCfg) error {
	start := time.Now()
	db, err := mdbx.Open(cfg.stateDb, log.Root(), true)
	if err != nil {
		log.Error("Error while opening database", "err", err.Error())
		return err
	}
	defer db.Close()

	vDb, err := mdbx.Open(cfg.verkleDb, log.Root(), false)
	if err != nil {
		log.Error("Error while opening db transaction", "err", err.Error())
		return err
	}
	defer vDb.Close()

	vTx, err := vDb.BeginRw(cfg.ctx)
	if err != nil {
		return err
	}
	defer vTx.Rollback()

	tx, err := db.BeginRo(cfg.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := initDB(vTx); err != nil {
		return err
	}
	logInterval := time.NewTicker(30 * time.Second)
	// START
	var progress uint64
	if progress, err = stages.GetStageProgress(tx, stages.VerkleTrie); err != nil {
		return err
	}
	headHeader := rawdb.ReadHeaderByNumber(tx, progress)

	nodeRlp, err := vTx.GetOne(VerkleTrie, headHeader.Root[:])
	if err != nil {
		return err
	}

	batchSize := 512 * datasize.MB
	var accumulated datasize.ByteSize = 0

	fmt.Println(progress)
	var currentNode verkle.VerkleNode
	if progress == 0 {
		currentNode = verkle.New()
	} else {
		currentNode, err = verkle.ParseNode(nodeRlp, 0, headHeader.Root[:])
		if err != nil {
			return err
		}
	}

	accountCursor, err := tx.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return err
	}
	defer accountCursor.Close()

	storageCursor, err := tx.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return err
	}
	defer storageCursor.Close()

	resolverFunc := func(root []byte) ([]byte, error) {
		return vTx.GetOne(VerkleTrie, root)
	}

	for k, v, err := storageCursor.Seek(dbutils.EncodeBlockNumber(progress)); k != nil; k, v, err = storageCursor.Next() {
		if err != nil {
			return err
		}
		blockNumber, changesetKey, storageValue, err := changeset.DecodeStorage(k, v)
		if err != nil {
			return err
		}
		storageKey, err := getPedersenStorageKey(vTx, changesetKey[:20], changesetKey[28:])
		if err != nil {
			return err
		}

		if len(storageValue) > 0 {
			storageValue32 := new(uint256.Int).SetBytes(storageValue).Bytes32()
			if err := currentNode.Insert(storageKey, storageValue32[:], resolverFunc); err != nil {
				return err
			}
			accumulated += 640
		} else {
			if err := currentNode.Insert(storageKey, nil, resolverFunc); err != nil {
				return err
			}
			accumulated += 320
		}
		if accumulated > batchSize {
			accumulated = 0
			nodeCount := 0
			currentNode.(*verkle.InternalNode).Flush(func(n verkle.VerkleNode) {
				nodeCount++
				rlp, err := n.Serialize()
				if err != nil {
					panic(err)
				}
				root := n.ComputeCommitment().Bytes()
				if err := vTx.Put(VerkleTrie, root[:], rlp); err != nil {
					panic(err)
				}
			})
			log.Info("Flushed tree", "nodes", nodeCount, "blockNum", blockNumber)
		}
		select {
		case <-logInterval.C:
			log.Info("Creating Verkle Trie Incrementally", "phase", "storage", "blockNum", blockNumber, "batchSize", accumulated)
		default:
		}
	}

	for k, v, err := accountCursor.Seek(dbutils.EncodeBlockNumber(progress)); k != nil; k, v, err = accountCursor.Next() {
		if err != nil {
			return err
		}
		blockNumber, address, encodedAccount, err := changeset.DecodeAccounts(k, v)
		if err != nil {
			return err
		}

		// Start
		if len(encodedAccount) == 0 {
			allocs, err := deleteAccount(tx, vTx, currentNode, address)
			if err != nil {
				return err
			}
			accumulated += datasize.ByteSize(allocs)
		} else {
			allocs, err := updateAccount(tx, vTx, currentNode, address, encodedAccount)
			if err != nil {
				return err
			}
			accumulated += datasize.ByteSize(allocs)
		}

		if accumulated > batchSize {
			accumulated = 0
			nodeCount := 0
			currentNode.(*verkle.InternalNode).Flush(func(n verkle.VerkleNode) {
				nodeCount++
				rlp, err := n.Serialize()
				if err != nil {
					panic(err)
				}
				root := n.ComputeCommitment().Bytes()
				if err := vTx.Put(VerkleTrie, root[:], rlp); err != nil {
					panic(err)
				}
			})
			log.Info("Flushed tree", "nodes", nodeCount, "blockNum", blockNumber)
		}
		select {
		case <-logInterval.C:
			log.Info("Creating Verkle Trie Incrementally", "phase", "storage", "blockNum", blockNumber)
		default:
		}
	}

	currentNode.(*verkle.InternalNode).Flush(func(n verkle.VerkleNode) {
		rlp, err := n.Serialize()
		if err != nil {
			panic(err)
		}
		root := n.ComputeCommitment().Bytes()
		if err := vTx.Put(VerkleTrie, root[:], rlp); err != nil {
			panic(err)
		}
	})

	if progress, err = stages.GetStageProgress(tx, stages.Execution); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(vTx, stages.VerkleTrie, progress); err != nil {
		return err
	}

	rootHash := currentNode.ComputeCommitment().Bytes()
	encodedNode, err := currentNode.Serialize()
	if err != nil {
		return err
	}
	if err := vTx.Put(VerkleTrie, rootHash[:], encodedNode); err != nil {
		return err
	}

	log.Info("Finished incremental tree creation", "elapsed", time.Since(start))

	return vTx.Commit()
}
