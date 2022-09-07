package main

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/gballet/go-verkle"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
	"github.com/ledgerwatch/log/v3"
)

func GenerateVerkleTree(cfg optionsCfg) error {
	db, err := mdbx.Open(cfg.verkleDb, log.Root(), true)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.BeginRw(cfg.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := initDB(tx); err != nil {
		return err
	}

	start := time.Now()
	pairCollector := etl.NewCollector(VerkleTrie, cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer pairCollector.Close()
	accCursor, err := tx.Cursor(PedersenHashedAccounts)
	if err != nil {
		return err
	}
	defer accCursor.Close()

	log.Info("Formatting Accounts")
	for versionKey, v, err := accCursor.First(); versionKey != nil; versionKey, v, err = accCursor.Next() {
		if err != nil {
			return err
		}
		acc := vtree.DecodeVerkleAccountForStorage(v)
		// Derive from versionkey all other keys
		var codeHashKey, nonceKey, balanceKey, codeSizeKey, nonce, balance, codeSize [32]byte
		copy(codeHashKey[:], versionKey[:31])
		copy(nonceKey[:], versionKey[:31])
		copy(balanceKey[:], versionKey[:31])
		copy(codeSizeKey[:], versionKey[:31])
		codeHashKey[31] = vtree.CodeKeccakLeafKey
		nonceKey[31] = vtree.NonceLeafKey
		balanceKey[31] = vtree.BalanceLeafKey
		codeSizeKey[31] = vtree.CodeSizeLeafKey
		// Compute balance value
		bbytes := acc.Balance.Bytes()
		if len(bbytes) > 0 {
			for i, b := range bbytes {
				balance[len(bbytes)-i-1] = b
			}
		}
		// compute nonce value
		binary.LittleEndian.PutUint64(nonce[:], acc.Nonce)
		// Compute code size value
		binary.LittleEndian.PutUint64(codeSize[:], acc.CodeSize)
		// Collect all data
		if err := pairCollector.Collect(versionKey, []byte{0}); err != nil {
			return err
		}

		if err := pairCollector.Collect(nonceKey[:], nonce[:]); err != nil {
			return err
		}
		if err := pairCollector.Collect(balanceKey[:], balance[:]); err != nil {
			return err
		}
		if err := pairCollector.Collect(codeHashKey[:], acc.CodeHash[:]); err != nil {
			return err
		}
		if err := pairCollector.Collect(codeSizeKey[:], codeSize[:]); err != nil {
			return err
		}
	}

	storageCursor, err := tx.Cursor(PedersenHashedStorage)
	if err != nil {
		return err
	}
	defer storageCursor.Close()

	log.Info("Formatting Storage slots")
	for storageKey, v, err := storageCursor.First(); storageKey != nil; storageKey, v, err = storageCursor.Next() {
		if err != nil {
			return err
		}
		storageValue := new(uint256.Int).SetBytes(v)
		formattedValue := storageValue.Bytes32()
		// Collect formatted data
		if err := pairCollector.Collect(storageKey, formattedValue[:]); err != nil {
			return err
		}
	}

	codeCursor, err := tx.Cursor(PedersenHashedCode)
	if err != nil {
		return err
	}
	defer codeCursor.Close()

	log.Info("Formatting Contract Code")
	for k, v, err := codeCursor.First(); k != nil; k, v, err = codeCursor.Next() {
		if err != nil {
			return err
		}
		if err := pairCollector.Collect(k, v); err != nil {
			return err
		}
	}

	verkleCollector := etl.NewCollector(VerkleTrie, cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer verkleCollector.Close()
	// Verkle Tree to be built
	root := verkle.New()
	log.Info("Started Verkle Tree creation")

	logInterval := time.NewTicker(30 * time.Second)
	if err := pairCollector.Load(tx, VerkleTrie, func(k []byte, v []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if err := root.InsertOrdered(common.CopyBytes(k), common.CopyBytes(v), func(node verkle.VerkleNode) {
			rootHash := node.ComputeCommitment().Bytes()
			encodedNode, err := node.Serialize()
			if err != nil {
				panic(err)
			}
			if err := verkleCollector.Collect(rootHash[:], encodedNode); err != nil {
				panic(err)
			}
			select {
			case <-logInterval.C:
				log.Info("[Verkle] Assembling Verkle Tree", "key", common.Bytes2Hex(k))
			default:
			}
		}); err != nil {
			return err
		}
		return next(k, nil, nil)
	}, etl.TransformArgs{Quit: context.Background().Done()}); err != nil {
		return err
	}
	log.Info("Started Verkle Tree Flushing")
	verkleCollector.Load(tx, VerkleTrie, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"key", common.Bytes2Hex(k)}
		}})

	log.Info("Verkle Tree Generation completed", "elapsed", time.Since(start))

	return tx.Commit()
}
