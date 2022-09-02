package main

import (
	"encoding/binary"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
	"github.com/ledgerwatch/log/v3"
)

func retrieveAccountKeys(address common.Address) (versionKey, balanceKey, codeSizeKey, codeHashKey, noncekey [32]byte) {
	// Process the polynomial
	versionkey := vtree.GetTreeKeyVersion(address[:])
	copy(balanceKey[:], versionkey)
	balanceKey[31] = vtree.BalanceLeafKey
	copy(noncekey[:], versionkey)
	noncekey[31] = vtree.NonceLeafKey
	copy(codeSizeKey[:], versionkey)
	codeSizeKey[31] = vtree.CodeSizeLeafKey
	copy(codeHashKey[:], versionkey)
	codeHashKey[31] = vtree.CodeKeccakLeafKey
	return
}

func RegeneratePedersenHashstate(outTx kv.RwTx, readTx kv.Tx) error {
	pedersenHashStateBucket := "PedersenHashState"
	pedersenHashStorageBucket := "PedersenHashStorage"
	start := time.Now()
	log.Info("Started Generation of the Pedersen Hashed State")
	if err := outTx.CreateBucket(pedersenHashStateBucket); err != nil {
		return err
	}
	if err := outTx.CreateBucket(pedersenHashStorageBucket); err != nil {
		return err
	}
	stateCollector := etl.NewCollector("Pedersen State", "/tmp/etl-temp", etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer stateCollector.Close()

	storageCollector := etl.NewCollector("Pedersen Storage", "/tmp/etl-temp", etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer storageCollector.Close()

	plainStateCursor, err := readTx.Cursor(kv.PlainState)
	if err != nil {
		return err
	}
	logInterval := time.NewTicker(30 * time.Second)
	for k, v, err := plainStateCursor.First(); k != nil; k, v, err = plainStateCursor.Next() {
		if err != nil {
			return err
		}
		if len(k) == 20 {
			versionKey, balanceKey, codeSizeKey, codeHashKey, nonceKey := retrieveAccountKeys(common.BytesToAddress(k))
			if err := stateCollector.Collect(versionKey[:], []byte{0}); err != nil {
				return err
			}
			// Process nonce
			nonceValue := make([]byte, 8)
			acc := accounts.NewAccount()
			if err := acc.DecodeForStorage(v); err != nil {
				return err
			}
			binary.LittleEndian.PutUint64(nonceValue, acc.Nonce)
			if err := stateCollector.Collect(nonceKey[:], nonceValue); err != nil {
				return err
			}
			// Process Balance
			balanceBytes := acc.Balance.ToBig().Bytes()
			balanceValue := make([]byte, 32)
			if len(balanceBytes) > 0 {
				for i := range balanceBytes {
					balanceValue[len(balanceBytes)-i-1] = balanceBytes[i]
				}
			}
			if err := stateCollector.Collect(balanceKey[:], balanceValue); err != nil {
				return err
			}
			// Process Code Size
			codeSizeValue := make([]byte, 8)
			if !accounts.IsEmptyCodeHash(acc.CodeHash) {
				code, err := readTx.GetOne(kv.Code, acc.CodeHash[:])
				if err != nil {
					return err
				}
				// Chunkify contract code and build keys for each chunks and insert them in the tree
				chunkedCode, err := vtree.ChunkifyCode(code)
				if err != nil {
					return err
				}
				// Write code chunks
				for i := 0; i < len(chunkedCode); i += 32 {
					stateCollector.Collect(vtree.GetTreeKeyCodeChunk(k, uint256.NewInt(uint64(i)/32)), chunkedCode[i:i+32])
				}

				// Set code size
				binary.LittleEndian.PutUint64(codeSizeValue, uint64(len(code)))
			}

			if err := stateCollector.Collect(codeSizeKey[:], codeSizeValue); err != nil {
				return err
			}
			// Process Code Hash
			if err := stateCollector.Collect(codeHashKey[:], acc.CodeHash[:]); err != nil {
				return err
			}

		} else if len(k) == 60 {
			// Process storage
			storageCollector.Collect(vtree.GetTreeKeyStorageSlot(k[:20], new(uint256.Int).SetBytes(k[28:])), v)
		}
		select {
		case <-logInterval.C:
			log.Info("[Pedersen Hashing] Current progress in Collection Phase", "key", common.Bytes2Hex(k))
		default:
		}
	}
	stateCollector.Load(outTx, pedersenHashStateBucket, etl.IdentityLoadFunc, etl.TransformArgs{})
	storageCollector.Load(outTx, pedersenHashStorageBucket, etl.IdentityLoadFunc, etl.TransformArgs{})

	log.Info("Pedersen hashed state finished", "elapsed", time.Until(start))
	return outTx.Commit()

}
