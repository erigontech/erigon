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
	pedersenHashstateBucket := "PedersenHashstate"
	start := time.Now()
	log.Info("Started Generation of the Pedersen Hashed State")
	if err := outTx.CreateBucket(pedersenHashstateBucket); err != nil {
		return err
	}
	collector := etl.NewCollector("Pedersen Hashing", "/tmp/etl-temp", etl.NewSortableBuffer(etl.BufferOptimalSize))

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
			if err := collector.Collect(versionKey[:], []byte{0}); err != nil {
				return err
			}
			// Process nonce
			nonceValue := make([]byte, 32)
			acc := accounts.NewAccount()
			if err := acc.DecodeForStorage(v); err != nil {
				return err
			}
			binary.LittleEndian.PutUint64(nonceValue, acc.Nonce)
			if err := collector.Collect(nonceKey[:], nonceValue); err != nil {
				return err
			}
			// Process Balance
			balanceValue := make([]byte, 32)
			balanceBytes := acc.Balance.Bytes()
			if len(balanceBytes) > 0 {
				for i := range balanceBytes {
					balanceValue[len(balanceBytes)-i-1] = balanceBytes[i]
				}
			}
			if err := collector.Collect(balanceKey[:], balanceValue); err != nil {
				return err
			}
			// Process Code Size
			codeSizeValue := make([]byte, 32)
			if !accounts.IsEmptyCodeHash(acc.CodeHash) {
				code, err := readTx.GetOne(kv.Code, acc.CodeHash[:])
				if err != nil {
					return err
				}
				// TODO(Giulio2002): Chunkify code
				// ----
				chunkedCode, err := vtree.ChunkifyCode(code)
				if err != nil {
					return err
				}
				// Write code chunks
				for i := 0; i < len(chunkedCode); i += 32 {
					collector.Collect(vtree.GetTreeKeyCodeChunk(k, uint256.NewInt(uint64(i)/32)), chunkedCode[i:i+32])
				}

				// Set code size
				binary.LittleEndian.PutUint64(codeSizeValue, uint64(len(code)))
			}

			if err := collector.Collect(codeSizeKey[:], codeSizeValue); err != nil {
				return err
			}
			// Process Code Hash
			if err := collector.Collect(codeHashKey[:], acc.CodeHash[:]); err != nil {
				return err
			}

		} else {
			// Process storage
			collector.Collect(vtree.GetTreeKeyStorageSlot(k[:20], new(uint256.Int).SetBytes(k[28:])), v)
		}
		select {
		case <-logInterval.C:
			log.Info("[Pedersen Hashing] Current progress in Collection Phase", "key", common.Bytes2Hex(k))
		default:
		}
	}
	collector.Load(outTx, pedersenHashstateBucket, etl.IdentityLoadFunc, etl.TransformArgs{})

	log.Info("Pedersen hashed state finished", "elapsed", time.Until(start))
	return outTx.Commit()

}
