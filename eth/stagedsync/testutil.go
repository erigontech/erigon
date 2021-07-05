package stagedsync

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/stretchr/testify/assert"
)

const (
	staticCodeStaticIncarnations         = iota // no incarnation changes, no code changes
	changeCodeWithIncarnations                  // code changes with incarnation
	changeCodeIndepenentlyOfIncarnations        // code changes with and without incarnation
)

func compareCurrentState(
	t *testing.T,
	db1 ethdb.Tx,
	db2 ethdb.Tx,
	buckets ...string,
) {
	for _, bucket := range buckets {
		compareBucket(t, db1, db2, bucket)
	}
}

func compareBucket(t *testing.T, db1, db2 ethdb.Tx, bucketName string) {
	var err error

	bucket1 := make(map[string][]byte)
	c1, err := db1.Cursor(bucketName)
	if err != nil {
		assert.NoError(t, err)
	}
	defer c1.Close()
	err = ethdb.ForEach(c1, func(k, v []byte) (bool, error) {
		bucket1[string(k)] = v
		return true, nil
	})
	assert.NoError(t, err)

	bucket2 := make(map[string][]byte)
	c2, err := db2.Cursor(bucketName)
	if err != nil {
		assert.NoError(t, err)
	}
	defer c2.Close()
	err = ethdb.ForEach(c2, func(k, v []byte) (bool, error) {
		bucket2[string(k)] = v
		return true, nil
	})
	assert.NoError(t, err)

	assert.Equalf(t, bucket1 /*expected*/, bucket2 /*actual*/, "bucket %q", bucketName)
}

type stateWriterGen func(uint64) state.WriterWithChangeSets

func hashedWriterGen(tx ethdb.RwTx) stateWriterGen {
	return func(blockNum uint64) state.WriterWithChangeSets {
		return state.NewDbStateWriter(kv.WrapIntoTxDB(tx), blockNum)
	}
}

func plainWriterGen(tx ethdb.RwTx) stateWriterGen {
	return func(blockNum uint64) state.WriterWithChangeSets {
		return state.NewPlainStateWriter(tx, tx, blockNum)
	}
}
func generateBlocks(t *testing.T, from uint64, numberOfBlocks uint64, stateWriterGen stateWriterGen, difficulty int) {
	acc1 := accounts.NewAccount()
	acc1.Incarnation = 1
	acc1.Initialised = true
	acc1.Balance.SetUint64(0)

	acc2 := accounts.NewAccount()
	acc2.Incarnation = 0
	acc2.Initialised = true
	acc2.Balance.SetUint64(0)

	testAccounts := []*accounts.Account{
		&acc1,
		&acc2,
	}

	for blockNumber := uint64(1); blockNumber < from+numberOfBlocks; blockNumber++ {
		updateIncarnation := difficulty != staticCodeStaticIncarnations && blockNumber%10 == 0
		blockWriter := stateWriterGen(blockNumber)

		for i, oldAcc := range testAccounts {
			addr := common.HexToAddress(fmt.Sprintf("0x1234567890%d", i))

			newAcc := oldAcc.SelfCopy()
			newAcc.Balance.SetUint64(blockNumber)
			if updateIncarnation && oldAcc.Incarnation > 0 /* only update for contracts */ {
				newAcc.Incarnation = oldAcc.Incarnation + 1
			}

			if blockNumber == 1 && newAcc.Incarnation > 0 {
				if blockNumber >= from {
					if err := blockWriter.CreateContract(addr); err != nil {
						t.Fatal(err)
					}
				}
			}
			if blockNumber == 1 || updateIncarnation || difficulty == changeCodeIndepenentlyOfIncarnations {
				if newAcc.Incarnation > 0 {
					code := []byte(fmt.Sprintf("acc-code-%v", blockNumber))
					codeHash, _ := common.HashData(code)
					if blockNumber >= from {
						if err := blockWriter.UpdateAccountCode(addr, newAcc.Incarnation, codeHash, code); err != nil {
							t.Fatal(err)
						}
					}
					newAcc.CodeHash = codeHash
				}
			}

			if newAcc.Incarnation > 0 {
				var oldValue, newValue uint256.Int
				newValue.SetOne()
				var location common.Hash
				location.SetBytes(big.NewInt(int64(blockNumber)).Bytes())
				if blockNumber >= from {
					if err := blockWriter.WriteAccountStorage(addr, newAcc.Incarnation, &location, &oldValue, &newValue); err != nil {
						t.Fatal(err)
					}
				}
			}
			if blockNumber >= from {
				if err := blockWriter.UpdateAccountData(addr, oldAcc, newAcc); err != nil {
					t.Fatal(err)
				}
			}
			testAccounts[i] = newAcc
		}
		if blockNumber >= from {
			if err := blockWriter.WriteChangeSets(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
