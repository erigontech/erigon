package stagedsync

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	state2 "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/stretchr/testify/assert"
)

const (
	staticCodeStaticIncarnations         = iota // no incarnation changes, no code changes
	changeCodeWithIncarnations                  // code changes with incarnation
	changeCodeIndepenentlyOfIncarnations        // code changes with and without incarnation
)

func compareCurrentState(
	t *testing.T,
	agg *state2.AggregatorV3,
	db1 kv.Tx,
	db2 kv.Tx,
	buckets ...string,
) {
	for _, bucket := range buckets {
		if ethconfig.EnableHistoryV4InTest {
			compareDomain(t, agg, db1, db2, bucket)
			continue
		}
		compareBucket(t, db1, db2, bucket)
	}
}

func compareDomain(t *testing.T, agg *state2.AggregatorV3, db1, db2 kv.Tx, bucketName string) {
	panic("implement me")
	/*
		ac := agg.MakeContext()
		defer ac.Close()

		switch bucketName {
		case kv.PlainState:
			bucket1 := make(map[string][]byte)
			ac.DeprecatedLatestAcc(db1.(kv.RwTx), func(k, v []byte) {
				bucket1[string(k)] = v
			})
			require.True(t, len(bucket1) > 0)
			bucket2 := make(map[string][]byte)
			ac.DeprecatedLatestAcc(db2.(kv.RwTx), func(k, v []byte) {
				bucket2[string(k)] = v
			})
			assert.Equalf(t, bucket1, bucket2, "bucket %q", bucketName)

			bucket1 = make(map[string][]byte)
			ac.DeprecatedLatestSt(db1.(kv.RwTx), func(k, v []byte) {
				bucket1[string(k)] = v
			})
			bucket2 = make(map[string][]byte)
			ac.DeprecatedLatestSt(db2.(kv.RwTx), func(k, v []byte) {
				bucket2[string(k)] = v
			})
			assert.Equalf(t, bucket1, bucket2, "bucket %q", bucketName)
		case kv.PlainContractCode:
			bucket1 := make(map[string][]byte)
			ac.DeprecatedLatestCode(db1.(kv.RwTx), func(k, v []byte) {
				bucket1[string(k)] = v
			})
			bucket2 := make(map[string][]byte)
			ac.DeprecatedLatestCode(db2.(kv.RwTx), func(k, v []byte) {
				bucket2[string(k)] = v
			})
			assert.Equalf(t, bucket1, bucket2, "bucket %q", bucketName)

			bucket1 = make(map[string][]byte)
			ac.DeprecatedLatestSt(db1.(kv.RwTx), func(k, v []byte) {
				bucket1[string(k)] = v
			})
			bucket2 = make(map[string][]byte)
			ac.DeprecatedLatestSt(db2.(kv.RwTx), func(k, v []byte) {
				bucket2[string(k)] = v
			})
			assert.Equalf(t, bucket1, bucket2, "bucket %q", bucketName)
		default:
			panic(bucketName)
		}
	*/
}

func compareBucket(t *testing.T, db1, db2 kv.Tx, bucketName string) {
	var err error

	bucket1 := make(map[string][]byte)
	err = db1.ForEach(bucketName, nil, func(k, v []byte) error {
		bucket1[string(k)] = v
		return nil
	})
	assert.NoError(t, err)

	bucket2 := make(map[string][]byte)
	err = db2.ForEach(bucketName, nil, func(k, v []byte) error {
		bucket2[string(k)] = v
		return nil
	})
	assert.NoError(t, err)

	assert.Equalf(t, bucket1 /*expected*/, bucket2 /*actual*/, "bucket %q", bucketName)
}

type stateWriterGen func(uint64) state.WriterWithChangeSets

func hashedWriterGen(tx kv.RwTx) stateWriterGen {
	return func(blockNum uint64) state.WriterWithChangeSets {
		return state.NewDbStateWriter(tx, blockNum)
	}
}

func plainWriterGen(tx kv.RwTx) stateWriterGen {
	return func(blockNum uint64) state.WriterWithChangeSets {
		return state.NewPlainStateWriter(tx, tx, blockNum)
	}
}

type testGenHook func(n, from, numberOfBlocks uint64)

func generateBlocks2(t *testing.T, from uint64, numberOfBlocks uint64, blockWriter state.StateWriter, beforeBlock, afterBlock testGenHook, difficulty int) {
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
		beforeBlock(blockNumber, from, numberOfBlocks)
		updateIncarnation := difficulty != staticCodeStaticIncarnations && blockNumber%10 == 0

		for i, oldAcc := range testAccounts {
			addr := libcommon.HexToAddress(fmt.Sprintf("0x1234567890%d", i))

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
					codeHash, _ := libcommon.HashData(code)
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
				var location libcommon.Hash
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
		afterBlock(blockNumber, from, numberOfBlocks)
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
			addr := libcommon.HexToAddress(fmt.Sprintf("0x1234567890%d", i))

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
					codeHash, _ := libcommon.HashData(code)
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
				var location libcommon.Hash
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
