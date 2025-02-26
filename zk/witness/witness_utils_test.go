package witness

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"math/rand"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/smt/pkg/smt"
	"github.com/ledgerwatch/erigon/turbo/trie"
	"github.com/status-im/keycard-go/hexutils"
	"github.com/stretchr/testify/assert"
)

func TestMergeWitnesses(t *testing.T) {
	smt1 := smt.NewSMT(nil, false)
	smt2 := smt.NewSMT(nil, false)
	smtFull := smt.NewSMT(nil, false)

	random := rand.New(rand.NewSource(0))

	numberOfAccounts := 500

	for i := 0; i < numberOfAccounts; i++ {
		a := getAddressForIndex(i)
		addressBytes := crypto.Keccak256(a[:])
		address := common.BytesToAddress(addressBytes).String()
		balance := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))
		nonce := new(big.Int).Rand(random, new(big.Int).Exp(common.Big2, common.Big256, nil))
		bytecode := "afafaf"
		contractStorage := make(map[string]string)
		for j := 0; j < 10; j++ {
			storageKey := genRandomByteArrayOfLen(32)
			storageValue := genRandomByteArrayOfLen(32)
			contractStorage[common.BytesToHash(storageKey).Hex()] = common.BytesToHash(storageValue).Hex()
		}
		var smtPart *smt.SMT

		if i&1 == 0 {
			smtPart = smt1
		} else {
			smtPart = smt2
		}

		if _, err := smtPart.SetAccountBalance(address, balance); err != nil {
			t.Error(err)
			return
		}
		if _, err := smtPart.SetAccountNonce(address, nonce); err != nil {
			t.Error(err)
			return
		}
		if err := smtPart.SetContractBytecode(address, bytecode); err != nil {
			t.Error(err)
			return
		}
		if err := smtPart.Db.AddCode(hexutils.HexToBytes(bytecode)); err != nil {
			t.Error(err)
			return
		}
		if _, err := smtPart.SetContractStorage(address, contractStorage, nil); err != nil {
			t.Error(err)
			return
		}

		if _, err := smtFull.SetAccountBalance(address, balance); err != nil {
			t.Error(err)
			return
		}
		if _, err := smtFull.SetAccountNonce(address, nonce); err != nil {
			t.Error(err)
			return
		}
		if err := smtFull.SetContractBytecode(address, bytecode); err != nil {
			t.Error(err)
			return
		}
		if err := smtFull.Db.AddCode(hexutils.HexToBytes(bytecode)); err != nil {
			t.Error(err)
			return
		}
		if _, err := smtFull.SetContractStorage(address, contractStorage, nil); err != nil {
			t.Error(err)
			return
		}
	}

	rl1 := &trie.AlwaysTrueRetainDecider{}
	rl2 := &trie.AlwaysTrueRetainDecider{}
	rlFull := &trie.AlwaysTrueRetainDecider{}
	witness1, err := smt1.BuildWitness(rl1, context.Background())
	if err != nil {
		t.Error(err)
		return
	}

	witness2, err := smt2.BuildWitness(rl2, context.Background())
	if err != nil {
		t.Error(err)
		return
	}

	witnessFull, err := smtFull.BuildWitness(rlFull, context.Background())
	if err != nil {
		t.Error(err)
		return
	}
	mergedWitness, err := MergeWitnesses(context.Background(), []*trie.Witness{witness1, witness2})
	assert.Nil(t, err, "should successfully merge witnesses")

	//create writer
	var buff bytes.Buffer
	mergedWitness.WriteDiff(witnessFull, &buff)
	diff := buff.String()
	assert.Equal(t, 0, len(diff), "witnesses should be equal")
	if len(diff) > 0 {
		fmt.Println(diff)
	}
}

func getAddressForIndex(index int) [20]byte {
	var address [20]byte
	binary.BigEndian.PutUint32(address[:], uint32(index))
	return address
}

func genRandomByteArrayOfLen(length uint) []byte {
	array := make([]byte, length)
	for i := uint(0); i < length; i++ {
		array[i] = byte(rand.Intn(256))
	}
	return array
}

func TestMergeRealWitnesses(t *testing.T) {
	witnessBytes1, err := hex.DecodeString(witness1)
	assert.NoError(t, err, "error decoding witness1")
	witnessBytes2, err := hex.DecodeString(witness2)
	assert.NoError(t, err, "error decoding witness2")
	expectedWitnessBytes, err := hex.DecodeString(resultWitness)
	assert.NoError(t, err, "error decoding expectedWitness")

	blockWitness1, err := ParseWitnessFromBytes(witnessBytes1, false)
	assert.NoError(t, err, "error parsing witness1")
	blockWitness2, err := ParseWitnessFromBytes(witnessBytes2, false)
	assert.NoError(t, err, "error parsing witness2")
	expectedWitness, err := ParseWitnessFromBytes(expectedWitnessBytes, false)
	assert.NoError(t, err, "error parsing expectedWitness")

	mergedWitness, err := MergeWitnesses(context.Background(), []*trie.Witness{blockWitness1, blockWitness2})
	assert.NoError(t, err, "error merging witnesses")

	//create writer
	var buff bytes.Buffer
	expectedWitness.WriteDiff(mergedWitness, &buff)
	diff := buff.String()
	if len(diff) > 0 {
		fmt.Println(diff)
	}
	assert.Equal(t, 0, len(diff), "witnesses should be equal")
}

func TestMergeWitnessesWithHashNodes(t *testing.T) {
	smt1 := smt.NewSMT(nil, false)
	smt2 := smt.NewSMT(nil, false)
	smtFull := smt.NewSMT(nil, false)

	_, err := smt1.InsertHashNode([]int{0, 0, 0}, new(big.Int).SetUint64(1))
	assert.NoError(t, err, "error inserting hash node")
	_, err = smt2.InsertHashNode([]int{0, 0}, new(big.Int).SetUint64(2))
	assert.NoError(t, err, "error inserting hash node")
	_, err = smtFull.InsertHashNode([]int{0, 0, 0}, new(big.Int).SetUint64(1))
	assert.NoError(t, err, "error inserting hash node")

	// get witnesses
	rl1 := &trie.AlwaysTrueRetainDecider{}
	rl2 := &trie.AlwaysTrueRetainDecider{}
	rlFull := &trie.AlwaysTrueRetainDecider{}
	blockWitness1, err := smt1.BuildWitness(rl1, context.Background())
	assert.NoError(t, err, "error building witness")
	blockWitness2, err := smt2.BuildWitness(rl2, context.Background())
	assert.NoError(t, err, "error building witness")
	expectedWitness, err := smtFull.BuildWitness(rlFull, context.Background())
	assert.NoError(t, err, "error building witness")

	mergedWitness, err := MergeWitnesses(context.Background(), []*trie.Witness{blockWitness1, blockWitness2})
	assert.NoError(t, err, "error merging witnesses")

	//create writer
	var buff bytes.Buffer
	expectedWitness.WriteDiff(mergedWitness, &buff)
	diff := buff.String()
	if len(diff) > 0 {
		fmt.Println(diff)
	}
	assert.Equal(t, 0, len(diff), "witnesses should be equal")
}

func TestGetBatchesToCache(t *testing.T) {
	scenarios := map[string]struct {
		highestVerifiedBatch uint64
		highestExecutedBatch uint64
		latestCachedBatch    uint64

		offsetAhead  uint64
		offsetBehind uint64

		expectedStartBatch uint64
		expectedEndBatch   uint64
	}{
		"5 ahead, 5 behind": {
			highestVerifiedBatch: 10,
			highestExecutedBatch: 100,
			latestCachedBatch:    0,

			offsetAhead:  5,
			offsetBehind: 5,

			expectedStartBatch: 5,
			expectedEndBatch:   15,
		},
		"5 ahead, 5 behind, latest cached 10": {
			highestVerifiedBatch: 10,
			highestExecutedBatch: 100,
			latestCachedBatch:    10,

			offsetAhead:  5,
			offsetBehind: 5,

			expectedStartBatch: 11,
			expectedEndBatch:   15,
		},
		"5 ahead, 5 behind, latest cached 10, highest verified 5": {
			highestVerifiedBatch: 5,
			highestExecutedBatch: 100,
			latestCachedBatch:    10,

			offsetAhead:  5,
			offsetBehind: 5,

			expectedStartBatch: 10,
			expectedEndBatch:   10,
		},
		"highest ver 0, latest cached 0, highest executed 10": {
			highestVerifiedBatch: 0,
			highestExecutedBatch: 10,
			latestCachedBatch:    0,

			offsetAhead:  500,
			offsetBehind: 500,

			expectedStartBatch: 1,
			expectedEndBatch:   10,
		},
		"highest ver 0, latest cached 0, highest executed 0": {
			highestVerifiedBatch: 0,
			highestExecutedBatch: 0,
			latestCachedBatch:    0,

			offsetAhead:  500,
			offsetBehind: 500,

			expectedStartBatch: 0,
			expectedEndBatch:   0,
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			startBatch, endBatch, _ := GetBatchesToCache(scenario.highestVerifiedBatch, scenario.highestExecutedBatch, scenario.latestCachedBatch, scenario.offsetAhead, scenario.offsetBehind)
			assert.Equal(t, scenario.expectedStartBatch, startBatch, "start batch should be equal")
			assert.Equal(t, scenario.expectedEndBatch, endBatch, "end batch should be equal")
		})
	}
}
