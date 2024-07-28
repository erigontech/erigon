package smt

import (
	"context"
	"fmt"
	"math/big"
	"math/rand"
	"testing"
	"time"

	"github.com/ledgerwatch/erigon/smt/pkg/utils"
)

func TestSMT_Create_Insert(t *testing.T) {
	testCases := []struct {
		name         string
		kvMap        map[utils.NodeKey]utils.NodeValue8
		expectedRoot string
	}{
		{
			"TestSMT_Insert_1Key_0Value",
			map[utils.NodeKey]utils.NodeValue8{
				utils.ScalarToNodeKey(big.NewInt(1)): utils.ScalarToNodeValue8(big.NewInt(0)),
			},
			"0x0",
		},
		{
			"TestSMT_Insert1Key_XValue",
			map[utils.NodeKey]utils.NodeValue8{
				utils.ScalarToNodeKey(big.NewInt(1)): utils.ScalarToNodeValue8(new(big.Int).SetUint64(1)),
			},
			"0xb26e0de762d186d2efc35d9ff4388def6c96ec15f942d83d779141386fe1d2e1",
		},
		{
			"TestSMT_Insert2",
			map[utils.NodeKey]utils.NodeValue8{
				utils.ScalarToNodeKey(big.NewInt(1)): utils.ScalarToNodeValue8(big.NewInt(1)),
				utils.ScalarToNodeKey(big.NewInt(2)): utils.ScalarToNodeValue8(big.NewInt(2)),
			},
			"0xa399847134a9987c648deabc85a7310fbe854315cbeb6dc3a7efa1a4fa2a2c86",
		},
		{
			"TestSMT_InsertMultiple",
			map[utils.NodeKey]utils.NodeValue8{
				utils.ScalarToNodeKey(big.NewInt(1)): utils.ScalarToNodeValue8(big.NewInt(1)),
				utils.ScalarToNodeKey(big.NewInt(2)): utils.ScalarToNodeValue8(big.NewInt(2)),
				utils.ScalarToNodeKey(big.NewInt(3)): utils.ScalarToNodeValue8(big.NewInt(3)),
			},
			"0xb5a4b1b7a8c3a7c11becc339bbd7f639229cd14f14f76ee3a0e9170074399da4",
		},
		{
			"TestSMT_InsertMultiple2",
			map[utils.NodeKey]utils.NodeValue8{
				utils.ScalarToNodeKey(big.NewInt(18)): utils.ScalarToNodeValue8(big.NewInt(18)),
				utils.ScalarToNodeKey(big.NewInt(19)): utils.ScalarToNodeValue8(big.NewInt(19)),
			},
			"0xfa2d3062e11e44668ab79c595c0c916a82036a017408377419d74523569858ea",
		},
	}
	ctx := context.Background()
	for _, scenario := range testCases {
		t.Run(scenario.name, func(t *testing.T) {
			s := NewSMT(nil, false)
			keys := []utils.NodeKey{}
			for k, v := range scenario.kvMap {
				if !v.IsZero() {
					s.Db.InsertAccountValue(k, v)
					keys = append(keys, k)
				}
			}
			// set scenario old root if fail
			newRoot, err := s.GenerateFromKVBulk(ctx, "", keys)
			if err != nil {
				t.Errorf("Insert failed: %v", err)
			}

			hex := fmt.Sprintf("0x%0x", utils.ArrayToScalar(newRoot[:]))
			if hex != scenario.expectedRoot {
				t.Errorf("root hash is not as expected, got %v wanted %v", hex, scenario.expectedRoot)
			}
		})
	}
}

func TestSMT_Create_CompareWithRandomData(t *testing.T) {
	limit := 5000
	ctx := context.Background()

	kvMap := map[utils.NodeKey]utils.NodeValue8{}
	for i := 1; i <= limit; i++ {
		bigInt := big.NewInt(rand.Int63n(int64(i)))
		kvMap[utils.ScalarToNodeKey(bigInt)] = utils.ScalarToNodeValue8(bigInt)
	}

	//build and benchmark the tree the first way
	startTime := time.Now()
	s1 := NewSMT(nil, false)

	var root1 *big.Int
	for k, v := range kvMap {
		r, err := s1.insertSingle(k, v, [4]uint64{})
		if err != nil {
			t.Error(err)
			break
		}

		root1 = r.NewRootScalar.ToBigInt()
	}

	firstBuildTime := time.Since(startTime)
	s1 = nil

	//build the tree the from kvbulk
	startTime = time.Now()
	s2 := NewSMT(nil, false)
	// set scenario old root if fail
	keys := []utils.NodeKey{}
	for k, v := range kvMap {
		if !v.IsZero() {
			s2.Db.InsertAccountValue(k, v)
			keys = append(keys, k)
		}
	}
	// set scenario old root if fail
	root2, err := s2.GenerateFromKVBulk(ctx, "", keys)
	if err != nil {
		t.Errorf("Insert failed: %v", err)
	}
	secondBuildTime := time.Since(startTime)
	s2 = nil

	hexExpected := fmt.Sprintf("0x%0x", root1)
	hexResult := fmt.Sprintf("0x%0x", utils.ArrayToScalar(root2[:]))
	if hexExpected != hexResult {
		t.Errorf("root hash is not as expected, got %v wanted %v", hexExpected, hexResult)
	}

	fmt.Println("Number of values: ", limit)
	fmt.Println("First build time: ", firstBuildTime)
	fmt.Println("Second build time: ", secondBuildTime)
}

func TestSMT_Create_Benchmark(t *testing.T) {
	limit := 100000
	ctx := context.Background()

	kvMap := map[utils.NodeKey]utils.NodeValue8{}
	for i := 1; i <= limit; i++ {
		bigInt := big.NewInt(rand.Int63n(int64(i)))
		kvMap[utils.ScalarToNodeKey(bigInt)] = utils.ScalarToNodeValue8(bigInt)
	}

	//build and benchmark the tree the first way
	startTime := time.Now()
	//build the tree the from kvbulk
	s := NewSMT(nil, false)
	// set scenario old root if fail
	keys := []utils.NodeKey{}
	for k, v := range kvMap {
		if !v.IsZero() {
			s.Db.InsertAccountValue(k, v)
			keys = append(keys, k)
		}
	}

	_, err := s.GenerateFromKVBulk(ctx, "", keys)
	if err != nil {
		t.Errorf("Insert failed: %v", err)
	}
	secondBuildTime := time.Since(startTime)
	s = nil

	fmt.Println("Number of values: ", limit)
	fmt.Println("Build time: ", secondBuildTime)
}

func Test_findLastNode(t *testing.T) {
	leftTreeRoot := SmtNode{}
	currentNode := &leftTreeRoot
	for i := 0; i < 3; i++ {
		node := SmtNode{}
		currentNode.node0 = &node
		currentNode = &node
	}

	testCases := []struct {
		rootNode               *SmtNode
		keys                   []int
		expectedLevel          int
		expectedSiblingsCount  int
		expectedLastNodeIsleaf bool
	}{
		{
			rootNode:               &leftTreeRoot,
			keys:                   []int{1, 1, 0, 1},
			expectedLevel:          0,
			expectedSiblingsCount:  0,
			expectedLastNodeIsleaf: false,
		}, {
			rootNode:               &leftTreeRoot,
			keys:                   []int{0, 1, 0, 1},
			expectedLevel:          0,
			expectedSiblingsCount:  1,
			expectedLastNodeIsleaf: false,
		}, {
			rootNode:               &leftTreeRoot,
			keys:                   []int{0, 0, 0, 1},
			expectedLevel:          2,
			expectedSiblingsCount:  3,
			expectedLastNodeIsleaf: true,
		},
	}

	for i, testCase := range testCases {
		siblings, resultLevel := testCase.rootNode.findLastNode(testCase.keys)

		if resultLevel != testCase.expectedLevel {
			t.Errorf("testcase: %d, level mismatch. Expected: %d, Got: %d", i, testCase.expectedLevel, resultLevel)
		}

		if len(siblings) != testCase.expectedSiblingsCount {
			t.Errorf("testcase: %d, expected num of siblings mismatch. Expected: %d, Got: %d, Siblings: %v", i, testCase.expectedSiblingsCount, len(siblings), siblings)
		}

		if testCase.expectedSiblingsCount != 0 {
			if siblings[len(siblings)-1].isLeaf() != testCase.expectedLastNodeIsleaf {
				t.Errorf("testcase: %d, last sibling type mismatch. Expected to be leaf: %v, Got: %v", i, testCase.expectedLastNodeIsleaf, siblings[resultLevel].isLeaf())
			}
		}
	}
}
