/*
   Copyright 2022 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package bptree

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertTwoThreeTree(t *testing.T, tree *Tree23, expectedKeysLevelOrder []Felt) {
	t.Helper()
	treeValid, err := tree.IsValid()
	assert.True(t, treeValid, "2-3-tree properties do not hold for tree: %v, error: %v", tree.KeysInLevelOrder(), err)
	if expectedKeysLevelOrder != nil {
		assert.Equal(t, expectedKeysLevelOrder, tree.KeysInLevelOrder(), "different keys by level")
	}
}

func require23Tree(t *testing.T, tree *Tree23, expectedKeysLevelOrder []Felt, input1, input2 []byte) {
	t.Helper()
	treeValid, err := tree.IsValid()
	require.True(t, treeValid, "2-3-tree properties do not hold: input [%v %v] [%+q %+q], error: %v",
		input1, input2, string(input1), string(input2), err)
	if expectedKeysLevelOrder != nil {
		assert.Equal(t, expectedKeysLevelOrder, tree.KeysInLevelOrder(), "different keys by level")
	}
}

type HeightTest struct {
	initialItems   KeyValues
	expectedHeight int
}

type IsTree23Test struct {
	initialItems           KeyValues
	expectedKeysLevelOrder []Felt
}

type RootHashTest struct {
	expectedHash string
	initialItems KeyValues
}

type UpsertTest struct {
	initialItems          KeyValues
	initialKeysLevelOrder []Felt
	deltaItems            KeyValues
	finalKeysLevelOrder   []Felt
}

type DeleteTest struct {
	initialItems          KeyValues
	initialKeysLevelOrder []Felt
	keysToDelete          []Felt
	finalKeysLevelOrder   []Felt
}

func K(keys []Felt) KeyValues {
	values := make([]Felt, len(keys))
	copy(values, keys)
	return KV(keys, values)
}

var heightTestTable = []HeightTest{
	{K([]Felt{}), 0},
	{K([]Felt{1}), 1},
	{K([]Felt{1, 2}), 1},
	{K([]Felt{1, 2, 3}), 2},
	{K([]Felt{1, 2, 3, 4}), 2},
	{K([]Felt{1, 2, 3, 4, 5}), 2},
	{K([]Felt{1, 2, 3, 4, 5, 6}), 2},
	{K([]Felt{1, 2, 3, 4, 5, 6, 7}), 3},
	{K([]Felt{1, 2, 3, 4, 5, 6, 7, 8}), 3},
}

var isTree23TestTable = []IsTree23Test{
	{K([]Felt{}), []Felt{}},
	{K([]Felt{1}), []Felt{1}},
	{K([]Felt{1, 2}), []Felt{1, 2}},
	{K([]Felt{1, 2, 3}), []Felt{3, 1, 2, 3}},
	{K([]Felt{1, 2, 3, 4}), []Felt{3, 1, 2, 3, 4}},
	{K([]Felt{1, 2, 3, 4, 5}), []Felt{3, 5, 1, 2, 3, 4, 5}},
	{K([]Felt{1, 2, 3, 4, 5, 6}), []Felt{3, 5, 1, 2, 3, 4, 5, 6}},
	{K([]Felt{1, 2, 3, 4, 5, 6, 7}), []Felt{5, 3, 7, 1, 2, 3, 4, 5, 6, 7}},
	{K([]Felt{1, 2, 3, 4, 5, 6, 7, 8}), []Felt{5, 3, 7, 1, 2, 3, 4, 5, 6, 7, 8}},
	{K([]Felt{1, 2, 3, 4, 5, 6, 7, 8, 9}), []Felt{5, 3, 7, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
	{K([]Felt{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}), []Felt{5, 3, 7, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
	{K([]Felt{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}), []Felt{5, 9, 3, 7, 11, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}},
	{K([]Felt{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}), []Felt{5, 9, 3, 7, 11, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
	{K([]Felt{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}), []Felt{9, 5, 13, 3, 7, 11, 15, 17, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17}},
	{K([]Felt{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}), []Felt{9, 5, 13, 3, 7, 11, 15, 17, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18}},
}

var rootHashTestTable = []RootHashTest{
	{"", K([]Felt{})},
	{"532deabf88729cb43995ab5a9cd49bf9b90a079904dc0645ecda9e47ce7345a9", K([]Felt{1})},
	{"d3782c59c224da5b6344108ef3431ba4e01d2c30b6570137a91b8b383908c361", K([]Felt{1, 2})},
}

var insertTestTable = []UpsertTest{
	{K([]Felt{}), []Felt{}, K([]Felt{1}), []Felt{1}},
	{K([]Felt{}), []Felt{}, K([]Felt{1, 2}), []Felt{1, 2}},
	{K([]Felt{}), []Felt{}, K([]Felt{1, 2, 3}), []Felt{3, 1, 2, 3}},
	{K([]Felt{}), []Felt{}, K([]Felt{1, 2, 3, 4}), []Felt{3, 1, 2, 3, 4}},

	{K([]Felt{1}), []Felt{1}, K([]Felt{0}), []Felt{0, 1}},
	{K([]Felt{1}), []Felt{1}, K([]Felt{2}), []Felt{1, 2}},
	{K([]Felt{1}), []Felt{1}, K([]Felt{0, 2}), []Felt{2, 0, 1, 2}},
	{K([]Felt{1}), []Felt{1}, K([]Felt{0, 2, 3}), []Felt{2, 0, 1, 2, 3}},
	{K([]Felt{1}), []Felt{1}, K([]Felt{0, 2, 3, 4}), []Felt{2, 4, 0, 1, 2, 3, 4}},
	{K([]Felt{2}), []Felt{2}, K([]Felt{0, 1, 3, 4}), []Felt{2, 4, 0, 1, 2, 3, 4}},
	{K([]Felt{3}), []Felt{3}, K([]Felt{0, 1, 2, 4}), []Felt{2, 4, 0, 1, 2, 3, 4}},
	{K([]Felt{4}), []Felt{4}, K([]Felt{0, 1, 2, 3}), []Felt{2, 4, 0, 1, 2, 3, 4}},

	{K([]Felt{1, 2}), []Felt{1, 2}, K([]Felt{0}), []Felt{2, 0, 1, 2}},
	{K([]Felt{1, 2}), []Felt{1, 2}, K([]Felt{0, 3}), []Felt{2, 0, 1, 2, 3}},
	{K([]Felt{1, 2}), []Felt{1, 2}, K([]Felt{0, 3, 4}), []Felt{2, 4, 0, 1, 2, 3, 4}},
	{K([]Felt{1, 2}), []Felt{1, 2}, K([]Felt{0, 3, 4, 5}), []Felt{2, 4, 0, 1, 2, 3, 4, 5}},
	{K([]Felt{2, 3}), []Felt{2, 3}, K([]Felt{0}), []Felt{3, 0, 2, 3}},
	{K([]Felt{2, 3}), []Felt{2, 3}, K([]Felt{0, 1}), []Felt{2, 0, 1, 2, 3}},
	{K([]Felt{2, 3}), []Felt{2, 3}, K([]Felt{5}), []Felt{5, 2, 3, 5}},
	{K([]Felt{2, 3}), []Felt{2, 3}, K([]Felt{4, 5}), []Felt{4, 2, 3, 4, 5}},
	{K([]Felt{2, 3}), []Felt{2, 3}, K([]Felt{0, 4, 5}), []Felt{3, 5, 0, 2, 3, 4, 5}},
	{K([]Felt{2, 3}), []Felt{2, 3}, K([]Felt{0, 1, 4, 5}), []Felt{2, 4, 0, 1, 2, 3, 4, 5}},
	{K([]Felt{4, 5}), []Felt{4, 5}, K([]Felt{0}), []Felt{5, 0, 4, 5}},
	{K([]Felt{4, 5}), []Felt{4, 5}, K([]Felt{0, 1}), []Felt{4, 0, 1, 4, 5}},
	{K([]Felt{4, 5}), []Felt{4, 5}, K([]Felt{0, 1, 2}), []Felt{2, 5, 0, 1, 2, 4, 5}},
	{K([]Felt{4, 5}), []Felt{4, 5}, K([]Felt{0, 1, 2, 3}), []Felt{2, 4, 0, 1, 2, 3, 4, 5}},
	{K([]Felt{1, 4}), []Felt{1, 4}, K([]Felt{0}), []Felt{4, 0, 1, 4}},
	{K([]Felt{1, 4}), []Felt{1, 4}, K([]Felt{0, 2}), []Felt{2, 0, 1, 2, 4}},
	{K([]Felt{1, 4}), []Felt{1, 4}, K([]Felt{0, 2, 5}), []Felt{2, 5, 0, 1, 2, 4, 5}},
	{K([]Felt{1, 4}), []Felt{1, 4}, K([]Felt{0, 2, 3, 5}), []Felt{2, 4, 0, 1, 2, 3, 4, 5}},

	{K([]Felt{1, 3, 5}), []Felt{5, 1, 3, 5}, K([]Felt{0}), []Felt{3, 5, 0, 1, 3, 5}},
	{K([]Felt{1, 3, 5}), []Felt{5, 1, 3, 5}, K([]Felt{0, 2, 4}), []Felt{4, 2, 5, 0, 1, 2, 3, 4, 5}},
	{K([]Felt{1, 3, 5}), []Felt{5, 1, 3, 5}, K([]Felt{6, 7, 8}), []Felt{5, 7, 1, 3, 5, 6, 7, 8}},
	{K([]Felt{1, 3, 5}), []Felt{5, 1, 3, 5}, K([]Felt{6, 7, 8, 9}), []Felt{7, 5, 9, 1, 3, 5, 6, 7, 8, 9}},

	{K([]Felt{1, 2, 3, 4}), []Felt{3, 1, 2, 3, 4}, K([]Felt{0}), []Felt{2, 3, 0, 1, 2, 3, 4}},
	{K([]Felt{1, 3, 5, 7}), []Felt{5, 1, 3, 5, 7}, K([]Felt{0}), []Felt{3, 5, 0, 1, 3, 5, 7}},

	{K([]Felt{1, 3, 5, 7, 9}), []Felt{5, 9, 1, 3, 5, 7, 9}, K([]Felt{0}), []Felt{5, 3, 9, 0, 1, 3, 5, 7, 9}},

	// Debug
	{K([]Felt{1, 2, 3, 5, 6, 7, 8}), []Felt{6, 3, 8, 1, 2, 3, 5, 6, 7, 8}, K([]Felt{4}), []Felt{6, 3, 5, 8, 1, 2, 3, 4, 5, 6, 7, 8}},

	{
		K([]Felt{10, 15, 20}),
		[]Felt{20, 10, 15, 20},
		K([]Felt{1, 2, 3, 4, 5, 11, 13, 18, 19, 30, 31}),
		[]Felt{15, 5, 20, 3, 11, 19, 31, 1, 2, 3, 4, 5, 10, 11, 13, 15, 18, 19, 20, 30, 31},
	},

	{
		K([]Felt{0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20}),
		[]Felt{8, 16, 4, 12, 20, 0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20},
		K([]Felt{1, 3, 5}),
		[]Felt{8, 4, 16, 2, 6, 12, 20, 0, 1, 2, 3, 4, 5, 6, 8, 10, 12, 14, 16, 18, 20},
	},

	{
		K([]Felt{4, 10, 17, 85, 104, 107, 112, 115, 136, 156, 191}),
		[]Felt{104, 136, 17, 112, 191, 4, 10, 17, 85, 104, 107, 112, 115, 136, 156, 191},
		K([]Felt{0, 96, 120, 129, 133, 164, 187, 189}),
		nil,
	},
}

var updateTestTable = []UpsertTest{
	{K([]Felt{10}), []Felt{10}, KV([]Felt{10}, []Felt{100}), []Felt{10}},
	{K([]Felt{10, 20}), []Felt{10, 20}, KV([]Felt{10, 20}, []Felt{100, 200}), []Felt{10, 20}},
}

var deleteTestTable = []DeleteTest{
	/// POSITIVE TEST CASES
	{K([]Felt{}), []Felt{}, []Felt{}, []Felt{}},

	{K([]Felt{1}), []Felt{1}, []Felt{}, []Felt{1}},
	{K([]Felt{1}), []Felt{1}, []Felt{1}, []Felt{}},

	{K([]Felt{1, 2}), []Felt{1, 2}, []Felt{}, []Felt{1, 2}},
	{K([]Felt{1, 2}), []Felt{1, 2}, []Felt{1}, []Felt{2}},
	{K([]Felt{1, 2}), []Felt{1, 2}, []Felt{2}, []Felt{1}},
	{K([]Felt{1, 2}), []Felt{1, 2}, []Felt{1, 2}, []Felt{}},

	{K([]Felt{1, 2, 3}), []Felt{3, 1, 2, 3}, []Felt{}, []Felt{3, 1, 2, 3}},
	{K([]Felt{1, 2, 3}), []Felt{3, 1, 2, 3}, []Felt{1}, []Felt{2, 3}},
	{K([]Felt{1, 2, 3}), []Felt{3, 1, 2, 3}, []Felt{2}, []Felt{1, 3}},
	{K([]Felt{1, 2, 3}), []Felt{3, 1, 2, 3}, []Felt{3}, []Felt{1, 2}},
	{K([]Felt{1, 2, 3}), []Felt{3, 1, 2, 3}, []Felt{1, 2}, []Felt{3}},
	{K([]Felt{1, 2, 3}), []Felt{3, 1, 2, 3}, []Felt{1, 3}, []Felt{2}},
	{K([]Felt{1, 2, 3}), []Felt{3, 1, 2, 3}, []Felt{2, 3}, []Felt{1}},
	{K([]Felt{1, 2, 3}), []Felt{3, 1, 2, 3}, []Felt{1, 2, 3}, []Felt{}},

	{K([]Felt{1, 2, 3, 4}), []Felt{3, 1, 2, 3, 4}, []Felt{1}, []Felt{3, 2, 3, 4}},
	{K([]Felt{1, 2, 3, 4}), []Felt{3, 1, 2, 3, 4}, []Felt{2}, []Felt{3, 1, 3, 4}},
	{K([]Felt{1, 2, 3, 4}), []Felt{3, 1, 2, 3, 4}, []Felt{3}, []Felt{4, 1, 2, 4}},
	{K([]Felt{1, 2, 3, 4}), []Felt{3, 1, 2, 3, 4}, []Felt{4}, []Felt{3, 1, 2, 3}},

	{K([]Felt{1, 2, 3, 4, 5}), []Felt{3, 5, 1, 2, 3, 4, 5}, []Felt{1}, []Felt{3, 5, 2, 3, 4, 5}},
	{K([]Felt{1, 2, 3, 4, 5}), []Felt{3, 5, 1, 2, 3, 4, 5}, []Felt{2}, []Felt{3, 5, 1, 3, 4, 5}},
	{K([]Felt{1, 2, 3, 4, 5}), []Felt{3, 5, 1, 2, 3, 4, 5}, []Felt{3}, []Felt{4, 5, 1, 2, 4, 5}},
	{K([]Felt{1, 2, 3, 4, 5}), []Felt{3, 5, 1, 2, 3, 4, 5}, []Felt{4}, []Felt{3, 5, 1, 2, 3, 5}},
	{K([]Felt{1, 2, 3, 4, 5}), []Felt{3, 5, 1, 2, 3, 4, 5}, []Felt{5}, []Felt{3, 1, 2, 3, 4}},
	{K([]Felt{1, 2, 3, 4, 5, 6, 7}), []Felt{5, 3, 7, 1, 2, 3, 4, 5, 6, 7}, []Felt{7}, []Felt{3, 5, 1, 2, 3, 4, 5, 6}},

	{K([]Felt{16, 25, 155, 182, 184, 210, 215}), []Felt{184, 155, 215, 16, 25, 155, 182, 184, 210, 215}, []Felt{155, 182}, []Felt{184, 215, 16, 25, 184, 210, 215}},

	/// NEGATIVE TEST CASES
	{K([]Felt{}), []Felt{}, []Felt{1}, []Felt{}},
	{K([]Felt{1}), []Felt{1}, []Felt{2}, []Felt{1}},
	{K([]Felt{1, 2}), []Felt{1, 2}, []Felt{3}, []Felt{1, 2}},
	{K([]Felt{1, 2, 3}), []Felt{3, 1, 2, 3}, []Felt{4}, []Felt{3, 1, 2, 3}},
	{K([]Felt{1, 2, 3, 4}), []Felt{3, 1, 2, 3, 4}, []Felt{5}, []Felt{3, 1, 2, 3, 4}},
	{K([]Felt{1, 2, 3, 4, 5}), []Felt{3, 5, 1, 2, 3, 4, 5}, []Felt{6}, []Felt{3, 5, 1, 2, 3, 4, 5}},

	/// MIXED TEST CASES
	{K([]Felt{0, 46, 50, 89, 134, 218}), []Felt{50, 134, 0, 46, 50, 89, 134, 218}, []Felt{46, 50, 89, 134, 218}, []Felt{0}},
}

func TestHeight(t *testing.T) {
	for _, data := range heightTestTable {
		tree := NewTree23(data.initialItems)
		assert.Equal(t, data.expectedHeight, tree.Height(), "different height")
	}
}

func TestIs23Tree(t *testing.T) {
	for _, data := range isTree23TestTable {
		tree := NewTree23(data.initialItems)
		//tree.GraphAndPicture("is23Tree")
		assertTwoThreeTree(t, tree, data.expectedKeysLevelOrder)
	}
}

func Test23TreeSeries(t *testing.T) {
	maxNumberOfNodes := 100
	for i := 0; i < maxNumberOfNodes; i++ {
		kvPairs := KeyValues{make([]*Felt, 0), make([]*Felt, 0)}
		for j := 0; j < i; j++ {
			key, value := Felt(j), Felt(j)
			kvPairs.keys = append(kvPairs.keys, &key)
			kvPairs.values = append(kvPairs.values, &value)
		}
		tree := NewTree23(kvPairs)
		assertTwoThreeTree(t, tree, nil)
	}
}

func TestRootHash(t *testing.T) {
	for _, data := range rootHashTestTable {
		tree := NewTree23(data.initialItems)
		assert.Equal(t, data.expectedHash, hex.EncodeToString(tree.RootHash()), "different root hash")
	}
}

func TestUpsertInsert(t *testing.T) {
	for _, data := range insertTestTable {
		tree := NewTree23(data.initialItems)
		assertTwoThreeTree(t, tree, data.initialKeysLevelOrder)
		//tree.GraphAndPicture("tree_step1")
		tree.Upsert(data.deltaItems)
		//tree.GraphAndPicture("tree_step2")
		assertTwoThreeTree(t, tree, data.finalKeysLevelOrder)
	}
}

func TestUpsertUpdate(t *testing.T) {
	for _, data := range updateTestTable {
		tree := NewTree23(data.initialItems)
		assertTwoThreeTree(t, tree, data.initialKeysLevelOrder)
		// TODO: add check for old values
		tree.Upsert(data.deltaItems)
		assertTwoThreeTree(t, tree, data.finalKeysLevelOrder)
		// TODO: add check for new values
	}
}

func TestUpsertIdempotent(t *testing.T) {
	for _, data := range isTree23TestTable {
		tree := NewTree23(data.initialItems)
		assertTwoThreeTree(t, tree, data.expectedKeysLevelOrder)
		tree.Upsert(data.initialItems)
		assertTwoThreeTree(t, tree, data.expectedKeysLevelOrder)
	}
}

func TestUpsertNextKey(t *testing.T) {
	dataCount := 4
	data := KeyValues{make([]*Felt, dataCount), make([]*Felt, dataCount)}
	for i := 0; i < dataCount; i++ {
		key, value := Felt(i*2), Felt(i*2)
		data.keys[i], data.values[i] = &key, &value
	}
	tn := NewTree23(data)
	//tn.GraphAndPicture("tn1")

	for i := 0; i < dataCount; i++ {
		key, value := Felt(i*2+1), Felt(i*2+1)
		data.keys[i], data.values[i] = &key, &value
	}
	tn = tn.Upsert(data)
	//tn.GraphAndPicture("tn2")
	assertTwoThreeTree(t, tn, []Felt{4, 2, 6, 0, 1, 2, 3, 4, 5, 6, 7})

	data = K([]Felt{100, 101, 200, 201, 202})
	tn = tn.Upsert(data)
	//tn.GraphAndPicture("tn3")
	assertTwoThreeTree(t, tn, []Felt{4, 100, 2, 6, 200, 202, 0, 1, 2, 3, 4, 5, 6, 7, 100, 101, 200, 201, 202})

	data = K([]Felt{10, 150, 250, 251, 252})
	tn = tn.Upsert(data)
	//tn.GraphAndPicture("tn4")
	assertTwoThreeTree(t, tn, []Felt{100, 4, 200, 2, 6, 10, 150, 202, 251, 0, 1, 2, 3, 4, 5, 6, 7, 10, 100, 101, 150, 200, 201, 202, 250, 251, 252})
}

func TestUpsertFirstKey(t *testing.T) {
}

func TestDelete(t *testing.T) {
	for _, data := range deleteTestTable {
		tree := NewTree23(data.initialItems)
		assertTwoThreeTree(t, tree, data.initialKeysLevelOrder)
		//tree.GraphAndPicture("tree_delete1")
		tree.Delete(data.keysToDelete)
		//tree.GraphAndPicture("tree_delete2")
		assertTwoThreeTree(t, tree, data.finalKeysLevelOrder)
	}
}

func FuzzUpsert(f *testing.F) {
	f.Fuzz(func(t *testing.T, input1, input2 []byte) {
		//t.Parallel()
		keyFactory := NewKeyBinaryFactory(1)
		bytesReader1 := bytes.NewReader(input1)
		kvStatePairs := keyFactory.NewUniqueKeyValues(bufio.NewReader(bytesReader1))
		require.True(t, sort.IsSorted(kvStatePairs), "kvStatePairs is not sorted")
		bytesReader2 := bytes.NewReader(input2)
		kvStateChangesPairs := keyFactory.NewUniqueKeyValues(bufio.NewReader(bytesReader2))
		//fmt.Printf("kvStatePairs=%v kvStateChangesPairs=%v\n", kvStatePairs, kvStateChangesPairs)
		require.True(t, sort.IsSorted(kvStateChangesPairs), "kvStateChangesPairs is not sorted")
		tree := NewTree23(kvStatePairs)
		//tree.GraphAndPicture("fuzz_tree_upsert1")
		assertTwoThreeTree(t, tree, nil)
		tree = tree.Upsert(kvStateChangesPairs)
		//tree.GraphAndPicture("fuzz_tree_upsert2")
		assertTwoThreeTree(t, tree, nil)
	})
}

func FuzzDelete(f *testing.F) {
	f.Fuzz(func(t *testing.T, input1, input2 []byte) {
		//t.Parallel()
		//fmt.Printf("input1=%v input2=%v\n", input1, input2)
		keyFactory := NewKeyBinaryFactory(1)
		bytesReader1 := bytes.NewReader(input1)
		kvStatePairs := keyFactory.NewUniqueKeyValues(bufio.NewReader(bytesReader1))
		require.True(t, sort.IsSorted(kvStatePairs), "kvStatePairs is not sorted")
		bytesReader2 := bytes.NewReader(input2)
		keysToDelete := keyFactory.NewUniqueKeys(bufio.NewReader(bytesReader2))
		//fmt.Printf("kvStatePairs=%v keysToDelete=%v\n", kvStatePairs, keysToDelete)
		require.True(t, sort.IsSorted(keysToDelete), "keysToDelete is not sorted")
		tree1 := NewTree23(kvStatePairs)
		//tree1.GraphAndPicture("fuzz_tree_delete1")
		require23Tree(t, tree1, nil, input1, input2)
		tree2 := tree1.Delete(keysToDelete)
		//tree2.GraphAndPicture("fuzz_tree_delete2")
		require23Tree(t, tree2, nil, input1, input2)
		// TODO: check the difference properties
		// Check that *each* T1 node is present either in Td or in T2
		// Check that *each* T2 node is not present in Td
		// Check that *each* Td node is present in T1 but not in T2
	})
}

func BenchmarkNewTree23(b *testing.B) {
	const dataCount = 1_000_000
	data := KeyValues{make([]*Felt, dataCount), make([]*Felt, dataCount)}
	for i := 0; i < dataCount; i++ {
		key, value := Felt(i*2), Felt(i*2)
		data.keys[i], data.values[i] = &key, &value
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		NewTree23(data)
	}
}

func BenchmarkUpsert(b *testing.B) {
	dataCount := 5_000_000
	data := KeyValues{make([]*Felt, dataCount), make([]*Felt, dataCount)}
	for i := 0; i < dataCount; i++ {
		key, value := Felt(i*2), Felt(i*2)
		data.keys[i], data.values[i] = &key, &value
	}
	tree := NewTree23(data)
	dataCount = 500_000
	data = KeyValues{make([]*Felt, dataCount), make([]*Felt, dataCount)}
	for i := 0; i < dataCount; i++ {
		key, value := Felt(i*2+1), Felt(i*2+1)
		data.keys[i], data.values[i] = &key, &value
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Upsert(data)
	}
}
