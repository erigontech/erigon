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
	"testing"

	"github.com/stretchr/testify/assert"
)

func assertNodeEqual(t *testing.T, expected, actual *Node23) {
	t.Helper()
	assert.Equal(t, expected.keysInLevelOrder(), actual.keysInLevelOrder(), "different keys by level")
}

type MergeTest struct {
	left  *Node23
	right *Node23
	final *Node23
}

func KV(keys []Felt, values []Felt) KeyValues {
	keyPointers := make([]*Felt, len(keys))
	valuePointers := make([]*Felt, len(values))
	for i := 0; i < len(keyPointers); i++ {
		keyPointers[i] = &keys[i]
		valuePointers[i] = &values[i]
	}
	return KeyValues{keyPointers, valuePointers}
}

func K2K(keys []Felt) []*Felt {
	kv := KV(keys, keys)
	return kv.keys
}

func K2KV(keys []Felt) ([]*Felt, []*Felt) {
	values := make([]Felt, len(keys))
	copy(values, keys)
	kv := KV(keys, values)
	return kv.keys, kv.values
}

func newInternalNode(children []*Node23, keys []*Felt) *Node23 {
	return makeInternalNode(children, keys, &Stats{})
}

func newLeafNode(keys, values []*Felt) *Node23 {
	return makeLeafNode(keys, values, &Stats{})
}

var mergeLeft2RightTestTable = []MergeTest{
	{
		newInternalNode([]*Node23{
			newLeafNode(K2KV([]Felt{12, 127})),
		}, K2K([]Felt{127})),
		newInternalNode([]*Node23{
			newLeafNode(K2KV([]Felt{127, 128})),
			newLeafNode(K2KV([]Felt{128, 135, 173})),
		}, K2K([]Felt{128})),
		newInternalNode([]*Node23{
			newLeafNode(K2KV([]Felt{12, 127})),
			newLeafNode(K2KV([]Felt{127, 128})),
			newLeafNode(K2KV([]Felt{128, 135, 173})),
		}, K2K([]Felt{127, 128})),
	},
	{
		newInternalNode([]*Node23{
			newInternalNode([]*Node23{
				newLeafNode(K2KV([]Felt{12, 127})),
			}, K2K([]Felt{127})),
		}, K2K([]Felt{44})),
		newInternalNode([]*Node23{
			newInternalNode([]*Node23{
				newLeafNode(K2KV([]Felt{127, 128})),
				newLeafNode(K2KV([]Felt{128, 135, 173})),
			}, K2K([]Felt{128})),
			newInternalNode([]*Node23{
				newLeafNode(K2KV([]Felt{173, 237})),
				newLeafNode(K2KV([]Felt{237, 1000})),
			}, K2K([]Felt{237})),
		}, K2K([]Felt{173})),
		newInternalNode([]*Node23{
			newInternalNode([]*Node23{
				newLeafNode(K2KV([]Felt{12, 127})),
				newLeafNode(K2KV([]Felt{127, 128})),
				newLeafNode(K2KV([]Felt{128, 135, 173})),
			}, K2K([]Felt{127, 128})),
			newInternalNode([]*Node23{
				newLeafNode(K2KV([]Felt{173, 237})),
				newLeafNode(K2KV([]Felt{237, 1000})),
			}, K2K([]Felt{237})),
		}, K2K([]Felt{173})),
	},
}

var mergeRight2LeftTestTable = []MergeTest{
	{
		newInternalNode([]*Node23{
			newLeafNode(K2KV([]Felt{127, 128})),
			newLeafNode(K2KV([]Felt{128, 135, 173})),
		}, K2K([]Felt{128})),
		newInternalNode([]*Node23{
			newLeafNode(K2KV([]Felt{173, 190})),
		}, K2K([]Felt{190})),
		newInternalNode([]*Node23{
			newLeafNode(K2KV([]Felt{127, 128})),
			newLeafNode(K2KV([]Felt{128, 135, 173})),
			newLeafNode(K2KV([]Felt{173, 190})),
		}, K2K([]Felt{128, 173})),
	},
	{
		newInternalNode([]*Node23{
			newInternalNode([]*Node23{
				newLeafNode(K2KV([]Felt{127, 128})),
				newLeafNode(K2KV([]Felt{128, 135, 173})),
			}, K2K([]Felt{128})),
			newInternalNode([]*Node23{
				newLeafNode(K2KV([]Felt{173, 237})),
				newLeafNode(K2KV([]Felt{237, 1000})),
			}, K2K([]Felt{237})),
		}, K2K([]Felt{173})),
		newInternalNode([]*Node23{
			newInternalNode([]*Node23{
				newLeafNode(K2KV([]Felt{1000, 1002})),
			}, K2K([]Felt{1002})),
		}, K2K([]Felt{1100})),
		newInternalNode([]*Node23{
			newInternalNode([]*Node23{
				newLeafNode(K2KV([]Felt{127, 128})),
				newLeafNode(K2KV([]Felt{128, 135, 173})),
			}, K2K([]Felt{128})),
			newInternalNode([]*Node23{
				newLeafNode(K2KV([]Felt{173, 237})),
				newLeafNode(K2KV([]Felt{237, 1000})),
				newLeafNode(K2KV([]Felt{1000, 1002})),
			}, K2K([]Felt{237, 1000})),
		}, K2K([]Felt{173})),
	},
}

func TestMergeLeft2Right(t *testing.T) {
	for _, data := range mergeLeft2RightTestTable {
		_, merged := mergeLeft2Right(data.left, data.right, &Stats{})
		assertNodeEqual(t, data.final, merged)
	}
}

func TestMergeRight2Left(t *testing.T) {
	for _, data := range mergeRight2LeftTestTable {
		merged, _ := mergeRight2Left(data.left, data.right, &Stats{})
		assertNodeEqual(t, data.final, merged)
	}
}
