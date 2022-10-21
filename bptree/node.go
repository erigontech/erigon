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
	"fmt"
	"strings"
	"unsafe"
)

type Keys []Felt

func (keys Keys) Len() int { return len(keys) }

func (keys Keys) Less(i, j int) bool { return keys[i] < keys[j] }

func (keys Keys) Swap(i, j int) { keys[i], keys[j] = keys[j], keys[i] }

func (keys Keys) Contains(key Felt) bool {
	for _, k := range keys {
		if k == key {
			return true
		}
	}
	return false
}

func (keys Keys) String() string {
	b := strings.Builder{}
	for i, k := range keys {
		fmt.Fprintf(&b, "%v", k)
		if i != len(keys)-1 {
			fmt.Fprintf(&b, " ")
		}
	}
	return b.String()
}

type KeyValues struct {
	keys   []*Felt
	values []*Felt
}

func (kv KeyValues) Len() int { return len(kv.keys) }

func (kv KeyValues) Less(i, j int) bool { return *kv.keys[i] < *kv.keys[j] }

func (kv KeyValues) Swap(i, j int) {
	kv.keys[i], kv.keys[j] = kv.keys[j], kv.keys[i]
	kv.values[i], kv.values[j] = kv.values[j], kv.values[i]
}

func (kv KeyValues) String() string {
	b := strings.Builder{}
	for i, k := range kv.keys {
		v := kv.values[i]
		fmt.Fprintf(&b, "{%v, %v}", *k, *v)
		if i != len(kv.keys)-1 {
			fmt.Fprintf(&b, " ")
		}
	}
	return b.String()
}

type Node23 struct {
	children []*Node23
	keys     []*Felt
	values   []*Felt
	isLeaf   bool
	exposed  bool
	updated  bool
}

func (n *Node23) String() string {
	s := fmt.Sprintf("{%p isLeaf=%t keys=%v-%v children=[", n, n.isLeaf, deref(n.keys), n.keys)
	for i, child := range n.children {
		s += fmt.Sprintf("%p", child)
		if i != len(n.children)-1 {
			s += " "
		}
	}
	s += "]}"
	return s
}

func makeInternalNode(children []*Node23, keys []*Felt, stats *Stats) *Node23 {
	stats.CreatedCount++
	n := &Node23{isLeaf: false, children: children, keys: keys, values: make([]*Felt, 0), exposed: true, updated: true}
	return n
}

func makeLeafNode(keys, values []*Felt, stats *Stats) *Node23 {
	ensure(len(keys) > 0, "number of keys is zero")
	ensure(len(keys) == len(values), "keys and values have different cardinality")
	stats.CreatedCount++
	n := &Node23{isLeaf: true, children: make([]*Node23, 0), keys: keys, values: values, exposed: true, updated: true}
	return n
}

func makeEmptyLeafNode() *Node23 {
	// At least nil next key is always present
	return makeLeafNode(make([]*Felt, 1), make([]*Felt, 1), &Stats{}) // do not count it into stats
}

func promote(nodes []*Node23, intermediateKeys []*Felt, stats *Stats) *Node23 {
	if len(nodes) > 3 {
		promotedNodes := make([]*Node23, 0)
		promotedKeys := make([]*Felt, 0)
		for len(nodes) > 3 {
			promotedNodes = append(promotedNodes, makeInternalNode(nodes[:2], intermediateKeys[:1], stats))
			nodes = nodes[2:]
			promotedKeys = append(promotedKeys, intermediateKeys[1])
			intermediateKeys = intermediateKeys[2:]
		}
		promotedNodes = append(promotedNodes, makeInternalNode(nodes, intermediateKeys, stats))
		return promote(promotedNodes, promotedKeys, stats)
	}
	promotedRoot := makeInternalNode(nodes, intermediateKeys, stats)
	return promotedRoot
}

func (n *Node23) reset() {
	n.exposed = false
	n.updated = false
	if !n.isLeaf {
		for _, child := range n.children {
			child.reset()
		}
	}
}

func (n *Node23) isValid() (bool, error) {
	ensure(n.exposed || !n.updated, "isValid: node is not exposed but updated")
	if n.isLeaf {
		return n.isValidLeaf()
	}
	return n.isValidInternal()
}

func (n *Node23) isValidLeaf() (bool, error) {
	ensure(n.isLeaf, "isValidLeaf: node is not leaf")

	/* Any leaf node shall have no children */
	if n.childrenCount() != 0 {
		return false, fmt.Errorf("invalid %d children in %v", n.childrenCount(), n)
	}
	/* Any leaf node can have either 1 or 2 keys (plus next key) */
	return n.keyCount() == 1+1 || n.keyCount() == 2+1, fmt.Errorf("invalid %d keys in %v", n.keyCount(), n)
}

func (n *Node23) isValidInternal() (bool, error) {
	ensure(!n.isLeaf, "isValidInternal: node is leaf")

	/* Any internal node can have either 1 keys and 2 children or 2 keys and 3 children */
	if n.keyCount() != 1 && n.keyCount() != 2 {
		return false, fmt.Errorf("invalid %d keys in %v", n.keyCount(), n)
	}
	if n.keyCount() == 1 && n.childrenCount() != 2 {
		return false, fmt.Errorf("invalid %d keys %d children in %v", n.keyCount(), n.childrenCount(), n)
	}
	if n.keyCount() == 2 && n.childrenCount() != 3 {
		return false, fmt.Errorf("invalid %d children in %v", n.keyCount(), n)
	}
	subtree := n.walkNodesPostOrder()
	// Check that each internal node has unique keys corresponding to leaf next keys
	for _, key := range n.keys {
		hasNextKey := false
		for _, node := range subtree {
			if !node.isLeaf {
				if node != n && node.hasKey(key) {
					return false, fmt.Errorf("internal key %d not unique", *key)
				}
				continue
			}
			leafNextKey := node.nextKey()
			if leafNextKey != nil && *key == *leafNextKey {
				hasNextKey = true
			}
		}
		if !hasNextKey {
			return false, fmt.Errorf("internal key %d not present in next keys", *key)
		}
	}
	// Check that leaves in subtree are chained together (next key -> first key)
	for i, node := range subtree {
		if !node.isLeaf {
			// Post-order walk => previous and next nodes are contiguous leaves except last
			if i == len(subtree)-1 {
				continue
			}
			previous, next := subtree[i], subtree[i+1]
			if previous.isLeaf && next.isLeaf {
				// Previous node's next key must be equal to next node's first key
				if previous.nextKey() != next.firstKey() {
					return false, fmt.Errorf("nodes %v and %v not chained by next key", previous, next)
				}
			}
			continue
		}
	}
	for i := len(n.children) - 1; i >= 0; i-- {
		child := n.children[i]
		// Check that each child subtree is a 2-3 tree
		childValid, err := child.isValid()
		if !childValid {
			return false, fmt.Errorf("invalid child %v in %v, error: %w", child, n, err)
		}
	}
	return true, nil
}

func (n *Node23) keyCount() int {
	return len(n.keys)
}

func (n *Node23) childrenCount() int {
	return len(n.children)
}

func (n *Node23) valueCount() int {
	return len(n.values)
}

func (n *Node23) firstKey() *Felt {
	ensure(len(n.keys) > 0, "firstKey: node has no key")
	return n.keys[0]
}

func (n *Node23) firstValue() *Felt {
	ensure(len(n.values) > 0, "firstValue: node has no value")
	return n.values[0]
}

func (n *Node23) firstChild() *Node23 {
	ensure(len(n.children) > 0, "firstChild: node has no children")
	return n.children[0]
}

func (n *Node23) firstLeaf() *Node23 {
	if n.isLeaf {
		return n
	}
	firstLeaf := n.firstChild()
	for !firstLeaf.isLeaf {
		firstLeaf = firstLeaf.firstChild()
	}
	ensure(firstLeaf.isLeaf, "firstLeaf: last is not leaf")
	return firstLeaf
}

func (n *Node23) lastChild() *Node23 {
	ensure(len(n.children) > 0, "lastChild: node has no children")
	return n.children[len(n.children)-1]
}

func (n *Node23) lastLeaf() *Node23 {
	if n.isLeaf {
		return n
	}
	lastLeaf := n.lastChild()
	for !lastLeaf.isLeaf {
		lastLeaf = lastLeaf.lastChild()
	}
	ensure(lastLeaf.isLeaf, "lastLeaf: last is not leaf")
	return lastLeaf
}

func (n *Node23) nextKey() *Felt {
	ensure(len(n.keys) > 0, "nextKey: node has no key")
	return n.keys[len(n.keys)-1]
}

func (n *Node23) nextValue() *Felt {
	ensure(len(n.values) > 0, "nextValue: node has no value")
	return n.values[len(n.values)-1]
}

func (n *Node23) rawPointer() uintptr {
	return uintptr(unsafe.Pointer(n))
}

func (n *Node23) setNextKey(nextKey *Felt, stats *Stats) {
	ensure(len(n.keys) > 0, "setNextKey: node has no key")
	n.keys[len(n.keys)-1] = nextKey
	if !n.exposed {
		n.exposed = true
		stats.ExposedCount++
		stats.OpeningHashes += n.howManyHashes()
	}
	n.updated = true
	stats.UpdatedCount++
}

func (n *Node23) canonicalKeys() []Felt {
	if n.isLeaf {
		ensure(len(n.keys) > 0, "canonicalKeys: node has no key")
		return deref(n.keys[:len(n.keys)-1])
	} else {
		return deref(n.keys)
	}
}

func (n *Node23) hasKey(targetKey *Felt) bool {
	var keys []*Felt
	if n.isLeaf {
		ensure(len(n.keys) > 0, "hasKey: node has no key")
		keys = n.keys[:len(n.keys)-1]
	} else {
		keys = n.keys
	}
	for _, key := range keys {
		if *key == *targetKey {
			return true
		}
	}
	return false
}

func (n *Node23) isEmpty() bool {
	if n.isLeaf {
		// At least next key is always present
		return n.keyCount() == 1
	} else {
		return n.childrenCount() == 0
	}
}

func (n *Node23) height() int {
	if n.isLeaf {
		return 1
	} else {
		ensure(len(n.children) > 0, "height: internal node has zero children")
		return n.children[0].height() + 1
	}
}

func (n *Node23) keysInLevelOrder() []Felt {
	keysByLevel := make([]Felt, 0)
	for i := 0; i < n.height(); i++ {
		keysByLevel = append(keysByLevel, n.keysByLevel(i)...)
	}
	return keysByLevel
}

func (n *Node23) keysByLevel(level int) []Felt {
	if level == 0 {
		return n.canonicalKeys()
	} else {
		levelKeys := make([]Felt, 0)
		for _, child := range n.children {
			childLevelKeys := child.keysByLevel(level - 1)
			levelKeys = append(levelKeys, childLevelKeys...)
		}
		return levelKeys
	}
}

type Walker func(*Node23) interface{}

func (n *Node23) walkPostOrder(w Walker) []interface{} {
	items := make([]interface{}, 0)
	if !n.isLeaf {
		for _, child := range n.children {
			childItems := child.walkPostOrder(w)
			items = append(items, childItems...)
		}
	}
	items = append(items, w(n))
	return items
}

func (n *Node23) walkNodesPostOrder() []*Node23 {
	nodeItems := n.walkPostOrder(func(n *Node23) interface{} { return n })
	nodes := make([]*Node23, len(nodeItems))
	for i := range nodeItems {
		nodes[i] = nodeItems[i].(*Node23)
	}
	return nodes
}

func (n *Node23) howManyHashes() uint {
	if n.isLeaf {
		// all leaves except last one: 2 or 3 keys + 1 or 2 values => 3 or 5 data => 2 or 4 hashes
		// last leaf: 1 or 2 keys + 1 or 2 values => 2 or 4 data => 1 or 3 hashes
		switch n.keyCount() {
		case 2:
			nextKey := n.keys[1]
			if nextKey == nil {
				return 1
			} else {
				return 2
			}
		case 3:
			nextKey := n.keys[2]
			if nextKey == nil {
				return 3
			} else {
				return 4
			}
		default:
			ensure(false, fmt.Sprintf("howManyHashes: unexpected keyCount=%d\n", n.keyCount()))
			return 0
		}
	} else {
		// internal node: 2 or 3 children => 1 or 2 hashes
		switch n.childrenCount() {
		case 2:
			return 1
		case 3:
			return 2
		default:
			ensure(false, fmt.Sprintf("howManyHashes: unexpected childrenCount=%d\n", n.childrenCount()))
			return 0
		}
	}
}

func (n *Node23) hashNode() []byte {
	if n.isLeaf {
		return n.hashLeaf()
	} else {
		return n.hashInternal()
	}
}

func (n *Node23) hashLeaf() []byte {
	ensure(n.isLeaf, "hashLeaf: node is not leaf")
	ensure(n.valueCount() == n.keyCount(), "hashLeaf: insufficient number of values")
	switch n.keyCount() {
	case 2:
		k, nextKey, v := *n.keys[0], n.keys[1], *n.values[0]
		h := hash2(k.Binary(), v.Binary())
		if nextKey == nil {
			return h
		} else {
			return hash2(h, (*nextKey).Binary())
		}
	case 3:
		k1, k2, nextKey, v1, v2 := *n.keys[0], *n.keys[1], n.keys[2], *n.values[0], *n.values[1]
		h1 := hash2(k1.Binary(), v1.Binary())
		h2 := hash2(k2.Binary(), v2.Binary())
		h12 := hash2(h1, h2)
		if nextKey == nil {
			return h12
		} else {
			return hash2(h12, (*nextKey).Binary())
		}
	default:
		ensure(false, fmt.Sprintf("hashLeaf: unexpected keyCount=%d\n", n.keyCount()))
		return []byte{}
	}
}

func (n *Node23) hashInternal() []byte {
	ensure(!n.isLeaf, "hashInternal: node is not internal")
	switch n.childrenCount() {
	case 2:
		child1, child2 := n.children[0], n.children[1]
		return hash2(child1.hashNode(), child2.hashNode())
	case 3:
		child1, child2, child3 := n.children[0], n.children[1], n.children[2]
		return hash2(hash2(child1.hashNode(), child2.hashNode()), child3.hashNode())
	default:
		ensure(false, fmt.Sprintf("hashInternal: unexpected childrenCount=%d\n", n.childrenCount()))
		return []byte{}
	}
}
