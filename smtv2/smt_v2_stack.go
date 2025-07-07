package smtv2

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/erigontech/mdbx-go/mdbx"
	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/membatch"
	"github.com/erigontech/erigon/smt/pkg/utils"
	"github.com/erigontech/erigon-lib/log/v3"
)

type NodeType uint64

const (
	EmptyNodeType NodeType = iota
	BranchNodeType
	LeafNodeType
	RootNodeType
)

type HashType uint8

const (
	HashType_Branch HashType = iota
	HashType_Leaf
)

type HashSource struct {
	HashType HashType
	Key      SmtKey
	Value    SmtValue8
}

type Node struct {
	nodeType NodeType
	bit      int
	// this is the hash of the node itself in the case of an interhash where will get it from the tape rather than
	// calculating it
	hash        [4]uint64
	hash0       [4]uint64 // left hash of child
	hash0Source HashSource
	hash1       [4]uint64 // right hash of child
	hash1Source HashSource
	key         SmtKey
	value       SmtValue8
}

func (n *Node) SetHash(bit int, hash [4]uint64, hashSource HashSource) {
	if bit == 0 {
		copy(n.hash0[:], hash[:])
		n.hash0Source = hashSource
	} else {
		copy(n.hash1[:], hash[:])
		n.hash1Source = hashSource
	}
}

type SmtStack struct {
	stack       []Node
	currentPath []int
	lastItem    InputTapeItem
	nextElement int
}

func NewSmtStack() *SmtStack {
	stack := &SmtStack{
		stack:       make([]Node, 257),
		nextElement: 1,
	}

	for i := range stack.stack {
		stack.stack[i] = Node{
			nodeType: EmptyNodeType,
		}
	}

	stack.stack[0].nodeType = RootNodeType

	return stack
}

func (s *SmtStack) PushLeaf(bit int, key SmtKey, value SmtValue8) {
	node := &s.stack[s.nextElement]
	s.ResetCurrentNode()
	node.bit = bit
	node.key = key
	node.value = value
	node.nodeType = LeafNodeType
	s.nextElement++
	s.currentPath = append(s.currentPath, bit)
}

func (s *SmtStack) ResetCurrentNode() {
	node := &s.stack[s.nextElement]
	node.nodeType = EmptyNodeType
	node.bit = 0
	// reset the hash values to zero
	node.hash0 = [4]uint64{}
	node.hash1 = [4]uint64{}
	node.hash = [4]uint64{}
}

func (s *SmtStack) PushBranch(bit int) {
	s.ResetCurrentNode()
	node := &s.stack[s.nextElement]
	node.bit = bit
	node.nodeType = BranchNodeType
	s.nextElement++
	s.currentPath = append(s.currentPath, bit)
}

func (s *SmtStack) PopBranch() *Node {
	s.nextElement--
	branch := &s.stack[s.nextElement]
	if branch.nodeType != BranchNodeType && branch.nodeType != RootNodeType {
		panic("PopBranch: item is not a BranchNode")
	}

	if len(s.currentPath) > 0 {
		s.currentPath = s.currentPath[:len(s.currentPath)-1]
	}

	return branch
}

func (s *SmtStack) PopLeaf() *Node {
	s.nextElement--
	leaf := &s.stack[s.nextElement]
	if leaf.nodeType != LeafNodeType {
		panic("PopLeaf: item is not a LeafNode")
	}

	if len(s.currentPath) > 0 {
		s.currentPath = s.currentPath[:len(s.currentPath)-1]
	}

	return leaf
}

func (s *SmtStack) PeekLeaf() *Node {
	item := &s.stack[s.nextElement-1]
	if item.nodeType != LeafNodeType {
		log.Warn("PeekLeaf: item is not a LeafItem")
	}
	return item
}

func (s *SmtStack) PeekBranch() *Node {
	item := &s.stack[s.nextElement-1]
	if item.nodeType != BranchNodeType && item.nodeType != RootNodeType {
		panic("PeekBranch: item is not a BranchNode")
	}
	return item
}

func (s *SmtStack) JustRoot() bool {
	return s.nextElement == 1 // root node is always present
}

func (s *SmtStack) GetPathForIntermediateHash(finalBit int) [257]int {
	path := [257]int{}
	copy(path[:], s.currentPath)

	// add the final bit to the path
	path[len(s.currentPath)] = finalBit

	path[256] = len(s.currentPath) + 1

	return path
}

type IntersInputType uint8

const (
	IntersInputType_Unknown IntersInputType = iota
	IntersInputType_Value
	IntersInputType_IntermediateHashBranch
	IntersInputType_IntermediateHashValue
	IntersInputType_DeletedIntermediateHash
	IntersInputType_Terminator
)

// InputTapeItem is an item that needs to be handled by the RegenerateRoot function
// and will be returned by the InputTapeIterator.
type InputTapeItem struct {
	Type  IntersInputType
	Key   SmtKey
	Value SmtValue8
	Path  [257]int
	Hash  [4]uint64
}

func (i *InputTapeItem) String(keyTruncate int) string {
	typ := "Value"
	switch i.Type {
	case IntersInputType_IntermediateHashBranch:
		typ = "IH Branch"
	case IntersInputType_IntermediateHashValue:
		typ = "IH Value"
	case IntersInputType_Terminator:
		typ = "Terminator"
	case IntersInputType_DeletedIntermediateHash:
		typ = "Deleted IH"
	}

	path := ""
	for _, v := range i.Path[:i.Path[256]] {
		path += fmt.Sprintf("%d", v)
	}

	fullPath := i.Key.GetPath()[:keyTruncate]

	return fmt.Sprintf("InputTapeItem{Type: %v, Key: %v, KeyPath(Trunc): %v, Value: %v, Path: %v, Hash: %v}", typ, i.Key, fullPath, i.Value, path, i.Hash)
}

// InputTapeIterator is an iterator that iterates over values that need to be handled
// by the RegenerateRoot function and passed one by one to the stack to be handled.
// It will return either a value node or an intermediate hash node.
type InputTapeIterator interface {
	Next() (InputTapeItem, error)
	Size() int
	Processed() int
}

type IntermediateHashCollector interface {
	AddIntermediateHash(hash *IntermediateHash) error
}

type ImmediateWriteIntermediateHashCollector struct {
	batch *membatch.Mapmutation
}

func NewImmediateWriteIntermediateHashCollector(batch *membatch.Mapmutation) *ImmediateWriteIntermediateHashCollector {
	return &ImmediateWriteIntermediateHashCollector{batch: batch}
}

func (c *ImmediateWriteIntermediateHashCollector) AddIntermediateHash(hash *IntermediateHash) error {
	// convert the path to a byte slice for storage
	key := make([]byte, 257)
	for i, v := range hash.Path {
		key[i] = byte(v)
	}

	value := hash.Serialise()

	return c.batch.Put(kv.TableSmtIntermediateHashes, key, value)
}

type EtlIntermediateHashCollector struct {
	collector *etl.Collector
	collected int
	loaded    int
}

func NewEtlIntermediateHashCollector(location string) (*EtlIntermediateHashCollector, error) {
	log := log.New()
	collector := etl.NewCollector("", location, etl.NewSortableBuffer(etl.BufferOptimalSize), log)

	return &EtlIntermediateHashCollector{collector: collector}, nil
}

func (c *EtlIntermediateHashCollector) AddIntermediateHash(hash *IntermediateHash) error {
	// conver the path to a byte slice for storage
	key := PathToKeyBytes(hash.Path[:256], hash.Path[256])

	value := hash.Serialise()

	// because we must handle tombstones first when calling the Load function on the ETL we add a prefix here to the key to ensure
	// sorting is handled correctly
	if hash.HashType == IntermediateHashType_DeletedIntermediateHash {
		key = append([]byte{'D'}, key...)
	} else {
		key = append([]byte{'U'}, key...)
	}

	c.collector.Collect(key, value)

	c.collected++

	return nil
}

func (c *EtlIntermediateHashCollector) Close() {
	c.collector.Close()
}

func (c *EtlIntermediateHashCollector) Load(logPrefix string, logTime time.Duration, tx kv.RwTx, table string) error {
	ticker := time.NewTicker(logTime)
	defer ticker.Stop()

	err := c.collector.Load(tx, "", func(k, v []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
		select {
		case <-ticker.C:
			progress := float64(c.loaded) / float64(c.collected) * 100
			log.Info(fmt.Sprintf("[%s] ETL intermediate hash collector loaded %d/%d (%.2f%%)", logPrefix, c.loaded, c.collected, progress))
		default:
		}

		// check if this is a tombstone IH or not - we add in a 'D' or a 'U' to the front of the key to indicate this so that
		// we intentionally handle all deletes first by leaning on ETL sorting
		tombstone := k[0] == 'D'
		k = k[1:]

		ih := DeserialiseIntermediateHash(v)

		// if we are dealing with a tombstone here then we need to delete the entire IH path from the db
		if tombstone {
			paths := GetAllSharedPaths(ih.Path[:])

			for _, path := range paths {
				key := PathToKeyBytes(path[:256], path[256])
				tx.Delete(table, key)
			}
			return nil
		}

		// depending on the hash type we might do a few different things here.  If it's a deleted interhash
		// then we don't write it but allow the delete to happen below.  Anything else we write first
		// then handle the deletion.
		hashType := IntermediateHashType(v[0])

		// first write the key to the db
		if hashType != IntermediateHashType_DeletedIntermediateHash {
			tx.Put(table, k, v)
		}

		if hashType == IntermediateHashType_DeletedIntermediateHash {
			tx.Delete(table, k)
		}

		if hashType == IntermediateHashType_Value || hashType == IntermediateHashType_DeletedIntermediateHash {
			// the prefix we're going to delete from is the path of the interhash + one extra 0 bit as this will be the
			// leftmost node below our entry in the tree that we want to start deleting from
			pathSize := ih.Path[256]
			prefix := PathToKeyBytes(ih.Path[:pathSize], ih.Path[256])

			if hashType == IntermediateHashType_DeletedIntermediateHash {
				// we've already deleted the value so we want to go to the next node down the tree
				// by setting the length to 0 we basically fast forward to the next node
				prefix[32] = 0
			}

			c, err := tx.Cursor(table)
			if err != nil {
				return err
			}
			defer c.Close()

			k, _, err := c.Seek(prefix)
			if err != nil {
				return err
			}
			path, length, err := KeyBytesToPath(k)
			if err != nil {
				return err
			}

			if length > pathSize {
				if intsEqual(path[:length], ih.Path[:length]) {
					tx.Delete(table, k)
				}
			}

			for k, _, err := c.Next(); err == nil; k, _, err = c.Next() {
				if len(k) == 0 {
					break
				}
				path, length, err := KeyBytesToPath(k)
				if err != nil {
					return err
				}
				if length > pathSize {
					// check that the path is below our interhash in the tree
					if intsEqual(path[:pathSize], ih.Path[:pathSize]) {
						tx.Delete(table, k)
					} else {
						// no more overlapping paths means we are no longer processing children at this point
						break
					}
				} else {
					// no longer processing children at this point
					break
				}
			}
		}

		c.loaded++

		return nil
	}, etl.TransformArgs{})

	if err != nil {
		return err
	}

	return nil
}

type SmtStackHasher struct {
	stack     *SmtStack
	inputTape InputTapeIterator
	collector IntermediateHashCollector
	debug     bool
	mcp       int
}

func NewSmtStackHasher(inputTape InputTapeIterator, collector IntermediateHashCollector) *SmtStackHasher {
	return &SmtStackHasher{
		inputTape: inputTape,
		collector: collector,
	}
}

func (s *SmtStackHasher) SetDebug(debug bool) {
	s.debug = debug
}

func (s *SmtStackHasher) RegenerateRoot(logPrefix string, logInterval time.Duration) (*big.Int, utils.NodeValue8, int, error) {
	s.stack = NewSmtStack()
	ticker := time.NewTicker(logInterval)
	defer ticker.Stop()
	size := s.inputTape.Size()
	var item InputTapeItem
	var err error

	for item, err = s.inputTape.Next(); err == nil; item, err = s.inputTape.Next() {
		select {
		case <-ticker.C:
			processed := s.inputTape.Processed()
			percentage := float64(processed) / float64(size) * 100
			log.Info(fmt.Sprintf("[%s]: SMT stack regenerate root %d/%d (%.2f%%)", logPrefix, processed, size, percentage))
		default:
		}

		if item.Type == IntersInputType_DeletedIntermediateHash {
			s.collector.AddIntermediateHash(&IntermediateHash{
				HashType:  IntermediateHashType_DeletedIntermediateHash,
				Path:      item.Path,
				Hash:      item.Hash,
				LeafKey:   item.Key,
				LeafValue: item.Value,
			})
			continue
		}

		if err := s.stackStep(item); err != nil {
			return nil, utils.NodeValue8{}, 0, err
		}

		s.fillStack(item)
	}

	if !errors.Is(err, ErrNothingFound) {
		return nil, utils.NodeValue8{}, 0, err
	}

	if err := s.stackStep(InputTapeItem{Type: IntersInputType_Terminator}); err != nil {
		return nil, utils.NodeValue8{}, 0, err
	}

	root, finalNode := s.getStackRoot()

	log.Info(fmt.Sprintf("[%s] new max stack depth %v", logPrefix, s.mcp))

	// now we get the root hash from the last item on the stack which we
	// know is a branch node with no key effectively
	return root, finalNode, s.mcp, nil
}

func (s *SmtStackHasher) fillStack(item InputTapeItem) {
	if item.Type == IntersInputType_Value {
		s.fillStackFromValue(item)
	} else if item.Type == IntersInputType_IntermediateHashBranch || item.Type == IntersInputType_IntermediateHashValue {
		s.fillStackFromIntermediateHash(item)
	}
}

func (s *SmtStackHasher) fillStackFromValue(item InputTapeItem) {
	path := item.Key.GetPath()
	// copy(item.Path[:], path)

	// based on the stacks current path we need to find the common prefix with the path
	// we're flushing into the stack.  Using this we can later add branch nodes up to the leaf
	// we're trying to add
	commonPrefix := findCommonPrefix(s.stack.currentPath, path)

	// we need to push branches onto the stack until we fill it apart from the last item
	// where we push the leaf node
	for i := len(commonPrefix); i < len(path)-1; i++ {
		bit := path[i]
		s.stack.PushBranch(bit)
	}

	s.stack.PushLeaf(path[len(path)-1], item.Key, item.Value)

	s.stack.lastItem = item
}

func (s *SmtStackHasher) fillStackFromIntermediateHash(item InputTapeItem) {
	// tood: assert that the current path matches that of the item path apart from the last bit
	// otherwise panic
	pathSize := item.Path[256]

	// assert here that the intermediate hash path is at least as long as the current path, we don't care
	// about the last bit as this is the bit we are pushing onto the stack so we perform the -1 here
	if pathSize-1 < len(s.stack.currentPath) {
		panic("fillStackFromIntermediateHash: current path is longer than the intermediate hash path")
	}

	// assert that the current path overlaps with the intermediate hash path
	if !intsEqual(s.stack.currentPath, item.Path[:len(s.stack.currentPath)]) {
		panic("fillStackFromIntermediateHash: current path does not overlap with the intermediate hash path")
	}

	// now figure out how many filler branches we need to push onto the stack, with -1 because this is the last element
	// we'll push and we need to handle that differently so we can add in the pre-calculated intermediate hash
	fillerBranches := pathSize - len(s.stack.currentPath) - 1

	for i := 0; i < fillerBranches; i++ {
		s.stack.PushBranch(item.Path[len(s.stack.currentPath)])
	}

	// now we need to push the branch/leaf node onto the stack and set the hash value of it
	if item.Type == IntersInputType_IntermediateHashBranch {
		s.stack.PushBranch(item.Path[len(s.stack.currentPath)])
		s.stack.PeekBranch().hash = item.Hash
	} else if item.Type == IntersInputType_IntermediateHashValue {
		s.stack.PushLeaf(item.Path[pathSize-1], item.Key, item.Value)
		s.stack.PeekLeaf().hash = item.Hash
	}

	s.stack.lastItem = item
}

func (s *SmtStackHasher) stackStep(item InputTapeItem) error {
	if s.stack.JustRoot() {
		return nil
	}

	lastPath := []int{}
	if s.stack.lastItem.Type == IntersInputType_Value {
		lastPath = s.stack.lastItem.Key.GetPath()
	} else if s.stack.lastItem.Type == IntersInputType_IntermediateHashBranch || s.stack.lastItem.Type == IntersInputType_IntermediateHashValue {
		lastPath = s.stack.lastItem.Path[:256]
	}

	currentPath := []int{}
	if item.Type == IntersInputType_Value {
		currentPath = item.Key.GetPath()
	} else if item.Type == IntersInputType_IntermediateHashBranch || item.Type == IntersInputType_IntermediateHashValue {
		// if the last item handled was a value node then we know the stack has been filled to the lowest
		// depth so we need to ensure this is trimmed accordingly, if it was an intermediate hash then we
		// know the stack is as deep as the intermediate hash path at this point so we handle this case differently
		if s.stack.lastItem.Type == IntersInputType_Value {
			// we need to ensure things are trimmed down correctly
			currentPath = item.Path[:256]
		} else if s.stack.lastItem.Type == IntersInputType_IntermediateHashBranch || s.stack.lastItem.Type == IntersInputType_IntermediateHashValue {
			// we know the stack wasn't flooded so we can keep the path condensed
			relevantPathSize := item.Path[256]
			currentPath = item.Path[:relevantPathSize]
		}
	}

	// now we need to find the common ancestor between the item we're going to be adding next
	// and the last item added to the stack
	commonPrefix := findCommonPrefix(lastPath, currentPath)

	// now we need to pop the leaf from the stack and keep a reference to it - if the last item
	// was indeed a leaf, in the case of an intermediate hash it could actually be a branch node
	var leaf *Node
	lastItemWasLeaf := s.stack.lastItem.Type == IntersInputType_Value ||
		(s.stack.lastItem.Type == IntersInputType_IntermediateHashValue)

	if lastItemWasLeaf {
		leaf = s.stack.PopLeaf()
	}

	maximumPopSize := len(s.stack.currentPath) + 1

	// first reduce the stack to either the common prefix or the first branch node that has been initialised with a hash
	// on the left or right
	for i := 0; i < maximumPopSize; i++ {
		branch := s.stack.PeekBranch()

		if branch.nodeType == RootNodeType {
			break
		}

		// if the branch node has a left or right hash already set then we turn off trimmingTree mode
		// and start hashing upwards from that point up to the common prefix
		if branch.hash0 != [4]uint64{} || branch.hash1 != [4]uint64{} || branch.hash != [4]uint64{} {
			break
		}

		if intsEqual(s.stack.currentPath, commonPrefix) {
			break
		}

		s.stack.PopBranch()
	}

	if len(s.stack.currentPath) > s.mcp {
		s.mcp = len(s.stack.currentPath)
	}

	// always process the leaf that we are handling in this step
	if lastItemWasLeaf {
		hash, bit := s.processLeafHash(leaf.key, leaf.value, leaf.hash)
		parentBranch := s.stack.PeekBranch()
		parentBranch.SetHash(bit, hash, HashSource{HashType_Leaf, leaf.key, leaf.value})
		// might be useful for debugging to set this hash here
		leaf.hash = hash
	} else {
		s.processBranchHash()
	}

	// now we can start hashing the stack until we reach the common prefix
	for !intsEqual(s.stack.currentPath, commonPrefix) {
		s.processBranchHash()
	}

	return nil
}

func (s *SmtStackHasher) getStackRoot() (*big.Int, utils.NodeValue8) {
	// we should only be calling this if the stack has just the root node in it
	branch := s.stack.PopBranch()
	if branch.nodeType != RootNodeType {
		panic("getStackRoot: expected root node but didn't find it")
	}

	var finalNode utils.NodeValue8
	var rootHash SmtKey

	/*
		here we handle some different cases:
		1. only a left hash and it is a leaf hash = hash the leaf again with the special rule of a full rkey
		2. only a right hash and it is a leaf hash = hash the leaf again with the special rule of a full rkey
		3. nothing on either side means return 0x0 root
		4. there is a left and right hash set = continue as normal
	*/
	if branch.hash1 == [4]uint64{} && branch.hash0Source.HashType == HashType_Leaf {
		rootHash = s.processPolygonLeafHash(branch.hash0Source.Key, branch.hash0Source.Value)
		finalNode.SetHalfValue(rootHash, 0)
	} else if branch.hash0 == [4]uint64{} && branch.hash1Source.HashType == HashType_Leaf {
		rootHash = s.processPolygonLeafHash(branch.hash1Source.Key, branch.hash1Source.Value)
		finalNode.SetHalfValue(rootHash, 1)
	} else if branch.hash0 == [4]uint64{} && branch.hash1 == [4]uint64{} {
		return big.NewInt(0), utils.NodeValue8{}
	} else {
		finalNode.SetHalfValue(branch.hash0, 0)
		finalNode.SetHalfValue(branch.hash1, 1)
		rootHashBytes := finalNode.ToUintArray()
		rootHash = utils.Hash(rootHashBytes, utils.BranchCapacity)
	}

	if s.debug {
		rootHashBytes := finalNode.ToUintArray()
		b := strings.Builder{}
		b.WriteString("Root Hash:\n")
		b.WriteString(fmt.Sprintf("hashInput(%v)\n", rootHashBytes))
		b.WriteString(fmt.Sprintf("hashRaw(%v)\n", rootHash))
		b.WriteString(fmt.Sprintf("hashHex(%v)\n", utils.KeyToHex(rootHash)))
		b.WriteString("----------------------------------\n")
		fmt.Println(b.String())
	}

	return rootHash.ToBigInt(), finalNode
}

// isPrefix returns true if path1 is a prefix of path2
func isPrefix(path1, path2 []int) bool {
	matches := 0
	minLen := len(path1)
	if len(path2) < minLen {
		minLen = len(path2)
	}

	for i := 0; i < minLen; i++ {
		if path1[i] == path2[i] {
			matches++
		} else {
			break
		}
	}
	return matches == minLen
}

func findCommonPrefix(path1, path2 []int) []int {
	// Find the shorter length to avoid out of bounds access
	minLen := len(path1)
	if len(path2) < minLen {
		minLen = len(path2)
	}

	// Pre-allocate the result slice to avoid growing
	result := make([]int, 0, minLen)

	// Compare values and build result
	for i := 0; i < minLen && path1[i] == path2[i]; i++ {
		result = append(result, path1[i])
	}

	return result
}

func (s *SmtStackHasher) processLeafHash(
	key SmtKey,
	value SmtValue8,
	existingHash [4]uint64,
) ([4]uint64, int) {
	// now we are at the common prefix point so we can add the hash of our leaf node to the left
	// or right position - we can't really determine the bit of the leaf when we push it to the stack
	// because it goes to the very bottom of the tree by default and until we find the common prefix
	// we can't know if it is a left or right node so we lean on the stack current path to determine this
	bit := key.GetPath()[len(s.stack.currentPath)]

	// if the leaf node has a hash already set then we don't need to process it - it came from an interhash
	if existingHash != [4]uint64{} {
		parentBranch := s.stack.PeekBranch()
		parentBranch.SetHash(bit, existingHash, HashSource{HashType_Leaf, key, value})
		return existingHash, bit
	}

	remainingKey := RemoveKeyBits(key, len(s.stack.currentPath)+1)

	// we know we're dealing with a leaf at this point so lets get the leaf hash now
	leafInitialHash, _ := utils.HashKeyAndValueByPointers(value.ToUintArrayByPointer(), &utils.BranchCapacity)
	toHash := utils.ConcatArrays4ByPointers(remainingKey.AsUint64Pointer(), leafInitialHash)
	leafHash, _ := utils.HashKeyAndValueByPointers(toHash, &utils.LeafCapacity)

	if s.debug {
		b := strings.Builder{}
		b.WriteString("Leaf Hash:\n")
		b.WriteString(fmt.Sprintf("currentPath(%v) \n", s.stack.currentPath))
		b.WriteString(fmt.Sprintf("leafKey(%v) \n", key))
		b.WriteString(fmt.Sprintf("leafValue(%v) \n", value))
		b.WriteString(fmt.Sprintf("remainingKey(%v) \n", remainingKey))
		b.WriteString(fmt.Sprintf("hashInput(%v)\n", toHash))
		b.WriteString(fmt.Sprintf("hashRaw(%v)\n", *leafHash))
		b.WriteString(fmt.Sprintf("hashHex(%v)\n", utils.KeyToHex(*leafHash)))
		b.WriteString("----------------------------------\n")
		fmt.Println(b.String())
	}

	if s.collector != nil {
		paddedPath := s.stack.GetPathForIntermediateHash(bit)
		leafKey := SmtKey{key[0], key[1], key[2], key[3]}
		leafValue := SmtValue8{value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7]}
		s.collector.AddIntermediateHash(&IntermediateHash{
			Path:      paddedPath,
			HashType:  IntermediateHashType_Value,
			Hash:      *leafHash,
			LeafKey:   leafKey,
			LeafValue: leafValue,
		})
	}

	return *leafHash, bit
}

// a special variant of the leaf hash that handles a special case where the leaf node is the only node in the tree
func (s *SmtStackHasher) processPolygonLeafHash(
	key SmtKey,
	value SmtValue8,
) [4]uint64 {
	// no need for a common prefix here, we know we're at the root node so the bit is just the first bit of the key
	bit := key.GetPath()[0]

	// we know we're dealing with a leaf at this point so lets get the leaf hash now
	leafInitialHash, _ := utils.HashKeyAndValueByPointers(value.ToUintArrayByPointer(), &utils.BranchCapacity)
	// no rkey for this special case, we just use the full key
	toHash := utils.ConcatArrays4ByPointers(key.AsUint64Pointer(), leafInitialHash)
	leafHash, _ := utils.HashKeyAndValueByPointers(toHash, &utils.LeafCapacity)

	if s.debug {
		b := strings.Builder{}
		b.WriteString("Leaf Hash:\n")
		b.WriteString(fmt.Sprintf("currentPath(%v) \n", s.stack.currentPath))
		b.WriteString(fmt.Sprintf("leafKey(%v) \n", key))
		b.WriteString(fmt.Sprintf("leafValue(%v) \n", value))
		b.WriteString(fmt.Sprintf("remainingKey(%v) \n", key))
		b.WriteString(fmt.Sprintf("hashInput(%v)\n", toHash))
		b.WriteString(fmt.Sprintf("hashRaw(%v)\n", *leafHash))
		b.WriteString(fmt.Sprintf("hashHex(%v)\n", utils.KeyToHex(*leafHash)))
		b.WriteString("----------------------------------\n")
		fmt.Println(b.String())
	}

	if s.collector != nil {
		// hard coded path for this special case we know this will only be called when the tree has a single node
		// and we're already at the root calculation stage so the path will always be right under the root node
		path := [257]int{}
		path[0] = bit
		path[256] = 1
		leafKey := SmtKey{key[0], key[1], key[2], key[3]}
		leafValue := SmtValue8{value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7]}
		s.collector.AddIntermediateHash(&IntermediateHash{
			Path:      path,
			HashType:  IntermediateHashType_Value,
			Hash:      *leafHash,
			LeafKey:   leafKey,
			LeafValue: leafValue,
		})
	}

	return *leafHash
}

func (s *SmtStackHasher) processBranchHash() [4]uint64 {
	branch := s.stack.PopBranch()

	// if the branch node has a hash already set then we don't need to process it - it came from an interhash
	if branch.hash != [4]uint64{} {
		parentBranch := s.stack.PeekBranch()
		parentBranch.SetHash(branch.bit, branch.hash, HashSource{HashType_Branch, branch.key, branch.value})
		return branch.hash
	}

	var twoHalves utils.NodeValue8
	twoHalves.SetHalfValue(branch.hash0, 0)
	twoHalves.SetHalfValue(branch.hash1, 1)
	twoHalvesArray := twoHalves.ToUintArray()

	var rootHash SmtKey = utils.Hash(twoHalvesArray, utils.BranchCapacity)

	if s.debug {
		b := strings.Builder{}
		b.WriteString("Branch Hash:\n")
		b.WriteString(fmt.Sprintf("currentPath(%v) \n", s.stack.currentPath))
		b.WriteString(fmt.Sprintf("inputValue(%v)\n", twoHalvesArray))
		b.WriteString(fmt.Sprintf("hashRaw(%v)\n", rootHash))
		b.WriteString(fmt.Sprintf("hashHex(%v)\n", utils.KeyToHex(rootHash)))
		b.WriteString("----------------------------------\n")
		fmt.Println(b.String())
	}

	if s.collector != nil {
		paddedPath := s.stack.GetPathForIntermediateHash(branch.bit)
		s.collector.AddIntermediateHash(&IntermediateHash{
			Path:     paddedPath,
			HashType: IntermediateHashType_Branch,
			Hash:     rootHash,
		})
	}

	parentBranch := s.stack.PeekBranch()
	parentBranch.SetHash(branch.bit, rootHash, HashSource{HashType_Branch, branch.key, branch.value})

	// we don't strictly need to set this here as we have already added the hash to the parent branch
	// but it might help with debugging
	branch.hash = [4]uint64{rootHash[0], rootHash[1], rootHash[2], rootHash[3]}

	return rootHash
}

func intsEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}

	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

var ErrNothingFound = errors.New("nothing found")

type DbInputTapeIterator struct {
	cursor kv.Cursor
}

func NewDbInputTapeIterator(tx kv.Tx) (*DbInputTapeIterator, error) {
	c, err := tx.Cursor(kv.TableAccountValues)
	if err != nil {
		return nil, err
	}

	return &DbInputTapeIterator{
		cursor: c,
	}, nil
}

func (i *DbInputTapeIterator) Next() (InputTapeItem, error) {
	key, value, err := i.cursor.Next()
	if err != nil {
		if mdbx.IsNotFound(err) {
			return InputTapeItem{}, ErrNothingFound
		}
		return InputTapeItem{}, err
	}

	if len(key) == 0 {
		return InputTapeItem{}, ErrNothingFound
	}

	pathInts := make([]int, len(key))
	for i, v := range key {
		pathInts[i] = int(v)
	}

	nodeKey, err := SmtKeyFromPath(pathInts)
	if err != nil {
		return InputTapeItem{}, err
	}

	val := big.NewInt(0).SetBytes(value)
	val8 := ScalarToSmtValue8FromBits(val)

	item := InputTapeItem{
		Type:  IntersInputType_Value,
		Key:   nodeKey,
		Value: val8,
	}

	return item, nil
}

func (i *DbInputTapeIterator) Close() {
	i.cursor.Close()
}

type EtlInputTapeIterator struct {
	input     *etl.Collector
	nextItem  chan InputTapeItem
	err       error
	done      chan struct{}
	inputSize int
	processed int
}

func NewEtlInputTapeIterator(input *etl.Collector, inputSize int) *EtlInputTapeIterator {
	iterator := &EtlInputTapeIterator{
		input:     input,
		nextItem:  make(chan InputTapeItem),
		err:       nil,
		done:      make(chan struct{}),
		inputSize: inputSize,
	}

	return iterator
}

func (i *EtlInputTapeIterator) Size() int {
	return i.inputSize
}

func (i *EtlInputTapeIterator) Processed() int {
	return i.processed
}

func (i *EtlInputTapeIterator) Start() {
	go func() {
		defer close(i.nextItem)
		err := i.input.Load(nil, "", func(k, v []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
			select {
			case <-i.done:
				return ErrNothingFound
			default:
			}

			if len(k) == 0 {
				return ErrNothingFound
			}

			pathInts := make([]int, len(k))
			for i, v := range k {
				pathInts[i] = int(v)
			}

			nodeKey, err := SmtKeyFromPath(pathInts)
			if err != nil {
				return err
			}

			var val SmtValue8
			val.FromBytes(v)

			item := InputTapeItem{
				Type:  IntersInputType_Value,
				Key:   nodeKey,
				Value: val,
			}

			i.nextItem <- item

			i.processed++

			return nil
		}, etl.TransformArgs{})

		if err != nil {
			i.err = err
			return
		}

		// mark the end of the tape
		i.err = ErrNothingFound
	}()
}

func (i *EtlInputTapeIterator) Stop() {
	close(i.done)
}

func (i *EtlInputTapeIterator) Next() (InputTapeItem, error) {
	if i.err != nil {
		if errors.Is(i.err, ErrNothingFound) {
			return InputTapeItem{}, ErrNothingFound
		}
		return InputTapeItem{}, i.err
	}

	item := <-i.nextItem

	return item, nil
}

type IntermediateHashType uint8

const (
	IntermediateHashType_Branch IntermediateHashType = iota
	IntermediateHashType_Value
	IntermediateHashType_DeletedIntermediateHash
	IntermediateHashType_Terminator
)

type IntermediateHash struct {
	// fixed array to hold the relative path to the nodes position in the tree, always padded to 256 ints (filled with 0s).  The last int is the
	// position of relevance to the node in the tree.  For example a given full key could be 00011111 but if it is the only node in the tree
	// the relevant bits would just be 0 on it's own, so we pad 0 to 256 and get 0...000 then we store 1 as the last byte so we know that
	// only the first bit is relevant to the node in the tree.  This allows us to keep lexographical ordering in the table whilst
	// making sense of the data
	HashType  IntermediateHashType
	Path      [257]int
	Hash      [4]uint64
	LeafKey   SmtKey
	LeafValue SmtValue8
	keyPath   []int
}

func (i *IntermediateHash) GetKeyPath() []int {
	if i.keyPath == nil {
		i.keyPath = i.LeafKey.GetPath()
	}
	return i.keyPath
}

func (i *IntermediateHash) String() string {
	hashType := "Branch"
	switch i.HashType {
	case IntermediateHashType_Value:
		hashType = "Value"
	case IntermediateHashType_Terminator:
		hashType = "Terminator"
	}

	path := ""
	for _, v := range i.Path[:i.Path[256]] {
		path += fmt.Sprintf("%d", v)
	}

	return fmt.Sprintf("IntermediateHash{HashType: %v, Path: %v, Hash: %v, LeafKey: %v, LeafValue: %v}", hashType, path, i.Hash, i.LeafKey, i.LeafValue)
}

func (i *IntermediateHash) Serialise() []byte {
	// Pre-calculate the exact buffer size needed
	size := 1 + // HashType byte
		257 + // Path bytes
		32 + // Hash bytes (4 * 8)
		32 + // LeafKey bytes (4 * 8)
		64 // LeafValue bytes (8 * 8)

	buf := make([]byte, size)
	offset := 0

	// Write HashType
	buf[offset] = byte(i.HashType)
	offset++

	// Write Path
	for _, v := range i.Path {
		buf[offset] = byte(v)
		offset++
	}

	// Write Hash
	binary.BigEndian.PutUint64(buf[offset:offset+8], i.Hash[0])
	binary.BigEndian.PutUint64(buf[offset+8:offset+16], i.Hash[1])
	binary.BigEndian.PutUint64(buf[offset+16:offset+24], i.Hash[2])
	binary.BigEndian.PutUint64(buf[offset+24:offset+32], i.Hash[3])
	offset += 32

	// Write LeafKey
	binary.BigEndian.PutUint64(buf[offset:offset+8], i.LeafKey[0])
	binary.BigEndian.PutUint64(buf[offset+8:offset+16], i.LeafKey[1])
	binary.BigEndian.PutUint64(buf[offset+16:offset+24], i.LeafKey[2])
	binary.BigEndian.PutUint64(buf[offset+24:offset+32], i.LeafKey[3])
	offset += 32

	// Write LeafValue
	binary.BigEndian.PutUint64(buf[offset:offset+8], i.LeafValue[0])
	binary.BigEndian.PutUint64(buf[offset+8:offset+16], i.LeafValue[1])
	binary.BigEndian.PutUint64(buf[offset+16:offset+24], i.LeafValue[2])
	binary.BigEndian.PutUint64(buf[offset+24:offset+32], i.LeafValue[3])
	binary.BigEndian.PutUint64(buf[offset+32:offset+40], i.LeafValue[4])
	binary.BigEndian.PutUint64(buf[offset+40:offset+48], i.LeafValue[5])
	binary.BigEndian.PutUint64(buf[offset+48:offset+56], i.LeafValue[6])
	binary.BigEndian.PutUint64(buf[offset+56:offset+64], i.LeafValue[7])
	offset += 64

	return buf
}

func DeserialiseIntermediateHash(data []byte) *IntermediateHash {
	if len(data) == 0 {
		return &IntermediateHash{}
	}

	hashType := IntermediateHashType(data[0])

	path := [257]int{}
	for i := 0; i < 257; i++ {
		path[i] = int(data[1+i])
	}

	hash := [4]uint64{}
	for i := 0; i < 4; i++ {
		hash[i] = binary.BigEndian.Uint64(data[258+i*8 : 258+(i+1)*8])
	}

	var leafKey SmtKey
	var leafValue SmtValue8

	// some old records in a db that support the old format for branch nodes won't have
	// a leaf key or value so we can skip these
	if len(data) > 290 {
		lk := [4]uint64{}
		for i := 0; i < 4; i++ {
			lk[i] = binary.BigEndian.Uint64(data[290+i*8 : 290+(i+1)*8])
		}
		leafKey = SmtKey{lk[0], lk[1], lk[2], lk[3]}

		lv := [8]uint64{}
		for i := 0; i < 8; i++ {
			start := 322 + i*8 // 1 byte hashType + 257 bytes for path + 32 bytes hash + i*8 for each value
			end := start + 8
			valueBytes := data[start:end]
			lv[i] = binary.BigEndian.Uint64(valueBytes)
		}
		leafValue = SmtValue8{lv[0], lv[1], lv[2], lv[3], lv[4], lv[5], lv[6], lv[7]}
	}

	return &IntermediateHash{
		Path:      path,
		HashType:  hashType,
		Hash:      hash,
		LeafKey:   leafKey,
		LeafValue: leafValue,
	}
}

// HandleFinalHash handles exception cases in the SMT where we need to do some clear up work as
// in some cases there will be no emitted interhashes from the call to RegenerateRoot.  We have
// 3 cases to handle, an empty tree, a tree with nothing on the left/right, so we clear down the
// table accordingly in these cases.
func HandleFinalHash(tx kv.RwTx, finalNode utils.NodeValue8) error {
	asArray := finalNode.ToUintArray()
	if asArray[0] == 0 && asArray[1] == 0 && asArray[2] == 0 && asArray[3] == 0 {
		// we have a tree with nothing on the left
		// so we need to clear down the right half of the tree
		prefix := []byte{0}
		if err := tx.ForPrefix(kv.TableSmtIntermediateHashes, prefix, func(k, v []byte) error {
			return tx.Delete(kv.TableSmtIntermediateHashes, k)
		}); err != nil {
			return err
		}
	}

	if asArray[4] == 0 && asArray[5] == 0 && asArray[6] == 0 && asArray[7] == 0 {
		// we have a tree with nothing on the right
		// so we need to clear down the left half of the tree
		prefix := []byte{1}
		if err := tx.ForPrefix(kv.TableSmtIntermediateHashes, prefix, func(k, v []byte) error {
			return tx.Delete(kv.TableSmtIntermediateHashes, k)
		}); err != nil {
			return err
		}
	}

	return nil
}
