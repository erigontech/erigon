package qmtree

import (
	"errors"
	"fmt"
	"iter"
	"math/bits"
	"sort"
	"sync"

	"github.com/erigontech/erigon/common"
)

/*
             ____TwigRoot___                   Level_12
            /
           /
1       leftRoot                               Level_11
2       Level_10
4       Level_9
8       Level_8
16      Level_7
32      Level_6
64      Level_5
128     Level_4
256     Level_3
512     Level_2
1024    Level_1
2048    Level_0

activeBits have been removed. twigRoot == leftRoot.
*/

/*         1
    2             3
 4     5       6     7
8 9   a b     c d   e f
*/

const (
	TWIG_SHARD_COUNT       = 4
	NODE_SHARD_COUNT       = 4
	MAX_TREE_LEVEL         = 64
	FIRST_LEVEL_ABOVE_TWIG = 13
	twigRoot_LEVEL         = FIRST_LEVEL_ABOVE_TWIG - 1 // 12
	ENTRIES_PATH           = "entries"
	TWIG_PATH              = "twig"
	TWIG_SHIFT             = 11              // a twig has 2**11 leaves
	LEAF_COUNT_IN_TWIG     = 1 << TWIG_SHIFT // 2**11==2048
	TWIG_MASK              = LEAF_COUNT_IN_TWIG - 1
	MIN_PRUNE_COUNT        = 2
)

func calcMaxLevel(youngestTwigId uint64) uint8 {
	if youngestTwigId == 0 {
		return FIRST_LEVEL_ABOVE_TWIG
	}
	return FIRST_LEVEL_ABOVE_TWIG + 63 - uint8(bits.LeadingZeros64(youngestTwigId))
}

type NodePos uint64

func nodePos(level uint8, n uint64) NodePos {
	return NodePos((uint64(level) << 56) | n)
}

func (np NodePos) Level() uint64 {
	return uint64(np >> 56) // extract the high 8 bits
}

func (np NodePos) Nth() uint64 {
	return uint64(np<<8) >> 8 // extract the low 56 bits
}

func (np NodePos) String() string {
	return fmt.Sprintf("NodePos: %d (level: %d, nth: %d)", np, np.Level(), np.Nth())
}

type EdgeNode struct {
	pos   NodePos
	value [32]byte
}

type UpperTree struct {
	shardId int
	// the nodes in high level tree (higher than twigs)
	// this variable can be recovered from saved edge nodes and activeTwigs
	nodes [][NODE_SHARD_COUNT]map[NodePos][32]byte //MaxUpperLevel*NodeShardCount maps
	// this variable can be recovered from entry file
	activeTwigShards [TWIG_SHARD_COUNT]map[uint64]*Twig //TwigShardCount maps
}

func NewUpperTree(shardId int) UpperTree {
	nodes := make([][NODE_SHARD_COUNT]map[NodePos][32]byte, MAX_TREE_LEVEL)
	for i := range nodes {
		for j := range nodes[i] {
			nodes[i][j] = make(map[NodePos][32]byte)
		}
	}
	ut := UpperTree{
		shardId: shardId,
		nodes:   nodes,
	}
	for i := range ut.activeTwigShards {
		ut.activeTwigShards[i] = make(map[uint64]*Twig)
	}
	return ut
}

func (ut *UpperTree) IsEmpty() bool {
	for _, shard := range ut.nodes {
		for _, m := range shard {
			if len(m) > 0 {
				return false
			}
		}
	}
	return true
}

func (ut *UpperTree) AddTwigs(twigMap map[uint64]*Twig) {
	for twigId, twig := range twigMap {
		shardIdx, key := GetShardIdxAndKey(twigId)
		if ut.activeTwigShards[shardIdx] == nil {
			ut.activeTwigShards[shardIdx] = map[uint64]*Twig{
				key: twig,
			}
		} else {
			ut.activeTwigShards[shardIdx][key] = twig
		}
	}
}

func (ut *UpperTree) GetTwig(twigId uint64) (*Twig, bool) {
	shardIdx, key := GetShardIdxAndKey(twigId)
	if shardIdx < len(ut.activeTwigShards) {
		twig, ok := ut.activeTwigShards[shardIdx][key]
		return twig, ok
	}

	return nil, false
}

func (ut *UpperTree) GetTwigRoot(n uint64) ([32]byte, bool) {
	shardIdx, key := GetShardIdxAndKey(n)
	if twig, ok := ut.activeTwigShards[shardIdx][key]; ok {
		return twig.twigRoot, true
	}

	return ut.GetNode(nodePos(twigRoot_LEVEL, n))
}

func (ut *UpperTree) SetNode(pos NodePos, node [32]byte) {
	ut.nodes[pos.Level()][pos.Nth()%NODE_SHARD_COUNT][pos] = node
}

func (ut *UpperTree) GetNode(pos NodePos) ([32]byte, bool) {
	node, ok := ut.nodes[pos.Level()][pos.Nth()%NODE_SHARD_COUNT][pos]
	return node, ok
}

func (ut *UpperTree) deleteNode(pos NodePos) {
	delete(ut.nodes[pos.Level()][pos.Nth()%NODE_SHARD_COUNT], pos)
}

func (ut *UpperTree) PruneNodes(start uint64, end uint64, youngestTwigId uint64) []byte {
	maxLevel := calcMaxLevel(youngestTwigId)
	ut.removeUselessNodes(start, end, maxLevel)
	return EdgeNodesToBytes(ut.getEdgeNodes(end, maxLevel))
}

func (ut *UpperTree) removeUselessNodes(start uint64, end uint64, maxLevel uint8) {
	curStart := start
	curEnd := end
	for level := uint8(twigRoot_LEVEL); level < maxLevel; level++ {
		endBack := curEnd
		if curEnd%2 != 0 && level != twigRoot_LEVEL {
			endBack -= 1
		}

		startBack := curStart
		if startBack > 0 {
			startBack = startBack - 1
		}
		for i := startBack; i < endBack; i++ {
			pos := nodePos(level, i)
			ut.deleteNode(pos)
		}
		curStart >>= 1
		curEnd >>= 1
	}
}

func (ut *UpperTree) getEdgeNodes(end uint64, maxLevel uint8) (newEdgeNodes []EdgeNode) {
	curEnd := end
	for level := uint8(twigRoot_LEVEL); level < maxLevel; level++ {
		endBack := curEnd
		if curEnd%2 != 0 && level != twigRoot_LEVEL {
			endBack -= 1
		}
		pos := nodePos(level, endBack)
		if v, ok := ut.GetNode(pos); ok {
			newEdgeNodes = append(newEdgeNodes, EdgeNode{pos, v})
		} else {
			panic(
				fmt.Sprintf("can not find shardId=%d maxLevel=%d level=%d end=%d curEnd=%d",
					ut.shardId, maxLevel, level, end, curEnd))
		}
		curEnd >>= 1
	}

	return newEdgeNodes
}

const slowHashing = false

func (ut *UpperTree) SyncNodesByLevel(hasher Hasher, level uint8, nList []uint64, youngestTwigId uint64) []uint64 {
	maxn := MaxNAtLevel(youngestTwigId, level)
	npos := nodePos(level, maxn)
	ut.SetNode(npos, hasher.nullNodeInHigerTree(level))
	npos = nodePos(level, maxn+1)
	ut.SetNode(npos, hasher.nullNodeInHigerTree(level))
	// take written_nodes out from self.nodes
	writtenNodes := ut.nodes[level]
	ut.nodes[level] = [4]map[NodePos][32]byte{} // push a placeholder that will be removed
	newList := make([]uint64, 0, len(nList))

	var wg sync.WaitGroup
	// run flushing in a threads such that sync_* won't be blocked
	for shardId, nodes := range writtenNodes {
		nList = nList[:]
		id := shardId
		if !slowHashing {
			wg.Add(1)
			go func() {
				defer wg.Done()
				doSyncJob(ut, hasher, nodes, level, uint64(id), nList)
			}()
		} else {
			doSyncJob(ut, hasher, nodes, level, uint64(id), nList)
		}
	}

	wg.Wait()

	for _, i := range nList {
		if len(newList) == 0 || newList[len(newList)-1] != i/2 {
			newList = append(newList, i/2)
		}
	}

	// return written_nodes back to self.nodes
	ut.nodes[level] = writtenNodes // the placeholder is removed
	return newList
}

func (ut *UpperTree) SyncUpperNodes(hasher Hasher, nList []uint64, youngestTwigId uint64) ([]uint64, [32]byte) {
	maxLevel := calcMaxLevel(youngestTwigId)
	if len(nList) != 0 {
		for level := uint8(FIRST_LEVEL_ABOVE_TWIG); level <= maxLevel; level++ {
			nList = ut.SyncNodesByLevel(hasher, level, nList, youngestTwigId)
		}
	}
	root, _ := ut.GetNode(nodePos(maxLevel, 0))
	return nList, root
}

// EvictTwigs moves completed twigs from activeTwigShards into fixed upper-tree
// nodes and returns the twig-pair indices (twigId/2) for SyncUpperNodes.
// The nList passed in already contains those pair indices from FlushFiles.
func (ut *UpperTree) EvictTwigs(hasher Hasher, nList []uint64, twigEvictStart uint64, twigEvictEnd uint64) []uint64 {
	// evict the specified twig range
	for twigId := twigEvictStart; twigId < twigEvictEnd; twigId++ {
		pos := nodePos(twigRoot_LEVEL, twigId)
		twig, _ := ut.GetTwig(twigId)
		ut.SetNode(pos, twig.twigRoot)
		shardIdx, key := GetShardIdxAndKey(twigId)
		delete(ut.activeTwigShards[shardIdx], key)
	}
	return nList
}

func doSyncJob(upperTree *UpperTree, hasher Hasher, nodes map[NodePos][32]byte, level uint8, shardId uint64, nList []uint64) {
	childNodes := upperTree.nodes[level-1]
	for _, i := range nList {
		if i%NODE_SHARD_COUNT != shardId {
			continue
		}
		pos := nodePos(level, i)
		if level == FIRST_LEVEL_ABOVE_TWIG {
			left, ok := upperTree.GetTwigRoot(2 * i)
			if !ok {
				panic(fmt.Sprintf("Cannot find left twig root %d", 2*i))
			}

			right, ok := upperTree.GetTwigRoot(2*i + 1)
			if !ok {
				right = hasher.nullTwig().twigRoot
			}
			nodes[pos] = hasher.nodeHash(level-1, left[:], right[:])
		} else {
			nodePosL := nodePos((level - 1), 2*i)
			nodePosR := nodePos((level - 1), 2*i+1)
			sl := nodePosL.Nth() % NODE_SHARD_COUNT
			sr := nodePosR.Nth() % NODE_SHARD_COUNT
			nodeL, ok := childNodes[sl][nodePosL]

			if !ok {
				panic(fmt.Sprintf(
					"Cannot find left child %d-%d %d-%d %d %d",
					level,
					i,
					level-1,
					2*i,
					2*i+1,
					nodePosL))
			}

			nodeR, ok := childNodes[sr][nodePosR]

			if !ok {
				panic(fmt.Sprintf(
					"Cannot find right child %d-%d %d-%d %d %d",
					level,
					i,
					level-1,
					2*i,
					2*i+1,
					nodePosR))
			}

			nodes[pos] = hasher.nodeHash(level-1, nodeL[:], nodeR[:])
		}
	}
}

type Tree struct {
	shardId int

	upperTree  UpperTree
	newTwigMap map[uint64]*Twig

	entryStorage EntryStorage
	twigStorage  TwigStorage

	// these variables can be recovered from entry file
	youngestTwigId uint64

	mtreeForYoungestTwig TwigMT

	// The following variables are only used during the execution of one block
	mtreeForYtChangeStart int32
	mtreeForYtChangeEnd   int32

	// dirtyTwigs tracks which twigs had entries appended since the last FlushFiles.
	// Used to build the nList for SyncUpperNodes.
	dirtyTwigs map[uint64]struct{}

	hasher Hasher
}

func newEmptyTree(hasher Hasher, shardId int, entryStorage EntryStorage, twigStorage TwigStorage) *Tree {
	t := &Tree{
		shardId:               shardId,
		upperTree:             NewUpperTree(shardId),
		newTwigMap:            map[uint64]*Twig{},
		entryStorage:          entryStorage,
		twigStorage:           twigStorage,
		youngestTwigId:        0,
		mtreeForYoungestTwig:  hasher.nullMtForTwig().Clone(),
		mtreeForYtChangeStart: -1,
		mtreeForYtChangeEnd:   -1,
		dirtyTwigs:            map[uint64]struct{}{},
		hasher:                hasher,
	}
	return t
}

func NewTree(hasher Hasher, shardId int, entryStorage EntryStorage, twigStorage TwigStorage) *Tree {
	tree := newEmptyTree(
		hasher,
		shardId,
		entryStorage,
		twigStorage)

	tree.newTwigMap[0] = hasher.nullTwig().Clone()
	tree.upperTree.SetNode(nodePos(FIRST_LEVEL_ABOVE_TWIG, 0), [32]byte{})
	nullTwigVal := hasher.nullTwig()
	tree.upperTree.activeTwigShards[0][0] = &nullTwigVal

	return tree
}

// SyncAndRoot runs the full three-phase sync (FlushFiles, EvictTwigs,
// SyncUpperNodes) and returns the current tree root hash.
func (t *Tree) SyncAndRoot(hasher Hasher) [32]byte {
	nList := t.FlushFiles(0, 0)
	nList = t.upperTree.EvictTwigs(hasher, nList, 0, 0)
	_, root := t.upperTree.SyncUpperNodes(hasher, nList, t.youngestTwigId)
	return root
}

// UnwindTo rolls back the tree so that targetSN is the last (youngest)
// serial number in the tree. All entries with SN > targetSN are removed.
// After calling UnwindTo, the caller should call SyncAndRoot to recompute
// the root hash.
//
// entryHashes must provide the hash of each entry in the youngest twig
// from SN twigBase..targetSN (inclusive). This is needed to rebuild the
// youngest twig's merkle tree since that data is only on disk for
// completed (non-youngest) twigs.
func (t *Tree) UnwindTo(targetSN uint64, entryHashes []common.Hash) {
	targetTwigId := targetSN >> TWIG_SHIFT
	posInTwig := targetSN & TWIG_MASK

	// When targetSN is the last entry of a twig, the twig is fully complete.
	// In normal operation, AppendEntry flushes the full twig and creates a new
	// empty youngest twig. We replicate that here: the youngest twig becomes
	// targetTwigId+1 (empty).
	twigComplete := posInTwig == TWIG_MASK

	// The youngest twig after unwind.
	var youngestAfter uint64
	if twigComplete {
		youngestAfter = targetTwigId + 1
	} else {
		youngestAfter = targetTwigId
	}

	// 1. Truncate files.
	if t.entryStorage != nil {
		t.entryStorage.Truncate(int64((targetSN + 1) * entryHashSize))
	}
	if t.twigStorage != nil {
		// Keep completed twigs on disk. If twigComplete, targetTwigId is also
		// completed, so keep twigs 0..targetTwigId (truncate at targetTwigId+1).
		var keepTwigs uint64
		if twigComplete {
			keepTwigs = targetTwigId + 1
		} else {
			keepTwigs = targetTwigId
		}
		if err := t.twigStorage.Truncate(int64(keepTwigs * TWIG_SIZE)); err != nil {
			panic(fmt.Sprintf("UnwindTo: twigStorage.Truncate: %v", err))
		}
	}

	// 2. Clear in-memory twigs beyond youngestAfter.
	for i := range t.upperTree.activeTwigShards {
		for key := range t.upperTree.activeTwigShards[i] {
			twigId := key*TWIG_SHARD_COUNT + uint64(i)
			if twigId > youngestAfter {
				delete(t.upperTree.activeTwigShards[i], key)
			}
		}
	}

	// 3. Clear upper tree nodes.
	maxLevel := calcMaxLevel(t.youngestTwigId)
	for level := uint8(twigRoot_LEVEL); level <= maxLevel; level++ {
		for _, shard := range t.upperTree.nodes[level] {
			for pos := range shard {
				delete(shard, pos)
			}
		}
	}

	// 4. Reset youngest twig ID.
	t.youngestTwigId = youngestAfter
	t.dirtyTwigs = map[uint64]struct{}{}

	s, k := GetShardIdxAndKey(youngestAfter)

	if twigComplete {
		// The target twig is fully complete. Create an empty youngest twig
		// (same as what AppendEntry does when it fills a twig).
		t.newTwigMap = map[uint64]*Twig{
			youngestAfter: t.hasher.nullTwig().Clone(),
		}
		t.mtreeForYoungestTwig = t.hasher.nullMtForTwig().Clone()
		t.mtreeForYtChangeStart = -1
		t.mtreeForYtChangeEnd = -1
		// Touch the first position of the new empty twig.
		t.dirtyTwigs[youngestAfter] = struct{}{}
	} else {
		// Rebuild the youngest twig from provided entry hashes.
		t.newTwigMap = map[uint64]*Twig{
			targetTwigId: t.hasher.nullTwig().Clone(),
		}

		nullHash := NullEntry{}.Hash()
		t.mtreeForYoungestTwig = t.hasher.nullMtForTwig().Clone()

		expectedCount := int(posInTwig + 1)
		if len(entryHashes) != expectedCount {
			panic(fmt.Sprintf("UnwindTo: expected %d entry hashes for twig rebuild, got %d",
				expectedCount, len(entryHashes)))
		}

		for i := 0; i < expectedCount; i++ {
			t.mtreeForYoungestTwig[LEAF_COUNT_IN_TWIG+i] = entryHashes[i]
			_ = nullHash // activeBit tracking removed
		}

		t.mtreeForYtChangeStart = 0
		t.mtreeForYtChangeEnd = int32(posInTwig)

		t.dirtyTwigs[targetTwigId] = struct{}{}
	}

	// Re-register this twig in the upper tree.
	t.upperTree.SetNode(nodePos(FIRST_LEVEL_ABOVE_TWIG, 0), [32]byte{})
	nullTwigVal := t.hasher.nullTwig()
	t.upperTree.activeTwigShards[s][k] = &nullTwigVal
}

// NextSN returns the next serial number that would be assigned to a new entry.
// This equals (youngestTwigId * LEAF_COUNT_IN_TWIG) + count of entries in youngest twig.
func (t *Tree) NextSN() uint64 {
	// Count active entries in the youngest twig by checking the merkle tree leaves.
	nullHash := NullEntry{}.Hash()
	count := uint64(0)
	for i := 0; i < LEAF_COUNT_IN_TWIG; i++ {
		if t.mtreeForYoungestTwig[LEAF_COUNT_IN_TWIG+i] != nullHash {
			count = uint64(i) + 1
		}
	}
	return t.youngestTwigId*LEAF_COUNT_IN_TWIG + count
}

// YoungestTwigId returns the current youngest twig ID.
func (t *Tree) YoungestTwigId() uint64 {
	return t.youngestTwigId
}

func (t *Tree) Close() {
	// Close files
	if t.entryStorage != nil {
		t.entryStorage.Close()
	}
	if t.twigStorage != nil {
		t.twigStorage.Close()
	}
}

func (t *Tree) GetFileSizes() (int64, int64) {
	var entrySize, twigSize int64
	if t.entryStorage != nil {
		entrySize = t.entryStorage.Size()
	}

	if t.twigStorage != nil {
		twigSize = t.twigStorage.Size()
	}

	return entrySize, twigSize
}

func (t *Tree) TruncateFiles(entryFileSize int64, twigFileSize int64) {
	if t.entryStorage != nil {
		t.entryStorage.Truncate(entryFileSize)
	}
	if t.twigStorage != nil {
		if err := t.twigStorage.Truncate(twigFileSize); err != nil {
			panic(fmt.Sprintf("twigStorage.Truncate: %v", err))
		}
	}
}

func (t *Tree) AppendEntry(entry Entry) (int64, error) {
	sn := entry.TxNum()

	twigId := sn >> TWIG_SHIFT
	t.youngestTwigId = twigId
	// record change_start/change_end for endblock sync
	position := int32(sn & TWIG_MASK)
	if t.mtreeForYtChangeStart == -1 {
		t.mtreeForYtChangeStart = position
	} else if t.mtreeForYtChangeEnd+1 != position {
		panic("non-increasing position!")
	}
	t.mtreeForYtChangeEnd = position

	// mark this twig as dirty for upper-tree sync
	t.dirtyTwigs[twigId] = struct{}{}

	var pos int64

	if t.entryStorage != nil {
		pos = t.entryStorage.Append(entry)
	}

	t.mtreeForYoungestTwig[(LEAF_COUNT_IN_TWIG + position)] = entry.Hash()

	if position == TWIG_MASK {
		// when this is the last entry of current twig
		// write the merkle tree of youngest twig to twig_file
		t.syncMtForYoungestTwig(false)

		if t.twigStorage != nil {
			if err := t.twigStorage.Append(t.mtreeForYoungestTwig, pos+entry.Len()); err != nil {
				panic(fmt.Sprintf("twigStorage.Append: %v", err))
			}
		}
		// allocate new twig as youngest twig
		t.youngestTwigId += 1
		t.newTwigMap[t.youngestTwigId] = t.hasher.nullTwig().Clone()
		t.mtreeForYoungestTwig = t.hasher.nullMtForTwig()[:]
		// mark the new empty twig dirty too
		t.dirtyTwigs[t.youngestTwigId] = struct{}{}
	}
	return pos, nil
}

func (t *Tree) PruneTwigs(startId uint64, endId uint64, entryFileSize int64) {
	if endId-startId < MIN_PRUNE_COUNT {
		panic(fmt.Sprintf("The count of pruned twigs is too small: %d", endId-startId))
	}

	if t.entryStorage != nil {
		t.entryStorage.PruneHead(entryFileSize)
	}

	if t.twigStorage != nil {
		if err := t.twigStorage.PruneHead(endId * TWIG_SIZE); err != nil {
			panic(fmt.Sprintf("twigStorage.PruneHead: %v", err))
		}
	}
}

// FlushFiles flushes disk storage, moves all completed twigs into the upper
// tree, and returns the nList (sorted unique twigId/2 pair indices) for
// SyncUpperNodes. With activeBits removed there is no separate phase-1/phase-2
// active-bit Merkle sync; we just derive nList from dirtyTwigs.
func (t *Tree) FlushFiles(twigDeleteStart uint64, twigDeleteEnd uint64) []uint64 {
	entryStorage := t.entryStorage
	if entryStorage != nil {
		t.entryStorage = t.entryStorage.CloneTemp()
	}

	twigStorage := t.twigStorage
	if twigStorage != nil {
		t.twigStorage = t.twigStorage.CloneTemp()
	}

	var wg sync.WaitGroup

	// run flushing in a threads such that sync_* won't be blocked
	if entryStorage != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			entryStorage.Flush()
		}()
	}

	if twigStorage != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			twigStorage.Flush()
		}()
	}

	t.syncMtForYoungestTwig(false)
	// Move all accumulated twigs (including completed ones) to upperTree
	// before replacing newTwigMap with only the youngest twig.
	oldTwigMap := t.newTwigMap
	t.newTwigMap = map[uint64]*Twig{
		t.youngestTwigId: oldTwigMap[t.youngestTwigId].Clone(),
	}
	t.upperTree.AddTwigs(oldTwigMap)

	// Build nList: sorted unique twigId/2 for all dirty twigs.
	pairSet := make(map[uint64]struct{}, len(t.dirtyTwigs))
	for twigId := range t.dirtyTwigs {
		pairSet[twigId>>1] = struct{}{}
	}
	nList := make([]uint64, 0, len(pairSet))
	for pair := range pairSet {
		nList = append(nList, pair)
	}
	sort.Slice(nList, func(i, j int) bool { return nList[i] < nList[j] })

	// Delete active bit shards for pruned twigs (activeBits removed — no-op range).
	_ = twigDeleteStart
	_ = twigDeleteEnd

	t.dirtyTwigs = map[uint64]struct{}{}

	wg.Wait()

	t.entryStorage = entryStorage
	t.twigStorage = twigStorage
	return nList
}

func (t *Tree) syncMtForYoungestTwig(recover_mode bool) {
	if t.mtreeForYtChangeStart == -1 {
		return
	}
	t.mtreeForYoungestTwig.Sync(t.hasher, t.mtreeForYtChangeStart, t.mtreeForYtChangeEnd)

	t.mtreeForYtChangeStart = -1
	t.mtreeForYtChangeEnd = 0
	var youngest_twig *Twig
	if recover_mode {
		youngest_twig, _ = t.upperTree.GetTwig(t.youngestTwigId)
	} else {
		youngest_twig = t.newTwigMap[t.youngestTwigId]
	}
	youngest_twig.leftRoot = t.mtreeForYoungestTwig[1]
	youngest_twig.syncTop()
}

func (t *Tree) LoadMtForNonYoungestTwig(twigId uint64) {
	if t.mtreeForYtChangeStart == -1 {
		return
	}
	t.mtreeForYtChangeStart = -1
	t.mtreeForYtChangeEnd = 0
	activeTwig, _ := t.upperTree.GetTwig(t.youngestTwigId)
	if t.twigStorage != nil {
		h, err := t.twigStorage.GetHashRoot(twigId)
		if err != nil {
			panic(fmt.Sprintf("twigStorage.GetHashRoot twig=%d: %v", twigId, err))
		}
		activeTwig.leftRoot = h
		activeTwig.syncTop()
	}
}

func (t *Tree) getUpperPathAndRoot(twigId uint64) ([]ProofNode, [32]byte) {
	maxLevel := calcMaxLevel(t.youngestTwigId)

	peerHash := [32]byte{}
	// use '^ 1' to flip the lowest bit to get sibling
	peerHash, ok := t.upperTree.GetTwigRoot(twigId ^ 1)

	if !ok {
		peerHash = t.hasher.nullTwig().twigRoot
	}

	selfHash, ok := t.upperTree.GetTwigRoot(twigId)

	if !ok {
		return nil, [32]byte{}
	}

	upperPath := make([]ProofNode, 0, maxLevel-FIRST_LEVEL_ABOVE_TWIG+1)
	upperPath = append(upperPath, ProofNode{
		SelfHash:   selfHash,
		PeerHash:   peerHash,
		PeerAtLeft: (twigId & 1) != 0, //twigId's lowest bit == 1 so the peer is at left
	})

	n := twigId >> 1
	for level := uint8(FIRST_LEVEL_ABOVE_TWIG); level < maxLevel; level++ {
		peerAtLeft := (n & 1) != 0

		snode, ok := t.upperTree.GetNode(nodePos(level, n))

		if !ok {
			panic("Cannot find node")
		}

		pnode, ok := t.upperTree.GetNode(nodePos(level, n^1))

		if !ok {
			panic("Cannot find node")
		}

		upperPath = append(upperPath, ProofNode{
			SelfHash:   snode,
			PeerHash:   pnode,
			PeerAtLeft: peerAtLeft,
		})
		n >>= 1
	}

	root, ok := t.upperTree.GetNode(nodePos(maxLevel, 0))

	if !ok {
		panic(fmt.Sprintf("cannot find node %d-%d", maxLevel, 0))
	}

	return upperPath, root
}

func (t *Tree) GetProof(sn uint64) (ProofPath, error) {
	twigId := sn >> TWIG_SHIFT
	path := ProofPath{
		TxNum: sn,
	}

	if twigId > t.youngestTwigId {
		return ProofPath{}, errors.New("twigId > t.youngestTwigId")
	}

	path.UpperPath, path.Root = t.getUpperPathAndRoot(twigId)
	if len(path.UpperPath) == 0 {
		return ProofPath{}, errors.New("Cannot find upper path")
	}

	if twigId == t.youngestTwigId {
		path.LeftOfTwig = GetLeftPathInMem(t.mtreeForYoungestTwig, sn)
	} else {
		if t.twigStorage == nil {
			return ProofPath{}, errors.New("twig_file is empty")
		}
		path.LeftOfTwig = GetLeftPathOnDisk(t.twigStorage, twigId, sn)
	}

	return path, nil
}

type PosItem struct {
	level uint8
	n     uint64
}

func (t *Tree) GetHashesByPosList(posList []PosItem) []common.Hash {
	hashes := make([]common.Hash, 0, len(posList))
	for hash := range t.hashIter(posList) {
		hashes = append(hashes, hash)
	}
	return hashes
}

func (t *Tree) hashIter(posList []PosItem) iter.Seq[common.Hash] {
	return func(yield func(common.Hash) bool) {
		cache := map[uint64]common.Hash{}
		for _, next := range posList {
			hash := t.getHashByNode(next.level, next.n, cache)
			if !yield(hash) {
				return
			}
		}
	}
}

func (t *Tree) getHashByNode(level uint8, nth uint64, cache map[uint64]common.Hash) common.Hash {
	var twigId uint64
	var levelStride uint64
	if level <= 12 {
		levelStride = 4096 >> level
		twigId = nth / levelStride
	}

	// left tree of twig (level 0-11, left half)
	if level <= 11 && (nth%levelStride) < levelStride/2 {
		isYoungestTwigId := twigId == t.youngestTwigId
		selfId := nth % levelStride
		idx := levelStride/2 + selfId
		if isYoungestTwigId {
			return t.mtreeForYoungestTwig[idx]
		} else {
			if t.twigStorage == nil {
				return common.Hash{}
			}
			h, err := t.twigStorage.GetHashNode(twigId, idx, cache)
			if err != nil {
				return common.Hash{}
			}
			return h
		}
	}

	// levels 8-11 right side were activeBits — no longer exist; return zero.
	if level >= 8 && level <= 11 {
		return common.Hash{}
	}

	// upper tree (level 12+)
	if level == 12 {
		root, _ := t.upperTree.GetTwigRoot(twigId)
		return root
	}

	node, ok := t.upperTree.GetNode(nodePos(level, nth))

	if !ok {
		return t.hasher.nullNodeInHigerTree(level)
	}

	return node
}

func MaxNAtLevel(youngestTwigId uint64, level uint8) uint64 {
	if level < FIRST_LEVEL_ABOVE_TWIG {
		panic("level is too small")
	}
	shift := level - FIRST_LEVEL_ABOVE_TWIG + 1
	return youngestTwigId >> shift
}

func GetShardIdxAndKey(twigId uint64) (int, uint64) {
	idx := int(twigId % TWIG_SHARD_COUNT)
	key := twigId / TWIG_SHARD_COUNT
	return idx, key
}
