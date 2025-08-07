package qmtree

import (
	"errors"
	"fmt"
	"iter"
	"math/bits"
	"sort"
	"sync"

	"github.com/erigontech/erigon-lib/common"
)

/*
             ____TwigRoot___                   Level_12
            /               \
           /                 \
1       leftRoot              activeBitsMTL3   Level_11
2       Level_10        2     activeBitsMTL2
4       Level_9         4     activeBitsMTL1
8       Level_8    8*32bytes  activeBits
16      Level_7
32      Level_6
64      Level_5
128     Level_4
256     Level_3
512     Level_2
1024    Level_1
2048    Level_0
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
	return FIRST_LEVEL_ABOVE_TWIG + 63 - uint8(bits.LeadingZeros64(youngestTwigId))
}

type NodePos uint64

func nodePos(level uint8, n uint64) NodePos {
	return NodePos(uint64(level<<56) | n)
}

func (np NodePos) Level() uint64 {
	return uint64(np >> 56) // extract the high 8 bits
}

func (np NodePos) Nth() uint64 {
	return uint64(np<<8) >> 8 // extract the low 56 bits
}

func (np NodePos) string() string {
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
	return UpperTree{
		shardId: shardId,
		nodes:   make([][NODE_SHARD_COUNT]map[NodePos][32]byte, 0, MAX_TREE_LEVEL),
	}
}

func (ut *UpperTree) IsEmpty() bool {
	return len(ut.nodes) == 0
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

func (ut *UpperTree) EvictTwigs(hasher Hasher, nList []uint64, twigEvictStart uint64, twigEvictEnd uint64) []uint64 {
	newList := ut.SyncMtForActiveBitsPhase2(hasher, nList)
	// run the pending twig-eviction jobs
	// they were not evicted earlier because sync_mt_for_active_bits_phase2 needs their content
	for twigId := twigEvictStart; twigId < twigEvictEnd; twigId++ {
		// evict the twig and store its twigRoot in nodes
		pos := nodePos(twigRoot_LEVEL, twigId)
		twig, _ := ut.GetTwig(twigId)
		ut.SetNode(pos, twig.twigRoot)
		shardIdx, key := GetShardIdxAndKey(twigId)
		delete(ut.activeTwigShards[shardIdx], key)
	}
	return newList
}

func (ut *UpperTree) SyncMtForActiveBitsPhase2(hasher Hasher, nList []uint64) []uint64 {
	newList := make([]uint64, 0, len(nList))

	var wg sync.WaitGroup

	for sid, twigShard := range ut.activeTwigShards {
		if !slowHashing {
			nList := nList[:]
			shardId := sid
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, i := range nList {
					twigId := i >> 1
					s, k := GetShardIdxAndKey(twigId)
					if s != shardId {
						continue
					}
					twigShard[k].syncL2(hasher, (i & 1))
				}
			}()
		} else {
			for _, i := range nList {
				twigId := i >> 1
				s, k := GetShardIdxAndKey(twigId)
				if s != sid {
					continue
				}
				twigShard[k].syncL2(hasher, (i & 1))
			}
		}
	}

	wg.Wait()

	for _, i := range nList {
		if len(newList) == 0 || newList[len(newList)-1] != i/2 {
			newList = append(newList, i/2)
		}
	}

	nList = newList
	newList = nil

	for sid, twigShard := range ut.activeTwigShards {
		if !slowHashing {
			nList = nList[:]
			shardId := sid
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, twigId := range nList {
					s, k := GetShardIdxAndKey(twigId)
					if s != shardId {
						continue
					}
					twigShard[k].syncL3(hasher)
					twigShard[k].syncTop(hasher)
				}
			}()
		} else {
			for _, twigId := range nList {
				s, k := GetShardIdxAndKey(twigId)
				if s != sid {
					continue
				}
				twigShard[k].syncL3(hasher)
				twigShard[k].syncTop(hasher)
			}
		}
	}

	wg.Wait()

	for _, i := range nList {
		if len(newList) == 0 || newList[len(newList)-1] != i/2 {
			newList = append(newList, i/2)
		}
	}

	return newList
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
			nodes[pos] = hasher.nodeHash(level-1, left, right)
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

			nodes[pos] = hasher.nodeHash(level-1, nodeL, nodeR)
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
	// pub activeBitShards: [HashMap<uint64, [u8; 256]>; TWIG_SHARD_COUNT],
	activeBitShards      [TWIG_SHARD_COUNT]map[uint64]*ActiveBits
	mtreeForYoungestTwig TwigMT

	// The following variables are only used during the execution of one block
	mtreeForYtChangeStart int32
	mtreeForYtChangeEnd   int32
	touchedPosOf512b      map[uint64]struct{}

	hasher Hasher
}

func newEmptyTree(hasher Hasher, shardId int, entryStorage EntryStorage, twigStorage TwigStorage) *Tree {
	return &Tree{
		shardId:               shardId,
		upperTree:             NewUpperTree(shardId),
		newTwigMap:            map[uint64]*Twig{},
		entryStorage:          entryStorage,
		twigStorage:           twigStorage,
		youngestTwigId:        0,
		mtreeForYoungestTwig:  hasher.nullMtForTwig().Clone(),
		mtreeForYtChangeStart: -1,
		mtreeForYtChangeEnd:   -1,
		touchedPosOf512b:      map[uint64]struct{}{},
		hasher:                hasher,
	}
}

func NewTree(hasher Hasher, shardId int, entryStorage EntryStorage, twigStorage TwigStorage) *Tree {
	tree := newEmptyTree(
		hasher,
		shardId,
		entryStorage,
		twigStorage)

	tree.newTwigMap[0] = hasher.nullTwig().Clone()
	tree.upperTree.SetNode(nodePos(FIRST_LEVEL_ABOVE_TWIG, 0), [32]byte{})
	nullTwig := hasher.nullTwig()
	tree.upperTree.activeTwigShards[0][0] = &nullTwig
	tree.activeBitShards[0][0] = &ActiveBits{}

	return tree
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
		t.twigStorage.Truncate(twigFileSize)
	}
}

func (t *Tree) GetActiveBits(twigId uint64) *ActiveBits {
	shardIdx, key := GetShardIdxAndKey(twigId)
	bits, ok := t.activeBitShards[shardIdx][key]
	if !ok {
		panic(fmt.Sprintf("cannot find twig %s", twigId))
	}
	return bits
}

func (t *Tree) getActiveBits(twigId uint64) *ActiveBits {
	shardIdx, key := GetShardIdxAndKey(twigId)
	return t.activeBitShards[shardIdx][key]
}

func (t *Tree) GetActiveBit(sn uint64) bool {
	twigId := sn >> TWIG_SHIFT
	pos := uint32(sn & TWIG_MASK)
	return t.getActiveBits(twigId).GetBit(pos)
}

func (t *Tree) setEntryActiviation(sn uint64, active bool) {
	twigId := sn >> TWIG_SHIFT
	pos := uint32(sn & TWIG_MASK)
	active_bits := t.getActiveBits(twigId)
	if active {
		active_bits.SetBit(pos)
	} else {
		active_bits.ClearBit(pos)
	}
	t.touchPos(sn)
}

func (t *Tree) touchPos(sn uint64) {
	t.touchedPosOf512b[sn/512] = struct{}{}
}

func (t *Tree) clearTouchedPos() {
	t.touchedPosOf512b = map[uint64]struct{}{}
}

func (t *Tree) ActiveEntry(sn uint64) {
	t.setEntryActiviation(sn, true)
}

func (t *Tree) DeactiveEntry(sn uint64) {
	t.setEntryActiviation(sn, false)
}

func (t *Tree) AppendEntry(entry Entry) (int64, error) {
	sn := entry.SerialNumber()
	t.ActiveEntry(sn)

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
			t.twigStorage.Append(t.mtreeForYoungestTwig, pos+entry.Len())
		}
		// allocate new twig as youngest twig
		t.youngestTwigId += 1
		s, i := GetShardIdxAndKey(t.youngestTwigId)
		t.newTwigMap[t.youngestTwigId] = t.hasher.nullTwig().Clone()
		t.activeBitShards[s][i] = &ActiveBits{}
		t.mtreeForYoungestTwig = t.hasher.nullMtForTwig()[:]
		t.touchPos(sn + 1)
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
		t.twigStorage.PruneHead(int64(endId * TWIG_SIZE))
	}
}

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
	t.newTwigMap = map[uint64]*Twig{
		t.youngestTwigId: t.newTwigMap[t.youngestTwigId].Clone(),
	}
	//add newTwigMap's old content to upperTree
	t.upperTree.AddTwigs(t.newTwigMap)
	//now, newTwigMap only contains one member: youngest_twig.clone()

	nList := t.syncMtForActiveBitsPhase1()
	for twigId := twigDeleteStart; twigId < twigDeleteEnd; twigId++ {
		shardIdx, key := GetShardIdxAndKey(twigId)
		delete(t.activeBitShards[shardIdx], key)
	}
	t.touchedPosOf512b = map[uint64]struct{}{}

	t.entryStorage = entryStorage
	t.twigStorage = twigStorage
	return nList
}

func (t *Tree) syncMtForActiveBitsPhase1() []uint64 {
	nList := make([]uint64, 0, len(t.touchedPosOf512b))
	for n := range t.touchedPosOf512b {
		nList = append(nList, n)
	}
	sort.Slice(nList, func(i, j int) bool { return nList[i] < nList[j] })

	newList := make([]uint64, 0, len(nList))
	var wg sync.WaitGroup

	for sid, twig_shard := range t.upperTree.activeTwigShards {
		if !slowHashing {
			activeBitShards := &t.activeBitShards
			nList := nList[:]
			shardId := sid
			wg.Add(1)
			go func() {
				defer wg.Done()
				for _, i := range nList {
					twigId := i >> 2
					s, k := GetShardIdxAndKey(twigId)
					if s != shardId {
						continue
					}
					activeBits := activeBitShards[s][k]
					twig_shard[k].syncL1(t.hasher, (i & 3), *activeBits)
				}
			}()
		} else {
			for _, i := range nList {
				twigId := i >> 2
				s, k := GetShardIdxAndKey(twigId)
				if s != sid {
					continue
				}
				activeBits := t.activeBitShards[s][k]
				twig_shard[k].syncL1(t.hasher, (i & 3), *activeBits)
			}
		}
	}

	wg.Wait()

	for _, i := range nList {
		if len(newList) == 0 || newList[len(newList)-1] != i/2 {
			newList = append(newList, i/2)
		}
	}

	return newList
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
}

func (t *Tree) LoadMtForNonYoungestTwig(twigId uint64) {
	if t.mtreeForYtChangeStart == -1 {
		return
	}
	t.mtreeForYtChangeStart = -1
	t.mtreeForYtChangeEnd = 0
	activeTwig, _ := t.upperTree.GetTwig(t.youngestTwigId)
	if t.twigStorage != nil {
		t.twigStorage.GetHashRoot(twigId, activeTwig.leftRoot)
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
		SerialNum: sn,
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
	s, k := GetShardIdxAndKey(twigId)
	twig, ok := t.upperTree.activeTwigShards[s][k]
	if !ok {
		nullTwig := t.hasher.nullTwig()
		twig = &nullTwig
	}
	activeBits, ok := t.activeBitShards[s][k]
	if !ok {
		activeBits = &ActiveBits{}
	}
	path.RightOfTwig = GetRightPath(twig, activeBits, sn)

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
		cache := map[int64][32]byte{}
		for _, next := range posList {
			hash := t.getHashByNode(next.level, next.n, cache)
			if !yield(hash) {
				return
			}
		}
	}
}

func (t *Tree) getHashByNode(level uint8, nth uint64, cache map[int64][32]byte) common.Hash {
	var twigId uint64
	var levelStride uint64
	if level <= 12 {
		levelStride = 4096 >> level
		twigId = nth / levelStride
	}

	// left tree of twig
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
			return t.twigStorage.GetHashNode(twigId, idx, cache)
		}
	}

	// right tree of twig
	if level >= 8 && level <= 11 {
		s, k := GetShardIdxAndKey(twigId)
		activeBits, ok := t.activeBitShards[s][k]
		if ok {
			activeBits = &ActiveBits{}
		}
		selfId := (nth % levelStride) - levelStride/2
		if level == 8 {
			bits := activeBits.GetBits(int(selfId), 32)
			var hash common.Hash
			copy(hash[:], bits)
			return hash
		}
		twig, ok := t.upperTree.activeTwigShards[s][k]
		if !ok {
			nullTwig := t.hasher.nullTwig()
			twig = &nullTwig
		}

		if level == 9 {
			return twig.activeBitsMtl1[selfId]
		}
		if level == 10 {
			return twig.activeBitsMtl2[selfId]
		}
		if level == 11 {
			return twig.activeBitsMtl3
		}
	}

	// upper tree
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

// debug
func (t *Tree) Print() {
	/*var offset int64 = 0
	  buf := [2048]byte
	  for twigId := range self.youngestTwigId {
	      for _sn in twigId * 2048..(twigId + 1) * 2048 {
	          let n = self.entry_file_wr.entry_file.read_entry(offset, buf);
	          if n > buf.len() {
	              buf.resize(n, 0);
	              self.entry_file_wr.entry_file.read_entry(offset, buf);
	          }
	          offset += n as int64;

	          let entry_bz = EntryBz { bz: &buf[0..n] };
	          let entry = Entry::from_bz(&entry_bz);

	          println!(
	              "[entry] twig: {}, sn: {}, k: {}, v: {}",
	              twigId,
	              entry.serial_number,
	              hex::encode(entry.key),
	              hex::encode(entry.value)
	          );
	      }
	  }

	  let mut cache: HashMap<int64, Hash32> = HashMap::new();
	  for twigId in 0..self.youngestTwigId {
	      for _sn in twigId * 2048..(twigId + 1) * 2048 {
	          let _h = self.get_hash_by_node(0, _sn, cache);
	          println!(
	              "[hash] level: {}, nth: {}, hash: {}",
	              0,
	              _sn,
	              hex::encode(_h)
	          );
	      }
	  }*/
}
