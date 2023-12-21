package state

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/trie/vkutils"
)

// mode specifies how a tree location has been accessed
// for the byte value:
// * the first bit is set if the branch has been edited
// * the second bit is set if the branch has been read
type mode byte

const (
	AccessWitnessReadFlag  = mode(1)
	AccessWitnessWriteFlag = mode(2)
)

var zeroTreeIndex uint256.Int

// AccessWitness lists the locations of the state that are being accessed
// during the production of a block.
type AccessWitness struct {
	branches   map[branchAccessKey]mode
	chunks     map[chunkAccessKey]mode
	pointCache *vkutils.PointCache
}

func NewAccessWitness(pointCache *vkutils.PointCache) *AccessWitness {
	return &AccessWitness{
		branches:   make(map[branchAccessKey]mode),
		chunks:     make(map[chunkAccessKey]mode),
		pointCache: pointCache,
	}
}

// Merge is used to merge the witness that got generated during the execution
// of a tx, with the accumulation of witnesses that were generated during the
// execution of all the txs preceding this one in a given block.
func (aw *AccessWitness) Merge(other *AccessWitness) {
	for k := range other.branches {
		aw.branches[k] |= other.branches[k]
	}
	for k, chunk := range other.chunks {
		aw.chunks[k] |= chunk
	}
}

// Key returns, predictably, the list of keys that were touched during the
// buildup of the access witness.
func (aw *AccessWitness) Keys() [][]byte {
	// TODO: consider if parallelizing this is worth it, probably depending on len(aw.chunks).
	keys := make([][]byte, 0, len(aw.chunks))
	for chunk := range aw.chunks {
		basePoint := aw.pointCache.GetTreeKeyHeader(chunk.addr[:])
		key := vkutils.GetTreeKeyWithEvaluatedAddess(basePoint, &chunk.treeIndex, chunk.leafKey)
		keys = append(keys, key)
	}
	return keys
}

func (aw *AccessWitness) Copy() *AccessWitness {
	naw := &AccessWitness{
		branches:   make(map[branchAccessKey]mode),
		chunks:     make(map[chunkAccessKey]mode),
		pointCache: aw.pointCache,
	}
	naw.Merge(aw)
	return naw
}

func (aw *AccessWitness) TouchAndChargeProofOfAbsence(addr []byte) uint64 {
	var gas uint64
	gas += aw.TouchAddressOnReadAndComputeGas(addr, zeroTreeIndex, vkutils.VersionLeafKey)
	gas += aw.TouchAddressOnReadAndComputeGas(addr, zeroTreeIndex, vkutils.BalanceLeafKey)
	gas += aw.TouchAddressOnReadAndComputeGas(addr, zeroTreeIndex, vkutils.CodeSizeLeafKey)
	gas += aw.TouchAddressOnReadAndComputeGas(addr, zeroTreeIndex, vkutils.CodeKeccakLeafKey)
	gas += aw.TouchAddressOnReadAndComputeGas(addr, zeroTreeIndex, vkutils.NonceLeafKey)
	return gas
}

func (aw *AccessWitness) TouchAndChargeMessageCall(addr []byte) uint64 {
	var gas uint64
	gas += aw.TouchAddressOnReadAndComputeGas(addr, zeroTreeIndex, vkutils.VersionLeafKey)
	gas += aw.TouchAddressOnReadAndComputeGas(addr, zeroTreeIndex, vkutils.CodeSizeLeafKey)
	return gas
}

func (aw *AccessWitness) TouchAndChargeValueTransfer(callerAddr, targetAddr []byte) uint64 {
	var gas uint64
	gas += aw.TouchAddressOnWriteAndComputeGas(callerAddr, zeroTreeIndex, vkutils.BalanceLeafKey)
	gas += aw.TouchAddressOnWriteAndComputeGas(targetAddr, zeroTreeIndex, vkutils.BalanceLeafKey)
	return gas
}

// TouchAndChargeContractCreateInit charges access costs to initiate
// a contract creation
func (aw *AccessWitness) TouchAndChargeContractCreateInit(addr []byte, createSendsValue bool) uint64 {
	var gas uint64
	gas += aw.TouchAddressOnWriteAndComputeGas(addr, zeroTreeIndex, vkutils.VersionLeafKey)
	gas += aw.TouchAddressOnWriteAndComputeGas(addr, zeroTreeIndex, vkutils.NonceLeafKey)
	gas += aw.TouchAddressOnWriteAndComputeGas(addr, zeroTreeIndex, vkutils.CodeKeccakLeafKey)
	if createSendsValue {
		gas += aw.TouchAddressOnWriteAndComputeGas(addr, zeroTreeIndex, vkutils.BalanceLeafKey)
	}
	return gas
}

// TouchAndChargeContractCreateCompleted charges access access costs after
// the completion of a contract creation to populate the created account in
// the tree
func (aw *AccessWitness) TouchAndChargeContractCreateCompleted(addr []byte) uint64 {
	var gas uint64
	gas += aw.TouchAddressOnWriteAndComputeGas(addr, zeroTreeIndex, vkutils.VersionLeafKey)
	gas += aw.TouchAddressOnWriteAndComputeGas(addr, zeroTreeIndex, vkutils.BalanceLeafKey)
	gas += aw.TouchAddressOnWriteAndComputeGas(addr, zeroTreeIndex, vkutils.CodeSizeLeafKey)
	gas += aw.TouchAddressOnWriteAndComputeGas(addr, zeroTreeIndex, vkutils.CodeKeccakLeafKey)
	gas += aw.TouchAddressOnWriteAndComputeGas(addr, zeroTreeIndex, vkutils.NonceLeafKey)
	return gas
}

func (aw *AccessWitness) TouchTxOriginAndComputeGas(originAddr []byte) uint64 {
	var gas uint64
	gas += aw.TouchAddressOnReadAndComputeGas(originAddr, zeroTreeIndex, vkutils.VersionLeafKey)
	gas += aw.TouchAddressOnReadAndComputeGas(originAddr, zeroTreeIndex, vkutils.CodeSizeLeafKey)
	gas += aw.TouchAddressOnReadAndComputeGas(originAddr, zeroTreeIndex, vkutils.CodeKeccakLeafKey)
	gas += aw.TouchAddressOnWriteAndComputeGas(originAddr, zeroTreeIndex, vkutils.NonceLeafKey)
	gas += aw.TouchAddressOnWriteAndComputeGas(originAddr, zeroTreeIndex, vkutils.BalanceLeafKey)
	return gas
}

func (aw *AccessWitness) TouchTxExistingAndComputeGas(targetAddr []byte, sendsValue bool) uint64 {
	var gas uint64
	gas += aw.TouchAddressOnReadAndComputeGas(targetAddr, zeroTreeIndex, vkutils.VersionLeafKey)
	gas += aw.TouchAddressOnReadAndComputeGas(targetAddr, zeroTreeIndex, vkutils.CodeSizeLeafKey)
	gas += aw.TouchAddressOnReadAndComputeGas(targetAddr, zeroTreeIndex, vkutils.CodeKeccakLeafKey)
	gas += aw.TouchAddressOnReadAndComputeGas(targetAddr, zeroTreeIndex, vkutils.NonceLeafKey)
	if sendsValue {
		gas += aw.TouchAddressOnWriteAndComputeGas(targetAddr, zeroTreeIndex, vkutils.BalanceLeafKey)
	} else {
		gas += aw.TouchAddressOnReadAndComputeGas(targetAddr, zeroTreeIndex, vkutils.BalanceLeafKey)
	}
	return gas
}

func (aw *AccessWitness) TouchAddressOnWriteAndComputeGas(addr []byte, treeIndex uint256.Int, subIndex byte) uint64 {
	return aw.touchAddressAndChargeGas(addr, treeIndex, subIndex, true)
}

func (aw *AccessWitness) TouchAddressOnReadAndComputeGas(addr []byte, treeIndex uint256.Int, subIndex byte) uint64 {
	return aw.touchAddressAndChargeGas(addr, treeIndex, subIndex, false)
}

func (aw *AccessWitness) touchAddressAndChargeGas(addr []byte, treeIndex uint256.Int, subIndex byte, isWrite bool) uint64 {
	stemRead, selectorRead, stemWrite, selectorWrite, selectorFill := aw.touchAddress(addr, treeIndex, subIndex, isWrite)

	var gas uint64
	if stemRead {
		gas += params.WitnessBranchReadCost
	}
	if selectorRead {
		gas += params.WitnessChunkReadCost
	}
	if stemWrite {
		gas += params.WitnessBranchWriteCost
	}
	if selectorWrite {
		gas += params.WitnessChunkWriteCost
	}
	if selectorFill {
		gas += params.WitnessChunkFillCost
	}

	return gas
}

// touchAddress adds any missing access event to the witness.
func (aw *AccessWitness) touchAddress(addr []byte, treeIndex uint256.Int, subIndex byte, isWrite bool) (bool, bool, bool, bool, bool) {
	branchKey := newBranchAccessKey(addr, treeIndex)
	chunkKey := newChunkAccessKey(branchKey, subIndex)

	// Read access.
	var branchRead, chunkRead bool
	if _, hasStem := aw.branches[branchKey]; !hasStem {
		branchRead = true
		aw.branches[branchKey] = AccessWitnessReadFlag
	}
	if _, hasSelector := aw.chunks[chunkKey]; !hasSelector {
		chunkRead = true
		aw.chunks[chunkKey] = AccessWitnessReadFlag
	}

	// Write access.
	var branchWrite, chunkWrite, chunkFill bool
	if isWrite {
		if (aw.branches[branchKey] & AccessWitnessWriteFlag) == 0 {
			branchWrite = true
			aw.branches[branchKey] |= AccessWitnessWriteFlag
		}

		chunkValue := aw.chunks[chunkKey]
		if (chunkValue & AccessWitnessWriteFlag) == 0 {
			chunkWrite = true
			aw.chunks[chunkKey] |= AccessWitnessWriteFlag
		}

		// TODO: charge chunk filling costs if the leaf was previously empty in the state
	}

	return branchRead, chunkRead, branchWrite, chunkWrite, chunkFill
}

type branchAccessKey struct {
	addr      common.Address
	treeIndex uint256.Int
}

func newBranchAccessKey(addr []byte, treeIndex uint256.Int) branchAccessKey {
	var sk branchAccessKey
	copy(sk.addr[:], addr)
	sk.treeIndex = treeIndex
	return sk
}

type chunkAccessKey struct {
	branchAccessKey
	leafKey byte
}

func newChunkAccessKey(branchKey branchAccessKey, leafKey byte) chunkAccessKey {
	var lk chunkAccessKey
	lk.branchAccessKey = branchKey
	lk.leafKey = leafKey
	return lk
}
