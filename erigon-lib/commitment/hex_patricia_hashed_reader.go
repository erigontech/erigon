package commitment

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"runtime"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/length"
	ecrypto "github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/rlp"
	"github.com/erigontech/erigon-lib/trie"
	"github.com/erigontech/erigon-lib/types/accounts"
	"golang.org/x/crypto/sha3"

	witnesstypes "github.com/erigontech/erigon-lib/types/witness"

	libcommon "github.com/erigontech/erigon-lib/common"
)

// HexPatriciaHashed implements commitment based on patricia merkle tree with radix 16,
// with keys pre-hashed by keccak256
type HexPatriciaHashedReader struct {
	root cell // Root cell of the tree
	// How many rows (starting from row 0) are currently active and have corresponding selected columns
	// Last active row does not have selected column
	activeRows int
	// Length of the key that reflects current positioning of the grid. It may be larger than number of active rows,
	// if an account leaf cell represents multiple nibbles in the key
	currentKeyLen int
	accountKeyLen int
	// Rows of the grid correspond to the level of depth in the patricia tree
	// Columns of the grid correspond to pointers to the nodes further from the root
	grid          [128][16]cell // First 64 rows of this grid are for account trie, and next 64 rows are for storage trie
	currentKey    [128]byte     // For each row indicates which column is currently selected
	depths        [128]int      // For each row, the depth of cells in that row
	branchBefore  [128]bool     // For each row, whether there was a branch node in the database loaded in unfold
	touchMap      [128]uint16   // For each row, bitmap of cells that were either present before modification, or modified or deleted
	afterMap      [128]uint16   // For each row, bitmap of cells that were present after modification
	keccak        keccakState
	keccak2       keccakState
	rootChecked   bool // Set to false if it is not known whether the root is empty, set to true if it is checked
	rootTouched   bool
	rootPresent   bool
	trace         bool
	ctx           PatriciaContext
	hashAuxBuffer [128]byte     // buffer to compute cell hash or write hash-related things
	auxBuffer     *bytes.Buffer // auxiliary buffer used during branch updates encoding

	depthsToTxNum [129]uint64 // endTxNum of file with branch data for that depth
	hadToLoadL    map[uint64]skipStat

	//temp buffers
	accValBuf rlp.RlpEncodedBytes
}

// returned reader has to be set to some state before use. Use .SetState() for that and `SeekCommitment` to get this state
func NewHexPatriciaHashedReader(accountKeyLen int, ctx PatriciaContext, tmpdir string) *HexPatriciaHashedReader {
	hph := &HexPatriciaHashedReader{
		ctx:           ctx,
		keccak:        sha3.NewLegacyKeccak256().(keccakState),
		keccak2:       sha3.NewLegacyKeccak256().(keccakState),
		accountKeyLen: accountKeyLen,
		auxBuffer:     bytes.NewBuffer(make([]byte, 8192)),
		hadToLoadL:    make(map[uint64]skipStat),
		accValBuf:     make(rlp.RlpEncodedBytes, 128),
	}
	return hph
}

func (hph *HexPatriciaHashedReader) needUnfolding(hashedKey []byte) int {
	var cell *cell
	var depth int
	if hph.activeRows == 0 {
		if hph.trace {
			fmt.Printf("needUnfolding root, rootChecked = %t\n", hph.rootChecked)
		}
		if hph.root.hashedExtLen == 64 && hph.root.accountAddrLen > 0 && hph.root.storageAddrLen > 0 {
			// in case if root is a leaf node with storage and account, we need to derive storage part of a key
			if err := hph.root.deriveHashedKeys(depth, hph.keccak, hph.accountKeyLen); err != nil {
				log.Warn("deriveHashedKeys for root with storage", "err", err, "cell", hph.root.FullString())
				return 0
			}
			//copy(hph.currentKey[:], hph.root.hashedExtension[:])
			if hph.trace {
				fmt.Printf("derived prefix %x\n", hph.currentKey[:hph.currentKeyLen])
			}
		}
		if hph.root.hashedExtLen == 0 && hph.root.hashLen == 0 {
			if hph.rootChecked {
				return 0 // Previously checked, empty root, no unfolding needed
			}
			return 1 // Need to attempt to unfold the root
		}
		cell = &hph.root
	} else {
		col := int(hashedKey[hph.currentKeyLen])
		cell = &hph.grid[hph.activeRows-1][col]
		depth = hph.depths[hph.activeRows-1]
		if hph.trace {
			fmt.Printf("currentKey [%x] needUnfolding cell (%d, %x, depth=%d) cell.hash=[%x]\n", hph.currentKey[:hph.currentKeyLen], hph.activeRows-1, col, depth, cell.hash[:cell.hashLen])
		}
	}
	if len(hashedKey) <= depth {
		return 0
	}
	if cell.hashedExtLen == 0 {
		if cell.hashLen == 0 {
			// cell is empty, no need to unfold further
			return 0
		}
		// unfold branch node
		return 1
	}
	cpl := commonPrefixLen(hashedKey[depth:], cell.hashedExtension[:cell.hashedExtLen-1])
	if hph.trace {
		fmt.Printf("cpl=%d cell.hashedExtension=[%x] hashedKey[depth=%d:]=[%x]\n", cpl, cell.hashedExtension[:cell.hashedExtLen], depth, hashedKey[depth:])
	}
	unfolding := cpl + 1
	if depth < 64 && depth+unfolding > 64 {
		// This is to make sure that unfolding always breaks at the level where storage subtrees start
		unfolding = 64 - depth
		if hph.trace {
			fmt.Printf("adjusted unfolding=%d <- %d\n", unfolding, cpl+1)
		}
	}
	return unfolding
}

// unfoldBranchNode returns true if unfolding has been done
func (hph *HexPatriciaHashedReader) unfoldBranchNode(row, depth int, deleted bool) (bool, error) {
	key := hexNibblesToCompactBytes(hph.currentKey[:hph.currentKeyLen])
	branchData, fileEndTxNum, err := hph.ctx.Branch(key)
	if err != nil {
		return false, err
	}
	hph.depthsToTxNum[depth] = fileEndTxNum
	if len(branchData) >= 2 {
		branchData = branchData[2:] // skip touch map and keep the rest
	}
	if hph.trace {
		fmt.Printf("unfoldBranchNode prefix '%x', nibbles [%x] depth %d row %d '%x'\n", key, hph.currentKey[:hph.currentKeyLen], depth, row, branchData)
	}
	if !hph.rootChecked && hph.currentKeyLen == 0 && len(branchData) == 0 {
		// Special case - empty or deleted root
		hph.rootChecked = true
		return false, nil
	}
	if len(branchData) == 0 {
		log.Warn("got empty branch data during unfold", "key", hex.EncodeToString(key), "row", row, "depth", depth, "deleted", deleted)
		return false, fmt.Errorf("empty branch data read during unfold, prefix %x", key)
	}
	hph.branchBefore[row] = true
	bitmap := binary.BigEndian.Uint16(branchData[0:])
	pos := 2
	if deleted {
		// All cells come as deleted (touched but not present after)
		hph.afterMap[row] = 0
		hph.touchMap[row] = bitmap
	} else {
		hph.afterMap[row] = bitmap
		hph.touchMap[row] = 0
	}
	//fmt.Printf("unfoldBranchNode prefix '%x' [%x], afterMap = [%016b], touchMap = [%016b]\n", key, branchData, hph.afterMap[row], hph.touchMap[row])
	// Loop iterating over the set bits of modMask
	for bitset, j := bitmap, 0; bitset != 0; j++ {
		bit := bitset & -bitset
		nibble := bits.TrailingZeros16(bit)
		cell := &hph.grid[row][nibble]
		fieldBits := branchData[pos]
		pos++
		if pos, err = cell.fillFromFields(branchData, pos, cellFields(fieldBits)); err != nil {
			return false, fmt.Errorf("prefix [%x] branchData[%x]: %w", hph.currentKey[:hph.currentKeyLen], branchData, err)
		}
		if hph.trace {
			fmt.Printf("cell (%d, %x, depth=%d) %s\n", row, nibble, depth, cell.FullString())
		}

		// relies on plain account/storage key so need to be dereferenced before hashing
		if err = cell.deriveHashedKeys(depth, hph.keccak, hph.accountKeyLen); err != nil {
			return false, err
		}
		bitset ^= bit
	}
	return true, nil
}

func (hph *HexPatriciaHashedReader) unfold(hashedKey []byte, unfolding int) error {
	if hph.trace {
		fmt.Printf("unfold %d: activeRows: %d\n", unfolding, hph.activeRows)
	}
	var upCell *cell
	var touched, present bool
	var upDepth, depth int
	if hph.activeRows == 0 {
		if hph.rootChecked && hph.root.hashLen == 0 && hph.root.hashedExtLen == 0 {
			// No unfolding for empty root
			return nil
		}
		upCell = &hph.root
		touched = hph.rootTouched
		present = hph.rootPresent
		if hph.trace {
			fmt.Printf("unfold root: touched: %t present: %t %s\n", touched, present, upCell.FullString())
		}
	} else {
		upDepth = hph.depths[hph.activeRows-1]
		nib := hashedKey[upDepth-1]
		upCell = &hph.grid[hph.activeRows-1][nib]
		touched = hph.touchMap[hph.activeRows-1]&(uint16(1)<<nib) != 0
		present = hph.afterMap[hph.activeRows-1]&(uint16(1)<<nib) != 0
		if hph.trace {
			fmt.Printf("upCell (%d, %x, updepth=%d) touched: %t present: %t\n", hph.activeRows-1, nib, upDepth, touched, present)
		}
		hph.currentKey[hph.currentKeyLen] = nib
		hph.currentKeyLen++
	}
	row := hph.activeRows
	for i := 0; i < 16; i++ {
		hph.grid[row][i].reset()
	}
	hph.touchMap[row], hph.afterMap[row] = 0, 0
	hph.branchBefore[row] = false

	if upCell.hashedExtLen == 0 {
		depth = upDepth + 1
		unfolded, err := hph.unfoldBranchNode(row, depth, touched && !present)
		if err != nil {
			return err
		}
		if unfolded {
			hph.depths[hph.activeRows] = depth
			hph.activeRows++
		}
		// Return here to prevent activeRow from being incremented when !unfolded
		return nil
	}

	var nibble, copyLen int
	if upCell.hashedExtLen >= unfolding {
		depth = upDepth + unfolding
		nibble = int(upCell.hashedExtension[unfolding-1])
		copyLen = unfolding - 1
	} else {
		depth = upDepth + upCell.hashedExtLen
		nibble = int(upCell.hashedExtension[upCell.hashedExtLen-1])
		copyLen = upCell.hashedExtLen - 1
	}

	if touched {
		hph.touchMap[row] = uint16(1) << nibble
	}
	if present {
		hph.afterMap[row] = uint16(1) << nibble
	}

	cell := &hph.grid[row][nibble]
	cell.fillFromUpperCell(upCell, depth, min(unfolding, upCell.hashedExtLen))
	if hph.trace {
		fmt.Printf("unfolded cell (%d, %x, depth=%d) %s\n", row, nibble, depth, cell.FullString())
	}

	if row >= 64 {
		cell.accountAddrLen = 0
	}
	if copyLen > 0 {
		copy(hph.currentKey[hph.currentKeyLen:], upCell.hashedExtension[:copyLen])
	}
	hph.currentKeyLen += copyLen

	hph.depths[hph.activeRows] = depth
	hph.activeRows++
	return nil
}

func (hph *HexPatriciaHashedReader) needFolding(hashedKey []byte) bool {
	return !bytes.HasPrefix(hashedKey, hph.currentKey[:hph.currentKeyLen])
}

func (hph *HexPatriciaHashedReader) PrintGrid() {
	fmt.Printf("GRID:\n")
	for row := 0; row < hph.activeRows; row++ {
		fmt.Printf("row %d depth %d:\n", row, hph.depths[row])
		for col := 0; col < 16; col++ {
			cell := &hph.grid[row][col]
			if cell.hashedExtLen > 0 || cell.accountAddrLen > 0 {
				var cellHash []byte
				cellHash, _, _, err := hph.computeCellHashWithStorage(cell, hph.depths[row], nil)
				if err != nil {
					panic("failed to compute cell hash")
				}
				fmt.Printf("\t %x: %v cellHash=%x, \n", col, cell, cellHash)
			} else {
				fmt.Printf("\t %x: %v , \n", col, cell)
			}
		}
		fmt.Printf("\n")
	}
	fmt.Printf("\n")
}

// Generate the block witness. This works by loading each key from the list of updates (they are not really updates since we won't modify the trie,
// but currently need to be defined like that for the fold/unfold algorithm) into the grid and traversing the grid to convert it into `trie.Trie`.
// All the individual tries are combined to create the final witness trie.
// Because the grid is lacking information about the code in smart contract accounts which is also part of the witness, we need to provide that as an input parameter to this function (`codeReads`)
func (hph *HexPatriciaHashedReader) GenerateWitness(ctx context.Context, reads *Updates, codeReads map[libcommon.Hash]witnesstypes.CodeWithHash, expectedRootHash []byte, logPrefix string) (witnessTrie *trie.Trie, rootHash []byte, err error) {
	var (
		m  runtime.MemStats
		ki uint64

		readCount = reads.Size()
		logEvery  = time.NewTicker(20 * time.Second)
	)
	defer logEvery.Stop()
	var tries []*trie.Trie = make([]*trie.Trie, 0, len(reads.keys)) // slice of tries, i.e the witness for each key, these will be all merged into single trie
	err = reads.HashSort(ctx, func(hashedKey, plainKey []byte, stateUpdate *Update) error {
		select {
		case <-logEvery.C:
			dbg.ReadMemStats(&m)
			log.Info(fmt.Sprintf("[%s][agg] computing trie", logPrefix),
				"progress", fmt.Sprintf("%s/%s", common.PrettyCounter(ki), common.PrettyCounter(readCount)),
				"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))

		default:
		}

		var tr *trie.Trie
		var computedRootHash []byte

		fmt.Printf("\n%d/%d) plainKey [%x] hashedKey [%x] currentKey [%x]\n", ki+1, readCount, plainKey, hashedKey, hph.currentKey[:hph.currentKeyLen])
		if len(plainKey) == 20 { // account
			account, err := hph.ctx.Account(plainKey)
			if err != nil {
				return fmt.Errorf("account with plainkey=%x not found: %w", plainKey, err)
			} else {
				addrHash := ecrypto.Keccak256(plainKey)
				fmt.Printf("account with plainKey=%x, addrHash=%x FOUND = %v\n", plainKey, addrHash, account)
			}
		} else {
			storage, err := hph.ctx.Storage(plainKey)
			if err != nil {
				return fmt.Errorf("storage with plainkey=%x not found: %w", plainKey, err)
			}
			fmt.Printf("storage found = %v\n", storage.Storage)
		}
		// Keep folding until the currentKey is the prefix of the key we modify
		for hph.needFolding(hashedKey) {
			if err := hph.fold(); err != nil {
				return fmt.Errorf("fold: %w", err)
			}
		}
		// Now unfold until we step on an empty cell
		for unfolding := hph.needUnfolding(hashedKey); unfolding > 0; unfolding = hph.needUnfolding(hashedKey) {
			if err := hph.unfold(hashedKey, unfolding); err != nil {
				return fmt.Errorf("unfold: %w", err)
			}
		}
		hph.PrintGrid()

		// convert grid to trie.Trie
		tr, err = hph.ToTrie(hashedKey, codeReads) // build witness trie for this key, based on the current state of the grid
		if err != nil {
			return err
		}
		computedRootHash = tr.Root()
		fmt.Printf("computedRootHash = %x\n", computedRootHash)

		if !bytes.Equal(computedRootHash, expectedRootHash) {
			err = fmt.Errorf("root hash mismatch computedRootHash(%x)!=expectedRootHash(%x)", computedRootHash, expectedRootHash)
			return err
		}

		tries = append(tries, tr)
		ki++
		return nil
	})

	if err != nil {
		return nil, nil, fmt.Errorf("hash sort failed: %w", err)
	}

	// Folding everything up to the root
	for hph.activeRows > 0 {
		if err := hph.fold(); err != nil {
			return nil, nil, fmt.Errorf("final fold: %w", err)
		}
	}

	rootHash, err = hph.RootHash()
	if err != nil {
		return nil, nil, fmt.Errorf("root hash evaluation failed: %w", err)
	}
	if hph.trace {
		fmt.Printf("root hash %x reads %d\n", rootHash, readCount)
	}

	// merge all individual tries
	witnessTrie, err = trie.MergeTries(tries)
	if err != nil {
		return nil, nil, err
	}

	witnessTrieRootHash := witnessTrie.Root()

	fmt.Printf("mergedTrieRootHash = %x\n", witnessTrieRootHash)

	if !bytes.Equal(witnessTrieRootHash, expectedRootHash) {
		return nil, nil, fmt.Errorf("root hash mismatch witnessTrieRootHash(%x)!=expectedRootHash(%x)", witnessTrieRootHash, expectedRootHash)
	}

	return witnessTrie, rootHash, nil
}

func (hph *HexPatriciaHashedReader) Process(ctx context.Context, updates *Updates, logPrefix string) (rootHash []byte, err error) {
	panic("Process must not be called for HexPatriciaHashedReader")
}

func (hph *HexPatriciaHashedReader) Traverse(ctx context.Context, reads *Updates, logPrefix string) (rootHash []byte, err error) {
	var (
		m      runtime.MemStats
		ki     uint64
		update *Update

		readCount = reads.Size()
		logEvery  = time.NewTicker(20 * time.Second)
	)
	defer logEvery.Stop()
	//hph.trace = true

	err = reads.HashSort(ctx, func(hashedKey, plainKey []byte, stateUpdate *Update) error {
		select {
		case <-logEvery.C:
			dbg.ReadMemStats(&m)
			log.Info(fmt.Sprintf("[%s][agg] computing trie", logPrefix),
				"progress", fmt.Sprintf("%s/%s", common.PrettyCounter(ki), common.PrettyCounter(readCount)),
				"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))

		default:
		}

		if hph.trace {
			fmt.Printf("\n%d/%d) plainKey [%x] hashedKey [%x] currentKey [%x]\n", ki+1, readCount, plainKey, hashedKey, hph.currentKey[:hph.currentKeyLen])
		}
		// Keep folding until the currentKey is the prefix of the key we modify
		for hph.needFolding(hashedKey) {
			if err := hph.fold(); err != nil {
				return fmt.Errorf("fold: %w", err)
			}
		}
		// Now unfold until we step on an empty cell
		for unfolding := hph.needUnfolding(hashedKey); unfolding > 0; unfolding = hph.needUnfolding(hashedKey) {
			if err := hph.unfold(hashedKey, unfolding); err != nil {
				return fmt.Errorf("unfold: %w", err)
			}
		}

		if stateUpdate == nil {
			// Update the cell
			if len(plainKey) == hph.accountKeyLen {
				update, err = hph.ctx.Account(plainKey)
				if err != nil {
					return fmt.Errorf("GetAccount for key %x failed: %w", plainKey, err)
				}
			} else {
				update, err = hph.ctx.Storage(plainKey)
				if err != nil {
					return fmt.Errorf("GetStorage for key %x failed: %w", plainKey, err)
				}
			}
		} else {
			if update == nil {
				update = stateUpdate
			} else {
				update.Reset()
				update.Merge(stateUpdate)
			}
		}
		hph.updateCell(plainKey, hashedKey, update) // todo this may update the cell with value obtained with hph.ctx.

		mxTrieProcessedKeys.Inc()
		ki++
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("hash sort failed: %w", err)
	}

	// Folding everything up to the root
	for hph.activeRows > 0 {
		if err := hph.fold(); err != nil {
			return nil, fmt.Errorf("final fold: %w", err)
		}
	}

	rootHash, err = hph.RootHash()
	if err != nil {
		return nil, fmt.Errorf("root hash evaluation failed: %w", err)
	}
	if hph.trace {
		fmt.Printf("root hash %x reads %d\n", rootHash, readCount)
	}
	return rootHash, nil
}

func (hph *HexPatriciaHashedReader) SetTrace(trace bool) { hph.trace = trace }

func (hph *HexPatriciaHashedReader) Variant() TrieVariant { return VariantHexPatriciaTrieReader }

// Reset allows HexPatriciaHashed instance to be reused for the new commitment calculation
func (hph *HexPatriciaHashedReader) Reset() {
	hph.root.reset()
	hph.rootTouched = false
	hph.rootChecked = false
	hph.rootPresent = true
}

func (hph *HexPatriciaHashedReader) ResetContext(ctx PatriciaContext) {
	hph.ctx = ctx
}

// buf expected to be encoded hph state. Decode state and set up hph to that state.
func (hph *HexPatriciaHashedReader) SetState(buf []byte) error {
	hph.Reset()

	if buf == nil {
		// reset state to 'empty'
		hph.currentKeyLen = 0
		hph.rootChecked = false
		hph.rootTouched = false
		hph.rootPresent = false
		hph.activeRows = 0

		for i := 0; i < len(hph.depths); i++ {
			hph.depths[i] = 0
			hph.branchBefore[i] = false
			hph.touchMap[i] = 0
			hph.afterMap[i] = 0
		}
		return nil
	}
	if hph.activeRows != 0 {
		return errors.New("target trie has active rows, could not reset state before fold")
	}

	var s state
	if err := s.Decode(buf); err != nil {
		return err
	}

	if err := hph.root.Decode(s.Root); err != nil {
		return err
	}
	hph.rootChecked = s.RootChecked
	hph.rootTouched = s.RootTouched
	hph.rootPresent = s.RootPresent

	copy(hph.depths[:], s.Depths[:])
	copy(hph.branchBefore[:], s.BranchBefore[:])
	copy(hph.touchMap[:], s.TouchMap[:])
	copy(hph.afterMap[:], s.AfterMap[:])

	if hph.root.accountAddrLen > 0 {
		if hph.ctx == nil {
			panic("nil ctx")
		}

		update, err := hph.ctx.Account(hph.root.accountAddr[:hph.root.accountAddrLen])
		if err != nil {
			return err
		}
		hph.root.setFromUpdate(update)
	}
	if hph.root.storageAddrLen > 0 {
		if hph.ctx == nil {
			panic("nil ctx")
		}
		update, err := hph.ctx.Storage(hph.root.storageAddr[:hph.root.storageAddrLen])
		if err != nil {
			return err
		}
		hph.root.setFromUpdate(update)
		//hph.root.deriveHashedKeys(0, hph.keccak, hph.accountKeyLen)
	}

	return nil
}

// The purpose of fold is to reduce hph.currentKey[:hph.currentKeyLen]. It should be invoked
// until that current key becomes a prefix of hashedKey that we will process next
// (in other words until the needFolding function returns 0)
func (hph *HexPatriciaHashedReader) fold() (err error) {
	updateKeyLen := hph.currentKeyLen
	if hph.activeRows == 0 {
		return errors.New("cannot fold - no active rows")
	}
	if hph.trace {
		fmt.Printf("fold [%x] activeRows: %d touchMap: %016b afterMap: %016b\n", hph.currentKey[:hph.currentKeyLen], hph.activeRows, hph.touchMap[hph.activeRows-1], hph.afterMap[hph.activeRows-1])
	}
	// Move information to the row above
	var upCell *cell
	var nibble, upDepth int
	row := hph.activeRows - 1
	if row == 0 {
		if hph.trace {
			fmt.Printf("fold: parent is root %s\n", hph.root.FullString())
		}
		upCell = &hph.root
	} else {
		upDepth = hph.depths[hph.activeRows-2]
		nibble = int(hph.currentKey[upDepth-1])
		if hph.trace {
			fmt.Printf("fold: parent (%d, %x, depth=%d)\n", row-1, nibble, upDepth)
		}
		upCell = &hph.grid[row-1][nibble]
	}

	depth := hph.depths[row]
	updateKey := hexNibblesToCompactBytes(hph.currentKey[:updateKeyLen])
	partsCount := bits.OnesCount16(hph.afterMap[row])
	defer func() { hph.depthsToTxNum[depth] = 0 }()

	if hph.trace {
		fmt.Printf("fold: (row=%d, {%s}, depth=%d) prefix [%x] touchMap: %016b afterMap: %016b \n",
			row, updatedNibs(hph.touchMap[row]&hph.afterMap[row]), depth, hph.currentKey[:hph.currentKeyLen], hph.touchMap[row], hph.afterMap[row])
	}

	switch partsCount {
	case 0: // Everything deleted
		if hph.touchMap[row] != 0 {
			if row == 0 {
				// Root is deleted because the tree is empty
				hph.rootTouched = true
				hph.rootPresent = false
			} else if upDepth == 64 {
				// Special case - all storage items of an account have been deleted, but it does not automatically delete the account, just makes it empty storage
				// Therefore we are not propagating deletion upwards, but turn it into a modification
				hph.touchMap[row-1] |= uint16(1) << nibble
			} else {
				// Deletion is propagated upwards
				hph.touchMap[row-1] |= uint16(1) << nibble
				hph.afterMap[row-1] &^= uint16(1) << nibble
			}
		}

		upCell.reset()
		// if hph.branchBefore[row] {
		// 	_, err := hph.branchEncoder.CollectUpdate(hph.ctx, updateKey, 0, hph.touchMap[row], 0, RetrieveCellNoop)
		// 	if err != nil {
		// 		return fmt.Errorf("failed to encode leaf node update: %w", err)
		// 	}
		// }
		hph.activeRows--
		if upDepth > 0 {
			hph.currentKeyLen = upDepth - 1
		} else {
			hph.currentKeyLen = 0
		}
	case 1: // Leaf or extension node
		if hph.touchMap[row] != 0 {
			// any modifications
			if row == 0 {
				hph.rootTouched = true
			} else {
				// Modification is propagated upwards
				hph.touchMap[row-1] |= uint16(1) << nibble
			}
		}
		nibble := bits.TrailingZeros16(hph.afterMap[row])
		cell := &hph.grid[row][nibble]
		upCell.extLen = 0
		upCell.stateHashLen = 0
		upCell.fillFromLowerCell(cell, depth, hph.currentKey[upDepth:hph.currentKeyLen], nibble)
		// Delete if it existed
		// if hph.branchBefore[row] {
		// 	//fmt.Printf("delete existed row %d prefix %x\n", row, updateKey)
		// 	_, err := hph.branchEncoder.CollectUpdate(hph.ctx, updateKey, 0, hph.touchMap[row], 0, RetrieveCellNoop)
		// 	if err != nil {
		// 		return fmt.Errorf("failed to encode leaf node update: %w", err)
		// 	}
		// }
		hph.activeRows--
		hph.currentKeyLen = max(upDepth-1, 0)
		if hph.trace {
			fmt.Printf("formed leaf (%d %x, depth=%d) [%x] %s\n", row, nibble, depth, updateKey, cell.FullString())
		}
	default: // Branch node
		if hph.touchMap[row] != 0 { // any modifications
			if row == 0 {
				hph.rootTouched = true
				hph.rootPresent = true
			} else {
				// Modification is propagated upwards
				hph.touchMap[row-1] |= uint16(1) << nibble
			}
		}
		if !hph.branchBefore[row] {
			// There was no branch node before, so we need to touch even the singular child that existed
			hph.touchMap[row] |= hph.afterMap[row]
		}

		// Calculate total length of all hashes
		// totalBranchLen := 17 - partsCount // For every empty cell, one byte
		for bitset, j := hph.afterMap[row], 0; bitset != 0; j++ {
			bit := bitset & -bitset
			nibble := bits.TrailingZeros16(bit)
			cell := &hph.grid[row][nibble]

			/* memoization of state hashes*/
			counters := hph.hadToLoadL[hph.depthsToTxNum[depth]]
			if cell.stateHashLen > 0 && (hph.touchMap[row]&hph.afterMap[row]&uint16(1<<nibble) > 0 || cell.stateHashLen != length.Hash) {
				// drop state hash if updated or hashLen < 32 (corner case, may even not encode such leaf hashes)
				if hph.trace {
					fmt.Printf("DROP hash for (%d, %x, depth=%d) %s\n", row, nibble, depth, cell.FullString())
				}
				cell.stateHashLen = 0
				hadToReset.Add(1)
				if cell.accountAddrLen > 0 {
					counters.accReset++
				}
				if cell.storageAddrLen > 0 {
					counters.storReset++
				}
			}

			if cell.stateHashLen == 0 { // load state if needed
				if !cell.loaded.account() && cell.accountAddrLen > 0 {
					upd, err := hph.ctx.Account(cell.accountAddr[:cell.accountAddrLen])
					if err != nil {
						return fmt.Errorf("failed to get account: %w", err)
					}
					cell.setFromUpdate(upd)
					// if update is empty, loaded flag was not updated so do it manually
					cell.loaded = cell.loaded.addFlag(cellLoadAccount)
					counters.accLoaded++
				}
				if !cell.loaded.storage() && cell.storageAddrLen > 0 {
					upd, err := hph.ctx.Storage(cell.storageAddr[:cell.storageAddrLen])
					if err != nil {
						return fmt.Errorf("failed to get storage: %w", err)
					}
					cell.setFromUpdate(upd)
					// if update is empty, loaded flag was not updated so do it manually
					cell.loaded = cell.loaded.addFlag(cellLoadStorage)
					counters.storLoaded++
				}
				// computeCellHash can reset hash as well so have to check if node has been skipped  right after computeCellHash.
			}
			hph.hadToLoadL[hph.depthsToTxNum[depth]] = counters
			/* end of memoization */
			ch, err := hph.computeCellHash(cell, depth, hph.hashAuxBuffer[:0])
			if err != nil {
				return fmt.Errorf("failed to compute cell hash: %w", err)
			}
			_ = ch

			// totalBranchLen += hph.computeCellHashLen(cell, depth)
			bitset ^= bit
		}

		// hph.keccak2.Reset()
		// pt := rlp.GenerateStructLen(hph.hashAuxBuffer[:], totalBranchLen)
		// if _, err := hph.keccak2.Write(hph.hashAuxBuffer[:pt]); err != nil {
		// 	return err
		// }

		// b := [...]byte{0x80}
		// cellGetter := hph.createCellGetter(b[:], updateKey, row, depth)

		// _, lastNibble, err := hph.branchEncoder.EncodeBranch(bitmap, hph.touchMap[row], hph.afterMap[row], cellGetter)
		// if err != nil {
		// 	return fmt.Errorf("failed to encode branch update: %w", err)
		// }
		// for i := lastNibble; i < 17; i++ {
		// 	if _, err := hph.keccak2.Write(b[:]); err != nil {
		// 		return err
		// 	}
		// 	if hph.trace {
		// 		fmt.Printf("  %x: empty(%d, %x, depth=%d)\n", i, row, i, depth)
		// 	}
		// }
		upCell.extLen = depth - upDepth - 1
		upCell.hashedExtLen = upCell.extLen
		if upCell.extLen > 0 {
			copy(upCell.extension[:], hph.currentKey[upDepth:hph.currentKeyLen])
			copy(upCell.hashedExtension[:], hph.currentKey[upDepth:hph.currentKeyLen])
		}
		if depth < 64 {
			upCell.accountAddrLen = 0
		}
		upCell.storageAddrLen = 0
		// upCell.hashLen = 32
		//
		// TODO we suppose that we did not made any changes so branch hash should not really change, we just read existing vals from db
		// if _, err := hph.keccak2.Read(upCell.hash[:]); err != nil {
		// 	return err
		// }
		if hph.trace {
			fmt.Printf("} [%x]\n", upCell.hash[:])
		}
		hph.activeRows--
		if upDepth > 0 {
			hph.currentKeyLen = upDepth - 1
		} else {
			hph.currentKeyLen = 0
		}
	}
	return nil
}

func (hph *HexPatriciaHashedReader) deleteCell(hashedKey []byte) {
	if hph.trace {
		fmt.Printf("deleteCell, activeRows = %d\n", hph.activeRows)
	}
	var cell *cell
	if hph.activeRows == 0 { // Remove the root
		cell = &hph.root
		hph.rootTouched, hph.rootPresent = true, false
	} else {
		row := hph.activeRows - 1
		if hph.depths[row] < len(hashedKey) {
			if hph.trace {
				fmt.Printf("deleteCell skipping spurious delete depth=%d, len(hashedKey)=%d\n", hph.depths[row], len(hashedKey))
			}
			return
		}
		nibble := int(hashedKey[hph.currentKeyLen])
		cell = &hph.grid[row][nibble]
		col := uint16(1) << nibble
		if hph.afterMap[row]&col != 0 {
			// Prevent "spurios deletions", i.e. deletion of absent items
			hph.touchMap[row] |= col
			hph.afterMap[row] &^= col
			if hph.trace {
				fmt.Printf("deleteCell setting (%d, %x)\n", row, nibble)
			}
		} else {
			if hph.trace {
				fmt.Printf("deleteCell ignoring (%d, %x)\n", row, nibble)
			}
		}
	}
	cell.reset()
}

// fetches cell by key and set touch/after maps. Requires that prefix to be already unfolded
func (hph *HexPatriciaHashedReader) updateCell(plainKey, hashedKey []byte, u *Update) (cell *cell) {
	if u.Deleted() {
		hph.deleteCell(hashedKey)
		return nil
	}

	var depth int
	if hph.activeRows == 0 {
		cell = &hph.root
		hph.rootTouched, hph.rootPresent = true, true
	} else {
		row := hph.activeRows - 1
		depth = hph.depths[row]
		nibble := int(hashedKey[hph.currentKeyLen])
		cell = &hph.grid[row][nibble]
		col := uint16(1) << nibble

		hph.touchMap[row] |= col
		hph.afterMap[row] |= col
		if hph.trace {
			fmt.Printf("updateCell setting (%d, %x, depth=%d)\n", row, nibble, depth)
		}
	}
	if cell.hashedExtLen == 0 {
		copy(cell.hashedExtension[:], hashedKey[depth:])
		cell.hashedExtLen = len(hashedKey) - depth
		if hph.trace {
			fmt.Printf("set downHasheKey=[%x]\n", cell.hashedExtension[:cell.hashedExtLen])
		}
	} else {
		if hph.trace {
			fmt.Printf("keep downHasheKey=[%x]\n", cell.hashedExtension[:cell.hashedExtLen])
		}
	}
	if len(plainKey) == hph.accountKeyLen {
		cell.accountAddrLen = len(plainKey)
		copy(cell.accountAddr[:], plainKey)

		copy(cell.CodeHash[:], EmptyCodeHash) // todo check
	} else { // set storage key
		cell.storageAddrLen = len(plainKey)
		copy(cell.storageAddr[:], plainKey)
	}
	cell.stateHashLen = 0

	cell.setFromUpdate(u)
	if hph.trace {
		fmt.Printf("updateCell %x => %s\n", plainKey, u.String())
	}
	return cell
}

func (hph *HexPatriciaHashedReader) RootHash() ([]byte, error) {
	hph.root.stateHashLen = 0
	rootHash, err := hph.computeCellHash(&hph.root, 0, nil)
	if err != nil {
		return nil, err
	}
	return rootHash[1:], nil // first byte is 128+hash_len=160
}

func (hph *HexPatriciaHashedReader) computeCellHash(cell *cell, depth int, buf []byte) ([]byte, error) {
	var err error
	var storageRootHash [length.Hash]byte
	var storageRootHashIsSet bool
	if cell.storageAddrLen > 0 {
		var hashedKeyOffset int
		if depth >= 64 {
			hashedKeyOffset = depth - 64
		}
		singleton := depth <= 64
		koffset := hph.accountKeyLen
		if depth == 0 && cell.accountAddrLen == 0 {
			// if account key is empty, then we need to hash storage key from the key beginning
			koffset = 0
		}
		if err = cell.hashStorageKey(hph.keccak, koffset, 0, hashedKeyOffset); err != nil {
			return nil, err
		}
		cell.hashedExtension[64-hashedKeyOffset] = 16 // Add terminator

		if cell.stateHashLen > 0 {
			hph.keccak.Reset()
			if hph.trace {
				fmt.Printf("REUSED stateHash %x spk %x\n", cell.stateHash[:cell.stateHashLen], cell.storageAddr[:cell.storageAddrLen])
			}
			mxTrieStateSkipRate.Inc()
			skippedLoad.Add(1)
			if !singleton {
				return append(append(buf[:0], byte(160)), cell.stateHash[:cell.stateHashLen]...), nil
			}
			storageRootHashIsSet = true
			storageRootHash = *(*[length.Hash]byte)(cell.stateHash[:cell.stateHashLen])
		} else {
			if !cell.loaded.storage() {
				return nil, fmt.Errorf("storage %x was not loaded as expected: cell %v", cell.storageAddr[:cell.storageAddrLen], cell.String())
				// update, err := hph.ctx.Storage(cell.storageAddr[:cell.storageAddrLen])
				// if err != nil {
				// 	return nil, err
				// }
				// cell.setFromUpdate(update)
			}

			leafHash, err := hph.leafHashWithKeyVal(buf, cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], singleton)
			if err != nil {
				return nil, err
			}
			if hph.trace {
				fmt.Printf("leafHashWithKeyVal(singleton=%t) {%x} for [%x]=>[%x] %v\n",
					singleton, leafHash, cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], cell.String())
			}
			if !singleton {
				copy(cell.stateHash[:], leafHash[1:])
				cell.stateHashLen = len(leafHash) - 1
				return leafHash, nil
			}
			storageRootHash = *(*[length.Hash]byte)(leafHash[1:])
			storageRootHashIsSet = true
			cell.stateHashLen = 0
			hadToReset.Add(1)
		}
	}
	if cell.accountAddrLen > 0 {
		if err := cell.hashAccKey(hph.keccak, depth); err != nil {
			return nil, err
		}
		cell.hashedExtension[64-depth] = 16 // Add terminator
		if !storageRootHashIsSet {
			if cell.extLen > 0 { // Extension
				if cell.hashLen == 0 {
					return nil, errors.New("computeCellHash extension without hash")
				}
				if hph.trace {
					fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.hash[:cell.hashLen])
				}
				if storageRootHash, err = hph.extensionHash(cell.extension[:cell.extLen], cell.hash[:cell.hashLen]); err != nil {
					return nil, err
				}
				if hph.trace {
					fmt.Printf("EXTENSION HASH %x DROPS stateHash\n", storageRootHash)
				}
				cell.stateHashLen = 0
				hadToReset.Add(1)
			} else if cell.hashLen > 0 {
				storageRootHash = cell.hash
			} else {
				storageRootHash = *(*[length.Hash]byte)(EmptyRootHash)
			}
		}
		if !cell.loaded.account() {
			if cell.stateHashLen > 0 {
				hph.keccak.Reset()

				mxTrieStateSkipRate.Inc()
				skippedLoad.Add(1)
				if hph.trace {
					fmt.Printf("REUSED stateHash %x apk %x\n", cell.stateHash[:cell.stateHashLen], cell.accountAddr[:cell.accountAddrLen])
				}
				return append(append(buf[:0], byte(160)), cell.stateHash[:cell.stateHashLen]...), nil
			}
			// storage root update or extension update could invalidate older stateHash, so we need to reload state
			update, err := hph.ctx.Account(cell.accountAddr[:cell.accountAddrLen])
			if err != nil {
				return nil, err
			}
			cell.setFromUpdate(update)
		}

		valLen := cell.accountForHashing(hph.accValBuf, storageRootHash)
		buf, err = hph.accountLeafHashWithKey(buf, cell.hashedExtension[:65-depth], hph.accValBuf[:valLen])
		if err != nil {
			return nil, err
		}
		if hph.trace {
			fmt.Printf("accountLeafHashWithKey {%x} (memorised) for [%x]=>[%x]\n", buf, cell.hashedExtension[:65-depth], hph.accValBuf[:valLen])
		}
		copy(cell.stateHash[:], buf[1:])
		cell.stateHashLen = len(buf) - 1
		return buf, nil
	}

	buf = append(buf, 0x80+32)
	if cell.extLen > 0 { // Extension
		if cell.hashLen > 0 {
			if hph.trace {
				fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.hash[:cell.hashLen])
			}
			if storageRootHash, err = hph.extensionHash(cell.extension[:cell.extLen], cell.hash[:cell.hashLen]); err != nil {
				return nil, err
			}
			buf = append(buf, storageRootHash[:]...)
		} else {
			return nil, errors.New("computeCellHash extension without hash")
		}
	} else if cell.hashLen > 0 {
		buf = append(buf, cell.hash[:cell.hashLen]...)
	} else if storageRootHashIsSet {
		buf = append(buf, storageRootHash[:]...)
		copy(cell.hash[:], storageRootHash[:])
		cell.hashLen = len(storageRootHash)
	} else {
		buf = append(buf, EmptyRootHash...)
	}
	return buf, nil
}

func (hph *HexPatriciaHashedReader) completeLeafHash(buf []byte, compactLen int, key []byte, compact0 byte, ni int, val rlp.RlpSerializable, singleton bool) ([]byte, error) {
	// Compute the total length of binary representation
	var kp, kl int
	var keyPrefix [1]byte
	if compactLen > 1 {
		keyPrefix[0] = 0x80 + byte(compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}

	totalLen := kp + kl + val.DoubleRLPLen()
	var lenPrefix [4]byte
	pl := rlp.GenerateStructLen(lenPrefix[:], totalLen)
	canEmbed := !singleton && totalLen+pl < length.Hash
	var writer io.Writer
	if canEmbed {
		//hph.byteArrayWriter.Setup(buf)
		hph.auxBuffer.Reset()
		writer = hph.auxBuffer
	} else {
		hph.keccak.Reset()
		writer = hph.keccak
	}
	if _, err := writer.Write(lenPrefix[:pl]); err != nil {
		return nil, err
	}
	if _, err := writer.Write(keyPrefix[:kp]); err != nil {
		return nil, err
	}
	b := [1]byte{compact0}
	if _, err := writer.Write(b[:]); err != nil {
		return nil, err
	}
	for i := 1; i < compactLen; i++ {
		b[0] = key[ni]*16 + key[ni+1]
		if _, err := writer.Write(b[:]); err != nil {
			return nil, err
		}
		ni += 2
	}
	var prefixBuf [8]byte
	if err := val.ToDoubleRLP(writer, prefixBuf[:]); err != nil {
		return nil, err
	}
	if canEmbed {
		buf = hph.auxBuffer.Bytes()
	} else {
		var hashBuf [33]byte
		hashBuf[0] = 0x80 + length.Hash
		if _, err := hph.keccak.Read(hashBuf[1:]); err != nil {
			return nil, err
		}
		buf = append(buf, hashBuf[:]...)
	}
	return buf, nil
}

func (hph *HexPatriciaHashedReader) leafHashWithKeyVal(buf, key []byte, val rlp.RlpSerializableBytes, singleton bool) ([]byte, error) {
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	compactLen = (len(key)-1)/2 + 1
	if len(key)&1 == 0 {
		compact0 = 0x30 + key[0] // Odd: (3<<4) + first nibble
		ni = 1
	} else {
		compact0 = 0x20
	}
	return hph.completeLeafHash(buf, compactLen, key, compact0, ni, val, singleton)
}

func (hph *HexPatriciaHashedReader) accountLeafHashWithKey(buf, key []byte, val rlp.RlpSerializable) ([]byte, error) {
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	if hasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
		if len(key)&1 == 0 {
			compact0 = 48 + key[0] // Odd (1<<4) + first nibble
			ni = 1
		} else {
			compact0 = 32
		}
	} else {
		compactLen = len(key)/2 + 1
		if len(key)&1 == 1 {
			compact0 = 16 + key[0] // Odd (1<<4) + first nibble
			ni = 1
		}
	}
	return hph.completeLeafHash(buf, compactLen, key, compact0, ni, val, true)
}

func (hph *HexPatriciaHashedReader) extensionHash(key []byte, hash []byte) ([length.Hash]byte, error) {
	var hashBuf [length.Hash]byte

	// Compute the total length of binary representation
	var kp, kl int
	// Write key
	var compactLen int
	var ni int
	var compact0 byte
	if hasTerm(key) {
		compactLen = (len(key)-1)/2 + 1
		if len(key)&1 == 0 {
			compact0 = 0x30 + key[0] // Odd: (3<<4) + first nibble
			ni = 1
		} else {
			compact0 = 0x20
		}
	} else {
		compactLen = len(key)/2 + 1
		if len(key)&1 == 1 {
			compact0 = 0x10 + key[0] // Odd: (1<<4) + first nibble
			ni = 1
		}
	}
	var keyPrefix [1]byte
	if compactLen > 1 {
		keyPrefix[0] = 0x80 + byte(compactLen)
		kp = 1
		kl = compactLen
	} else {
		kl = 1
	}
	totalLen := kp + kl + 33
	var lenPrefix [4]byte
	pt := rlp.GenerateStructLen(lenPrefix[:], totalLen)
	hph.keccak.Reset()
	if _, err := hph.keccak.Write(lenPrefix[:pt]); err != nil {
		return hashBuf, err
	}
	if _, err := hph.keccak.Write(keyPrefix[:kp]); err != nil {
		return hashBuf, err
	}
	var b [1]byte
	b[0] = compact0
	if _, err := hph.keccak.Write(b[:]); err != nil {
		return hashBuf, err
	}
	for i := 1; i < compactLen; i++ {
		b[0] = key[ni]*16 + key[ni+1]
		if _, err := hph.keccak.Write(b[:]); err != nil {
			return hashBuf, err
		}
		ni += 2
	}
	b[0] = 0x80 + length.Hash
	if _, err := hph.keccak.Write(b[:]); err != nil {
		return hashBuf, err
	}
	if _, err := hph.keccak.Write(hash); err != nil {
		return hashBuf, err
	}
	// Replace previous hash with the new one
	if _, err := hph.keccak.Read(hashBuf[:]); err != nil {
		return hashBuf, err
	}
	return hashBuf, nil
}

func (hph *HexPatriciaHashedReader) computeCellHashWithStorage(cell *cell, depth int, buf []byte) ([]byte, bool, []byte, error) {
	var err error
	var storageRootHash [length.Hash]byte
	var storageRootHashIsSet bool
	if cell.storageAddrLen > 0 {
		var hashedKeyOffset int
		if depth >= 64 {
			hashedKeyOffset = depth - 64
		}
		singleton := depth <= 64
		koffset := hph.accountKeyLen
		if depth == 0 && cell.accountAddrLen == 0 {
			// if account key is empty, then we need to hash storage key from the key beginning
			koffset = 0
		}
		if err = hashKey(hph.keccak, cell.storageAddr[koffset:cell.storageAddrLen], cell.hashedExtension[:], hashedKeyOffset, cell.hashBuf[:]); err != nil {
			return nil, storageRootHashIsSet, nil, err
		}
		cell.hashedExtension[64-hashedKeyOffset] = 16 // Add terminator

		if cell.stateHashLen > 0 {
			res := append([]byte{160}, cell.stateHash[:cell.stateHashLen]...)
			hph.keccak.Reset()
			if hph.trace {
				fmt.Printf("REUSED stateHash %x spk %x\n", res, cell.storageAddr[:cell.storageAddrLen])
			}
			mxTrieStateSkipRate.Inc()
			skippedLoad.Add(1)
			if !singleton {
				return res, storageRootHashIsSet, nil, err
			} else {
				storageRootHashIsSet = true
				storageRootHash = *(*[length.Hash]byte)(res[1:])
				//copy(storageRootHash[:], res[1:])
				//cell.stateHashLen = 0
			}
		} else {
			if !cell.loaded.storage() {
				update, err := hph.ctx.Storage(cell.storageAddr[:cell.storageAddrLen])
				if err != nil {
					return nil, storageRootHashIsSet, nil, err
				}
				cell.setFromUpdate(update)
				fmt.Printf("Storage %x was not loaded\n", cell.storageAddr[:cell.storageAddrLen])
			}
			if singleton {
				if hph.trace {
					fmt.Printf("leafHashWithKeyVal(singleton) for [%x]=>[%x]\n", cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen])
				}
				aux := make([]byte, 0, 33)
				if aux, err = hph.leafHashWithKeyVal(aux, cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], true); err != nil {
					return nil, storageRootHashIsSet, nil, err
				}
				if hph.trace {
					fmt.Printf("leafHashWithKeyVal(singleton) storage hash [%x]\n", aux)
				}
				storageRootHash = *(*[length.Hash]byte)(aux[1:])
				storageRootHashIsSet = true
				cell.stateHashLen = 0
				hadToReset.Add(1)
			} else {
				if hph.trace {
					fmt.Printf("leafHashWithKeyVal for [%x]=>[%x] %v\n", cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], cell.String())
				}
				leafHash, err := hph.leafHashWithKeyVal(buf, cell.hashedExtension[:64-hashedKeyOffset+1], cell.Storage[:cell.StorageLen], false)
				if err != nil {
					return nil, storageRootHashIsSet, nil, err
				}

				copy(cell.stateHash[:], leafHash[1:])
				cell.stateHashLen = len(leafHash) - 1
				if hph.trace {
					fmt.Printf("STATE HASH storage memoized %x spk %x\n", leafHash, cell.storageAddr[:cell.storageAddrLen])
				}

				return leafHash, storageRootHashIsSet, storageRootHash[:], nil
			}
		}
	}
	if cell.accountAddrLen > 0 {
		if err := hashKey(hph.keccak, cell.accountAddr[:cell.accountAddrLen], cell.hashedExtension[:], depth, cell.hashBuf[:]); err != nil {
			return nil, storageRootHashIsSet, nil, err
		}
		cell.hashedExtension[64-depth] = 16 // Add terminator
		if !storageRootHashIsSet {
			if cell.extLen > 0 { // Extension
				if cell.hashLen == 0 {
					return nil, storageRootHashIsSet, nil, errors.New("computeCellHash extension without hash")
				}
				if hph.trace {
					fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.hash[:cell.hashLen])
				}
				if storageRootHash, err = hph.extensionHash(cell.extension[:cell.extLen], cell.hash[:cell.hashLen]); err != nil {
					return nil, storageRootHashIsSet, nil, err
				}
				if hph.trace {
					fmt.Printf("EXTENSION HASH %x DROPS stateHash\n", storageRootHash)
				}
				cell.stateHashLen = 0
				hadToReset.Add(1)
			} else if cell.hashLen > 0 {
				storageRootHash = cell.hash
				storageRootHashIsSet = true
			} else {
				storageRootHash = *(*[length.Hash]byte)(EmptyRootHash)
			}
		}
		if !cell.loaded.account() {
			if cell.stateHashLen > 0 {
				res := append([]byte{160}, cell.stateHash[:cell.stateHashLen]...)
				hph.keccak.Reset()

				mxTrieStateSkipRate.Inc()
				skippedLoad.Add(1)
				if hph.trace {
					fmt.Printf("REUSED stateHash %x apk %x\n", res, cell.accountAddr[:cell.accountAddrLen])
				}
				return res, storageRootHashIsSet, storageRootHash[:], nil
			}
			// storage root update or extension update could invalidate older stateHash, so we need to reload state
			update, err := hph.ctx.Account(cell.accountAddr[:cell.accountAddrLen])
			if err != nil {
				return nil, storageRootHashIsSet, storageRootHash[:], err
			}
			cell.setFromUpdate(update)
		}

		var valBuf [128]byte
		valLen := cell.accountForHashing(valBuf[:], storageRootHash)
		if hph.trace {
			fmt.Printf("accountLeafHashWithKey for [%x]=>[%x]\n", cell.hashedExtension[:65-depth], rlp.RlpEncodedBytes(valBuf[:valLen]))
		}
		leafHash, err := hph.accountLeafHashWithKey(buf, cell.hashedExtension[:65-depth], rlp.RlpEncodedBytes(valBuf[:valLen]))
		if err != nil {
			return nil, storageRootHashIsSet, nil, err
		}
		if hph.trace {
			fmt.Printf("STATE HASH account memoized %x\n", leafHash)
		}
		copy(cell.stateHash[:], leafHash[1:])
		cell.stateHashLen = len(leafHash) - 1
		return leafHash, storageRootHashIsSet, storageRootHash[:], nil
	}

	buf = append(buf, 0x80+32)
	if cell.extLen > 0 { // Extension
		if cell.hashLen > 0 {
			if hph.trace {
				fmt.Printf("extensionHash for [%x]=>[%x]\n", cell.extension[:cell.extLen], cell.hash[:cell.hashLen])
			}
			var hash [length.Hash]byte
			if hash, err = hph.extensionHash(cell.extension[:cell.extLen], cell.hash[:cell.hashLen]); err != nil {
				return nil, storageRootHashIsSet, storageRootHash[:], err
			}
			buf = append(buf, hash[:]...)
		} else {
			return nil, storageRootHashIsSet, storageRootHash[:], errors.New("computeCellHash extension without hash")
		}
	} else if cell.hashLen > 0 {
		buf = append(buf, cell.hash[:cell.hashLen]...)
	} else if storageRootHashIsSet {
		buf = append(buf, storageRootHash[:]...)
		copy(cell.hash[:], storageRootHash[:])
		cell.hashLen = len(storageRootHash)
	} else {
		buf = append(buf, EmptyRootHash...)
	}
	return buf, storageRootHashIsSet, storageRootHash[:], nil
}

// Traverse the grid following `hashedKey` and produce the witness `trie.Trie` for that key
func (hph *HexPatriciaHashedReader) ToTrie(hashedKey []byte, codeReads map[libcommon.Hash]witnesstypes.CodeWithHash) (*trie.Trie, error) {
	rootNode := &trie.FullNode{}
	var currentNode trie.Node = rootNode
	keyPos := 0 // current position in hashedKey (usually same as row, but could be different due to extension nodes)
	for row := 0; row < hph.activeRows && keyPos < len(hashedKey); row++ {
		currentNibble := hashedKey[keyPos]
		// determine the type of the next node to expand (in the next iteration)
		var nextNode trie.Node
		// need to check node type along the key path
		cellToExpand := &hph.grid[row][currentNibble]
		// determine the next node
		if cellToExpand.hashedExtLen > 0 { // extension cell
			keyPos += cellToExpand.hashedExtLen // jump ahead
			hashedExtKey := cellToExpand.hashedExtension[:cellToExpand.hashedExtLen]
			extKeyLength := len(hashedExtKey)
			if keyPos+1 == len(hashedKey) || keyPos+1 == 64 {
				extKeyLength++ //  +1 for the terminator 0x10 ([16])  byte when on a terminal extension node
			}
			extensionKey := make([]byte, extKeyLength)
			copy(extensionKey, hashedExtKey)
			if keyPos+1 == len(hashedKey) || keyPos+1 == 64 {
				extensionKey[len(extensionKey)-1] = terminatorHexByte // append terminator byte
			}
			nextNode = &trie.ShortNode{Key: extensionKey} // Value will be in the next iteration
			if keyPos+1 == len(hashedKey) {
				if cellToExpand.storageAddrLen > 0 {
					storageUpdate, err := hph.ctx.Storage(cellToExpand.storageAddr[:cellToExpand.storageAddrLen])
					if err != nil {
						return nil, err
					}
					storageValueNode := trie.ValueNode(storageUpdate.Storage[:storageUpdate.StorageLen])
					nextNode = &trie.ShortNode{Key: extensionKey, Val: storageValueNode}
				} else if cellToExpand.accountAddrLen > 0 {
					accNode, err := hph.createAccountNode(cellToExpand, row, hashedKey, codeReads)
					if err != nil {
						return nil, err
					}
					nextNode = &trie.ShortNode{Key: extensionKey, Val: accNode}
					extNodeSubTrie := trie.NewInMemoryTrie(nextNode)
					subTrieRoot := extNodeSubTrie.Root()
					cellHash, _, _, _ := hph.computeCellHashWithStorage(cellToExpand, hph.depths[row], nil)
					if !bytes.Equal(subTrieRoot, cellHash[1:]) {
						return nil, fmt.Errorf("subTrieRoot(%x) != cellHash(%x)", subTrieRoot, cellHash[1:])
					}
					// // DEBUG patch with cell hash which we know to be correct
					// nextNode = trie.NewHashNode(cellHash[1:])
				}
			}
		} else if cellToExpand.storageAddrLen > 0 { // storage cell
			storageUpdate, err := hph.ctx.Storage(cellToExpand.storageAddr[:cellToExpand.storageAddrLen])
			if err != nil {
				return nil, err
			}
			storageValueNode := trie.ValueNode(storageUpdate.Storage[:storageUpdate.StorageLen])
			nextNode = &storageValueNode //nolint:ineffassign, wastedassign
			break
		} else if cellToExpand.accountAddrLen > 0 { // account cell
			accNode, err := hph.createAccountNode(cellToExpand, row, hashedKey, codeReads)
			if err != nil {
				return nil, err
			}
			nextNode = accNode
			keyPos++ // only move one nibble
		} else if cellToExpand.hashLen > 0 { // hash cell means we will expand using a full node
			nextNode = &trie.FullNode{}
			keyPos++
		} else if cellToExpand.IsEmpty() {
			nextNode = nil // no more expanding can happen (this could be due )
		} else { // default for now before we handle extLen
			nextNode = &trie.FullNode{}
			keyPos++
		}

		// process the current node
		if fullNode, ok := currentNode.(*trie.FullNode); ok { // handle full node case
			for col := 0; col < 16; col++ {
				currentCell := &hph.grid[row][col]
				if currentCell.IsEmpty() {
					fullNode.Children[col] = nil
					continue
				}
				cellHash, _, _, err := hph.computeCellHashWithStorage(currentCell, hph.depths[row], nil)
				if err != nil {
					return nil, err
				}
				fullNode.Children[col] = trie.NewHashNode(cellHash[1:]) // because cellHash has 33 bytes and we want 32
			}
			fullNode.Children[currentNibble] = nextNode // ready to expand next nibble in the path
		} else if accNode, ok := currentNode.(*trie.AccountNode); ok {
			if len(hashedKey) <= 64 { // no storage, stop here
				nextNode = nil // nolint:ineffassign, wastedassign
				break
			}
			// there is storage so we need to expand further
			accNode.Storage = nextNode
		} else if extNode, ok := currentNode.(*trie.ShortNode); ok { // handle extension node case
			// expect only one item in this row, so take the first one
			// technically it should be at the last nibble of the key but we will adjust this later
			if extNode.Val != nil { // early termination
				break
			}
			extNode.Val = nextNode
		} else {
			break // break if currentNode is nil
		}
		// we need to check if we are dealing with the next node being an account node and we have a storage key,
		// in that case start a new tree for the storage
		if nextAccNode, ok := nextNode.(*trie.AccountNode); ok && len(hashedKey) > 64 {
			nextNode = &trie.FullNode{}
			nextAccNode.Storage = nextNode
		}
		currentNode = nextNode
	}
	tr := trie.NewInMemoryTrie(rootNode)
	return tr, nil
}

// this function is only related to the witness
func (hph *HexPatriciaHashedReader) createAccountNode(c *cell, row int, hashedKey []byte, codeReads map[libcommon.Hash]witnesstypes.CodeWithHash) (*trie.AccountNode, error) {
	_, storageIsSet, storageRootHash, err := hph.computeCellHashWithStorage(c, hph.depths[row], nil)
	if err != nil {
		return nil, err
	}
	accountUpdate, err := hph.ctx.Account(c.accountAddr[:c.accountAddrLen])
	if err != nil {
		return nil, err
	}
	var account accounts.Account
	account.Nonce = accountUpdate.Nonce
	account.Balance = accountUpdate.Balance
	account.Initialised = true
	account.Root = accountUpdate.Storage
	account.CodeHash = accountUpdate.CodeHash

	addrHash, err := compactKey(hashedKey[:64])
	if err != nil {
		return nil, err
	}

	// get code
	var code []byte
	codeWithHash, hasCode := codeReads[[32]byte(addrHash)]
	if !hasCode {
		code = nil
	} else {
		code = codeWithHash.Code
		// sanity check
		if account.CodeHash != codeWithHash.CodeHash {
			return nil, fmt.Errorf("account.CodeHash(%x)!=codeReads[%x].CodeHash(%x)", account.CodeHash, addrHash, codeWithHash.CodeHash)
		}
	}

	var accountNode *trie.AccountNode
	if !storageIsSet {
		account.Root = trie.EmptyRoot
		accountNode = &trie.AccountNode{Account: account, Storage: nil, RootCorrect: true, Code: code, CodeSize: -1}
	} else {
		accountNode = &trie.AccountNode{Account: account, Storage: trie.NewHashNode(storageRootHash), RootCorrect: true, Code: code, CodeSize: -1}
	}
	return accountNode, nil
}
