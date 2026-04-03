package jsonrpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/execution/commitment/qmtree"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
	ethapi2 "github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/rpc/transactions"
)

// QMAPI is the interface for the qm_ RPC namespace.
type QMAPI interface {
	// Call returns execution result + individual witnesses (one per accessed txNum).
	Call(ctx context.Context, args ethapi2.CallArgs, blockNrOrHash rpc.BlockNumberOrHash) (*QMCallResult, error)
	// CallProof returns the same execution result + a compact twig-grouped proof
	// that shares upper-tree paths across leaves belonging to the same twig.
	CallProof(ctx context.Context, args ethapi2.CallArgs, blockNrOrHash rpc.BlockNumberOrHash) (*QMCallProof, error)

	// Tx-level proofs: prove that a transaction was committed in the qmtree.
	GetProof(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*QMProofResult, error)
	GetWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*QMWitnessResult, error)
	GetTxWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, txIndex hexutil.Uint) (*QMProofResult, error)

	// State proofs: prove account/storage state values AND their qmtree commitment.
	GetAccountStateProof(ctx context.Context, address common.Address, storageKeys []common.Hash, blockNrOrHash rpc.BlockNumberOrHash) (*QMStateProofResult, error)
	GetTxStateProof(ctx context.Context, address common.Address, storageKeys []common.Hash, blockNrOrHash rpc.BlockNumberOrHash, txIndex hexutil.Uint) (*QMStateProofResult, error)
	GetLatestStateProof(ctx context.Context, address common.Address, storageKeys []common.Hash) (*QMStateProofResult, error)

	// Offline verification.
	VerifyProof(ctx context.Context, proofBytes hexutil.Bytes) (bool, error)
	VerifyWitness(ctx context.Context, witnessBytes hexutil.Bytes) (bool, error)
}

// QMAPIImpl implements the qm_ RPC namespace for qmtree proofs and witnesses.
type QMAPIImpl struct {
	*BaseAPI
	db      kv.TemporalRoDB
	tracker *qmtree.Tracker
	gasCap  uint64
	logger  log.Logger
}

func NewQMAPI(base *BaseAPI, db kv.TemporalRoDB, tracker *qmtree.Tracker, gasCap uint64, logger log.Logger) *QMAPIImpl {
	return &QMAPIImpl{
		BaseAPI: base,
		db:      db,
		tracker: tracker,
		gasCap:  gasCap,
		logger:  logger,
	}
}

// --- Response types ---

type QMLeafData struct {
	PreStateHash     common.Hash `json:"preStateHash"`
	StateChangeHash  common.Hash `json:"stateChangeHash"`
	TransitionHash   common.Hash `json:"transitionHash"`
	PreviousLeafHash common.Hash `json:"previousLeafHash"`
}

type QMProofResult struct {
	Address     common.Address `json:"address"`
	TxNum       hexutil.Uint64 `json:"txNum"`
	BlockNumber hexutil.Uint64 `json:"blockNumber"`
	LeafData    QMLeafData     `json:"leafData"`
	LeafHash    common.Hash    `json:"leafHash"`
	MerkleProof hexutil.Bytes  `json:"merkleProof"`
	QMTreeRoot  common.Hash    `json:"qmtreeRoot"`
	Verified    bool           `json:"verified"`
}

type QMWitnessLeaf struct {
	TxNum hexutil.Uint64 `json:"txNum"`
	LeafData  QMLeafData     `json:"leafData"`
	LeafHash  common.Hash    `json:"leafHash"`
}

type QMWitnessResult struct {
	BlockNumber hexutil.Uint64  `json:"blockNumber"`
	QMTreeRoot  common.Hash     `json:"qmtreeRoot"`
	FirstProof  hexutil.Bytes   `json:"firstProof"`
	LastProof   hexutil.Bytes   `json:"lastProof"`
	Leaves      []QMWitnessLeaf `json:"leaves"`
	Verified    bool            `json:"verified"`
}

// QMStorageSlotProof holds a single storage slot value with its qmtree witness.
type QMStorageSlotProof struct {
	Slot  common.Hash   `json:"slot"`
	Value hexutil.Bytes `json:"value"`
}

// QMStateProofResult combines account/storage state values with the qmtree
// witness for the transaction that last changed them. A verifier can:
//  1. Check that the account state matches the stateChangeHash in the qmtree leaf.
//  2. Verify the qmtree Merkle proof against the published root.
type QMStateProofResult struct {
	Address     common.Address       `json:"address"`
	BlockNumber hexutil.Uint64       `json:"blockNumber"`
	TxNum       hexutil.Uint64       `json:"txNum"`
	// Account state after the transaction at TxNum executed.
	Balance     *hexutil.Big         `json:"balance"`
	Nonce       hexutil.Uint64       `json:"nonce"`
	CodeHash    common.Hash          `json:"codeHash"`
	// Requested storage slots at the same point in history.
	StorageProofs []QMStorageSlotProof `json:"storageProofs"`
	// qmtree witness proving TxNum is committed.
	Witness  QMProofResult `json:"witness"`
	Verified bool          `json:"verified"`
}

// QMAccessedElement describes one state read that occurred during qm_call
// execution, together with the txNum of the transaction that most recently
// wrote that element (i.e., the "last writer" at the time of the call).
type QMAccessedElement struct {
	// Domain: 0 = accounts, 1 = storage, 2 = code (matches kv.Domain values).
	Domain uint          `json:"domain"`
	// Key: 20-byte address for accounts/code; 52-byte addr+slot for storage.
	Key    hexutil.Bytes `json:"key"`
	// Value: the value that was read (serialized account, storage bytes, or code).
	Value  hexutil.Bytes `json:"value"`
	// LastWriteTxNum: the most recent txNum ≤ blockEnd that wrote this element.
	// Zero if the element has never been written in the tracked history.
	LastWriteTxNum hexutil.Uint64 `json:"lastWriteTxNum"`
}

// QMCallResult is returned by qm_call. It carries the EVM execution result
// together with a combined witness covering all qmtree leaves for the
// transactions that last wrote each state element accessed during the call.
// A verifier can independently confirm that those pre-state values are
// committed by the qmtree.
type QMCallResult struct {
	ReturnData   hexutil.Bytes  `json:"returnData"`
	GasUsed      hexutil.Uint64 `json:"gasUsed"`
	Failed       bool           `json:"failed"`
	RevertReason hexutil.Bytes  `json:"revertReason,omitempty"`
	// Accessed lists every state element read during execution.
	Accessed []QMAccessedElement `json:"accessed"`
	// Witnesses holds the qmtree witnesses for all unique LastWriteTxNums in
	// Accessed, sorted ascending by TxNum. Each witness proves that the
	// corresponding transaction was committed in the qmtree.
	Witnesses []QMProofResult `json:"witnesses"`
}

// ---------------------------------------------------------------------------
// Compact twig-grouped proof format (QMCallProof)
//
// Size analysis vs. individual witnesses (U = upper tree levels ≈ 9 on hoodi):
//
//   Individual witness per txNum:   8 + 128 + 32 + (424+U*32) + 32 = 624+U*32 bytes
//   At U=9: ~912 bytes/witness
//
//   Twig-grouped proof:
//     Root (once):          32 bytes
//     Per unique twig:      8 + U*32 bytes  (288+8 = 296 bytes at U=9)
//     Per leaf:             8 + 128 + 32 + 11*32 = 520 bytes
//
//   Example — M=20 leaves, T=5 twigs (4 leaves/twig), U=9:
//     Individual:  20 * 912 =       18,240 bytes
//     Twig-grouped: 32 + 5*296 + 20*520 =  11,952 bytes  → 34% smaller
//
//   Example — M=20 leaves, T=20 twigs (all different), U=9:
//     Individual:  20 * 912 =       18,240 bytes
//     Twig-grouped: 32 + 20*296 + 20*520 =  16,432 bytes → 10% smaller
//
// The worst case (all leaves in different twigs) still saves 10% from the
// deduplicated root and the absence of redundant SelfHashes at upper levels.
// Best case (all leaves in one twig) saves ~41%.
// ---------------------------------------------------------------------------

// QMCallProofTwig holds the upper-tree proof for one twig. It is shared by
// all leaves in the same twig, eliminating repeated upper-path data.
type QMCallProofTwig struct {
	TwigId hexutil.Uint64 `json:"twigId"`
	// UpperPeerHashes: peer hashes at each level from the twig root (level 12)
	// up to (but not including) the tree root level. Length = upper tree height.
	// Combined with the twig root (derived from LeftOfTwig), they reproduce the
	// full path to Root. PeerAtLeft for level i is derivable from
	// (txNum >> (FIRST_LEVEL_ABOVE_TWIG - 2 + i)) & 1.
	UpperPeerHashes []hexutil.Bytes `json:"upperPeerHashes"`
}

// QMCallProofLeaf holds the per-leaf data and intra-twig Merkle path.
type QMCallProofLeaf struct {
	TxNum hexutil.Uint64 `json:"txNum"`
	// TwigIndex is the index of this leaf's twig in the containing QMCallProof.Twigs slice.
	TwigIndex int        `json:"twigIndex"`
	LeafData  QMLeafData `json:"leafData"`
	// SelfHash = LeftOfTwig[0].SelfHash = the leaf hash itself.
	SelfHash hexutil.Bytes `json:"selfHash"`
	// IntraTwigPeerHashes: 11 peer hashes for levels 0..10 within the twig.
	// PeerAtLeft for level i is derivable from (txNum >> i) & 1.
	IntraTwigPeerHashes []hexutil.Bytes `json:"intraTwigPeerHashes"`
	Verified            bool            `json:"verified"`
}

// QMCallProof is the compact multi-proof returned by qm_callProof. Upper-tree
// paths are shared across leaves that belong to the same twig, significantly
// reducing response size when accessed state elements cluster in few twigs.
//
// Digest is the 32-byte Merkle commitment over all proof fields (see
// callProofDigest). A verifier can store just the digest and later confirm
// that a full proof has not been modified.
type QMCallProof struct {
	ReturnData   hexutil.Bytes       `json:"returnData"`
	GasUsed      hexutil.Uint64      `json:"gasUsed"`
	Failed       bool                `json:"failed"`
	RevertReason hexutil.Bytes       `json:"revertReason,omitempty"`
	// Accessed lists every state element read during execution (same as Call).
	Accessed []QMAccessedElement `json:"accessed"`
	// Root is the single qmtree root, shared across all leaves.
	Root common.Hash `json:"root"`
	// Twigs lists unique twigs referenced by Leaves, each with its upper path.
	Twigs []QMCallProofTwig `json:"twigs"`
	// Leaves contains per-leaf proof data indexed into Twigs.
	Leaves []QMCallProofLeaf `json:"leaves"`
	// Digest is the SSZ-inspired hash_tree_root of this proof, serving as a
	// compact 32-byte commitment. See callProofDigest for the encoding.
	Digest common.Hash `json:"digest"`
}

// ---------------------------------------------------------------------------
// QMCallProof Merkle digest
//
// callProofDigest computes an SSZ-inspired hash_tree_root for QMCallProof,
// producing a 32-byte commitment over all proof fields. Encoding:
//
//	merkleize([
//	  keccak256(returnData),                        // chunk 0
//	  uint64_chunk(gasUsed),                        // chunk 1
//	  bool_chunk(failed),                           // chunk 2
//	  keccak256(revertReason),                      // chunk 3
//	  mixInLength(merkleize(accessed_hashes), n),   // chunk 4
//	  qmtreeRoot,                                   // chunk 5
//	  mixInLength(merkleize(twig_hashes), n),       // chunk 6
//	  mixInLength(merkleize(leaf_hashes), n),       // chunk 7
//	])
//
// Each list element is hashed to 32 bytes via keccak256 over its canonical
// binary encoding. The qmtree root (chunk 5) is already a 32-byte Merkle
// commitment and is embedded directly.
// ---------------------------------------------------------------------------

func callProofDigest(proof *QMCallProof) common.Hash {
	// Chunk 0: keccak256(returnData)
	c0 := crypto.Keccak256Hash(proof.ReturnData)

	// Chunk 1: gasUsed as little-endian uint64 in the first 8 bytes.
	var c1 common.Hash
	binary.LittleEndian.PutUint64(c1[:8], uint64(proof.GasUsed))

	// Chunk 2: failed as a single byte.
	var c2 common.Hash
	if proof.Failed {
		c2[0] = 1
	}

	// Chunk 3: keccak256(revertReason)
	c3 := crypto.Keccak256Hash(proof.RevertReason)

	// Chunk 4: list root for Accessed.
	accHashes := make([]common.Hash, len(proof.Accessed))
	for i, a := range proof.Accessed {
		var domainBuf [8]byte
		binary.LittleEndian.PutUint64(domainBuf[:], uint64(a.Domain))
		var txNumBuf [8]byte
		binary.LittleEndian.PutUint64(txNumBuf[:], uint64(a.LastWriteTxNum))
		accHashes[i] = crypto.Keccak256Hash(domainBuf[:], a.Key, a.Value, txNumBuf[:])
	}
	c4 := merkleizeList(accHashes)

	// Chunk 5: qmtree root (already a 32-byte Merkle commitment).
	c5 := proof.Root

	// Chunk 6: list root for Twigs.
	twigHashes := make([]common.Hash, len(proof.Twigs))
	for i, twig := range proof.Twigs {
		var twigIdBuf [8]byte
		binary.LittleEndian.PutUint64(twigIdBuf[:], uint64(twig.TwigId))
		parts := make([]byte, 0, 8+len(twig.UpperPeerHashes)*32)
		parts = append(parts, twigIdBuf[:]...)
		for _, ph := range twig.UpperPeerHashes {
			parts = append(parts, ph...)
		}
		twigHashes[i] = crypto.Keccak256Hash(parts)
	}
	c6 := merkleizeList(twigHashes)

	// Chunk 7: list root for Leaves.
	leafHashes := make([]common.Hash, len(proof.Leaves))
	for i, lf := range proof.Leaves {
		var txNumBuf [8]byte
		binary.LittleEndian.PutUint64(txNumBuf[:], uint64(lf.TxNum))
		parts := make([]byte, 0, 8+4*32+32+len(lf.IntraTwigPeerHashes)*32)
		parts = append(parts, txNumBuf[:]...)
		parts = append(parts, lf.LeafData.PreStateHash[:]...)
		parts = append(parts, lf.LeafData.StateChangeHash[:]...)
		parts = append(parts, lf.LeafData.TransitionHash[:]...)
		parts = append(parts, lf.LeafData.PreviousLeafHash[:]...)
		parts = append(parts, lf.SelfHash...)
		for _, ph := range lf.IntraTwigPeerHashes {
			parts = append(parts, ph...)
		}
		leafHashes[i] = crypto.Keccak256Hash(parts)
	}
	c7 := merkleizeList(leafHashes)

	return merkleize([]common.Hash{c0, c1, c2, c3, c4, c5, c6, c7})
}

// merkleize returns the Merkle root of chunks, padding to the next power of 2
// with zero-hashes (SSZ convention). Returns zero hash for an empty input.
func merkleize(chunks []common.Hash) common.Hash {
	if len(chunks) == 0 {
		return common.Hash{}
	}
	// Pad to next power of 2.
	n := 1
	for n < len(chunks) {
		n <<= 1
	}
	padded := make([]common.Hash, n)
	copy(padded, chunks)
	// Reduce bottom-up.
	for n > 1 {
		n >>= 1
		for i := 0; i < n; i++ {
			padded[i] = crypto.Keccak256Hash(padded[2*i][:], padded[2*i+1][:])
		}
	}
	return padded[0]
}

// merkleizeList computes an SSZ-style list root: merkleize(elements) mixed
// with the actual list length, so lists of different lengths with the same
// prefix are distinguished.
func merkleizeList(elems []common.Hash) common.Hash {
	root := merkleize(elems)
	var lenChunk common.Hash
	binary.LittleEndian.PutUint64(lenChunk[:8], uint64(len(elems)))
	return crypto.Keccak256Hash(root[:], lenChunk[:])
}

// ---------------------------------------------------------------------------
// readTracker: state.StateReader wrapper that records all reads for
// preStateHash computation. The algorithm mirrors exec.hashingReader.
// ---------------------------------------------------------------------------

type stateReadOp struct {
	domain kv.Domain
	key    []byte
	value  []byte
}

type readTracker struct {
	inner state.StateReader
	reads []stateReadOp
}

func newReadTracker(inner state.StateReader) *readTracker {
	return &readTracker{inner: inner, reads: make([]stateReadOp, 0, 16)}
}

func (r *readTracker) record(domain kv.Domain, key, value []byte) {
	r.reads = append(r.reads, stateReadOp{
		domain: domain,
		key:    common.Copy(key),
		value:  common.Copy(value),
	})
}

// --- state.StateReader methods ---

func (r *readTracker) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	acc, err := r.inner.ReadAccountData(address)
	if err == nil {
		addr := address.Value()
		var val []byte
		if acc != nil {
			val = accounts.SerialiseV3(acc)
		}
		r.record(kv.AccountsDomain, addr[:], val)
	}
	return acc, err
}

func (r *readTracker) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return r.inner.ReadAccountDataForDebug(address)
}

func (r *readTracker) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	val, ok, err := r.inner.ReadAccountStorage(address, key)
	if err == nil {
		addr := address.Value()
		k := key.Value()
		composite := make([]byte, 52) // 20 addr + 32 key
		copy(composite[:20], addr[:])
		copy(composite[20:], k[:])
		r.record(kv.StorageDomain, composite, val.Bytes())
	}
	return val, ok, err
}

func (r *readTracker) HasStorage(address accounts.Address) (bool, error) {
	return r.inner.HasStorage(address)
}

func (r *readTracker) ReadAccountCode(address accounts.Address) ([]byte, error) {
	code, err := r.inner.ReadAccountCode(address)
	if err == nil {
		addr := address.Value()
		r.record(kv.CodeDomain, addr[:], code)
	}
	return code, err
}

func (r *readTracker) ReadAccountCodeSize(address accounts.Address) (int, error) {
	return r.inner.ReadAccountCodeSize(address)
}

func (r *readTracker) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	return r.inner.ReadAccountIncarnation(address)
}

func (r *readTracker) SetTrace(trace bool, tracePrefix string) {
	r.inner.SetTrace(trace, tracePrefix)
}

func (r *readTracker) Trace() bool         { return r.inner.Trace() }
func (r *readTracker) TracePrefix() string { return r.inner.TracePrefix() }

// ---------------------------------------------------------------------------
// Call: qm_call — execute a simulated call and return a combined witness
// ---------------------------------------------------------------------------

// findLastWriteTxNum returns the most recent txNum ≤ endTxNum that wrote to
// (domain, key) using the inverted history index. Returns (0, false) if no
// write exists in the tracked range (e.g. genesis state).
func (api *QMAPIImpl) findLastWriteTxNum(ctx context.Context, tx kv.TemporalTx, domain kv.Domain, key []byte, endTxNum uint64) (uint64, bool) {
	var idx kv.InvertedIdx
	switch domain {
	case kv.AccountsDomain:
		idx = kv.AccountsHistoryIdx
	case kv.StorageDomain:
		idx = kv.StorageHistoryIdx
	case kv.CodeDomain:
		idx = kv.CodeHistoryIdx
	default:
		return 0, false
	}

	it, err := tx.IndexRange(idx, key, -1, int(endTxNum), false, -1)
	if err != nil {
		return 0, false
	}
	var lastTxNum uint64
	var found bool
	for it.HasNext() {
		txn, err := it.Next()
		if err != nil {
			break
		}
		if txn <= endTxNum {
			lastTxNum = txn
			found = true
		}
	}
	return lastTxNum, found
}

// Call implements qm_call. It executes a simulated call against the historical
// state at blockNrOrHash, tracks every state read, then for each accessed
// element looks up the qmtree witness for the transaction that last wrote it.
// The result includes the execution output plus a combined witness set that
// proves all pre-state values are committed in the qmtree.
func (api *QMAPIImpl) Call(ctx context.Context, args ethapi2.CallArgs, blockNrOrHash rpc.BlockNumberOrHash) (*QMCallResult, error) {
	if api.BaseAPI == nil {
		return nil, fmt.Errorf("qm_call not available (no base API)")
	}
	if api.tracker == nil {
		return nil, fmt.Errorf("qmtree tracker not available")
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	if args.Gas == nil || uint64(*args.Gas) == 0 {
		args.Gas = (*hexutil.Uint64)(&api.gasCap)
	}

	header, _, err := api.headerByNumberOrHash(ctx, tx, blockNrOrHash)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, fmt.Errorf("header not found")
	}

	blockNum := header.Number.Uint64()

	// endTxNum: first txNum of the next block — same bound used by GetProof.
	endTxNum, err := api._txNumReader.Min(ctx, tx, blockNum+1)
	if err != nil {
		return nil, fmt.Errorf("resolve end txnum: %w", err)
	}

	// State at the start of blockNum (before any of its transactions execute).
	histReader, err := rpchelper.CreateHistoryStateReader(ctx, tx, blockNum, 0, rawdbv3.TxNums)
	if err != nil {
		return nil, fmt.Errorf("create history state reader: %w", err)
	}

	rd := newReadTracker(histReader)

	result, err := transactions.DoCall(
		ctx, engine, args, tx, blockNrOrHash, header,
		nil, nil, // stateOverrides, blockOverrides
		api.gasCap, chainConfig, rd, api._blockReader, api.evmCallTimeout,
	)
	if err != nil {
		return nil, err
	}

	// Build Accessed list and collect unique last-writer txNums.
	hasher := &qmtree.Keccak256Hasher{}
	accessed := make([]QMAccessedElement, 0, len(rd.reads))
	seenTxNums := map[uint64]struct{}{}
	var orderedTxNums []uint64

	for _, r := range rd.reads {
		lastTxNum, found := api.findLastWriteTxNum(ctx, tx, r.domain, r.key, endTxNum)
		accessed = append(accessed, QMAccessedElement{
			Domain:         uint(r.domain),
			Key:            r.key,
			Value:          r.value,
			LastWriteTxNum: hexutil.Uint64(lastTxNum),
		})
		if found {
			if _, seen := seenTxNums[lastTxNum]; !seen {
				seenTxNums[lastTxNum] = struct{}{}
				orderedTxNums = append(orderedTxNums, lastTxNum)
			}
		}
	}

	// Sort txNums ascending for deterministic output.
	sort.Slice(orderedTxNums, func(i, j int) bool { return orderedTxNums[i] < orderedTxNums[j] })

	witnesses := make([]QMProofResult, 0, len(orderedTxNums))
	for _, txNum := range orderedTxNums {
		w, err := api.tracker.GetWitness(txNum)
		if err != nil {
			// Witness not available (e.g. pre-tracker history); skip.
			continue
		}
		verified := w.Verify(hasher) == nil
		witnesses = append(witnesses, QMProofResult{
			TxNum: hexutil.Uint64(txNum),
			LeafData: QMLeafData{
				PreStateHash:     w.PreStateHash,
				StateChangeHash:  w.StateChangeHash,
				TransitionHash:   w.TransitionHash,
				PreviousLeafHash: w.PreviousLeafHash,
			},
			LeafHash:    w.LeafHash(),
			MerkleProof: w.Proof.ToBytes(),
			QMTreeRoot:  w.Proof.Root,
			Verified:    verified,
		})
	}

	var revertReason hexutil.Bytes
	if result.Reverted {
		revertReason = result.Revert()
	}

	return &QMCallResult{
		ReturnData:   result.Return(),
		GasUsed:      hexutil.Uint64(result.ReceiptGasUsed),
		Failed:       result.Failed(),
		RevertReason: revertReason,
		Accessed:     accessed,
		Witnesses:    witnesses,
	}, nil
}

// CallProof implements qm_callProof. It runs the same execution as Call but
// returns a compact twig-grouped proof instead of individual witnesses.
// Upper-tree paths are stored once per twig and shared across all leaves in
// that twig, reducing response size by ~34% on average.
func (api *QMAPIImpl) CallProof(ctx context.Context, args ethapi2.CallArgs, blockNrOrHash rpc.BlockNumberOrHash) (*QMCallProof, error) {
	if api.BaseAPI == nil {
		return nil, fmt.Errorf("qm_callProof not available (no base API)")
	}
	if api.tracker == nil {
		return nil, fmt.Errorf("qmtree tracker not available")
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}
	engine := api.engine()

	if args.Gas == nil || uint64(*args.Gas) == 0 {
		args.Gas = (*hexutil.Uint64)(&api.gasCap)
	}

	header, _, err := api.headerByNumberOrHash(ctx, tx, blockNrOrHash)
	if err != nil {
		return nil, err
	}
	if header == nil {
		return nil, fmt.Errorf("header not found")
	}

	blockNum := header.Number.Uint64()
	endTxNum, err := api._txNumReader.Min(ctx, tx, blockNum+1)
	if err != nil {
		return nil, fmt.Errorf("resolve end txnum: %w", err)
	}

	histReader, err := rpchelper.CreateHistoryStateReader(ctx, tx, blockNum, 0, rawdbv3.TxNums)
	if err != nil {
		return nil, fmt.Errorf("create history state reader: %w", err)
	}

	rd := newReadTracker(histReader)

	result, err := transactions.DoCall(
		ctx, engine, args, tx, blockNrOrHash, header,
		nil, nil,
		api.gasCap, chainConfig, rd, api._blockReader, api.evmCallTimeout,
	)
	if err != nil {
		return nil, err
	}

	hasher := &qmtree.Keccak256Hasher{}
	accessed := make([]QMAccessedElement, 0, len(rd.reads))
	seenTxNums := map[uint64]struct{}{}
	var orderedTxNums []uint64

	for _, r := range rd.reads {
		lastTxNum, found := api.findLastWriteTxNum(ctx, tx, r.domain, r.key, endTxNum)
		accessed = append(accessed, QMAccessedElement{
			Domain:         uint(r.domain),
			Key:            r.key,
			Value:          r.value,
			LastWriteTxNum: hexutil.Uint64(lastTxNum),
		})
		if found {
			if _, seen := seenTxNums[lastTxNum]; !seen {
				seenTxNums[lastTxNum] = struct{}{}
				orderedTxNums = append(orderedTxNums, lastTxNum)
			}
		}
	}
	sort.Slice(orderedTxNums, func(i, j int) bool { return orderedTxNums[i] < orderedTxNums[j] })

	// Group leaves by twig. twigOrder preserves the order in which twigs are
	// first encountered (ascending txNum order).
	twigIndexMap := map[uint64]int{} // twigId → index in twigs slice
	var twigs []QMCallProofTwig
	var leaves []QMCallProofLeaf
	var root common.Hash

	for _, txNum := range orderedTxNums {
		w, err := api.tracker.GetWitness(txNum)
		if err != nil {
			continue
		}

		sn := w.Proof.TxNum
		twigId := sn >> qmtree.TWIG_SHIFT
		root = w.Proof.Root // same for all witnesses from the same synced tree

		twigIdx, seen := twigIndexMap[twigId]
		if !seen {
			// First time seeing this twig — record its upper-tree path.
			upperPeers := make([]hexutil.Bytes, len(w.Proof.UpperPath))
			for i, node := range w.Proof.UpperPath {
				h := node.PeerHash
				upperPeers[i] = h[:]
			}
			twigIdx = len(twigs)
			twigIndexMap[twigId] = twigIdx
			twigs = append(twigs, QMCallProofTwig{
				TwigId:          hexutil.Uint64(twigId),
				UpperPeerHashes: upperPeers,
			})
		}

		intraPeers := make([]hexutil.Bytes, len(w.Proof.LeftOfTwig))
		for i, node := range w.Proof.LeftOfTwig {
			h := node.PeerHash
			intraPeers[i] = h[:]
		}
		selfHash := w.Proof.LeftOfTwig[0].SelfHash

		verified := w.Verify(hasher) == nil
		leaves = append(leaves, QMCallProofLeaf{
			TxNum:               hexutil.Uint64(txNum),
			TwigIndex:           twigIdx,
			LeafData: QMLeafData{
				PreStateHash:     w.PreStateHash,
				StateChangeHash:  w.StateChangeHash,
				TransitionHash:   w.TransitionHash,
				PreviousLeafHash: w.PreviousLeafHash,
			},
			SelfHash:            selfHash[:],
			IntraTwigPeerHashes: intraPeers,
			Verified:            verified,
		})
	}

	var revertReason hexutil.Bytes
	if result.Reverted {
		revertReason = result.Revert()
	}

	proof := &QMCallProof{
		ReturnData:   result.Return(),
		GasUsed:      hexutil.Uint64(result.ReceiptGasUsed),
		Failed:       result.Failed(),
		RevertReason: revertReason,
		Accessed:     accessed,
		Root:         root,
		Twigs:        twigs,
		Leaves:       leaves,
	}
	proof.Digest = callProofDigest(proof)
	return proof, nil
}

// GetProof returns a qmtree inclusion proof for the given address at the
// specified block.
func (api *QMAPIImpl) GetProof(ctx context.Context, address common.Address, blockNrOrHash rpc.BlockNumberOrHash) (*QMProofResult, error) {
	if api.tracker == nil {
		return nil, fmt.Errorf("qmtree tracker not available")
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, _, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, nil)
	if err != nil {
		return nil, err
	}

	lastTxNum, err := api._txNumReader.Min(ctx, tx, uint64(blockNum)+1)
	if err != nil {
		return nil, fmt.Errorf("resolve block txnum: %w", err)
	}

	// Find latest txnum <= lastTxNum that touched this address via inverted index.
	it, err := tx.IndexRange(kv.AccountsHistoryIdx, address.Bytes(), -1, int(lastTxNum), false, -1)
	if err != nil {
		return nil, fmt.Errorf("index range: %w", err)
	}

	var targetTxNum uint64
	var found bool
	for it.HasNext() {
		txn, err := it.Next()
		if err != nil {
			return nil, err
		}
		if txn <= lastTxNum {
			targetTxNum = txn
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("no state change found for address %s up to block %d", address.Hex(), blockNum)
	}

	witness, err := api.tracker.GetWitness(targetTxNum)
	if err != nil {
		return nil, fmt.Errorf("get witness for txnum=%d: %w", targetTxNum, err)
	}

	hasher := &qmtree.Keccak256Hasher{}
	verified := witness.Verify(hasher) == nil

	return &QMProofResult{
		Address:     address,
		TxNum:       hexutil.Uint64(targetTxNum),
		BlockNumber: hexutil.Uint64(blockNum),
		LeafData: QMLeafData{
			PreStateHash:     witness.PreStateHash,
			StateChangeHash:  witness.StateChangeHash,
			TransitionHash:   witness.TransitionHash,
			PreviousLeafHash: witness.PreviousLeafHash,
		},
		LeafHash:    witness.LeafHash(),
		MerkleProof: witness.Proof.ToBytes(),
		QMTreeRoot:  witness.Proof.Root,
		Verified:    verified,
	}, nil
}

// GetWitness returns a qmtree range witness for all transactions in a block.
func (api *QMAPIImpl) GetWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*QMWitnessResult, error) {
	if api.tracker == nil {
		return nil, fmt.Errorf("qmtree tracker not available")
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, _, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, nil)
	if err != nil {
		return nil, err
	}

	fromTxNum, err := api._txNumReader.Min(ctx, tx, uint64(blockNum))
	if err != nil {
		return nil, fmt.Errorf("resolve block start txnum: %w", err)
	}
	toTxNum, err := api._txNumReader.Min(ctx, tx, uint64(blockNum)+1)
	if err != nil {
		return nil, fmt.Errorf("resolve block end txnum: %w", err)
	}

	if toTxNum <= fromTxNum {
		return nil, fmt.Errorf("empty block %d (from=%d, to=%d)", blockNum, fromTxNum, toTxNum)
	}

	rw, err := api.tracker.GetRangeWitness(fromTxNum, toTxNum-1)
	if err != nil {
		return nil, fmt.Errorf("get range witness: %w", err)
	}

	hasher := &qmtree.Keccak256Hasher{}
	verified := rw.Verify(hasher) == nil

	leaves := make([]QMWitnessLeaf, len(rw.Leaves))
	for i, ld := range rw.Leaves {
		leaves[i] = QMWitnessLeaf{
			TxNum: hexutil.Uint64(ld.TxNum),
			LeafData: QMLeafData{
				PreStateHash:     ld.PreStateHash,
				StateChangeHash:  ld.StateChangeHash,
				TransitionHash:   ld.TransitionHash,
				PreviousLeafHash: ld.PreviousLeafHash,
			},
			LeafHash: ld.LeafHash(),
		}
	}

	return &QMWitnessResult{
		BlockNumber: hexutil.Uint64(blockNum),
		QMTreeRoot:  rw.Root(),
		FirstProof:  rw.FirstProof.ToBytes(),
		LastProof:   rw.LastProof.ToBytes(),
		Leaves:      leaves,
		Verified:    verified,
	}, nil
}

// GetTxWitness returns a qmtree witness for a specific transaction by index.
func (api *QMAPIImpl) GetTxWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, txIndex hexutil.Uint) (*QMProofResult, error) {
	if api.tracker == nil {
		return nil, fmt.Errorf("qmtree tracker not available")
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, _, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, nil)
	if err != nil {
		return nil, err
	}

	fromTxNum, err := api._txNumReader.Min(ctx, tx, uint64(blockNum))
	if err != nil {
		return nil, err
	}

	targetTxNum := fromTxNum + uint64(txIndex)

	witness, err := api.tracker.GetWitness(targetTxNum)
	if err != nil {
		return nil, fmt.Errorf("get witness for txnum=%d: %w", targetTxNum, err)
	}

	hasher := &qmtree.Keccak256Hasher{}
	verified := witness.Verify(hasher) == nil

	return &QMProofResult{
		TxNum:       hexutil.Uint64(targetTxNum),
		BlockNumber: hexutil.Uint64(blockNum),
		LeafData: QMLeafData{
			PreStateHash:     witness.PreStateHash,
			StateChangeHash:  witness.StateChangeHash,
			TransitionHash:   witness.TransitionHash,
			PreviousLeafHash: witness.PreviousLeafHash,
		},
		LeafHash:    witness.LeafHash(),
		MerkleProof: witness.Proof.ToBytes(),
		QMTreeRoot:  witness.Proof.Root,
		Verified:    verified,
	}, nil
}

// readStateAtTxNum reads account data and storage slots using a history reader
// positioned just after targetTxNum (i.e., state as of after that tx executed).
func (api *QMAPIImpl) readStateAtTxNum(tx kv.TemporalTx, address common.Address, storageKeys []common.Hash, targetTxNum uint64) (
	bal *hexutil.Big, nonce hexutil.Uint64, codeHash common.Hash, slots []QMStorageSlotProof, err error,
) {
	// txNum+1: history reader returns state as of "before txNum", so we add 1
	// to get the state after targetTxNum has been applied.
	reader := state.NewHistoryReaderV3(tx, targetTxNum+1)

	addr := accounts.InternAddress(address)
	acct, err := reader.ReadAccountData(addr)
	if err != nil {
		return nil, 0, common.Hash{}, nil, fmt.Errorf("read account: %w", err)
	}
	if acct != nil {
		bal = (*hexutil.Big)(acct.Balance.ToBig())
		nonce = hexutil.Uint64(acct.Nonce)
		codeHash = acct.CodeHash.Value()
	} else {
		bal = new(hexutil.Big)
	}

	slots = make([]QMStorageSlotProof, len(storageKeys))
	for i, slot := range storageKeys {
		val, _, err := reader.ReadAccountStorage(addr, accounts.InternKey(slot))
		if err != nil {
			return nil, 0, common.Hash{}, nil, fmt.Errorf("read storage slot %s: %w", slot.Hex(), err)
		}
		var valBytes [32]byte
		val.WriteToArray32(&valBytes)
		slots[i] = QMStorageSlotProof{
			Slot:  slot,
			Value: valBytes[:],
		}
	}
	return bal, nonce, codeHash, slots, nil
}

// readLatestState reads account data and storage slots at the current chain tip.
func (api *QMAPIImpl) readLatestState(tx kv.TemporalTx, address common.Address, storageKeys []common.Hash) (
	bal *hexutil.Big, nonce hexutil.Uint64, codeHash common.Hash, slots []QMStorageSlotProof, err error,
) {
	reader := state.NewReaderV3(tx)
	addr := accounts.InternAddress(address)
	acct, err := reader.ReadAccountData(addr)
	if err != nil {
		return nil, 0, common.Hash{}, nil, fmt.Errorf("read account: %w", err)
	}
	if acct != nil {
		bal = (*hexutil.Big)(acct.Balance.ToBig())
		nonce = hexutil.Uint64(acct.Nonce)
		codeHash = acct.CodeHash.Value()
	} else {
		bal = new(hexutil.Big)
	}

	slots = make([]QMStorageSlotProof, len(storageKeys))
	for i, slot := range storageKeys {
		val, _, err := reader.ReadAccountStorage(addr, accounts.InternKey(slot))
		if err != nil {
			return nil, 0, common.Hash{}, nil, fmt.Errorf("read storage slot %s: %w", slot.Hex(), err)
		}
		var valBytes [32]byte
		val.WriteToArray32(&valBytes)
		slots[i] = QMStorageSlotProof{
			Slot:  slot,
			Value: valBytes[:],
		}
	}
	return bal, nonce, codeHash, slots, nil
}

// buildStateProofResult assembles a QMStateProofResult from a targetTxNum,
// state values, and the qmtree witness for that txnum.
func (api *QMAPIImpl) buildStateProofResult(
	address common.Address, blockNum uint64, targetTxNum uint64,
	bal *hexutil.Big, nonce hexutil.Uint64, codeHash common.Hash,
	slots []QMStorageSlotProof,
) (*QMStateProofResult, error) {
	witness, err := api.tracker.GetWitness(targetTxNum)
	if err != nil {
		return nil, fmt.Errorf("get witness for txnum=%d: %w", targetTxNum, err)
	}
	hasher := &qmtree.Keccak256Hasher{}
	verified := witness.Verify(hasher) == nil

	return &QMStateProofResult{
		Address:       address,
		BlockNumber:   hexutil.Uint64(blockNum),
		TxNum:         hexutil.Uint64(targetTxNum),
		Balance:       bal,
		Nonce:         nonce,
		CodeHash:      codeHash,
		StorageProofs: slots,
		Witness: QMProofResult{
			Address:     address,
			TxNum:       hexutil.Uint64(targetTxNum),
			BlockNumber: hexutil.Uint64(blockNum),
			LeafData: QMLeafData{
				PreStateHash:     witness.PreStateHash,
				StateChangeHash:  witness.StateChangeHash,
				TransitionHash:   witness.TransitionHash,
				PreviousLeafHash: witness.PreviousLeafHash,
			},
			LeafHash:    witness.LeafHash(),
			MerkleProof: witness.Proof.ToBytes(),
			QMTreeRoot:  witness.Proof.Root,
			Verified:    verified,
		},
		Verified: verified,
	}, nil
}

// GetAccountStateProof returns the account/storage state AND a qmtree proof
// for the last transaction that touched this address up to the given block.
func (api *QMAPIImpl) GetAccountStateProof(ctx context.Context, address common.Address, storageKeys []common.Hash, blockNrOrHash rpc.BlockNumberOrHash) (*QMStateProofResult, error) {
	if api.tracker == nil {
		return nil, fmt.Errorf("qmtree tracker not available")
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, _, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, nil)
	if err != nil {
		return nil, err
	}

	lastTxNum, err := api._txNumReader.Min(ctx, tx, uint64(blockNum)+1)
	if err != nil {
		return nil, fmt.Errorf("resolve block txnum: %w", err)
	}

	// Find the latest txnum <= lastTxNum that touched this address.
	it, err := tx.IndexRange(kv.AccountsHistoryIdx, address.Bytes(), -1, int(lastTxNum), false, -1)
	if err != nil {
		return nil, fmt.Errorf("index range: %w", err)
	}
	var targetTxNum uint64
	var found bool
	for it.HasNext() {
		txn, err := it.Next()
		if err != nil {
			return nil, err
		}
		if txn <= lastTxNum {
			targetTxNum = txn
			found = true
		}
	}
	if !found {
		return nil, fmt.Errorf("no state change found for address %s up to block %d", address.Hex(), blockNum)
	}

	bal, nonce, codeHash, slots, err := api.readStateAtTxNum(tx, address, storageKeys, targetTxNum)
	if err != nil {
		return nil, err
	}
	return api.buildStateProofResult(address, uint64(blockNum), targetTxNum, bal, nonce, codeHash, slots)
}

// GetTxStateProof returns the account/storage state after a specific
// transaction in a block, together with the qmtree proof for that transaction.
func (api *QMAPIImpl) GetTxStateProof(ctx context.Context, address common.Address, storageKeys []common.Hash, blockNrOrHash rpc.BlockNumberOrHash, txIndex hexutil.Uint) (*QMStateProofResult, error) {
	if api.tracker == nil {
		return nil, fmt.Errorf("qmtree tracker not available")
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	blockNum, _, _, err := rpchelper.GetCanonicalBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, nil)
	if err != nil {
		return nil, err
	}

	fromTxNum, err := api._txNumReader.Min(ctx, tx, uint64(blockNum))
	if err != nil {
		return nil, fmt.Errorf("resolve block start txnum: %w", err)
	}
	targetTxNum := fromTxNum + uint64(txIndex)

	bal, nonce, codeHash, slots, err := api.readStateAtTxNum(tx, address, storageKeys, targetTxNum)
	if err != nil {
		return nil, err
	}
	return api.buildStateProofResult(address, uint64(blockNum), targetTxNum, bal, nonce, codeHash, slots)
}

// GetLatestStateProof returns the current account/storage state together with
// the qmtree proof for the most recent transaction that touched this address.
func (api *QMAPIImpl) GetLatestStateProof(ctx context.Context, address common.Address, storageKeys []common.Hash) (*QMStateProofResult, error) {
	if api.tracker == nil {
		return nil, fmt.Errorf("qmtree tracker not available")
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Find the most recent txnum that touched this address across all history.
	it, err := tx.IndexRange(kv.AccountsHistoryIdx, address.Bytes(), -1, int(api.tracker.NextTxNum), false, -1)
	if err != nil {
		return nil, fmt.Errorf("index range: %w", err)
	}
	var targetTxNum uint64
	var found bool
	for it.HasNext() {
		txn, err := it.Next()
		if err != nil {
			return nil, err
		}
		targetTxNum = txn
		found = true
	}
	if !found {
		return nil, fmt.Errorf("no state change found for address %s", address.Hex())
	}

	bal, nonce, codeHash, slots, err := api.readLatestState(tx, address, storageKeys)
	if err != nil {
		return nil, err
	}
	return api.buildStateProofResult(address, 0, targetTxNum, bal, nonce, codeHash, slots)
}

// VerifyProof verifies a serialized qmtree proof offline.
func (api *QMAPIImpl) VerifyProof(_ context.Context, proofBytes hexutil.Bytes) (bool, error) {
	proof, err := qmtree.BytesToProofPath(proofBytes)
	if err != nil {
		return false, fmt.Errorf("deserialize proof: %w", err)
	}
	hasher := &qmtree.Keccak256Hasher{}
	return proof.Check(hasher, true) == nil, nil
}

// VerifyWitness verifies a serialized qmtree witness offline.
func (api *QMAPIImpl) VerifyWitness(_ context.Context, witnessBytes hexutil.Bytes) (bool, error) {
	w, err := qmtree.BytesToWitness(witnessBytes)
	if err != nil {
		return false, fmt.Errorf("deserialize witness: %w", err)
	}
	hasher := &qmtree.Keccak256Hasher{}
	return w.Verify(hasher) == nil, nil
}
