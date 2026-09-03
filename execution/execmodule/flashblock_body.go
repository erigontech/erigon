package execmodule

import (
	"context"
	"fmt"
	"sync"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// FlashblockInputs are the FIXED header inputs for the in-progress flashblock — everything BuildFlashHeader
// needs that is NOT derived from the body. Supplied by the consensus half (the cocoon driver) which owns the
// payload attributes; the execution half owns the body. Generic (no cocoon types) so it lives in erigon.
type FlashblockInputs struct {
	Parent                common.Hash
	Number                uint64
	GasLimit              uint64
	BaseFee               uint256.Int
	Timestamp             uint64
	PrevRandao            common.Hash // header.MixDigest
	FeeRecipient          common.Address
	ParentBeaconBlockRoot common.Hash
	Withdrawals           []*types.Withdrawal
}

// FlashblockOutputs is the CLOSE's computed output side of the header (all-zero for an in-progress /
// unsealed block, whose output is deferred to the seal).
type FlashblockOutputs struct {
	Root        common.Hash
	GasUsed     uint64
	ReceiptHash common.Hash
	Bloom       types.Bloom
}

// BuildFlashHeader constructs a flashblock header from its FIXED inputs, body, and (possibly zero) computed
// output side. The SINGLE header constructor for the flashblock flow so the in-progress and sealed headers
// stay byte-for-byte in lockstep. Moved from the cocoon driver into the execution half: the exec module owns
// the block body it maintains + executes, so it also owns the header derived from that body.
func BuildFlashHeader(in FlashblockInputs, txs [][]byte, out FlashblockOutputs) *types.Header {
	txHash := types.DeriveSha(types.BinaryTransactions(txs))
	wh := types.DeriveSha(types.Withdrawals(in.Withdrawals))
	var blockNum uint256.Int
	blockNum.SetUint64(in.Number)
	baseFee := in.BaseFee
	zeroBlobGas := uint64(0)
	pbbr := in.ParentBeaconBlockRoot
	reqHash := empty.RequestsHash
	return &types.Header{
		ParentHash:            in.Parent,
		UncleHash:             empty.UncleHash,
		Coinbase:              in.FeeRecipient,
		Root:                  out.Root,
		TxHash:                txHash,
		ReceiptHash:           out.ReceiptHash,
		Bloom:                 out.Bloom,
		Difficulty:            uint256.Int{},
		Number:                blockNum,
		GasLimit:              in.GasLimit,
		GasUsed:               out.GasUsed,
		Time:                  in.Timestamp,
		MixDigest:             in.PrevRandao,
		BaseFee:               &baseFee,
		WithdrawalsHash:       &wh,
		BlobGasUsed:           &zeroBlobGas,
		ExcessBlobGas:         &zeroBlobGas,
		ParentBeaconBlockRoot: &pbbr,
		RequestsHash:          &reqHash,
	}
}

// snKeyExec identifies a tx by sender+nonce for in-body dedup (a resubmit under a new hash reuses the pair).
type snKeyExec struct {
	addr  accounts.Address
	nonce uint64
}

// flashBodyState is the exec-owned in-progress block body. The execution half maintains it across PreExecute
// rounds: the driver streams UNFILTERED committed txs, exec filters them against its own SD (drop stale /
// duplicate nonces) and keeps ONLY the applicable ones here — so the body it builds, inserts, and executes is
// the filtered set by construction. This is the fix for the "filter then throw the result away" bug: there is
// no second, externally-inserted unfiltered body. Guarded by flashMu.
type flashBodyState struct {
	mu   sync.Mutex
	num  uint64             // block number this body belongs to; 0 = none open
	body [][]byte           // filtered tx RLPs, in order
	seen map[snKeyExec]bool // (sender,nonce) already decided (kept OR dropped) — never re-filter/re-add
	// The in-progress block's IDENTITY + the inputs it was built under, so exec (not the driver) owns the
	// whole in-progress flashblock. The driver reads these via FlashblockState() instead of caching its own
	// ib* under ibMu; the OUTPUT root/receipts are the last pre-exec's computed values (the final sealed
	// values come from the CLOSE — SealBlock's returned block). Set on a successful PreExecute, cleared on
	// reset. This is what removes the driver-data-lock-vs-exec-semaphore deadlock class.
	valid    bool             // a PreExecute has validated for this in-progress block
	built    FlashblockInputs // the header inputs the current in-progress header was built under (attrs compare)
	hash     common.Hash      // the current in-progress header hash
	root     common.Hash      // last pre-exec computed state root (per-round; seal recomputes the final)
	receipts int              // receipts accumulated across rounds (== body txs)
}

// AssembleInProgress seals the CURRENT in-progress flashblock — block-end over its maintained SD (ValidateChain,
// ZERO body re-execution) — and returns the sealed header WITHOUT advancing the frontier or re-keying the fork.
// It is the NON-MUTATING seal a follower uses to verify a delivered newPayload against what it would itself
// produce (verifyNewPayload's pre-marker fallback). Returns (nil,false) when no valid in-progress flashblock is
// recorded. Acquires the semaphore. This is the verb that replaced the driver reading FlashblockState/-Body +
// ValidateChain to seal from mirrored ib* state.
func (e *ExecModule) AssembleInProgress(ctx context.Context) (*types.Header, bool, error) {
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		return nil, false, err
	}
	defer e.semaphore.Release(1)
	e.flash.mu.Lock()
	num, hash, valid := e.flash.num, e.flash.hash, e.flash.valid
	built := e.flash.built
	body := append([][]byte(nil), e.flash.body...)
	e.flash.mu.Unlock()
	if !valid || hash == (common.Hash{}) {
		return nil, false, nil
	}
	res, err := e.validateChainLocked(ctx, hash, num)
	if err != nil {
		return nil, false, err
	}
	if res.ValidationStatus != ExecutionStatusSuccess {
		return nil, false, fmt.Errorf("AssembleInProgress: close status=%v err=%q", res.ValidationStatus, res.ValidationError)
	}
	sealed := BuildFlashHeader(built, body, FlashblockOutputs{
		Root:        res.ComputedRoot,
		GasUsed:     res.GasUsed,
		ReceiptHash: res.ReceiptHash,
		Bloom:       res.Bloom,
	})
	return sealed, true, nil
}

// InProgressRoot returns the last pre-exec computed state root of the in-progress flashblock and whether one is
// valid — a test/inspection hook on exec (the owner of the state), not a driver-orchestration surface.
func (e *ExecModule) InProgressRoot() (common.Hash, bool) {
	e.flash.mu.Lock()
	defer e.flash.mu.Unlock()
	return e.flash.root, e.flash.valid
}

// InProgressReceiptCount returns the receipts accumulated across the in-progress flashblock's rounds (== body
// txs executed so far) — a test/inspection hook on exec.
func (e *ExecModule) InProgressReceiptCount() int {
	e.flash.mu.Lock()
	defer e.flash.mu.Unlock()
	return e.flash.receipts
}

// InProgressBlock returns the in-progress flashblock's current header hash + number + whether it is valid — a
// test/inspection hook on exec (the owner of the in-progress identity), used to drive a ValidateChain close.
func (e *ExecModule) InProgressBlock() (common.Hash, uint64, bool) {
	e.flash.mu.Lock()
	defer e.flash.mu.Unlock()
	return e.flash.hash, e.flash.num, e.flash.valid
}

// resetLocked starts a fresh in-progress body for block num.
func (f *flashBodyState) resetLocked(num uint64) {
	f.num = num
	f.body = nil
	f.seen = make(map[snKeyExec]bool)
	f.valid = false
	f.built = FlashblockInputs{}
	f.hash = common.Hash{}
	f.root = common.Hash{}
	f.receipts = 0
}

// PreExecuteFlashblock is the encapsulated execution half: the driver hands it the UNFILTERED committed tx
// stream for the in-progress block plus the consensus-owned header inputs; exec filters the stream against its
// own frontier SD, appends the survivors to the body it MAINTAINS, builds the header from that filtered body,
// inserts it, and pre-executes the new txs. Returns the current (filtered) body + its hash. The filter and the
// body it guards live together, so the inserted/executed/sealed body is always the filtered set — the driver
// no longer builds a body, computes a header, or calls InsertBlocks.
func (e *ExecModule) PreExecuteFlashblock(ctx context.Context, inputs FlashblockInputs, newTxRLPs [][]byte) (*types.RawBody, common.Hash, ValidationResult, error) {
	// Hold the exec semaphore across the WHOLE round (insert + pre-execute) so no other exec op interleaves
	// mid-round, and so the atomic assemble can reuse preExecuteFlashblockLocked while already holding it.
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		return nil, common.Hash{}, ValidationResult{ValidationStatus: ExecutionStatusBusy}, err
	}
	defer e.semaphore.Release(1)
	return e.preExecuteFlashblockLocked(ctx, inputs, newTxRLPs)
}

// preExecuteFlashblockLocked is PreExecuteFlashblock's body with the caller ALREADY holding e.semaphore, so
// SealBlock can open the successor flashblock (empty round) atomically inside its own hold.
func (e *ExecModule) preExecuteFlashblockLocked(ctx context.Context, inputs FlashblockInputs, newTxRLPs [][]byte) (*types.RawBody, common.Hash, ValidationResult, error) {
	e.flash.mu.Lock()
	if e.flash.num != inputs.Number {
		e.flash.resetLocked(inputs.Number)
	}
	// Filter the incoming stream against the frontier SD nonce + in-body dedup, appending survivors. A tx below
	// the sender's frontier nonce is already sealed on the frontier (a DAG double-commit that straddled a block
	// boundary) — drop it and REMEMBER it (seen) so a re-drain does not reconsider it. This is where the stale
	// operator resolveBlock re-injection is removed, so it never reaches execution.
	kept, err := e.filterStreamLocked(ctx, newTxRLPs)
	if err != nil {
		e.flash.mu.Unlock()
		return nil, common.Hash{}, ValidationResult{ValidationStatus: ExecutionStatusBusy}, err
	}
	e.flash.body = append(e.flash.body, kept...)
	body := append([][]byte(nil), e.flash.body...)
	e.flash.mu.Unlock()

	header := BuildFlashHeader(inputs, body, FlashblockOutputs{})
	hash := header.Hash()
	rawBlock := &types.RawBlock{Header: header, Body: &types.RawBody{Transactions: body}}
	status, err := e.insertBlocksLocked(ctx, []*types.RawBlock{rawBlock})
	if err != nil || status != ExecutionStatusSuccess {
		return nil, common.Hash{}, ValidationResult{ValidationStatus: status}, fmt.Errorf("PreExecuteFlashblock: insert num=%d bodyTxs=%d status=%v: %w", inputs.Number, len(body), status, err)
	}
	vr, err := e.preExecuteLocked(ctx, hash, inputs.Number)
	if err != nil {
		return nil, common.Hash{}, vr, err
	}
	{
		var forkTxNum, curTxNum uint64
		if _, _, esd := e.forkValidator.ExtendingFork(); esd != nil {
			forkTxNum = esd.TxNum()
		}
		if e.currentContext != nil {
			curTxNum = e.currentContext.TxNum()
		}
		e.logger.Info("[TRACE-preexec] round", "block", inputs.Number,
			"roundKept", len(kept), "bodyLen", len(body), "forkTxNum", forkTxNum, "curTxNum", curTxNum,
			"receipts", vr.FlashblockReceiptCount, "gasUsed", vr.GasUsed, "gasLimit", inputs.GasLimit, "status", vr.ValidationStatus, "root", vr.ComputedRoot)
	}
	// Exec OWNS the in-progress state: record identity + outputs so the driver reads them via FlashblockState()
	// instead of caching ib* under ibMu. (num/body are already maintained above.)
	e.flash.mu.Lock()
	if e.flash.num == inputs.Number {
		e.flash.valid = true
		e.flash.built = inputs
		e.flash.hash = hash
		e.flash.root = vr.ComputedRoot
		e.flash.receipts = vr.FlashblockReceiptCount
	}
	e.flash.mu.Unlock()
	return &types.RawBody{Transactions: body}, hash, vr, nil
}

// filterStreamLocked filters newTxRLPs against the frontier SD account nonces (sequencing per sender across the
// batch), dropping stale (nonce below the account's frontier nonce) and duplicate (sender,nonce already
// decided) candidates, and RECORDS every decided (sender,nonce) in seen so a re-drain is never reconsidered.
// Caller holds e.flash.mu. Returns the survivors in order. Reuses filterCandidatesByNonce's sequencing via the
// frontier state reader.
func (e *ExecModule) filterStreamLocked(ctx context.Context, newTxRLPs [][]byte) ([][]byte, error) {
	if len(newTxRLPs) == 0 {
		return nil, nil
	}
	reader, closeReader, err := e.frontierStateReader(ctx)
	if err != nil {
		return nil, err
	}
	defer closeReader()
	signer := types.LatestSignerForChainID(e.config.ChainID)

	// Base nonce per sender: the frontier account nonce, advanced by whatever this in-progress body already
	// holds for that sender (those txs will execute before these). next[] tracks the sequencing.
	next := make(map[accounts.Address]uint64)
	nextNonce := func(a accounts.Address) uint64 {
		if n, ok := next[a]; ok {
			return n
		}
		if acc, aerr := reader.ReadAccountData(a); aerr == nil && acc != nil {
			return acc.Nonce
		}
		return 0
	}

	survivors := make([][]byte, 0, len(newTxRLPs))
	for _, rlp := range newTxRLPs {
		tx, derr := types.DecodeTransaction(rlp)
		if derr != nil {
			continue // undecodable — drop
		}
		s, ok := tx.GetSender()
		if !ok {
			if rec, serr := signer.Sender(tx); serr == nil {
				s, ok = rec, true
			}
		}
		if !ok {
			continue // unrecoverable sender — drop
		}
		k := snKeyExec{addr: s, nonce: tx.GetNonce()}
		if e.flash.seen[k] {
			continue // already decided (kept or dropped) — never reconsider
		}
		e.flash.seen[k] = true
		exp := nextNonce(s)
		if tx.GetNonce() < exp {
			continue // stale: already sealed on the frontier — dropped (and now remembered)
		}
		if tx.GetNonce() > exp {
			// Future/gap: leave it for a later drain (do NOT mark advanced). Remembered as seen so the exact
			// (sender,nonce) is not re-decided this block; a genuine gap-fill arrives under a different nonce.
			continue
		}
		survivors = append(survivors, rlp)
		next[s] = exp + 1
	}
	return survivors, nil
}

// frontierStateReader opens a StateReader over the current frontier (the in-progress extending-fork SD when
// present, else the canonical head) so the stream filter reads the SAME account nonces execution will see.
// Returns the reader plus a cleanup that rolls back the borrowed tx.
func (e *ExecModule) frontierStateReader(ctx context.Context) (state.StateReader, func(), error) {
	roTx, err := e.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, func() {}, err
	}
	// Mirror PreExecute's frontier-parent selection so the filter reads the SAME accumulated state execution
	// will build on: the live extending fork if present, else the newest PARKED pre-executed gen (run-ahead:
	// the last sealed block is parked, not yet canonical), else the canonical context. Reading lagging
	// canonical DB here would make a valid next-nonce tx look like a future gap and be wrongly dropped.
	var sd *execctx.SharedDomains
	if _, _, extSD := e.forkValidator.ExtendingFork(); extSD != nil {
		sd = extSD
	} else if p := e.forkValidator.NewestFrontierSD(); p != nil {
		sd = p
	} else {
		sd = e.currentContext
	}
	if sd == nil {
		// No in-progress or canonical SD yet (the very first block): read genesis/canonical state straight off
		// the DB tx (a TemporalTx is itself a TemporalGetter). A fresh account reads nonce 0 — correct base.
		return state.NewReaderV3(roTx), func() { roTx.Rollback() }, nil
	}
	ov := sd.BlockOverlay()
	if ov != nil {
		ov.UpdateTxn(roTx)
		return state.NewReaderV3(sd.AsGetter(ov)), func() { roTx.Rollback() }, nil
	}
	return state.NewReaderV3(sd.AsGetter(roTx)), func() { roTx.Rollback() }, nil
}
