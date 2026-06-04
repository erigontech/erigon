package jsonrpc

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/commitment/trie"
	witnesstypes "github.com/erigontech/erigon/execution/commitment/witness"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/rpc/transactions"
)

// RecordingState combines a StateReader and StateWriter with an in-memory overlay.
// Reads check the overlay first (accounting for deletes and modifications), then
// fall back to the inner reader. Writes go to the overlay. All accesses and
// modifications are recorded for witness generation.
type RecordingState struct {
	inner  state.StateReader
	trace  bool
	prefix string

	// Read tracking (all accessed keys, including reads that hit the overlay)
	AccessedAccounts  map[common.Address]struct{}
	AccessedStorage   map[common.Address]map[common.Hash]struct{}
	AccessedCode      map[common.Address][]byte // all code seen during execution
	PreStateCode      map[common.Address][]byte // code read from the inner reader (pre-block state only)
	emptyCodeAccessed bool                      // an empty-code account had its code loaded (legacy emits one empty bytecode)
	// createdCodeHashes holds code hashes written in-block; a pre-state read of a hash
	// already created in-block is redundant in the witness (the verifier replays the create).
	createdCodeHashes map[common.Hash]struct{}

	//HashedCodes map[common.Hash][]byte // set of code hashes seen during execution, used to avoid duplicate code entries in result.Codes

	// In-memory state overlay (writes)
	accountOverlay map[common.Address]*accounts.Account // non-nil = updated, entry present with nil value=deleted
	storageOverlay map[common.Address]map[common.Hash]uint256.Int
	codeOverlay    map[common.Address][]byte

	// Write tracking
	ModifiedAccounts      map[common.Address]struct{}
	ReallyChangedAccounts map[common.Address]struct{} // subset of ModifiedAccounts where actual state (nonce/balance/code) changed
	ModifiedStorage       map[common.Address]map[common.Hash]struct{}
	ModifiedCode          map[common.Address][]byte
	DeletedAccounts       map[common.Address]struct{}
	CreatedContracts      map[common.Address]struct{}

	// for debugging: addresses to trace operations on
	accountsToTrace map[common.Address]struct{}

	// The system address is touched as msg.sender on every block's system calls;
	// that alone is not a witness access. A real opcode access during a user tx
	// (seen via the per-tx access set) sets this so it is kept (EIP-7928).
	systemAddrTouchedInTx bool
}

// NewRecordingState creates a new RecordingState wrapping the given inner reader.
func NewRecordingState(inner state.StateReader) *RecordingState {
	return &RecordingState{
		inner:                 inner,
		AccessedAccounts:      make(map[common.Address]struct{}),
		AccessedStorage:       make(map[common.Address]map[common.Hash]struct{}),
		AccessedCode:          make(map[common.Address][]byte),
		PreStateCode:          make(map[common.Address][]byte),
		createdCodeHashes:     make(map[common.Hash]struct{}),
		accountOverlay:        make(map[common.Address]*accounts.Account),
		storageOverlay:        make(map[common.Address]map[common.Hash]uint256.Int),
		codeOverlay:           make(map[common.Address][]byte),
		ModifiedAccounts:      make(map[common.Address]struct{}),
		ReallyChangedAccounts: make(map[common.Address]struct{}),
		ModifiedStorage:       make(map[common.Address]map[common.Hash]struct{}),
		ModifiedCode:          make(map[common.Address][]byte),
		DeletedAccounts:       make(map[common.Address]struct{}),
		CreatedContracts:      make(map[common.Address]struct{}),
	}
}

// MarkSystemAddrTouchedInTx records that a user transaction accessed the system
// address via an opcode, so it is kept in the witness even without a state change.
func (s *RecordingState) MarkSystemAddrTouchedInTx() { s.systemAddrTouchedInTx = true }

func (s *RecordingState) SetAccountsToTrace(addrs []common.Address) {
	if len(addrs) == 0 {
		return
	}
	s.trace = true
	s.accountsToTrace = make(map[common.Address]struct{}, len(addrs))
	for _, a := range addrs {
		s.accountsToTrace[a] = struct{}{}
	}
}

func (s *RecordingState) tracing(addr common.Address) bool {
	if !s.trace {
		return false
	}
	_, ok := s.accountsToTrace[addr]
	return ok
}

// --- StateReader implementation ---

func (s *RecordingState) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	addr := address.Value()
	s.AccessedAccounts[addr] = struct{}{}
	// Check overlay: deleted accounts return nil
	if _, deleted := s.DeletedAccounts[addr]; deleted {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountData %s -> deleted\n", addr.Hex())
		}
		return nil, nil
	}
	if acc, ok := s.accountOverlay[addr]; ok {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountData %s -> overlay nonce=%d balance=%d codeHash=%x\n", addr.Hex(), acc.Nonce, &acc.Balance, acc.CodeHash)
		}
		return acc, nil
	}
	acc, err := s.inner.ReadAccountData(address)
	if acc != nil && acc.IsEmptyCodeHash() {
		// Loading an empty-code account materializes the empty bytecode; legacy
		// mode emits that single empty entry.
		s.emptyCodeAccessed = true
	}
	if s.tracing(addr) {
		if acc != nil {
			fmt.Printf("[TRACE] ReadAccountData %s -> inner nonce=%d balance=%d codeHash=%x\n", addr.Hex(), acc.Nonce, &acc.Balance, acc.CodeHash)
		} else {
			fmt.Printf("[TRACE] ReadAccountData %s -> inner nil (err=%v)\n", addr.Hex(), err)
		}
	}
	return acc, err
}

func (s *RecordingState) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	addr := address.Value()
	s.AccessedAccounts[addr] = struct{}{}
	if _, deleted := s.DeletedAccounts[addr]; deleted {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountDataForDebug %s -> deleted\n", addr.Hex())
		}
		return nil, nil
	}
	if acc, ok := s.accountOverlay[addr]; ok {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountDataForDebug %s -> overlay nonce=%d balance=%d codeHash=%x\n", addr.Hex(), acc.Nonce, &acc.Balance, acc.CodeHash)
		}
		return acc, nil
	}
	acc, err := s.inner.ReadAccountDataForDebug(address)
	if s.tracing(addr) {
		if acc != nil {
			fmt.Printf("[TRACE] ReadAccountDataForDebug %s -> inner nonce=%d balance=%d codeHash=%x\n", addr.Hex(), acc.Nonce, &acc.Balance, acc.CodeHash)
		} else {
			fmt.Printf("[TRACE] ReadAccountDataForDebug %s -> inner nil (err=%v)\n", addr.Hex(), err)
		}
	}
	return acc, err
}

func (s *RecordingState) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addr := address.Value()
	s.AccessedAccounts[addr] = struct{}{}
	if s.AccessedStorage[addr] == nil {
		s.AccessedStorage[addr] = make(map[common.Hash]struct{})
	}
	s.AccessedStorage[addr][key.Value()] = struct{}{}
	// Deleted accounts have no storage
	if _, deleted := s.DeletedAccounts[addr]; deleted {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountStorage %s key=%s -> deleted\n", addr.Hex(), key.Value().Hex())
		}
		return uint256.Int{}, false, nil
	}
	// Check if this storage slot has been written in the overlay
	if mods, ok := s.ModifiedStorage[addr]; ok {
		if _, modified := mods[key.Value()]; modified {
			val := s.storageOverlay[addr][key.Value()]
			if s.tracing(addr) {
				fmt.Printf("[TRACE] ReadAccountStorage %s key=%s -> overlay val=%d\n", addr.Hex(), key.Value().Hex(), &val)
			}
			return val, !val.IsZero(), nil
		}
	}
	val, ok, err := s.inner.ReadAccountStorage(address, key)
	if s.tracing(addr) {
		fmt.Printf("[TRACE] ReadAccountStorage %s key=%s -> inner val=%d ok=%v err=%v\n", addr.Hex(), key.Value().Hex(), &val, ok, err)
	}
	return val, ok, err
}

func (s *RecordingState) HasStorage(address accounts.Address) (bool, error) {
	addr := address.Value()
	s.AccessedAccounts[addr] = struct{}{}
	// Check overlay for any non-zero storage
	if mods, ok := s.storageOverlay[addr]; ok {
		for _, val := range mods {
			if !val.IsZero() {
				if s.tracing(addr) {
					fmt.Printf("[TRACE] HasStorage %s -> overlay true\n", addr.Hex())
				}
				return true, nil
			}
		}
	}
	if _, deleted := s.DeletedAccounts[addr]; deleted {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] HasStorage %s -> deleted false\n", addr.Hex())
		}
		return false, nil
	}
	has, err := s.inner.HasStorage(address)
	if s.tracing(addr) {
		fmt.Printf("[TRACE] HasStorage %s -> inner %v (err=%v)\n", addr.Hex(), has, err)
	}
	return has, err
}

func (s *RecordingState) ReadAccountCode(address accounts.Address) ([]byte, error) {
	addr := address.Value()
	s.AccessedAccounts[addr] = struct{}{}
	if _, deleted := s.DeletedAccounts[addr]; deleted {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountCode %s -> deleted\n", addr.Hex())
		}
		return nil, nil
	}
	if code, ok := s.codeOverlay[addr]; ok {
		if len(code) > 0 {
			s.AccessedCode[addr] = code
		}
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountCode %s -> overlay len=%d\n", addr.Hex(), len(code))
		}
		return code, nil
	}
	code, err := s.inner.ReadAccountCode(address)
	if err != nil {
		return nil, err
	}
	if len(code) > 0 {
		s.AccessedCode[addr] = code
		if _, already := s.PreStateCode[addr]; !already {
			if _, created := s.createdCodeHashes[crypto.Keccak256Hash(code)]; !created {
				s.PreStateCode[addr] = code
			}
		}
	} else {
		s.emptyCodeAccessed = true
	}
	if s.tracing(addr) {
		fmt.Printf("[TRACE] ReadAccountCode %s -> inner len=%d\n", addr.Hex(), len(code))
	}
	return code, nil
}

func (s *RecordingState) ReadAccountCodeSize(address accounts.Address) (int, error) {
	addr := address.Value()
	s.AccessedAccounts[addr] = struct{}{}
	if _, deleted := s.DeletedAccounts[addr]; deleted {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountCodeSize %s -> deleted 0\n", addr.Hex())
		}
		return 0, nil
	}
	if code, ok := s.codeOverlay[addr]; ok {
		if s.tracing(addr) {
			fmt.Printf("[TRACE] ReadAccountCodeSize %s -> overlay %d\n", addr.Hex(), len(code))
		}
		return len(code), nil
	}
	_, err := s.ReadAccountCode(address) // need to read code here because witness has no way of knowing code size without reading the code first
	if err != nil {
		return 0, err
	}
	size, err := s.inner.ReadAccountCodeSize(address)
	if s.tracing(addr) {
		fmt.Printf("[TRACE] ReadAccountCodeSize %s -> inner %d (err=%v)\n", addr.Hex(), size, err)
	}
	return size, err
}

func (s *RecordingState) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	addr := address.Value()
	s.AccessedAccounts[addr] = struct{}{}
	inc, err := s.inner.ReadAccountIncarnation(address)
	if s.tracing(addr) {
		fmt.Printf("[TRACE] ReadAccountIncarnation %s -> %d (err=%v)\n", addr.Hex(), inc, err)
	}
	return inc, err
}

func (s *RecordingState) SetTrace(trace bool, tracePrefix string) {
	s.trace = trace
	s.prefix = tracePrefix
}

func (s *RecordingState) Trace() bool {
	return s.trace
}

func (s *RecordingState) TracePrefix() string {
	return s.prefix
}

// --- StateWriter implementation ---

func (s *RecordingState) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	addr := address.Value()
	s.ModifiedAccounts[addr] = struct{}{}
	if original == nil || account.Nonce != original.Nonce || !account.Balance.Eq(&original.Balance) || account.CodeHash != original.CodeHash {
		s.ReallyChangedAccounts[addr] = struct{}{}
	}
	// Store a copy in the overlay
	acctCopy := *account
	s.accountOverlay[addr] = &acctCopy
	delete(s.DeletedAccounts, addr)
	if s.tracing(addr) {
		fmt.Printf("[TRACE] UpdateAccountData %s nonce=%d balance=%d codeHash=%x\n", addr.Hex(), account.Nonce, &account.Balance, account.CodeHash)
	}
	return nil
}

func (s *RecordingState) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	addr := address.Value()
	s.ModifiedAccounts[addr] = struct{}{}
	s.codeOverlay[addr] = common.Copy(code)
	s.ModifiedCode[addr] = common.Copy(code)
	if len(code) > 0 {
		s.createdCodeHashes[codeHash.Value()] = struct{}{}
	}
	// Keep accountOverlay CodeHash in sync so ReadAccountData returns a
	// consistent CodeHash even before UpdateAccountData is called.
	if acc, ok := s.accountOverlay[addr]; ok && acc != nil {
		acc.CodeHash = codeHash
	}
	if s.tracing(addr) {
		fmt.Printf("[TRACE] UpdateAccountCode %s codeHash=%x len=%d\n", addr.Hex(), codeHash, len(code))
	}
	return nil
}

func (s *RecordingState) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	addr := address.Value()
	s.ModifiedAccounts[addr] = struct{}{}
	s.DeletedAccounts[addr] = struct{}{}
	delete(s.accountOverlay, addr)
	// Clear storage overlay for this account
	delete(s.storageOverlay, addr)
	delete(s.codeOverlay, addr)
	if s.tracing(addr) {
		fmt.Printf("[TRACE] DeleteAccount %s\n", addr.Hex())
	}
	return nil
}

func (s *RecordingState) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	addr := address.Value()
	s.ModifiedAccounts[addr] = struct{}{}
	if s.ModifiedStorage[addr] == nil {
		s.ModifiedStorage[addr] = make(map[common.Hash]struct{})
	}
	s.ModifiedStorage[addr][key.Value()] = struct{}{}
	// Store in overlay
	if s.storageOverlay[addr] == nil {
		s.storageOverlay[addr] = make(map[common.Hash]uint256.Int)
	}
	s.storageOverlay[addr][key.Value()] = value
	if s.tracing(addr) {
		fmt.Printf("[TRACE] WriteAccountStorage %s key=%s val=%d\n", addr.Hex(), key.Value().Hex(), &value)
	}
	return nil
}

func (s *RecordingState) CreateContract(address accounts.Address) error {
	addr := address.Value()
	s.ModifiedAccounts[addr] = struct{}{}
	s.CreatedContracts[addr] = struct{}{}
	delete(s.DeletedAccounts, addr)
	if s.tracing(addr) {
		fmt.Printf("[TRACE] CreateContract %s\n", addr.Hex())
	}
	return nil
}

// accountExists reports whether the account exists in the post-state: present and
// non-nil in the write overlay, otherwise not deleted and present in the inner
// (pre-block) reader. The inner reader is read directly so the check does not
// re-mark AccessedAccounts.
func (s *RecordingState) accountExists(addr common.Address) bool {
	if _, deleted := s.DeletedAccounts[addr]; deleted {
		return false
	}
	if acc, ok := s.accountOverlay[addr]; ok {
		return acc != nil
	}
	acc, err := s.inner.ReadAccountData(accounts.InternAddress(addr))
	return err == nil && acc != nil
}

// --- Query methods ---

// GetAccessedKeys returns all accessed account addresses and storage keys (reads + writes)
func (s *RecordingState) GetAccessedKeys() ([]common.Address, map[common.Address][]common.Hash) {
	addresses := make([]common.Address, 0, len(s.AccessedAccounts))
	for addr := range s.AccessedAccounts {
		addresses = append(addresses, addr)
	}

	storageKeys := make(map[common.Address][]common.Hash)
	for addr, keys := range s.AccessedStorage {
		keySlice := make([]common.Hash, 0, len(keys))
		for key := range keys {
			keySlice = append(keySlice, key)
		}
		storageKeys[addr] = keySlice
	}

	return addresses, storageKeys
}

// GetModifiedKeys returns all modified account addresses and storage keys
func (s *RecordingState) GetModifiedKeys() ([]common.Address, map[common.Address][]common.Hash) {
	addresses := make([]common.Address, 0, len(s.ModifiedAccounts))
	for addr := range s.ModifiedAccounts {
		addresses = append(addresses, addr)
	}

	storageKeys := make(map[common.Address][]common.Hash)
	for addr, keys := range s.ModifiedStorage {
		keySlice := make([]common.Hash, 0, len(keys))
		for key := range keys {
			keySlice = append(keySlice, key)
		}
		storageKeys[addr] = keySlice
	}

	return addresses, storageKeys
}

// OnCodeAccess tracks code that bypasses ReadAccountCode via stateObject cache hits.
func (s *RecordingState) OnCodeAccess(address accounts.Address, code []byte) {
	if len(code) > 0 {
		s.AccessedCode[address.Value()] = code
		//s.HashedCodes[crypto.Keccak256Hash(code)] = code
	}
}

// GetAccessedCode returns all code seen during execution (overlay + inner reads)
func (s *RecordingState) GetAccessedCode() map[common.Address][]byte {
	result := make(map[common.Address][]byte, len(s.AccessedCode))
	for addr, code := range s.AccessedCode {
		result[addr] = common.Copy(code)
	}
	return result
}

// GetPreStateCode returns code read from the inner reader only (pre-block state).
// This is the code that existed at the start of the block, before any
// transactions modified it. Used for witness trie generation where the
// CodeHash must match the parent-state commitment.
func (s *RecordingState) GetPreStateCode() map[common.Address][]byte {
	result := make(map[common.Address][]byte, len(s.PreStateCode))
	for addr, code := range s.PreStateCode {
		result[addr] = common.Copy(code)
	}
	return result
}

// GetModifiedCode returns all modified contract code
func (s *RecordingState) GetModifiedCode() map[common.Address][]byte {
	result := make(map[common.Address][]byte, len(s.ModifiedCode))
	for addr, code := range s.ModifiedCode {
		result[addr] = common.Copy(code)
	}
	return result
}

// witnessBlockInfo bundles the inputs that downstream witness-building phases need
// after resolving the target block: the block itself plus its txnum range and the
// parent block number. BlockNum is stored as a field (rather than recomputed via
// Block.NumberU64) because it's referenced in many downstream call sites.
type witnessBlockInfo struct {
	Block             *types.Block
	BlockNum          uint64
	FirstTxNumInBlock uint64
	EndTxNum          uint64
	ParentNum         uint64
}

// ExecutionWitnessResult is the response format for debug_executionWitness
type ExecutionWitnessResult struct {
	// State contains the list of RLP-encoded trie nodes in the witness trie
	State []hexutil.Bytes `json:"state"`
	// Codes is the list of bytecodes accessed via GetCode/GetCodeSize during block execution,
	// matching Geth's witness.AddCode semantics. Code that was deployed/modified but never
	// read (e.g. CREATE without a subsequent call) is intentionally excluded.
	Codes []hexutil.Bytes `json:"codes"`
	// Keys holds the standalone preimages of every accessed account address (20B) and
	// storage slot (32B), matching the reference legacy witness; populated in both modes.
	Keys []hexutil.Bytes `json:"keys,omitempty"`
	// Headers is the contiguous chain of RLP-encoded ancestor headers from the parent
	// back to the oldest block reached via BLOCKHASH.
	Headers []hexutil.Bytes `json:"headers,omitempty"`

	// lookup map for BLOCKHASH opcode, not serialized to JSON
	headerByNumber map[uint64]*types.Header
}

func (m *ExecutionWitnessResult) getHashFn(blockNum uint64) (common.Hash, error) {
	if header, ok := m.headerByNumber[blockNum]; ok {
		return header.Hash(), nil
	}
	return common.Hash{}, nil
}

// witnessMode selects the debug_executionWitness output format. legacy is the
// default full format; canonical is the minimized ethereum/execution-specs zkevm
// format (no empty nodes, no in-block-created bytecode, minimum siblings).
type witnessMode int

const (
	witnessModeLegacy witnessMode = iota
	witnessModeCanonical
)

// resolveWitnessMode resolves the witness mode with precedence: request param > env > legacy.
// An explicit param value other than "legacy"/"canonical" is rejected.
func resolveWitnessMode(modeParam *string) (witnessMode, error) {
	if modeParam != nil {
		switch *modeParam {
		case "legacy":
			return witnessModeLegacy, nil
		case "canonical":
			return witnessModeCanonical, nil
		default:
			return witnessModeLegacy, fmt.Errorf("invalid witness mode %q: must be \"legacy\" or \"canonical\"", *modeParam)
		}
	}
	if dbg.EnvString("ERIGON_WITNESS_MODE", "legacy") == "canonical" {
		return witnessModeCanonical, nil
	}
	return witnessModeLegacy, nil
}

// ExecutionWitness implements debug_executionWitness.
// It executes a block using a historical state reader, records all state accesses
// (accounts, storage, code), and builds merkle proofs for the accessed keys.
func (api *DebugAPIImpl) ExecutionWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash, mode *string) (*ExecutionWitnessResult, error) {
	resolvedMode, err := resolveWitnessMode(mode)
	if err != nil {
		return nil, err
	}

	tx, err := api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	commitmentHistoryEnabled, _, err := rawdb.ReadDBCommitmentHistoryEnabled(tx)
	if err != nil {
		return nil, err
	}
	if !commitmentHistoryEnabled {
		return nil, fmt.Errorf("debug_executionWitness requires commitment history: restart the node with --prune.experimental.include-commitment-history")
	}

	info, err := api.resolveWitnessBlock(ctx, tx, blockNrOrHash)
	if err != nil {
		return nil, err
	}
	blockNum := info.BlockNum
	block := info.Block
	firstTxNumInBlock := info.FirstTxNumInBlock
	endTxNum := info.EndTxNum
	parentNum := info.ParentNum

	chainConfig, err := api.chainConfig(ctx, tx)
	if err != nil {
		return nil, err
	}

	engine := api.engine()

	// Create a state reader at the parent block state using the exact txnum
	var stateReader state.StateReader = state.NewHistoryReaderV3(tx, firstTxNumInBlock)

	// Create a combined recording state (reader + writer with in-memory overlay)
	recordingState := NewRecordingState(stateReader)
	recordingState.SetAccountsToTrace([]common.Address{
		// Add addresses to trace here, e.g.:
		// common.HexToAddress("0x8863786beBE8eB9659DF00b49f8f1eeEc7e2C8c1"),
	})

	// Create the in-block state with the recording state as reader
	ibs := state.New(recordingState)

	// Get header for block context
	header := block.Header()

	// Create EVM block context
	blockCtx := transactions.NewEVMBlockContext(engine, header, true /* requireCanonical */, tx, api._blockReader, chainConfig)
	blockRules := blockCtx.Rules(chainConfig)
	signer := types.MakeSigner(chainConfig, blockNum, header.Time)

	// Track accessed block hashes for BLOCKHASH opcode
	var accessedBlockHashes []uint64
	originalGetHash := blockCtx.GetHash
	blockCtx.GetHash = func(n uint64) (common.Hash, error) {
		accessedBlockHashes = append(accessedBlockHashes, n)
		return originalGetHash(n)
	}

	// Run block initialization (e.g. EIP-2935 blockhash contract, EIP-4788 beacon root)
	fullEngine, ok := engine.(rules.Engine)
	if !ok {
		return nil, fmt.Errorf("engine does not support full rules.Engine interface")
	}
	chainReader := consensuschain.NewReader(chainConfig, tx, api._blockReader, log.Root())
	systemCallCustom := func(contract accounts.Address, data []byte, ibState *state.IntraBlockState, hdr *types.Header, constCall bool) ([]byte, error) {
		return protocol.SysCallContract(contract, data, chainConfig, ibState, hdr, fullEngine, constCall, vm.Config{})
	}
	if err = fullEngine.Initialize(chainConfig, chainReader, header, ibs, systemCallCustom, log.Root(), nil); err != nil {
		return nil, fmt.Errorf("failed to initialize block: %w", err)
	}
	if err = ibs.FinalizeTx(blockRules, recordingState); err != nil {
		return nil, fmt.Errorf("failed to finalize engine.Initialize tx: %w", err)
	}

	// Execute all transactions in the block
	for txIndex, txn := range block.Transactions() {
		msg, err := txn.AsMessage(*signer, header.BaseFee, blockRules)
		if err != nil {
			return nil, fmt.Errorf("failed to convert tx %d to message: %w", txIndex, err)
		}

		txCtx := protocol.NewEVMTxContext(msg)
		evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{})

		gp := new(protocol.GasPool).AddGas(header.GasLimit).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(header.Time))
		ibs.SetTxContext(blockNum, txIndex)

		_, err = protocol.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, engine)
		// A user tx that accesses the system address via an opcode keeps it in the
		// witness; the per-tx access set captures this even on state-cache hits.
		if acc := ibs.AccessedAddresses(); acc != nil {
			if _, ok := acc[params.SystemAddress]; ok {
				recordingState.MarkSystemAddrTouchedInTx()
			}
		}
		if err != nil {
			return nil, fmt.Errorf("failed to apply tx %d: %w", txIndex, err)
		}

		if err = ibs.FinalizeTx(blockRules, recordingState); err != nil {
			return nil, fmt.Errorf("failed to finalize tx %d: %w", txIndex, err)
		}
	}

	syscall := func(contract accounts.Address, data []byte) ([]byte, error) {
		return protocol.SysCallContract(contract, data, chainConfig, ibs, header, fullEngine, false /* constCall */, vm.Config{})
	}

	// Collect logs accumulated during transaction execution into a synthetic receipt
	// so that Finalize can parse EIP-6110 deposit requests from them.
	// Finalize only uses receipt.Logs from each receipt — it doesn't read Status, GasUsed, CumulativeGasUsed, or any other field. It just concatenates all logs
	// into a flat slice and passes them to ParseDepositLogs.
	allLogs := ibs.Logs()
	receipts := types.Receipts{&types.Receipt{Logs: allLogs}}

	if _, err = fullEngine.Finalize(chainConfig, types.CopyHeader(header), ibs, block.Uncles(), receipts, block.Withdrawals(), chainReader, syscall, false /* skipReceiptsEval */, log.Root()); err != nil {
		return nil, fmt.Errorf("failed to finalize block: %w", err)
	}

	if err = ibs.CommitBlock(blockRules, recordingState); err != nil {
		return nil, fmt.Errorf("failed to commit block: %w", err)
	}

	accessed := collectAccessedState(recordingState, resolvedMode)

	result := &ExecutionWitnessResult{
		State:          []hexutil.Bytes{},
		Codes:          accessed.SortedCodes,
		Keys:           accessed.WitnessKeys,
		headerByNumber: make(map[uint64]*types.Header),
	}

	// Build merkle proofs for all accessed accounts
	// Use the proof infrastructure from the commitment context
	domains, err := execctx.NewSharedDomains(ctx, tx, log.New())
	if err != nil {
		return nil, err
	}
	defer domains.Close()
	sdCtx := domains.GetCommitmentContext()
	sdCtx.SetDeferBranchUpdates(false)

	// Get the expected parent state root for verification
	var expectedParentRoot common.Hash

	// Get the parent header for state root verification
	parentHeader, err := api._blockReader.HeaderByNumber(ctx, tx, parentNum)
	if err != nil {
		return nil, err
	}
	if parentHeader == nil {
		return nil, fmt.Errorf("parent header %d not found", parentNum)
	}
	expectedParentRoot = parentHeader.Root
	log.Debug("expected parent root", "stateRoot", expectedParentRoot)

	commitmentStartingTxNum := tx.Debug().HistoryStartFrom(kv.CommitmentDomain)
	if firstTxNumInBlock < commitmentStartingTxNum {
		return nil, fmt.Errorf("commitment history pruned: start %d, last tx: %d", commitmentStartingTxNum, firstTxNumInBlock)
	}

	if accessed.isEmpty() { // nothing touched, return empty witness
		return result, nil
	}

	siblingPaths, err := detectCollapseSiblings(ctx, tx, domains, sdCtx,
		firstTxNumInBlock, endTxNum, blockNum, parentNum,
		block.Root(), accessed, resolvedMode)
	if err != nil {
		return nil, err
	}

	nodes, err := buildWitnessTrie(ctx, tx, domains, sdCtx, firstTxNumInBlock, expectedParentRoot, siblingPaths, accessed, resolvedMode)
	if err != nil {
		return nil, err
	}
	result.State = nodes

	headers, byNumber, err := api.collectAccessedHeaders(ctx, tx, parentNum, accessedBlockHashes)
	if err != nil {
		return nil, err
	}
	result.Headers = headers
	result.headerByNumber = byNumber

	if err := api.verifyWitnessStateless(ctx, tx, result, block, fullEngine); err != nil {
		return nil, err
	}

	// legacy carries the empty storage-trie node (0x80) once when some account has an
	// empty storage root (EmptyRoot appears only as an account-leaf storage-root field);
	// canonical omits it. Added after stateless verification, which rejects the bare node.
	if resolvedMode == witnessModeLegacy {
		for _, node := range result.State {
			if bytes.Contains(node, trie.EmptyRoot[:]) {
				result.State = append(result.State, hexutil.Bytes{0x80})
				break
			}
		}
	}

	// Sort after verifyWitnessStateless: RLPDecode treats result.State[0] as the trie root.
	slices.SortFunc(result.State, func(a, b hexutil.Bytes) int {
		return bytes.Compare(a, b)
	})

	return result, nil
}

// accessedState summarizes everything the witness needs from a recorded execution:
// the deduplicated set of accessed accounts/storage/code addresses, the sorted code
// blobs that go into result.Codes, and the pre-state code reads that feed witness
// trie generation. WitnessKeys holds the standalone preimages (20B addresses, 32B
// slots) that go into result.Keys.
type accessedState struct {
	WitnessKeys []hexutil.Bytes
	Addresses   map[common.Address]struct{}
	Storage     map[common.Address]map[common.Hash]struct{}
	CodeAddrs   map[common.Address]struct{}
	SortedCodes []hexutil.Bytes
	CodeReads   map[common.Hash]witnesstypes.CodeWithHash
}

// isEmpty reports whether no accounts, storage slots, or code addresses were touched.
// Note: CodeReads / SortedCodes are intentionally NOT considered — a block can read
// code without touching the underlying account if the bytecode was already cached.
func (a *accessedState) isEmpty() bool {
	return len(a.Addresses)+len(a.Storage)+len(a.CodeAddrs) == 0
}

// touchNonZeroKeys touches only keys whose value is non-zero in at least one of pre/post state,
// skipping zero→zero no-ops that would corrupt the HPH trie's internal maps.
func (a *accessedState) touchNonZeroKeys(sdCtx *commitmentdb.SharedDomainsCommitmentContext, post, pre commitmentdb.StateReader, stepSize uint64) {
	for addr := range a.Addresses {
		plainKey := addr[:]
		postEnc, _, _ := post.Read(kv.AccountsDomain, plainKey, stepSize)
		if len(postEnc) == 0 {
			preEnc, _, _ := pre.Read(kv.AccountsDomain, plainKey, stepSize)
			if len(preEnc) == 0 {
				continue
			}
		}
		sdCtx.TouchKey(kv.AccountsDomain, string(plainKey), nil)
	}
	for addr := range a.CodeAddrs {
		sdCtx.TouchKey(kv.CodeDomain, string(addr[:]), nil)
	}
	for addr, keys := range a.Storage {
		for key := range keys {
			plainKey := append(addr[:], key[:]...)
			postEnc, _, _ := post.Read(kv.StorageDomain, plainKey, stepSize)
			if len(postEnc) == 0 {
				preEnc, _, _ := pre.Read(kv.StorageDomain, plainKey, stepSize)
				if len(preEnc) == 0 {
					continue
				}
			}
			sdCtx.TouchKey(kv.StorageDomain, string(plainKey), nil)
		}
	}
}

// touchAll touches every accessed account, storage slot, and code address on the
// commitment context. Order matches the original inline implementation: accounts
// first, then storage, then code.
func (a *accessedState) touchAll(sdCtx *commitmentdb.SharedDomainsCommitmentContext) {
	for addr := range a.Addresses {
		sdCtx.TouchKey(kv.AccountsDomain, string(addr[:]), nil)
	}
	for addr, keys := range a.Storage {
		for key := range keys {
			storageKey := string(append(addr[:], key[:]...))
			sdCtx.TouchKey(kv.StorageDomain, storageKey, nil)
		}
	}
	for addr := range a.CodeAddrs {
		sdCtx.TouchKey(kv.CodeDomain, string(addr[:]), nil)
	}
}

// collectAccessedState rolls the RecordingState read/write maps and the three
// code-tracking maps into a single accessedState. SortedCodes is sourced from
// rs.GetPreStateCode() (pre-block reads only): the witness must carry the
// bytecode that existed at the start of the block, not code created in-block.
// A stateless verifier re-derives in-block-created code by replaying the
// transactions, so emitting it would be redundant over-inclusion.
//
// SortedCodes is initialized to an empty (non-nil) slice so callers can assign
// result.Codes = accessed.SortedCodes without risking a "codes": null JSON
// regression for empty-touch blocks; the Codes field has no omitempty.
func collectAccessedState(rs *RecordingState, mode witnessMode) *accessedState {
	out := &accessedState{
		Addresses:   make(map[common.Address]struct{}),
		Storage:     make(map[common.Address]map[common.Hash]struct{}),
		CodeAddrs:   make(map[common.Address]struct{}),
		SortedCodes: []hexutil.Bytes{},
		WitnessKeys: []hexutil.Bytes{},
		CodeReads:   make(map[common.Hash]witnesstypes.CodeWithHash),
	}

	readAddresses, readStorageKeys := rs.GetAccessedKeys()
	writeAddresses, writeStorageKeys := rs.GetModifiedKeys()

	for _, addr := range readAddresses {
		out.Addresses[addr] = struct{}{}
	}
	for _, addr := range writeAddresses {
		out.Addresses[addr] = struct{}{}
	}

	for addr, keys := range readStorageKeys {
		if out.Storage[addr] == nil {
			out.Storage[addr] = make(map[common.Hash]struct{})
		}
		for _, key := range keys {
			out.Storage[addr][key] = struct{}{}
		}
	}
	for addr, keys := range writeStorageKeys {
		if out.Storage[addr] == nil {
			out.Storage[addr] = make(map[common.Hash]struct{})
		}
		for _, key := range keys {
			out.Storage[addr][key] = struct{}{}
		}
	}

	// Per EIP-7928, the system address (0xff...fe) must not appear in the witness
	// unless it has actual state changes. It is touched as msg.sender on every
	// block's system call but that alone does not constitute a state access.
	sysAddr := common.Address(params.SystemAddress.Value())
	if _, inAddresses := out.Addresses[sysAddr]; inAddresses {
		_, reallyChanged := rs.ReallyChangedAccounts[sysAddr]
		hasStorage := len(out.Storage[sysAddr]) > 0
		preAcct, _ := rs.inner.ReadAccountData(params.SystemAddress)
		preExists := preAcct != nil
		_, deleted := rs.DeletedAccounts[sysAddr]
		postOverlay, hasOverlay := rs.accountOverlay[sysAddr]
		var postExists bool
		if deleted {
			postExists = false
		} else if hasOverlay {
			postExists = postOverlay != nil
		} else {
			postExists = preExists
		}
		existenceChanged := preExists != postExists
		if !reallyChanged && !hasStorage && !existenceChanged && !rs.systemAddrTouchedInTx {
			delete(out.Addresses, sysAddr)
		}
	}

	witnessKeySet := make(map[string]struct{}, len(out.Addresses))
	for addr := range out.Addresses {
		if !rs.accountExists(addr) {
			continue
		}
		witnessKeySet[string(addr[:])] = struct{}{}
	}
	for _, keys := range out.Storage {
		for key := range keys {
			witnessKeySet[string(key[:])] = struct{}{}
		}
	}
	witnessKeys := make([]hexutil.Bytes, 0, len(witnessKeySet))
	for k := range witnessKeySet {
		witnessKeys = append(witnessKeys, []byte(k))
	}
	slices.SortFunc(witnessKeys, func(a, b hexutil.Bytes) int {
		return bytes.Compare(a, b)
	})
	out.WitnessKeys = witnessKeys

	allCodesByHash := make(map[common.Hash][]byte)
	emptyEntry := false
	switch mode {
	case witnessModeCanonical:
		// canonical: only pre-state bytecode (excludes in-block-created), non-empty.
		for _, code := range rs.GetPreStateCode() {
			if len(code) > 0 {
				allCodesByHash[crypto.Keccak256Hash(code)] = code
			}
		}
	default:
		// legacy: every bytecode loaded during execution plus a single empty bytecode if any
		// empty-code account was loaded. PreStateCode is folded in because AccessedCode is
		// keyed by address, so a delegated account's EIP-7702 designator is overwritten there
		// by its resolved target code and survives only in PreStateCode.
		for _, code := range rs.GetAccessedCode() {
			if len(code) > 0 {
				allCodesByHash[crypto.Keccak256Hash(code)] = code
			}
		}
		for _, code := range rs.GetModifiedCode() {
			if len(code) > 0 {
				allCodesByHash[crypto.Keccak256Hash(code)] = code
			}
		}
		for _, code := range rs.GetPreStateCode() {
			if len(code) > 0 {
				allCodesByHash[crypto.Keccak256Hash(code)] = code
			}
		}
		emptyEntry = rs.emptyCodeAccessed
	}

	uniqueCodes := make([][]byte, 0, len(allCodesByHash)+1)
	if emptyEntry {
		uniqueCodes = append(uniqueCodes, []byte{})
	}
	for _, code := range allCodesByHash {
		uniqueCodes = append(uniqueCodes, code)
	}
	slices.SortFunc(uniqueCodes, bytes.Compare)
	for _, c := range uniqueCodes {
		out.SortedCodes = append(out.SortedCodes, c)
	}

	preCode := rs.GetPreStateCode()
	for addr, code := range preCode {
		if len(code) > 0 {
			codeHash := crypto.Keccak256Hash(code)
			addrHash := crypto.Keccak256Hash(addr[:])
			out.CodeReads[addrHash] = witnesstypes.CodeWithHash{
				Code:     code,
				CodeHash: accounts.InternCodeHash(codeHash),
			}
		}
	}

	modCode := rs.GetModifiedCode()
	for addr := range preCode {
		out.CodeAddrs[addr] = struct{}{}
	}
	for addr := range modCode {
		out.CodeAddrs[addr] = struct{}{}
	}

	return out
}

// detectCollapseSiblings runs STEP 1 of witness construction: compute the full
// commitment for this block against a split reader (commitment from parent state,
// plain state from end of block) and record every sibling path the trie collapses
// through. The collected paths are returned so STEP 2 can touch them while building
// the witness — without those touches, collapsed-sibling data would be missing from
// the witness and stateless re-execution would diverge from the canonical root.
//
// Triggers SeekCommitment #2 (split history reader). Preserves pre-refactor behavior
// including the lupin012 seekBlockNum != parentNum guard.
func detectCollapseSiblings(
	ctx context.Context,
	tx kv.TemporalTx,
	domains *execctx.SharedDomains,
	sdCtx *commitmentdb.SharedDomainsCommitmentContext,
	firstTxNumInBlock, endTxNum, blockNum, parentNum uint64,
	expectedBlockRoot common.Hash,
	accessed *accessedState,
	mode witnessMode,
) (siblingPaths [][]byte, err error) {
	// Set up split reader: commitment from block beginning, plain state from block end.
	// withHistory=false so branch updates are written using PutBranch().
	splitStateReader := commitmentdb.NewSplitHistoryReader(tx, firstTxNumInBlock, endTxNum, false /* withHistory */)
	sdCtx.SetCustomHistoryStateReader(splitStateReader)
	_, seekBlockNum, err := domains.SeekCommitment(ctx, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to re-seek commitment for collapse detection: %w", err)
	}
	// With commitment history enabled, SeekCommitment at firstTxNumInBlock must land on the
	// parent block's committed state. Any other position means the history has been pruned
	// for this block range.
	if seekBlockNum != parentNum {
		return nil, fmt.Errorf(
			"debug_executionWitness: commitment trie for block %d is at block %d instead of parent %d; "+
				"commitment history may be pruned for this block range",
			blockNum, seekBlockNum, parentNum)
	}

	preReader := commitmentdb.NewHistoryStateReader(tx, firstTxNumInBlock)
	accessed.touchNonZeroKeys(sdCtx, splitStateReader, preReader, domains.StepSize())

	type collapseCandidate struct {
		siblingPath  []byte
		branchPrefix []byte
	}
	var candidates []collapseCandidate
	sdCtx.SetCollapseTracer(func(hashedKeyPath, branchPrefix []byte) {
		candidates = append(candidates, collapseCandidate{
			siblingPath:  common.Copy(hashedKeyPath),
			branchPrefix: common.Copy(branchPrefix),
		})
	})
	defer sdCtx.SetCollapseTracer(nil)

	computedRootHash, err := sdCtx.ComputeCommitment(ctx, tx, false, blockNum, firstTxNumInBlock, "debug_executionWitness_collapse_detection", nil)
	if err != nil {
		return nil, fmt.Errorf("[debug_executionWitness] collapse detection via ComputeCommitment failed: %v\n", err)
	}

	if common.Hash(computedRootHash) != expectedBlockRoot {
		return nil, fmt.Errorf("[debug_executionWitness] computedRootHash(%x)!= expectedRootHash(%x)", computedRootHash, expectedBlockRoot)
	}

	// The canonical witness (insert-before-delete order) does not touch the sibling of
	// a branch whose net block change leaves it with >=2 children; erigon's replay can
	// transiently collapse such a branch, so drop those siblings to match membership.
	// Legacy keeps every collapse sibling.
	siblingPaths = make([][]byte, 0, len(candidates))
	for _, c := range candidates {
		if mode == witnessModeCanonical {
			childCount, err := sdCtx.BranchChildCount(tx, c.branchPrefix)
			if err != nil {
				return nil, fmt.Errorf("[debug_executionWitness] read post-state branch for collapse filter: %w", err)
			}
			if childCount >= 2 {
				log.Debug("[debug_executionWitness] dropping transient-collapse sibling",
					"branchPrefix", commitment.NibblesToString(c.branchPrefix), "childCount", childCount)
				continue
			}
		}
		siblingPaths = append(siblingPaths, c.siblingPath)
	}
	return siblingPaths, nil
}

// buildWitnessTrie runs STEP 2 of witness construction: re-seek the commitment
// against the parent-state reader, touch every accessed key plus the sibling
// paths collected during collapse detection, then generate the witness trie and
// RLP-encode it. The pre-state root is verified against expectedParentRoot
// before the encoded nodes are returned.
//
// Triggers SeekCommitment #3 (parent-state reader). Preserves pre-refactor behavior.
func buildWitnessTrie(
	ctx context.Context,
	tx kv.TemporalTx,
	domains *execctx.SharedDomains,
	sdCtx *commitmentdb.SharedDomainsCommitmentContext,
	firstTxNumInBlock uint64,
	expectedParentRoot common.Hash,
	siblingPaths [][]byte,
	accessed *accessedState,
	mode witnessMode,
) (encodedNodes []hexutil.Bytes, err error) {
	encodedNodes = []hexutil.Bytes{}

	sdCtx.SetHistoryStateReader(tx, firstTxNumInBlock)
	if _, _, err := domains.SeekCommitment(ctx, tx); err != nil {
		return nil, fmt.Errorf("failed to reset commitment for regular witness: %w", err)
	}

	accessed.touchAll(sdCtx)

	if len(siblingPaths) > 0 {
		log.Debug("[debug_executionWitness] detected sibling paths", "count", len(siblingPaths))
		for _, siblingPath := range siblingPaths {
			compactSiblingPath := commitment.NibblesToString(siblingPath)
			log.Debug("[debug_executionWitness] touching sibling hashed key", "path", compactSiblingPath, "len", len(siblingPath))
			sdCtx.TouchHashedKey(siblingPath)
		}
	}

	witnessTrie, witnessRoot, err := sdCtx.Witness(ctx, accessed.CodeReads, mode == witnessModeLegacy, "debug_executionWitness_witness_construction")
	if err != nil {
		return nil, fmt.Errorf("failed to generate witness: %w", err)
	}
	if !bytes.Equal(witnessRoot, expectedParentRoot[:]) {
		return nil, fmt.Errorf("collapse witness root mismatch: calculated=%x, expected=%x", common.BytesToHash(witnessRoot), expectedParentRoot)
	}

	allNodes, err := witnessTrie.RLPEncode()
	if err != nil {
		return nil, fmt.Errorf("failed to encode trie nodes: %w", err)
	}
	for _, node := range allNodes {
		encodedNodes = append(encodedNodes, common.Copy(node))
	}
	return encodedNodes, nil
}

// resolveWitnessBlock resolves the target block from blockNrOrHash and computes
// the txnum range and parent block number used by subsequent witness phases.
// The genesis block has no parent and zero txns, so firstTxNumInBlock is collapsed
// to endTxNum and parentNum is forced to 0.
func (api *DebugAPIImpl) resolveWitnessBlock(
	ctx context.Context,
	tx kv.TemporalTx,
	blockNrOrHash rpc.BlockNumberOrHash,
) (*witnessBlockInfo, error) {
	blockNum, hash, _, err := rpchelper.GetBlockNumber(ctx, blockNrOrHash, tx, api._blockReader, api.filters)
	if err != nil {
		return nil, err
	}

	block, err := api.blockWithSenders(ctx, tx, hash, blockNum)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("block %d not found", blockNum)
	}

	firstTxNumInBlock, err := api._txNumReader.Min(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	lastTxNumInBlock, err := api._txNumReader.Max(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}

	endTxNum := lastTxNumInBlock + 1
	var parentNum uint64
	if blockNum == 0 {
		firstTxNumInBlock = endTxNum
		parentNum = 0
	} else {
		parentNum = blockNum - 1
	}

	return &witnessBlockInfo{
		Block:             block,
		BlockNum:          blockNum,
		FirstTxNumInBlock: firstTxNumInBlock,
		EndTxNum:          endTxNum,
		ParentNum:         parentNum,
	}, nil
}

// collectAccessedHeaders gathers the headers a stateless verifier needs to anchor
// pre-state and resolve BLOCKHASH lookups. The headers form a contiguous chain
// from the parent back to the oldest block reached via the BLOCKHASH opcode, so
// each header can be validated against the next one's parentHash.
func (api *DebugAPIImpl) collectAccessedHeaders(
	ctx context.Context,
	tx kv.TemporalTx,
	parentNum uint64,
	accessedBlockNums []uint64,
) (headers []hexutil.Bytes, byNumber map[uint64]*types.Header, err error) {
	headers = []hexutil.Bytes{}
	byNumber = make(map[uint64]*types.Header)

	addHeader := func(bn uint64) error {
		if _, seen := byNumber[bn]; seen {
			return nil
		}

		h, err := api._blockReader.HeaderByNumber(ctx, tx, bn)
		if err != nil {
			return fmt.Errorf("failed to load header for block %d: %w", bn, err)
		}
		if h == nil {
			return fmt.Errorf("missing header for block %d", bn)
		}
		encoded, err := rlp.EncodeToBytes(h)
		if err != nil {
			return fmt.Errorf("failed to encode header for block %d: %w", bn, err)
		}
		headers = append(headers, encoded)
		byNumber[h.Number.Uint64()] = h

		return nil
	}

	oldest := parentNum
	for _, bn := range accessedBlockNums {
		if bn < oldest {
			oldest = bn
		}
	}
	for bn := oldest; bn <= parentNum; bn++ {
		if err := addHeader(bn); err != nil {
			return nil, nil, err
		}
	}

	return headers, byNumber, nil
}

// verifyWitnessStateless optionally re-executes the block statelessly against the
// generated witness and asserts the resulting state root matches. Verification is
// a no-op when ERIGON_WITNESS_NO_VERIFY=true (it roughly doubles execution cost).
func (api *DebugAPIImpl) verifyWitnessStateless(
	ctx context.Context,
	tx kv.TemporalTx,
	result *ExecutionWitnessResult,
	block *types.Block,
	fullEngine rules.Engine,
) error {
	if dbg.EnvBool("ERIGON_WITNESS_NO_VERIFY", false) {
		return nil
	}

	chainCfg, err := api.chainConfig(ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to get chain config: %w", err)
	}

	newStateRoot, _, err := execBlockStatelessly(result, block, chainCfg, fullEngine)
	if err != nil {
		return fmt.Errorf("[debug_executionWitness] stateless block execution failed: %w", err)
	}

	expectedRoot := block.Root()
	if newStateRoot != expectedRoot {
		return fmt.Errorf("[debug_executionWitness] state root mismatch after stateless execution : got %x, expected %x", newStateRoot, expectedRoot)
	}

	log.Debug("[debug_executionWitness] witness verified", "blockNum", block.NumberU64())
	return nil
}

// buildExpectedPostState queries the actual state DB to build expected post-state for verification.
func (api *DebugAPIImpl) buildExpectedPostState(
	ctx context.Context,
	tx kv.TemporalTx,
	blockNum uint64,
	block *types.Block,
	readAddresses, writeAddresses []common.Address,
	readStorageKeys, writeStorageKeys map[common.Address][]common.Hash,
) (map[common.Address]*accounts.Account, map[common.Address]map[common.Hash]uint256.Int, error) {
	expectedState := make(map[common.Address]*accounts.Account)
	expectedStorage := make(map[common.Address]map[common.Hash]uint256.Int)

	// Create commitment context for accurate storage roots (since they are not stored explicitly)
	postDomains, err := execctx.NewSharedDomains(ctx, tx, log.New())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create post-state domains: %w", err)
	}
	defer postDomains.Close()
	postSdCtx := postDomains.GetCommitmentContext()
	postSdCtx.SetDeferBranchUpdates(false)

	// Set up to read state at current block (after execution)
	latestBlock, err := rpchelper.GetLatestBlockNumber(tx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get latest block: %w", err)
	}
	if blockNum < latestBlock {
		// Get first txnum of blockNum+1 to ensure correct state root
		lastTxnInBlock, err := api._txNumReader.Min(ctx, tx, blockNum+1)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get last txn in block: %w", err)
		}
		postSdCtx.SetHistoryStateReader(tx, lastTxnInBlock)
		if _, _, err := postDomains.SeekCommitment(ctx, tx); err != nil {
			return nil, nil, fmt.Errorf("failed to seek commitment: %w", err)
		}
	}

	// Touch all modified accounts and storage keys for the post-state trie
	for _, addr := range writeAddresses {
		postSdCtx.TouchKey(kv.AccountsDomain, string(addr[:]), nil)
	}
	for addr, keys := range writeStorageKeys {
		for _, key := range keys {
			storageKey := string(append(addr[:], key[:]...))
			postSdCtx.TouchKey(kv.StorageDomain, storageKey, nil)
		}
	}

	// Generate the trie with correct storage roots
	postTrie, postRoot, err := postSdCtx.Witness(ctx, nil, false /* legacy */, "debug_executionWitness_postState")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate post-state trie: %w", err)
	}

	// Verify the post-state root matches the block's state root
	blockRoot := block.Root()
	if !bytes.Equal(postRoot, blockRoot[:]) {
		// only warn, so we can see comparison later
		fmt.Printf("Warning: post-state trie root %x doesn't match block root %x\n", postRoot, block.Root())
	}

	// Read account data from the post-state trie (with correct storage roots)
	// Include both read and write addresses
	for _, addr := range readAddresses {
		addrHash := crypto.Keccak256(addr[:])
		acc, _ := postTrie.GetAccount(addrHash)
		expectedState[addr] = acc
	}
	for _, addr := range writeAddresses {
		addrHash := crypto.Keccak256(addr[:])
		acc, _ := postTrie.GetAccount(addrHash)
		expectedState[addr] = acc
	}

	// Read storage values from the state reader
	currentBlockNum := rpc.BlockNumber(blockNum)
	currentNrOrHash := rpc.BlockNumberOrHash{BlockNumber: &currentBlockNum}
	postStateReader, err := rpchelper.CreateStateReader(ctx, tx, api._blockReader, currentNrOrHash, 0, api.filters, api.stateCache, api._txNumReader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create postStateReader: %w", err)
	}
	// Include both read and write storage keys
	for addr, keys := range readStorageKeys {
		if expectedStorage[addr] == nil {
			expectedStorage[addr] = make(map[common.Hash]uint256.Int)
		}
		for _, key := range keys {
			storageKey := accounts.InternKey(key)
			val, _, err := postStateReader.ReadAccountStorage(accounts.InternAddress(addr), storageKey)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read expected storage in post state: %x key %x: %w", addr, key, err)
			}
			expectedStorage[addr][key] = val
		}
	}
	for addr, keys := range writeStorageKeys {
		if expectedStorage[addr] == nil {
			expectedStorage[addr] = make(map[common.Hash]uint256.Int)
		}
		for _, key := range keys {
			storageKey := accounts.InternKey(key)
			val, _, err := postStateReader.ReadAccountStorage(accounts.InternAddress(addr), storageKey)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read expected storage in post state: %x key %x: %w", addr, key, err)
			}
			expectedStorage[addr][key] = val
		}
	}

	return expectedState, expectedStorage, nil
}

// compareComputedVsExpectedState compares the post execution state computed by witnessStateless against the expected state.
func compareComputedVsExpectedState(stateless *witnessStateless, expectedState map[common.Address]*accounts.Account, expectedStorage map[common.Address]map[common.Hash]uint256.Int, storageDeletes map[common.Address]map[common.Hash]struct{}) {
	fmt.Printf("\n=== Comparing computed vs expected state ===\n")
	for addr, expectedAcc := range expectedState {
		addrHash, _ := common.HashData(addr[:])
		computedAcc, found := stateless.t.GetAccount(addrHash[:])

		fmt.Printf("\nAccount %s (hash %x):\n", addr.Hex(), addrHash[:8])
		if expectedAcc != nil {
			fmt.Printf("  EXPECTED: Nonce=%d, Balance=%s, Root=%x, CodeHash=%x\n",
				expectedAcc.Nonce, expectedAcc.Balance.String(), expectedAcc.Root, expectedAcc.CodeHash)
		} else {
			fmt.Printf("  EXPECTED: nil (deleted)\n")
		}
		if found && computedAcc != nil {
			fmt.Printf("  COMPUTED: Nonce=%d, Balance=%s, Root=%x, CodeHash=%x\n",
				computedAcc.Nonce, computedAcc.Balance.String(), computedAcc.Root, computedAcc.CodeHash)
			// Check for differences - only print mismatches or a single tick if all match
			if expectedAcc != nil {
				allMatch := true
				if _, ok := storageDeletes[addr]; ok {
					fmt.Printf("   ⛔️ STORAGE deletes on this account!\n")
				}
				if expectedAcc.Nonce != computedAcc.Nonce {
					fmt.Printf("    ❌ NONCE MISMATCH!\n")
					allMatch = false
				}
				if !expectedAcc.Balance.Eq(&computedAcc.Balance) {
					fmt.Printf("    ❌ BALANCE MISMATCH! (diff: %d wei)\n", new(uint256.Int).Sub(&expectedAcc.Balance, &computedAcc.Balance).Uint64())
					allMatch = false
				}
				if expectedAcc.Root != computedAcc.Root {
					fmt.Printf("    ❌ STORAGE ROOT MISMATCH!\n")
					allMatch = false
				}
				if expectedAcc.CodeHash != computedAcc.CodeHash {
					fmt.Printf("    ❌ CODE HASH MISMATCH!\n")
					allMatch = false
				}
				if allMatch {
					fmt.Printf("    ✅ All fields match\n")
				}
			}
		} else {
			fmt.Printf("  COMPUTED: NOT FOUND or nil\n")
		}
	}

	// Compare storage values
	for addr, expectedKeys := range expectedStorage {
		addrHash, _ := common.HashData(addr[:])
		fmt.Printf("\nStorage for %s (hash %x):\n", addr.Hex(), addrHash[:8])
		for key, expectedVal := range expectedKeys {
			keyHash, _ := common.HashData(key[:])
			cKey := dbutils.GenerateCompositeTrieKey(addrHash, keyHash)
			computedBytes, found := stateless.t.Get(cKey)
			var computedVal uint256.Int
			if found && len(computedBytes) > 0 {
				computedVal.SetBytes(computedBytes)
			}
			fmt.Printf("  Key %x (hash %x):\n", key, keyHash[:8])
			fmt.Printf("    EXPECTED: %s (hex: %x)\n", expectedVal.String(), expectedVal.Bytes())
			fmt.Printf("    COMPUTED: %s (hex: %x)\n", computedVal.String(), computedVal.Bytes())
			if !expectedVal.Eq(&computedVal) {
				fmt.Printf("    ❌ STORAGE VALUE MISMATCH!\n")
			} else {
				fmt.Printf("    ✅\n")
			}
		}
	}
}

// witnessStateless is a StateReader/StateWriter implementation that operates on a witness trie.
// It's used for stateless block verification.
type witnessStateless struct {
	t              *trie.Trie                                     // Witness trie decoded from ExecutionWitnessResult.State
	codeMap        map[common.Hash][]byte                         // Code hash -> bytecode
	codeUpdates    map[common.Hash][]byte                         // Code updates during execution
	storageWrites  map[common.Address]map[common.Hash]uint256.Int // addr -> key -> value
	storageDeletes map[common.Address]map[common.Hash]struct{}    // addr -> key
	accountUpdates map[common.Address]*accounts.Account           // addr -> account
	deleted        map[common.Address]struct{}                    // deleted accounts
	created        map[common.Address]struct{}                    // created contracts
	trace          bool

	// Debug: addresses to trace operations on
	accountsToTrace map[common.Address]struct{}
}

func (s *witnessStateless) SetAccountsToTrace(addrs []common.Address) {
	if len(addrs) == 0 {
		return // nothing to trace
	}
	s.trace = true
	s.accountsToTrace = make(map[common.Address]struct{}, len(addrs))
	for _, a := range addrs {
		s.accountsToTrace[a] = struct{}{}
	}
}

func (s *witnessStateless) tracing(addr common.Address) bool {
	if s.accountsToTrace == nil {
		return false
	}
	_, ok := s.accountsToTrace[addr]
	return ok
}

// Ensure witnessStateless implements both interfaces
var _ state.StateReader = (*witnessStateless)(nil)
var _ state.StateWriter = (*witnessStateless)(nil)

// newWitnessStateless creates a new witnessStateless from ExecutionWitnessResult
func newWitnessStateless(result *ExecutionWitnessResult) (*witnessStateless, error) {
	// Decode the witness trie from RLP-encoded nodes
	encodedNodes := make([][]byte, len(result.State))
	for i, node := range result.State {
		encodedNodes[i] = node
	}

	witnessTrie, err := trie.RLPDecode(encodedNodes)
	if err != nil {
		return nil, fmt.Errorf("failed to decode witness trie: %w", err)
	}

	// Build code map from codes list
	codeMap := make(map[common.Hash][]byte)
	for _, code := range result.Codes {
		codeHash := crypto.Keccak256Hash(code)
		codeMap[codeHash] = code
	}

	return &witnessStateless{
		t:              witnessTrie,
		codeMap:        codeMap,
		codeUpdates:    make(map[common.Hash][]byte),
		storageWrites:  make(map[common.Address]map[common.Hash]uint256.Int),
		storageDeletes: make(map[common.Address]map[common.Hash]struct{}),
		accountUpdates: make(map[common.Address]*accounts.Account),
		deleted:        make(map[common.Address]struct{}),
		created:        make(map[common.Address]struct{}),
		trace:          false,
	}, nil
}

// StateReader interface implementation

func (s *witnessStateless) SetTrace(trace bool, tracePrefix string) {
	s.trace = trace
}

func (s *witnessStateless) Trace() bool {
	return s.trace
}

func (s *witnessStateless) TracePrefix() string {
	return ""
}

func (s *witnessStateless) ReadAccountDataForDebug(address accounts.Address) (*accounts.Account, error) {
	return s.ReadAccountData(address)
}

func (s *witnessStateless) ReadAccountData(address accounts.Address) (*accounts.Account, error) {
	addr := address.Value()
	addrHash, err := common.HashData(addr[:])
	if err != nil {
		return nil, err
	}

	// Check if account has been updated in memory
	if acc, ok := s.accountUpdates[addr]; ok {
		if s.tracing(addr) {
			if acc != nil {
				fmt.Printf("[TRACE-S] ReadAccountData %s -> updates nonce=%d balance=%d codeHash=%x\n", addr.Hex(), acc.Nonce, &acc.Balance, acc.CodeHash)
			} else {
				fmt.Printf("[TRACE-S] ReadAccountData %s -> updates nil\n", addr.Hex())
			}
		}
		return acc, nil
	}

	// Check if account has been deleted
	if _, ok := s.deleted[addr]; ok {
		if s.tracing(addr) {
			fmt.Printf("[TRACE-S] ReadAccountData %s -> deleted\n", addr.Hex())
		}
		return nil, nil
	}

	// Read from trie
	acc, ok := s.t.GetAccount(addrHash[:])
	if s.tracing(addr) {
		if ok && acc != nil {
			fmt.Printf("[TRACE-S] ReadAccountData %s -> trie nonce=%d balance=%d codeHash=%x\n", addr.Hex(), acc.Nonce, &acc.Balance, acc.CodeHash)
		} else {
			fmt.Printf("[TRACE-S] ReadAccountData %s -> trie nil\n", addr.Hex())
		}
	}
	if ok {
		return acc, nil
	}
	return nil, nil
}

func (s *witnessStateless) ReadAccountStorage(address accounts.Address, key accounts.StorageKey) (uint256.Int, bool, error) {
	addr := address.Value()
	keyValue := key.Value()

	addrHash, err := common.HashData(addr[:])
	if err != nil {
		return uint256.Int{}, false, err
	}

	seckey, err := common.HashData(keyValue[:])
	if err != nil {
		return uint256.Int{}, false, err
	}

	// Check if storage has been updated in memory
	if m, ok := s.storageWrites[addr]; ok {
		if v, ok := m[keyValue]; ok {
			if s.tracing(addr) {
				fmt.Printf("[TRACE-S] ReadAccountStorage %s key=%s -> writes val=%d\n", addr.Hex(), keyValue.Hex(), &v)
			}
			return v, true, nil
		}
	}

	// Check if storage has been deleted
	if d, ok := s.storageDeletes[addr]; ok {
		if _, ok := d[keyValue]; ok {
			if s.tracing(addr) {
				fmt.Printf("[TRACE-S] ReadAccountStorage %s key=%s -> deleted\n", addr.Hex(), keyValue.Hex())
			}
			return uint256.Int{}, false, nil
		}
	}

	// Read from trie
	cKey := dbutils.GenerateCompositeTrieKey(addrHash, seckey)
	if enc, ok := s.t.Get(cKey); ok {
		var res uint256.Int
		res.SetBytes(enc)
		if s.tracing(addr) {
			fmt.Printf("[TRACE-S] ReadAccountStorage %s key=%s -> trie val=%d\n", addr.Hex(), keyValue.Hex(), &res)
		}
		return res, true, nil
	}

	if s.tracing(addr) {
		fmt.Printf("[TRACE-S] ReadAccountStorage %s key=%s -> not found\n", addr.Hex(), keyValue.Hex())
	}
	return uint256.Int{}, false, nil
}

func (s *witnessStateless) ReadAccountCode(address accounts.Address) ([]byte, error) {
	addr := address.Value()
	addrHash, err := common.HashData(addr[:])
	if err != nil {
		return nil, err
	}

	// Check code updates first — look up by the account's code hash (matching UpdateAccountCode key)
	acc, err := s.ReadAccountData(address)
	if err != nil {
		return nil, err
	}
	if acc != nil {
		codeHashValue := acc.CodeHash.Value()
		if code, ok := s.codeUpdates[codeHashValue]; ok {
			if s.tracing(addr) {
				fmt.Printf("[TRACE-S] ReadAccountCode %s -> codeUpdates len=%d\n", addr.Hex(), len(code))
			}
			return code, nil
		}
	}

	// Check trie for code
	if code, ok := s.t.GetAccountCode(addrHash[:]); ok {
		if s.tracing(addr) {
			fmt.Printf("[TRACE-S] ReadAccountCode %s -> trie len=%d\n", addr.Hex(), len(code))
		}
		return code, nil
	}

	// Check code map (from witness)
	if acc != nil {
		codeHashValue := acc.CodeHash.Value()
		if code, ok := s.codeMap[codeHashValue]; ok {
			if s.tracing(addr) {
				fmt.Printf("[TRACE-S] ReadAccountCode %s -> codeMap len=%d\n", addr.Hex(), len(code))
			}
			return code, nil
		}
	}

	if s.tracing(addr) {
		fmt.Printf("[TRACE-S] ReadAccountCode %s -> not found\n", addr.Hex())
	}
	return nil, nil
}

func (s *witnessStateless) ReadAccountCodeSize(address accounts.Address) (int, error) {
	code, err := s.ReadAccountCode(address)
	if err != nil {
		return 0, err
	}
	addr := address.Value()
	if s.tracing(addr) {
		fmt.Printf("[TRACE-S] ReadAccountCodeSize %s -> %d\n", addr.Hex(), len(code))
	}
	return len(code), nil
}

func (s *witnessStateless) ReadAccountIncarnation(address accounts.Address) (uint64, error) {
	addr := address.Value()
	if s.tracing(addr) {
		fmt.Printf("[TRACE-S] ReadAccountIncarnation %s -> 0\n", addr.Hex())
	}
	return 0, nil
}

func (s *witnessStateless) HasStorage(address accounts.Address) (bool, error) {
	addr := address.Value()
	addrHash, err := common.HashData(addr[:])
	if err != nil {
		return false, err
	}

	// Check if account has been deleted
	if _, ok := s.deleted[addr]; ok {
		if s.tracing(addr) {
			fmt.Printf("[TRACE-S] HasStorage %s -> deleted false\n", addr.Hex())
		}
		return false, nil
	}

	// Check if we know about any storage updates with non-empty values
	for _, v := range s.storageWrites[addr] {
		if !v.IsZero() {
			if s.tracing(addr) {
				fmt.Printf("[TRACE-S] HasStorage %s -> writes true\n", addr.Hex())
			}
			return true, nil
		}
	}

	// Check account in trie
	acc, ok := s.t.GetAccount(addrHash[:])
	if !ok {
		if s.tracing(addr) {
			fmt.Printf("[TRACE-S] HasStorage %s -> trie not found false\n", addr.Hex())
		}
		return false, nil
	}

	has := acc != nil && acc.Root != trie.EmptyRoot
	if s.tracing(addr) {
		fmt.Printf("[TRACE-S] HasStorage %s -> trie root=%x has=%v\n", addr.Hex(), acc.Root, has)
	}
	return has, nil
}

// StateWriter interface implementation

func (s *witnessStateless) UpdateAccountData(address accounts.Address, original, account *accounts.Account) error {
	addr := address.Value()
	// Make a copy to avoid the account being modified later
	if account != nil {
		accCopy := new(accounts.Account)
		accCopy.Copy(account)
		s.accountUpdates[addr] = accCopy
		if s.tracing(addr) {
			fmt.Printf("[TRACE-S] UpdateAccountData %s nonce=%d balance=%d codeHash=%x\n", addr.Hex(), account.Nonce, &account.Balance, account.CodeHash)
		}
	} else {
		s.accountUpdates[addr] = nil
		if s.tracing(addr) {
			fmt.Printf("[TRACE-S] UpdateAccountData %s nil\n", addr.Hex())
		}
	}
	return nil
}

func (s *witnessStateless) DeleteAccount(address accounts.Address, original *accounts.Account) error {
	addr := address.Value()
	addrHash, err := common.HashData(addr[:])
	if err != nil {
		return err
	}
	// Only delete if the account exists in the original state (trie or was previously updated)
	// Skip deletes for accounts that weren't in the witness - they don't affect the state root
	accInTrie, isInTrie := s.t.GetAccount(addrHash[:])
	_, wasUpdated := s.accountUpdates[addr]
	if (!isInTrie || accInTrie == nil) && !wasUpdated {
		if s.tracing(addr) {
			fmt.Printf("[TRACE-S] DeleteAccount %s -> skipped (not in trie or updates)\n", addr.Hex())
		}
		return nil
	}
	s.accountUpdates[addr] = nil
	s.deleted[addr] = struct{}{}
	if s.tracing(addr) {
		fmt.Printf("[TRACE-S] DeleteAccount %s\n", addr.Hex())
	}
	return nil
}

func (s *witnessStateless) UpdateAccountCode(address accounts.Address, incarnation uint64, codeHash accounts.CodeHash, code []byte) error {
	s.codeUpdates[codeHash.Value()] = code
	// Keep accountUpdates CodeHash in sync so ReadAccountData returns a
	// consistent CodeHash even before UpdateAccountData is called.
	addr := address.Value()
	if acc, ok := s.accountUpdates[addr]; ok && acc != nil {
		acc.CodeHash = codeHash
	}
	if s.tracing(addr) {
		fmt.Printf("[TRACE-S] UpdateAccountCode %s codeHash=%x len=%d\n", addr.Hex(), codeHash, len(code))
	}
	return nil
}

func (s *witnessStateless) WriteAccountStorage(address accounts.Address, incarnation uint64, key accounts.StorageKey, original, value uint256.Int) error {
	addr := address.Value()
	keyValue := key.Value()

	if value.IsZero() {
		// Delete: add to storageDeletes, remove from storageWrites
		d, ok := s.storageDeletes[addr]
		if !ok {
			d = make(map[common.Hash]struct{})
			s.storageDeletes[addr] = d
		}
		d[keyValue] = struct{}{}

		// Remove from writes if present
		if m, ok := s.storageWrites[addr]; ok {
			delete(m, keyValue)
		}
		if s.tracing(addr) {
			fmt.Printf("[TRACE-S] WriteAccountStorage %s key=%s -> delete\n", addr.Hex(), keyValue.Hex())
		}
	} else {
		// Write: add to storageWrites, remove from storageDeletes
		m, ok := s.storageWrites[addr]
		if !ok {
			m = make(map[common.Hash]uint256.Int)
			s.storageWrites[addr] = m
		}
		m[keyValue] = value

		// Remove from deletes if present
		if d, ok := s.storageDeletes[addr]; ok {
			delete(d, keyValue)
		}
		if s.tracing(addr) {
			fmt.Printf("[TRACE-S] WriteAccountStorage %s key=%s val=%d\n", addr.Hex(), keyValue.Hex(), &value)
		}
	}
	return nil
}

func (s *witnessStateless) CreateContract(address accounts.Address) error {
	addr := address.Value()
	s.created[addr] = struct{}{}
	delete(s.deleted, addr)
	if s.tracing(addr) {
		fmt.Printf("[TRACE-S] CreateContract %s\n", addr.Hex())
	}
	return nil
}

// Finalize applies all pending updates to the trie and returns the new root hash
func (s *witnessStateless) Finalize() (common.Hash, error) {
	// fmt.Printf("\n=== Finalize: Applying updates ===\n")

	// Handle created contracts - clear their storage subtries
	for addr := range s.created {
		if account, ok := s.accountUpdates[addr]; ok && account != nil {
			account.Root = trie.EmptyRoot
		}
		addrHash, _ := common.HashData(addr[:])
		s.t.DeleteSubtree(addrHash[:])
		// fmt.Printf("  Created contract %x: cleared subtrie\n", addr[:8])
	}

	// Apply account updates
	for addr, account := range s.accountUpdates {
		addrHash, _ := common.HashData(addr[:])
		if account != nil {
			// fmt.Printf("  UpdateAccount %x: Nonce=%d, Balance=%s\n", addr[:8], account.Nonce, account.Balance.String())
			s.t.UpdateAccount(addrHash[:], account)
		} else {
			s.t.Delete(addrHash[:])
		}
	}

	// Apply code updates - must be done after account updates so accounts exist in trie
	for addr, account := range s.accountUpdates {
		if account == nil {
			continue
		}
		addrHash, _ := common.HashData(addr[:])
		codeHashValue := account.CodeHash.Value()
		if code, ok := s.codeUpdates[codeHashValue]; ok {
			// fmt.Printf("  UpdateAccountCode %x: codeHash=%x, len=%d\n", addr[:8], codeHashValue[:8], len(code))
			if err := s.t.UpdateAccountCode(addrHash[:], code); err != nil {
				return common.Hash{}, fmt.Errorf("failed to update account code for addr %x: %v\n", addr, err)
			}
		}
	}

	updatedAccounts := map[common.Address]struct{}{}

	// Apply storage writes
	for addr, m := range s.storageWrites {
		if _, ok := s.deleted[addr]; ok {
			continue
		}
		updatedAccounts[addr] = struct{}{}
		addrHash, _ := common.HashData(addr[:])
		for key, v := range m {
			keyHash, _ := common.HashData(key[:])
			cKey := dbutils.GenerateCompositeTrieKey(addrHash, keyHash)
			// fmt.Printf("  Storage write: account=%x, key=%x, value=%x\n", addr[:8], key[:8], v.Bytes())
			s.t.Update(cKey, v.Bytes())
			s.t.DeepHash(addrHash[:])
		}
	}

	// Apply storage deletes
	for addr, m := range s.storageDeletes {
		if _, ok := s.deleted[addr]; ok {
			continue
		}
		updatedAccounts[addr] = struct{}{}
		addrHash, _ := common.HashData(addr[:])
		for key := range m {
			keyHash, _ := common.HashData(key[:])
			cKey := dbutils.GenerateCompositeTrieKey(addrHash, keyHash)
			// fmt.Printf("DELETING Storage Key at path %x\n", cKey)
			s.t.Delete(cKey)
		}
	}

	// Update storage roots for modified accounts
	// DeepHash computes the storage root, then we update the account with it
	for addr := range updatedAccounts {
		if account, ok := s.accountUpdates[addr]; ok && account != nil {
			addrHash, _ := common.HashData(addr[:])
			gotRoot, root := s.t.DeepHash(addrHash[:])
			if gotRoot {
				// Update the account's storage root and re-apply to trie
				account.Root = root
				s.t.UpdateAccount(addrHash[:], account)
			}
		}
	}

	// Handle deleted accounts
	for addr := range s.deleted {
		if _, ok := s.created[addr]; ok {
			continue
		}
		if account, ok := s.accountUpdates[addr]; ok && account != nil {
			account.Root = trie.EmptyRoot
		}
		addrHash, _ := common.HashData(addr[:])
		s.t.DeleteSubtree(addrHash[:])
	}

	// Compute and return the final hash
	finalHash := s.t.Hash()
	return finalHash, nil
}

// execBlockStatelessly executes the block statelessly.
// It decodes the witness trie, executes all transactions and returns the resulting state root
func execBlockStatelessly(result *ExecutionWitnessResult, block *types.Block, chainConfig *chain.Config, engine rules.Engine) (postStateRoot common.Hash, stateless *witnessStateless, err error) {
	// Skip verification for genesis block - it has no transactions to execute
	// but has pre-allocated accounts which would cause a state root mismatch
	if block.NumberU64() == 0 {
		return block.Root(), nil, nil
	}

	// Skip verification if the witness trie is empty
	if len(result.State) == 0 {
		return common.Hash{}, nil, fmt.Errorf("empty State field in witness")
	}
	// Create stateless state from the witness - this is both reader and writer
	stateless, err = newWitnessStateless(result)
	if err != nil {
		return common.Hash{}, nil, fmt.Errorf("failed to create witness stateless: %w", err)
	}
	stateless.SetAccountsToTrace([]common.Address{
		// Add addresses to trace here, e.g.:
		// common.HexToAddress("0x8863786beBE8eB9659DF00b49f8f1eeEc7e2C8c1"),
	})

	// Create the in-block state with the witness stateless as reader
	ibs := state.New(stateless)
	header := block.Header()
	blockNum := block.NumberU64()

	// Create EVM block context - pass header.Coinbase as the author/beneficiary
	// This ensures gas fees go to the correct address based on the block header
	coinbase := accounts.InternAddress(header.Coinbase)
	blockCtx := protocol.NewEVMBlockContext(header, result.getHashFn, nil, coinbase, chainConfig)
	blockRules := blockCtx.Rules(chainConfig)
	signer := types.MakeSigner(chainConfig, blockNum, header.Time)

	// Run block initialization (e.g. EIP-2935 blockhash contract, EIP-4788 beacon root)
	systemCallCustom := func(contract accounts.Address, data []byte, ibState *state.IntraBlockState, hdr *types.Header, constCall bool) ([]byte, error) {
		return protocol.SysCallContract(contract, data, chainConfig, ibState, hdr, engine, constCall, vm.Config{})
	}
	if err = engine.Initialize(chainConfig, nil /* chainReader */, header, ibs, systemCallCustom, log.Root(), nil); err != nil {
		return common.Hash{}, stateless, fmt.Errorf("verification: failed to initialize block: %w", err)
	}
	if err = ibs.FinalizeTx(blockRules, stateless); err != nil {
		return common.Hash{}, stateless, fmt.Errorf("verification: failed to finalize engine.Initialize tx: %w", err)
	}

	// Execute all transactions in the block
	for txIndex, txn := range block.Transactions() {
		msg, err := txn.AsMessage(*signer, header.BaseFee, blockRules)
		if err != nil {
			return common.Hash{}, stateless, fmt.Errorf("[statelessExec] failed to convert tx %d to message: %w", txIndex, err)
		}

		txCtx := protocol.NewEVMTxContext(msg)
		evm := vm.NewEVM(blockCtx, txCtx, ibs, chainConfig, vm.Config{})

		gp := new(protocol.GasPool).AddGas(header.GasLimit).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(header.Time))
		ibs.SetTxContext(blockNum, txIndex)

		// Apply the message - gasBailout must be false to properly deduct gas from sender
		_, err = protocol.ApplyMessage(evm, msg, gp, true /* refunds */, false /* gasBailout */, engine)
		if err != nil {
			return common.Hash{}, stateless, fmt.Errorf("[statelessExec] failed to apply tx %d: %w", txIndex, err)
		}

		// Finalize tx - state changes go to the witness stateless
		if err = ibs.FinalizeTx(blockRules, stateless); err != nil {
			return common.Hash{}, stateless, fmt.Errorf("[statelessExec] failed to finalize tx %d: %w", txIndex, err)
		}
	}

	syscall := func(contract accounts.Address, data []byte) ([]byte, error) {
		return protocol.SysCallContract(contract, data, chainConfig, ibs, header, engine, false /* constCall */, vm.Config{})
	}
	// Collect logs accumulated during transaction execution into a synthetic receipt
	// so that Finalize can parse EIP-6110 deposit requests from them.
	allLogs := ibs.Logs()
	statelessReceipts := types.Receipts{&types.Receipt{Logs: allLogs}}

	// only Bor and AuRa engine use ChainReader. And the ChainReader is only used to read headers. This means their
	// witness may need to be augmented with headers accessed during their engine.Finalize(). This is something that
	// can be implemented later. For now use ChainReader = nil, as this is sufficient for Ethereum.
	_, err = engine.Finalize(chainConfig, types.CopyHeader(header), ibs, block.Uncles(), statelessReceipts, block.Withdrawals(), nil /* chainReader */, syscall, false /*skipReceiptsEval*/, log.Root())
	if err != nil {
		return common.Hash{}, stateless, fmt.Errorf("[statelessExec] engine.Finalize failed: %w", err)
	}

	err = ibs.CommitBlock(blockRules, stateless)
	if err != nil {
		return common.Hash{}, stateless, fmt.Errorf("[statelessExec] ibs.CommitBlock() failed : %w", err)
	}

	// Finalize and compute the resulting state root
	newStateRoot, err := stateless.Finalize()
	if err != nil {
		return common.Hash{}, stateless, fmt.Errorf("[statelessExec] stateless.Finalize() failed: %w", err)
	}
	return newStateRoot, stateless, nil
}
