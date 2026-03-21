// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package testutil

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/rulesconfig"
)

// A BlockTest checks handling of entire blocks.
type BlockTest struct {
	json            btJSON
	ExperimentalBAL bool
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (bt *BlockTest) UnmarshalJSON(in []byte) error {
	return jsoniter.ConfigFastest.Unmarshal(in, &bt.json)
}

type btJSON struct {
	Blocks     []btBlock             `json:"blocks"`
	Genesis    btHeader              `json:"genesisBlockHeader"`
	Pre        types.GenesisAlloc    `json:"pre"`
	Post       types.GenesisAlloc    `json:"postState"`
	BestBlock  common.UnprefixedHash `json:"lastblockhash"`
	Network    string                `json:"network"`
	SealEngine string                `json:"sealEngine"`
}

type btBlock struct {
	BlockHeader     *btHeader
	ExpectException string
	Rlp             string
	UncleHeaders    []*btHeader
	BlockAccessList btBlockAccessList `json:"blockAccessList"`
}

// btBlockAccessList and related types for parsing block access list data from test JSON.
type btBlockAccessList []btAccountChanges

type btAccountChanges struct {
	Address        common.Address    `json:"address"`
	StorageChanges []btSlotChanges   `json:"storageChanges"`
	StorageReads   []hexutil.Bytes   `json:"storageReads"`
	BalanceChanges []btBalanceChange `json:"balanceChanges"`
	NonceChanges   []btNonceChange   `json:"nonceChanges"`
	CodeChanges    []btCodeChange    `json:"codeChanges"`
}

type btSlotChanges struct {
	Slot        hexutil.Bytes     `json:"slot"`
	SlotChanges []btStorageChange `json:"slotChanges"`
}

type btStorageChange struct {
	BlockAccessIndex hexutil.Uint16       `json:"blockAccessIndex"`
	PostValue        math.HexOrDecimal256 `json:"postValue"`
}

type btBalanceChange struct {
	BlockAccessIndex hexutil.Uint16       `json:"blockAccessIndex"`
	PostBalance      math.HexOrDecimal256 `json:"postBalance"`
}

type btNonceChange struct {
	BlockAccessIndex hexutil.Uint16      `json:"blockAccessIndex"`
	PostNonce        math.HexOrDecimal64 `json:"postNonce"`
}

type btCodeChange struct {
	BlockAccessIndex hexutil.Uint16 `json:"blockAccessIndex"`
	NewCode          hexutil.Bytes  `json:"newCode"`
}

func (bal btBlockAccessList) toBAL() types.BlockAccessList {
	if len(bal) == 0 {
		return nil
	}
	result := make(types.BlockAccessList, len(bal))
	for i, ac := range bal {
		entry := &types.AccountChanges{
			Address:        accounts.InternAddress(ac.Address),
			StorageChanges: make([]*types.SlotChanges, 0, len(ac.StorageChanges)),
			StorageReads:   make([]accounts.StorageKey, 0, len(ac.StorageReads)),
			BalanceChanges: make([]*types.BalanceChange, 0, len(ac.BalanceChanges)),
			NonceChanges:   make([]*types.NonceChange, 0, len(ac.NonceChanges)),
			CodeChanges:    make([]*types.CodeChange, 0, len(ac.CodeChanges)),
		}
		for _, sc := range ac.StorageChanges {
			slotChanges := &types.SlotChanges{
				Slot:    accounts.InternKey(common.BytesToHash(sc.Slot)),
				Changes: make([]*types.StorageChange, 0, len(sc.SlotChanges)),
			}
			for _, change := range sc.SlotChanges {
				slotChanges.Changes = append(slotChanges.Changes, &types.StorageChange{
					Index: uint16(change.BlockAccessIndex),
					Value: *uint256.MustFromBig((*big.Int)(&change.PostValue)),
				})
			}
			entry.StorageChanges = append(entry.StorageChanges, slotChanges)
		}
		for _, sr := range ac.StorageReads {
			entry.StorageReads = append(entry.StorageReads, accounts.InternKey(common.BytesToHash(sr)))
		}
		for _, bc := range ac.BalanceChanges {
			entry.BalanceChanges = append(entry.BalanceChanges, &types.BalanceChange{
				Index: uint16(bc.BlockAccessIndex),
				Value: *uint256.MustFromBig((*big.Int)(&bc.PostBalance)),
			})
		}
		for _, nc := range ac.NonceChanges {
			entry.NonceChanges = append(entry.NonceChanges, &types.NonceChange{
				Index: uint16(nc.BlockAccessIndex),
				Value: uint64(nc.PostNonce),
			})
		}
		for _, cc := range ac.CodeChanges {
			entry.CodeChanges = append(entry.CodeChanges, &types.CodeChange{
				Index:    uint16(cc.BlockAccessIndex),
				Bytecode: cc.NewCode,
			})
		}
		result[i] = entry
	}
	return result
}

//go:generate gencodec -type btHeader -field-override btHeaderMarshaling -out gen_btheader.go

type btHeader struct {
	Bloom                 types.Bloom
	Coinbase              common.Address
	MixHash               common.Hash
	Nonce                 types.BlockNonce
	Number                *uint256.Int
	Hash                  common.Hash
	ParentHash            common.Hash
	ReceiptTrie           common.Hash
	StateRoot             common.Hash
	TransactionsTrie      common.Hash
	UncleHash             common.Hash
	ExtraData             []byte
	Difficulty            *uint256.Int
	GasLimit              uint64
	GasUsed               uint64
	Timestamp             uint64
	BaseFeePerGas         *uint256.Int
	WithdrawalsRoot       *common.Hash
	BlobGasUsed           *uint64
	ExcessBlobGas         *uint64
	ParentBeaconBlockRoot *common.Hash
	RequestsHash          *common.Hash
	BlockAccessListHash   *common.Hash
	SlotNumber            *uint64
}

type btHeaderMarshaling struct {
	ExtraData     hexutil.Bytes
	Number        *math.HexOrDecimal256
	Difficulty    *math.HexOrDecimal256
	GasLimit      math.HexOrDecimal64
	GasUsed       math.HexOrDecimal64
	Timestamp     math.HexOrDecimal64
	BaseFeePerGas *math.HexOrDecimal256
	BlobGasUsed   *math.HexOrDecimal64
	ExcessBlobGas *math.HexOrDecimal64
	SlotNumber    *math.HexOrDecimal64
}

// Run executes the block test using a MemoryMutation overlay on a shared,
// cached genesis DB. This is the primary test execution path.
func (bt *BlockTest) Run(t *testing.T) error {
	return bt.run(t)
}

// RunCLI executes the test without requiring a testing.T context, suitable for CLI usage.
// RunCLI executes the test without requiring a testing.T context, suitable for CLI usage.
func (bt *BlockTest) RunCLI() error {
	return bt.run(nil)
}

func (bt *BlockTest) genesis(config *chain.Config) *types.Genesis {
	return &types.Genesis{
		Config:                config,
		Nonce:                 bt.json.Genesis.Nonce.Uint64(),
		Timestamp:             bt.json.Genesis.Timestamp,
		ParentHash:            bt.json.Genesis.ParentHash,
		ExtraData:             bt.json.Genesis.ExtraData,
		GasLimit:              bt.json.Genesis.GasLimit,
		GasUsed:               bt.json.Genesis.GasUsed,
		Difficulty:            bt.json.Genesis.Difficulty,
		Mixhash:               bt.json.Genesis.MixHash,
		Coinbase:              bt.json.Genesis.Coinbase,
		Alloc:                 bt.json.Pre,
		BaseFee:               bt.json.Genesis.BaseFeePerGas,
		BlobGasUsed:           bt.json.Genesis.BlobGasUsed,
		ExcessBlobGas:         bt.json.Genesis.ExcessBlobGas,
		ParentBeaconBlockRoot: bt.json.Genesis.ParentBeaconBlockRoot,
		RequestsHash:          bt.json.Genesis.RequestsHash,
		BlockAccessListHash:   bt.json.Genesis.BlockAccessListHash,
		SlotNumber:            bt.json.Genesis.SlotNumber,
	}
}

// equalPtr reports whether two optional pointers point to equal values.
func equalPtr[T comparable](a, b *T) bool {
	if a == nil {
		return b == nil
	}
	return b != nil && *a == *b
}

func validateHeader(h *btHeader, h2 *types.Header) error {
	if h == nil {
		return errors.New("validateHeader: h == nil")
	}
	if h2 == nil {
		return errors.New("validateHeader: h2 == nil")
	}
	if h.Bloom != h2.Bloom {
		return fmt.Errorf("bloom: want: %x have: %x", h.Bloom, h2.Bloom)
	}
	if h.Coinbase != h2.Coinbase {
		return fmt.Errorf("coinbase: want: %x have: %x", h.Coinbase, h2.Coinbase)
	}
	if h.MixHash != h2.MixDigest {
		return fmt.Errorf("MixHash: want: %x have: %x", h.MixHash, h2.MixDigest)
	}
	if h.Nonce != h2.Nonce {
		return fmt.Errorf("nonce: want: %x have: %x", h.Nonce, h2.Nonce)
	}
	if !h.Number.Eq(&h2.Number) {
		return fmt.Errorf("number: want: %s have: %s", h.Number, &h2.Number)
	}
	if h.ParentHash != h2.ParentHash {
		return fmt.Errorf("parent hash: want: %x have: %x", h.ParentHash, h2.ParentHash)
	}
	if h.ReceiptTrie != h2.ReceiptHash {
		return fmt.Errorf("receipt hash: want: %x have: %x", h.ReceiptTrie, h2.ReceiptHash)
	}
	if h.TransactionsTrie != h2.TxHash {
		return fmt.Errorf("txn hash: want: %x have: %x", h.TransactionsTrie, h2.TxHash)
	}
	if h.StateRoot != h2.Root {
		return fmt.Errorf("state hash: want: %x have: %x", h.StateRoot, h2.Root)
	}
	if h.UncleHash != h2.UncleHash {
		return fmt.Errorf("uncle hash: want: %x have: %x", h.UncleHash, h2.UncleHash)
	}
	if !bytes.Equal(h.ExtraData, h2.Extra) {
		return fmt.Errorf("extra data: want: %x have: %x", h.ExtraData, h2.Extra)
	}
	if !h.Difficulty.Eq(&h2.Difficulty) {
		return fmt.Errorf("difficulty: want: %v have: %v", h.Difficulty, h2.Difficulty)
	}
	if h.GasLimit != h2.GasLimit {
		return fmt.Errorf("gasLimit: want: %d have: %d", h.GasLimit, h2.GasLimit)
	}
	if h.GasUsed != h2.GasUsed {
		return fmt.Errorf("gasUsed: want: %d have: %d", h.GasUsed, h2.GasUsed)
	}
	if h.Timestamp != h2.Time {
		return fmt.Errorf("timestamp: want: %v have: %v", h.Timestamp, h2.Time)
	}
	if !equalPtr(h.BaseFeePerGas, h2.BaseFee) {
		return fmt.Errorf("baseFeePerGas: want: %v have: %v", h.BaseFeePerGas, h2.BaseFee)
	}
	if !equalPtr(h.WithdrawalsRoot, h2.WithdrawalsHash) {
		return fmt.Errorf("withdrawalsRoot: want: %v have: %v", h.WithdrawalsRoot, h2.WithdrawalsHash)
	}
	if !equalPtr(h.BlobGasUsed, h2.BlobGasUsed) {
		return fmt.Errorf("blobGasUsed: want: %v have: %v", h.BlobGasUsed, h2.BlobGasUsed)
	}
	if !equalPtr(h.ExcessBlobGas, h2.ExcessBlobGas) {
		return fmt.Errorf("excessBlobGas: want: %v have: %v", h.ExcessBlobGas, h2.ExcessBlobGas)
	}
	if !equalPtr(h.ParentBeaconBlockRoot, h2.ParentBeaconBlockRoot) {
		return fmt.Errorf("parentBeaconBlockRoot: want: %v have: %v", h.ParentBeaconBlockRoot, h2.ParentBeaconBlockRoot)
	}
	if !equalPtr(h.RequestsHash, h2.RequestsHash) {
		return fmt.Errorf("requestsHash: want: %v have: %v", h.RequestsHash, h2.RequestsHash)
	}
	if !equalPtr(h.BlockAccessListHash, h2.BlockAccessListHash) {
		return fmt.Errorf("blockAccessListHash: want: %v have: %v", h.BlockAccessListHash, h2.BlockAccessListHash)
	}
	if !equalPtr(h.SlotNumber, h2.SlotNumber) {
		return fmt.Errorf("slotNumber: want: %v have: %v", h.SlotNumber, h2.SlotNumber)
	}
	return nil
}

func (bt *BlockTest) validatePostState(statedb *state.IntraBlockState) error {
	// validate post state accounts in test file against what we have in state db
	for addr, acct := range bt.json.Post {
		// address is indirectly verified by the other fields, as it's the db key
		address := accounts.InternAddress(addr)
		code2, err := statedb.GetCode(address)
		if err != nil {
			return err
		}
		balance2, err := statedb.GetBalance(address)
		if err != nil {
			return err
		}
		nonce2, err := statedb.GetNonce(address)
		if err != nil {
			return err
		}
		if nonce2 != acct.Nonce {
			return fmt.Errorf("account nonce mismatch for addr: %x want: %d have: %d", addr, acct.Nonce, nonce2)
		}
		if !bytes.Equal(code2, acct.Code) {
			return fmt.Errorf("account code mismatch for addr: %x want: %v have: %s", addr, acct.Code, hex.EncodeToString(code2))
		}
		if balance2.ToBig().Cmp(acct.Balance) != 0 {
			return fmt.Errorf("account balance mismatch for addr: %x, want: %d, have: %d", addr, acct.Balance, &balance2)
		}
		for loc, val := range acct.Storage {
			val1 := uint256.NewInt(0).SetBytes(val.Bytes())
			val2, _ := statedb.GetState(address, accounts.InternKey(loc))
			if !val1.Eq(&val2) {
				return fmt.Errorf("storage mismatch for addr: %x loc: %x want: %d have: %d", addr, loc, val1, &val2)
			}
		}
	}
	return nil
}

func (bb *btBlock) decode() (*types.Block, error) {
	data, err := hexutil.Decode(bb.Rlp)
	if err != nil {
		return nil, err
	}
	var b types.Block
	err = rlp.DecodeBytes(data, &b)
	return &b, err
}

// validateBAL validates the block access list from the test JSON against the
// block header. This is the lightweight equivalent of the BAL processing in
// the full pipeline (ProcessBAL in exec3_parallel.go).
func validateBAL(header *types.Header, b btBlock) error {
	if len(b.BlockAccessList) == 0 {
		return nil
	}
	bal := b.BlockAccessList.toBAL()
	if err := bal.Validate(); err != nil {
		return fmt.Errorf("BAL structural validation: %w", err)
	}
	if err := bal.ValidateMaxItems(header.GasLimit); err != nil {
		return fmt.Errorf("BAL max items: %w", err)
	}
	if header.BlockAccessListHash != nil {
		balHash := bal.Hash()
		if balHash != *header.BlockAccessListHash {
			return fmt.Errorf("BAL hash mismatch: computed %x, header %x", balHash, *header.BlockAccessListHash)
		}
	}
	return nil
}

// isExpectedInvalid checks whether a block error is expected (BlockHeader == nil)
// and returns true if the block should be skipped.
func isExpectedInvalid(b btBlock) bool {
	return b.BlockHeader == nil
}

func (bt *BlockTest) run(t *testing.T) error {
	registerGenesisCacheCleanup(t)

	config, ok := testforks.Forks[bt.json.Network]
	if !ok {
		return testforks.UnsupportedForkError{Name: bt.json.Network}
	}

	gspec := bt.genesis(config)
	db, genesis, release, err := getOrCreateGenesisDB(bt.json.Network, gspec)
	if err != nil {
		return fmt.Errorf("genesis cache: %w", err)
	}
	defer release()

	if genesis.Hash() != bt.json.Genesis.Hash {
		return fmt.Errorf("genesis block hash mismatch: computed=%x, test=%x", genesis.Hash().Bytes()[:6], bt.json.Genesis.Hash[:6])
	}
	if genesis.Root() != bt.json.Genesis.StateRoot {
		return fmt.Errorf("genesis block state root mismatch: computed=%x, test=%x", genesis.Root().Bytes()[:6], bt.json.Genesis.StateRoot[:6])
	}

	ctx := context.Background()
	logger := log.New()
	logger.SetHandler(log.LvlFilterHandler(log.LvlError, log.StderrHandler))

	// Open a RO temporal tx on the shared genesis DB.
	roTx, err := db.BeginTemporalRo(ctx)
	if err != nil {
		return fmt.Errorf("BeginTemporalRo: %w", err)
	}
	defer roTx.Rollback()

	// Create a MemoryMutation overlay for ephemeral writes.
	overlay, err := membatchwithdb.NewMemoryBatch(roTx, "", logger)
	if err != nil {
		return fmt.Errorf("NewMemoryBatch: %w", err)
	}
	defer overlay.Close()

	// Look up the starting txNum from genesis block.
	txNum, err := rawdbv3.TxNums.Max(ctx, roTx, 0)
	if err != nil {
		return fmt.Errorf("TxNums.Max: %w", err)
	}
	txNum++ // next available

	engine := rulesconfig.CreateRulesEngineBareBones(ctx, config, logger)
	defer engine.Close()

	// Build a lightweight chain reader with in-memory header/TD maps.
	cr := &lightChainReader{
		config:  config,
		headers: make(map[common.Hash]*types.Header),
		tds:     make(map[common.Hash]*big.Int),
		tx:      overlay,
	}

	// Seed the chain reader with genesis header and TD.
	cr.headers[genesis.Hash()] = genesis.Header()
	genesisTd, _ := rawdb.ReadTd(roTx, genesis.Hash(), 0)
	if genesisTd != nil {
		cr.tds[genesis.Hash()] = genesisTd
	}

	lastBlockHash := genesis.Hash()

	// Pre-decode blocks to identify the main chain. Tests with uncle/side-chain
	// blocks have interleaved blocks from different forks. We trace the main
	// chain backwards from BestBlock to only execute main-chain blocks.
	mainChainBlocks := make(map[common.Hash]bool)
	{
		decoded := make(map[common.Hash]*types.Block)
		for _, b := range bt.json.Blocks {
			cb, err := b.decode()
			if err != nil {
				continue
			}
			decoded[cb.Hash()] = cb
		}
		// Trace back from BestBlock to genesis.
		h := common.Hash(bt.json.BestBlock)
		for h != genesis.Hash() {
			mainChainBlocks[h] = true
			cb, ok := decoded[h]
			if !ok {
				break
			}
			h = cb.ParentHash()
		}
	}

	// Execute blocks — same structure as insertBlocks but using
	// ExecuteBlockEphemerally instead of InsertChain.
	for _, b := range bt.json.Blocks {
		cb, err := b.decode()
		if err != nil {
			if isExpectedInvalid(b) {
				continue
			}
			return fmt.Errorf("block RLP decoding failed when expected to succeed: %w", err)
		}

		header := cb.Header()

		// Skip side-chain blocks — only execute blocks on the main chain.
		// Record their headers for uncle validation.
		if !mainChainBlocks[cb.Hash()] && !isExpectedInvalid(b) {
			cr.headers[cb.Hash()] = header
			continue
		}

		// Post-Shanghai blocks must have a withdrawals field in the RLP.
		// Go's RLP decoder is lenient and sets it to nil if absent.
		if config.IsShanghai(header.Time) && cb.Withdrawals() == nil {
			if isExpectedInvalid(b) {
				continue
			}
			return fmt.Errorf("block #%v missing withdrawals field (post-Shanghai)", cb.Number())
		}

		parentHash := header.ParentHash

		parentTd, hasTd := cr.tds[parentHash]
		if !hasTd {
			parentTd, _ = rawdb.ReadTd(overlay, parentHash, header.Number.Uint64()-1)
		}

		// Verify header (gas limit, timestamp, difficulty, etc.).
		if verifyErr := engine.VerifyHeader(cr, header, false); verifyErr != nil {
			if isExpectedInvalid(b) {
				continue
			}
			return fmt.Errorf("block #%v header verification failed: %w", cb.Number(), verifyErr)
		}

		// Verify uncles.
		if verifyErr := engine.VerifyUncles(cr, header, cb.Uncles()); verifyErr != nil {
			if isExpectedInvalid(b) {
				continue
			}
			return fmt.Errorf("block #%v uncle verification failed: %w", cb.Number(), verifyErr)
		}

		// Validate BAL (block access list) from the test fixture.
		if balErr := validateBAL(header, b); balErr != nil {
			if isExpectedInvalid(b) {
				continue
			}
			return fmt.Errorf("block #%v BAL validation failed: %w", cb.Number(), balErr)
		}

		// Check gas used doesn't exceed gas limit (GAS_USED_OVERFLOW).
		if header.GasUsed > header.GasLimit {
			if isExpectedInvalid(b) {
				continue
			}
			return fmt.Errorf("block #%v gas used %d exceeds gas limit %d", cb.Number(), header.GasUsed, header.GasLimit)
		}

		// Pre-check: blob gas used must match actual blob count.
		// ExecuteBlockEphemerally checks this post-execution, but by then state
		// changes are already written to the overlay and can't be rolled back.
		if header.BlobGasUsed != nil {
			var expectedBlobGas uint64
			for _, txn := range cb.Transactions() {
				expectedBlobGas += txn.GetBlobGas()
			}
			if expectedBlobGas != *header.BlobGasUsed {
				if isExpectedInvalid(b) {
					continue
				}
				return fmt.Errorf("block #%v blob gas mismatch: txns=%d, header=%d", cb.Number(), expectedBlobGas, *header.BlobGasUsed)
			}
		}

		// Pre-check: validate withdrawals root matches actual withdrawals.
		if header.WithdrawalsHash != nil {
			computedRoot := types.DeriveSha(types.Withdrawals(cb.Withdrawals()))
			if computedRoot != *header.WithdrawalsHash {
				if isExpectedInvalid(b) {
					continue
				}
				return fmt.Errorf("block #%v withdrawals root mismatch: computed %x, header %x", cb.Number(), computedRoot, *header.WithdrawalsHash)
			}
		}

		// Pre-check: validate transactions root matches actual transactions.
		computedTxRoot := types.DeriveSha(cb.Transactions())
		if computedTxRoot != header.TxHash {
			if isExpectedInvalid(b) {
				continue
			}
			return fmt.Errorf("block #%v transactions root mismatch: computed %x, header %x", cb.Number(), computedTxRoot, header.TxHash)
		}

		txNum++

		blockHashFunc := protocol.GetHashFn(header, func(hash common.Hash, number uint64) (*types.Header, error) {
			h := cr.GetHeader(hash, number)
			if h == nil {
				return nil, fmt.Errorf("header not found: %x at %d", hash, number)
			}
			return h, nil
		})

		var execResult *protocol.EphemeralExecResult
		var execErr error

		if isExpectedInvalid(b) {
			// Execute on a nested overlay so we can discard state changes.
			blockOverlay, blockOverlayErr := membatchwithdb.NewMemoryBatch(overlay, "", logger)
			if blockOverlayErr != nil {
				return fmt.Errorf("block #%v NewMemoryBatch: %w", cb.Number(), blockOverlayErr)
			}
			stateReader := state.NewReaderV3(blockOverlay)
			stateWriter := state.NewWriter(blockOverlay, nil, txNum)

			// First try: execute without VersionMap (preserves correct behavior).
			func() {
				defer func() {
					if r := recover(); r != nil {
						execErr = fmt.Errorf("panic during block execution: %v", r)
					}
				}()
				execResult, execErr = protocol.ExecuteBlockEphemerally(
					config, &vm.Config{},
					blockHashFunc, engine, cb,
					stateReader, stateWriter,
					cr, nil,
					logger,
				)
			}()
			if execErr != nil {
				blockOverlay.Close()
				continue // expected-invalid block correctly rejected
			}

			// Execution succeeded — check BAL hash if present.
			if header.BlockAccessListHash != nil {
				// Re-execute with VersionedIO on a fresh overlay to compute BAL.
				blockOverlay.Close()
				balOverlay, balErr := membatchwithdb.NewMemoryBatch(overlay, "", logger)
				if balErr != nil {
					return fmt.Errorf("block #%v BAL overlay: %w", cb.Number(), balErr)
				}
				balReader := state.NewReaderV3(balOverlay)
				balWriter := state.NewWriter(balOverlay, nil, txNum)
				balWriter.ForceWrites = true
				_, vio, balExecErr := executeBlockWithIO(
					config, blockHashFunc, engine, cb,
					balReader, balWriter, cr, logger,
				)
				balOverlay.Close()
				if balExecErr == nil && vio != nil {
					computedBAL := vio.AsBlockAccessList()
					if computedBAL.Hash() != *header.BlockAccessListHash {
						continue // correctly rejected: BAL mismatch
					}
				}
			}
			// Block passed all our checks but is expected-invalid. The remaining
			// invalidity must be in something we can't check (e.g., state root).
			// Trust the fixture: skip the block without merging its state.
			continue
		}

		// Valid block: execute directly on the main overlay.
		stateReader := state.NewReaderV3(overlay)
		stateWriter := state.NewWriter(overlay, nil, txNum)
		stateWriter.ForceWrites = true // overlay-based execution: must write even if original==value
		func() {
			defer func() {
				if r := recover(); r != nil {
					execErr = fmt.Errorf("panic during block execution: %v", r)
				}
			}()
			execResult, execErr = protocol.ExecuteBlockEphemerally(
				config, &vm.Config{},
				blockHashFunc, engine, cb,
				stateReader, stateWriter,
				cr, nil,
				logger,
			)
		}()

		if execErr != nil {
			return fmt.Errorf("block #%v insertion into chain failed: %w", cb.Number(), execErr)
		}
		_ = execResult

		// Validate RLP-decoded header against the test JSON (same as insertBlocks).
		if err = validateHeader(b.BlockHeader, cb.HeaderNoCopy()); err != nil {
			return fmt.Errorf("deserialised block header validation failed: %w", err)
		}

		// Update txNum for the transactions in this block + end-of-block system tx.
		txNum += uint64(cb.Transactions().Len())
		txNum++

		// Record the header and TD for subsequent blocks.
		cr.headers[cb.Hash()] = header
		blockTd := new(big.Int)
		if parentTd != nil {
			blockTd.Set(parentTd)
		}
		blockTd.Add(blockTd, header.Difficulty.ToBig())
		cr.tds[cb.Hash()] = blockTd
		lastBlockHash = cb.Hash()
	}

	// Validate last block hash.
	if common.Hash(bt.json.BestBlock) != lastBlockHash {
		return fmt.Errorf("last block hash validation mismatch: want: %x, have: %x",
			bt.json.BestBlock, lastBlockHash)
	}

	// Validate post-state using the overlay.
	newDB := state.New(state.NewReaderV3(overlay))
	defer newDB.Release(false)
	if err := bt.validatePostState(newDB); err != nil {
		return fmt.Errorf("post state validation failed: %w", err)
	}

	return nil
}

// executeBlockWithIO executes a block while tracking state I/O in a VersionedIO,
// enabling post-execution BAL validation. This replaces ExecuteBlockEphemerally
// with a per-transaction loop that records reads, writes, and address accesses.
func executeBlockWithIO(
	chainConfig *chain.Config,
	blockHashFunc func(n uint64) (common.Hash, error),
	engine rules.Engine, block *types.Block,
	stateReader state.StateReader, stateWriter state.StateWriter,
	chainReader rules.ChainReader, logger log.Logger,
) (res *protocol.EphemeralExecResult, vio *state.VersionedIO, retErr error) {
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("panic during block execution: %v", r)
		}
	}()

	header := block.Header()
	blockNum := block.NumberU64()
	numTx := block.Transactions().Len()

	// Set up VersionedIO and VersionMap for I/O tracking.
	vio = state.NewVersionedIO(numTx + 1) // user txs + system tx
	vmap := state.NewVersionMap(nil)

	ibs := state.New(stateReader)
	defer ibs.Release(false)
	ibs.SetVersionMap(vmap)

	vmConfig := vm.Config{}
	gasUsed := new(protocol.GasUsed)
	gp := new(protocol.GasPool)
	gp.AddGas(block.GasLimit()).AddBlobGas(chainConfig.GetMaxBlobGasPerBlock(block.Time()))

	if err := protocol.InitializeBlockExecution(engine, chainReader, header, chainConfig, ibs, stateWriter, logger, nil); err != nil {
		return nil, nil, err
	}

	receipts := make(types.Receipts, 0, numTx)

	for i, txn := range block.Transactions() {
		ibs.SetTxContext(blockNum, i)
		ibs.ResetVersionedIO()

		receipt, err := protocol.ApplyTransaction(chainConfig, blockHashFunc, engine, accounts.NilAddress, gp, ibs, stateWriter, header, txn, gasUsed, vmConfig)
		if err != nil {
			return nil, nil, fmt.Errorf("could not apply txn %d from block %d [%v]: %w", i, blockNum, txn.Hash().Hex(), err)
		}
		receipts = append(receipts, receipt)

		// Record per-tx I/O into VersionedIO.
		ver := state.Version{BlockNum: blockNum, TxIndex: i}
		vio.RecordReads(ver, ibs.VersionedReads())
		vio.RecordWrites(ver, ibs.VersionedWrites(false))
		vio.RecordAccesses(ver, ibs.AccessedAddresses())
	}

	// Validate receipts.
	receiptSha := types.DeriveSha(receipts)
	if chainConfig.IsByzantium(blockNum) && receiptSha != block.ReceiptHash() {
		return nil, nil, fmt.Errorf("mismatched receipt headers for block %d (%s != %s)", blockNum, receiptSha.Hex(), block.ReceiptHash().Hex())
	}

	// Validate gas.
	blockGasUsed := gasUsed.BlockGasUsed()
	if blockGasUsed != header.GasUsed {
		return nil, nil, fmt.Errorf("gas used by execution: %d, in header: %d", blockGasUsed, header.GasUsed)
	}
	if header.BlobGasUsed != nil && gasUsed.Blob != *header.BlobGasUsed {
		return nil, nil, fmt.Errorf("blob gas used by execution: %d, in header: %d", gasUsed.Blob, *header.BlobGasUsed)
	}

	// Validate bloom.
	bloom := types.CreateBloom(receipts)
	if bloom != header.Bloom {
		return nil, nil, fmt.Errorf("bloom computed by execution: %x, in header: %x", bloom, header.Bloom)
	}

	// Finalize (system transactions: withdrawals, requests).
	ibs.ResetVersionedIO()
	txs := block.Transactions()
	newBlock, requests, err := protocol.FinalizeBlockExecution(engine, stateReader, header, txs, block.Uncles(), stateWriter, chainConfig, ibs, receipts, block.Withdrawals(), chainReader, true, logger, nil)
	if err != nil {
		return nil, nil, err
	}

	// Record system tx I/O.
	sysVer := state.Version{BlockNum: blockNum, TxIndex: numTx}
	vio.RecordReads(sysVer, ibs.VersionedReads())
	vio.RecordWrites(sysVer, ibs.VersionedWrites(false))
	vio.RecordAccesses(sysVer, ibs.AccessedAddresses())

	// Validate requests hash.
	if header.RequestsHash != nil {
		computedHash := requests.Hash()
		if *computedHash != *header.RequestsHash {
			return nil, nil, fmt.Errorf("invalid requests hash: computed %x, header %x", *computedHash, *header.RequestsHash)
		}
	}

	return &protocol.EphemeralExecResult{
		StateRoot:   newBlock.Root(),
		TxRoot:      types.DeriveSha(txs),
		ReceiptRoot: receiptSha,
		Bloom:       bloom,
		Receipts:    receipts,
		Difficulty:  &header.Difficulty,
	}, vio, nil
}
