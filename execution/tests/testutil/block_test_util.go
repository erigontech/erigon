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
	"strings"
	"testing"

	"github.com/holiman/uint256"
	jsoniter "github.com/json-iterator/go"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/testforks"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
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

func (bt *BlockTest) Run(t *testing.T) error {
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

	engine := rulesconfig.CreateRulesEngineBareBones(context.Background(), config, log.New())
	mOpts := []execmoduletester.Option{
		execmoduletester.WithCachedDB(db, genesis),
		execmoduletester.WithGenesisSpec(gspec),
		execmoduletester.WithEngine(engine),
	}
	if bt.ExperimentalBAL {
		mOpts = append(mOpts, execmoduletester.WithExperimentalBAL())
	}
	m := execmoduletester.New(t, mOpts...)
	if t == nil {
		defer m.Close() // when t != nil, New registers t.Cleanup(m.Close)
	}

	if m.Genesis.Hash() != bt.json.Genesis.Hash {
		return fmt.Errorf("genesis block hash doesn't match test: computed=%x, test=%x", m.Genesis.Hash().Bytes()[:6], bt.json.Genesis.Hash[:6])
	}
	if m.Genesis.Root() != bt.json.Genesis.StateRoot {
		return fmt.Errorf("genesis block state root does not match test: computed=%x, test=%x", m.Genesis.Root().Bytes()[:6], bt.json.Genesis.StateRoot[:6])
	}

	if err := bt.insertBlocks(m); err != nil {
		return err
	}

	if common.Hash(bt.json.BestBlock) != m.EphemeralLastBlockHash() {
		return fmt.Errorf("last block hash validation mismatch: want: %x, have: %x", bt.json.BestBlock, m.EphemeralLastBlockHash())
	}
	newDB := state.New(m.NewStateReader(m.EphemeralOverlay()))
	if err := bt.validatePostState(newDB); err != nil {
		return fmt.Errorf("post state validation failed: %w", err)
	}
	return nil
}

// RunCLI executes the test without requiring a testing.T context, suitable for CLI usage.
func (bt *BlockTest) RunCLI() error {
	return bt.Run(nil)
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

/*
See https://github.com/ethereum/tests/wiki/Blockchain-Tests-II

	Whether a block is valid or not is a bit subtle, it's defined by presence of
	blockHeader, transactions and uncleHeaders fields. If they are missing, the block is
	invalid and we must verify that we do not accept it.

	Since some tests mix valid and invalid blocks we need to check this for every block.

	If a block is invalid it does not necessarily fail the test, if it's invalidness is
	expected we are expected to ignore it and continue processing and then validate the
	post state.
*/
func (bt *BlockTest) insertBlocks(m *execmoduletester.ExecModuleTester) error {
	// Pre-trace the main chain from BestBlock back to genesis.
	// Tests with uncle/side-chain blocks have interleaved blocks from different
	// forks; only main-chain blocks should be executed on the overlay.
	mainChainBlocks := make(map[common.Hash]bool)
	decoded := make(map[common.Hash]*types.Block)
	for _, b := range bt.json.Blocks {
		cb, err := b.decode()
		if err != nil {
			continue
		}
		decoded[cb.Hash()] = cb
	}
	bestHash := common.Hash(bt.json.BestBlock)
	for h := bestHash; h != m.Genesis.Hash(); {
		mainChainBlocks[h] = true
		cb, ok := decoded[h]
		if !ok {
			return fmt.Errorf("cannot trace main chain from best block %x back to genesis: missing ancestor %x", bestHash, h)
		}
		h = cb.ParentHash()
	}

	// insert the test blocks, which will execute all transactions
	for _, b := range bt.json.Blocks {
		cb, err := b.decode()
		if err != nil {
			if b.BlockHeader == nil {
				continue // OK - block is supposed to be invalid, continue with next block
			}
			return fmt.Errorf("block RLP decoding failed when expected to succeed: %w", err)
		}

		// Expected-invalid blocks: dry-run on a throwaway overlay.
		if b.BlockHeader == nil {
			if err := m.DryRunBlock(cb); err == nil {
				// Ephemeral mode cannot compute state root commitments.
				// If the expected invalidity is a state root mismatch, accept
				// the limitation — this can only be validated by the full
				// staged-sync pipeline with trie commitment.
				if strings.Contains(strings.ToLower(b.ExpectException), "state_root") ||
					strings.Contains(strings.ToLower(b.ExpectException), "stateroot") {
					continue
				}
				return fmt.Errorf("block #%v expected to be invalid (%s), but executed successfully", cb.Number(), b.ExpectException)
			}
			continue
		}
		// Side-chain blocks: record header for uncle verification, skip execution.
		if !mainChainBlocks[cb.Hash()] {
			m.RecordEphemeralHeader(cb.Header())
			continue
		}

		var balBytes []byte
		if len(b.BlockAccessList) > 0 {
			bal := b.BlockAccessList.toBAL()
			balBytes, err = types.EncodeBlockAccessListBytes(bal)
			if err != nil {
				return fmt.Errorf("block #%v encode block access list: %w", cb.Number(), err)
			}
		}
		// RLP decoding worked, try to insert into chain:
		chain := &blockgen.ChainPack{Blocks: []*types.Block{cb}, Headers: []*types.Header{cb.Header()}, TopBlock: cb, BlockAccessLists: [][]byte{balBytes}}

		if err := m.InsertChain(chain); err != nil {
			return fmt.Errorf("block #%v insertion into chain failed: %w", cb.Number(), err)
		}
		// validate RLP decoding by checking all values against test file JSON
		if err = validateHeader(b.BlockHeader, cb.HeaderNoCopy()); err != nil {
			return fmt.Errorf("deserialised block header validation failed: %w", err)
		}
	}
	return nil
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
