// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package tests implements execution of Ethereum JSON tests.
package tests

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/stages"
)

// A BlockTest checks handling of entire blocks.
type BlockTest struct {
	json btJSON
}

// UnmarshalJSON implements json.Unmarshaler interface.
func (t *BlockTest) UnmarshalJSON(in []byte) error {
	return json.Unmarshal(in, &t.json)
}

type btJSON struct {
	Blocks     []btBlock             `json:"blocks"`
	Genesis    btHeader              `json:"genesisBlockHeader"`
	Pre        core.GenesisAlloc     `json:"pre"`
	Post       core.GenesisAlloc     `json:"postState"`
	BestBlock  common.UnprefixedHash `json:"lastblockhash"`
	Network    string                `json:"network"`
	SealEngine string                `json:"sealEngine"`
}

type btBlock struct {
	BlockHeader     *btHeader
	ExpectException string
	Rlp             string
	UncleHeaders    []*btHeader
}

//go:generate gencodec -type btHeader -field-override btHeaderMarshaling -out gen_btheader.go

type btHeader struct {
	Bloom            types.Bloom
	Coinbase         libcommon.Address
	MixHash          libcommon.Hash
	Nonce            types.BlockNonce
	Number           *big.Int
	Hash             libcommon.Hash
	ParentHash       libcommon.Hash
	ReceiptTrie      libcommon.Hash
	StateRoot        libcommon.Hash
	TransactionsTrie libcommon.Hash
	UncleHash        libcommon.Hash
	ExtraData        []byte
	Difficulty       *big.Int
	GasLimit         uint64
	GasUsed          uint64
	Timestamp        uint64
	BaseFee          *big.Int
}

type btHeaderMarshaling struct {
	ExtraData  hexutil.Bytes
	Number     *math.HexOrDecimal256
	Difficulty *math.HexOrDecimal256
	GasLimit   math.HexOrDecimal64
	GasUsed    math.HexOrDecimal64
	Timestamp  math.HexOrDecimal64
	BaseFee    *math.HexOrDecimal256
}

func (t *BlockTest) Run(tst *testing.T, _ bool) error {
	config, ok := Forks[t.json.Network]
	if !ok {
		return UnsupportedForkError{t.json.Network}
	}
	var engine consensus.Engine
	if t.json.SealEngine == "NoProof" {
		engine = ethash.NewFaker()
	} else {
		engine = ethash.NewShared()
	}
	if config.TerminalTotalDifficulty != nil {
		engine = serenity.New(engine) // the Merge
	}
	m := stages.MockWithGenesisEngine(tst, t.genesis(config), engine, false)

	// import pre accounts & construct test genesis block & state root
	if m.Genesis.Hash() != t.json.Genesis.Hash {
		return fmt.Errorf("genesis block hash doesn't match test: computed=%x, test=%x", m.Genesis.Hash().Bytes()[:6], t.json.Genesis.Hash[:6])
	}
	if m.Genesis.Root() != t.json.Genesis.StateRoot {
		return fmt.Errorf("genesis block state root does not match test: computed=%x, test=%x", m.Genesis.Root().Bytes()[:6], t.json.Genesis.StateRoot[:6])
	}

	validBlocks, err := t.insertBlocks(m)
	if err != nil {
		return err
	}

	tx, err1 := m.DB.BeginRo(context.Background())
	if err1 != nil {
		return fmt.Errorf("blockTest create tx: %w", err1)
	}
	defer tx.Rollback()
	cmlast := rawdb.ReadHeadBlockHash(tx)
	if libcommon.Hash(t.json.BestBlock) != cmlast {
		return fmt.Errorf("last block hash validation mismatch: want: %x, have: %x", t.json.BestBlock, cmlast)
	}
	newDB := state.New(state.NewPlainStateReader(tx))
	if err = t.validatePostState(newDB); err != nil {
		return fmt.Errorf("post state validation failed: %w", err)
	}
	return t.validateImportedHeaders(tx, validBlocks)
}

func (t *BlockTest) genesis(config *chain.Config) *core.Genesis {
	return &core.Genesis{
		Config:     config,
		Nonce:      t.json.Genesis.Nonce.Uint64(),
		Timestamp:  t.json.Genesis.Timestamp,
		ParentHash: t.json.Genesis.ParentHash,
		ExtraData:  t.json.Genesis.ExtraData,
		GasLimit:   t.json.Genesis.GasLimit,
		GasUsed:    t.json.Genesis.GasUsed,
		Difficulty: t.json.Genesis.Difficulty,
		Mixhash:    t.json.Genesis.MixHash,
		Coinbase:   t.json.Genesis.Coinbase,
		Alloc:      t.json.Pre,
		BaseFee:    t.json.Genesis.BaseFee,
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
func (t *BlockTest) insertBlocks(m *stages.MockSentry) ([]btBlock, error) {
	validBlocks := make([]btBlock, 0)
	// insert the test blocks, which will execute all transaction
	for bi, b := range t.json.Blocks {
		cb, err := b.decode()
		if err != nil {
			if b.BlockHeader == nil {
				continue // OK - block is supposed to be invalid, continue with next block
			} else {
				return nil, fmt.Errorf("block RLP decoding failed when expected to succeed: %w", err)
			}
		}
		// RLP decoding worked, try to insert into chain:
		chain := &core.ChainPack{Blocks: []*types.Block{cb}, Headers: []*types.Header{cb.Header()}, TopBlock: cb}
		err1 := m.InsertChain(chain)
		if err1 != nil {
			if b.BlockHeader == nil {
				continue // OK - block is supposed to be invalid, continue with next block
			} else {
				return nil, fmt.Errorf("block #%v insertion into chain failed: %w", cb.Number(), err1)
			}
		} else if b.BlockHeader == nil {
			if err := m.DB.View(context.Background(), func(tx kv.Tx) error {
				canonical, cErr := rawdb.ReadCanonicalHash(tx, cb.NumberU64())
				if cErr != nil {
					return cErr
				}
				if canonical == cb.Hash() {
					return fmt.Errorf("block (index %d) insertion should have failed due to: %v", bi, b.ExpectException)
				}
				return nil
			}); err != nil {
				return nil, err
			}
		}
		if b.BlockHeader == nil {
			continue
		}
		// validate RLP decoding by checking all values against test file JSON
		if err = validateHeader(b.BlockHeader, cb.Header()); err != nil {
			return nil, fmt.Errorf("deserialised block header validation failed: %w", err)
		}
		validBlocks = append(validBlocks, b)
	}
	return validBlocks, nil
}

func validateHeader(h *btHeader, h2 *types.Header) error {
	if h == nil {
		return fmt.Errorf("validateHeader: h == nil")
	}
	if h2 == nil {
		return fmt.Errorf("validateHeader: h2 == nil")
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
	if h.Number.Cmp(h2.Number) != 0 {
		return fmt.Errorf("number: want: %v have: %v", h.Number, h2.Number)
	}
	if h.ParentHash != h2.ParentHash {
		return fmt.Errorf("parent hash: want: %x have: %x", h.ParentHash, h2.ParentHash)
	}
	if h.ReceiptTrie != h2.ReceiptHash {
		return fmt.Errorf("receipt hash: want: %x have: %x", h.ReceiptTrie, h2.ReceiptHash)
	}
	if h.TransactionsTrie != h2.TxHash {
		return fmt.Errorf("tx hash: want: %x have: %x", h.TransactionsTrie, h2.TxHash)
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
	if h.Difficulty.Cmp(h2.Difficulty) != 0 {
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
	return nil
}

func (t *BlockTest) validatePostState(statedb *state.IntraBlockState) error {
	// validate post state accounts in test file against what we have in state db
	for addr, acct := range t.json.Post {
		// address is indirectly verified by the other fields, as it's the db key
		code2 := statedb.GetCode(addr)
		balance2 := statedb.GetBalance(addr)
		nonce2 := statedb.GetNonce(addr)
		if !bytes.Equal(code2, acct.Code) {
			return fmt.Errorf("account code mismatch for addr: %x want: %v have: %s", addr, acct.Code, hex.EncodeToString(code2))
		}
		if balance2.ToBig().Cmp(acct.Balance) != 0 {
			return fmt.Errorf("account balance mismatch for addr: %x, want: %d, have: %d", addr, acct.Balance, balance2)
		}
		if nonce2 != acct.Nonce {
			return fmt.Errorf("account nonce mismatch for addr: %x want: %d have: %d", addr, acct.Nonce, nonce2)
		}
		for loc, val := range acct.Storage {
			val1 := uint256.NewInt(0).SetBytes(val.Bytes())
			val2 := uint256.NewInt(0)
			statedb.GetState(addr, &loc, val2)
			if !val1.Eq(val2) {
				return fmt.Errorf("storage mismatch for addr: %x loc: %x want: %d have: %d", addr, loc, val1, val2)
			}
		}
	}
	return nil
}

func (t *BlockTest) validateImportedHeaders(tx kv.Tx, validBlocks []btBlock) error {
	// to get constant lookup when verifying block headers by hash (some tests have many blocks)
	bmap := make(map[libcommon.Hash]btBlock, len(t.json.Blocks))
	for _, b := range validBlocks {
		bmap[b.BlockHeader.Hash] = b
	}
	// iterate over blocks backwards from HEAD and validate imported
	// headers vs test file. some tests have reorgs, and we import
	// block-by-block, so we can only validate imported headers after
	// all blocks have been processed by BlockChain, as they may not
	// be part of the longest chain until last block is imported.
	for b := rawdb.ReadCurrentBlock(tx); b != nil && b.NumberU64() != 0; {
		if err := validateHeader(bmap[b.Hash()].BlockHeader, b.Header()); err != nil {
			return fmt.Errorf("imported block header validation failed: %w", err)
		}
		b, _ = rawdb.ReadBlockByHash(tx, b.ParentHash())
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
