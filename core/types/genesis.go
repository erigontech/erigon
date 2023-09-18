// Copyright 2014 The go-ethereum Authors
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

package types

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"

	common2 "github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/params"
)

//go:generate gencodec -type Genesis -field-override genesisSpecMarshaling -out gen_genesis.go
//go:generate gencodec -type GenesisAccount -field-override genesisAccountMarshaling -out gen_genesis_account.go

var ErrGenesisNoConfig = errors.New("genesis has no chain configuration")

// Genesis specifies the header fields, state of a genesis block. It also defines hard
// fork switch-over blocks through the chain configuration.
type Genesis struct {
	Config     *chain.Config  `json:"config"`
	Nonce      uint64         `json:"nonce"`
	Timestamp  uint64         `json:"timestamp"`
	ExtraData  []byte         `json:"extraData"`
	GasLimit   uint64         `json:"gasLimit"   gencodec:"required"`
	Difficulty *big.Int       `json:"difficulty" gencodec:"required"`
	Mixhash    common.Hash    `json:"mixHash"`
	Coinbase   common.Address `json:"coinbase"`
	Alloc      GenesisAlloc   `json:"alloc"      gencodec:"required"`

	AuRaStep uint64 `json:"auRaStep"`
	AuRaSeal []byte `json:"auRaSeal"`

	// These fields are used for consensus tests. Please don't use them
	// in actual genesis blocks.
	Number     uint64      `json:"number"`
	GasUsed    uint64      `json:"gasUsed"`
	ParentHash common.Hash `json:"parentHash"`

	// Header fields added in London and later hard forks
	BaseFee               *big.Int     `json:"baseFeePerGas"`         // EIP-1559
	BlobGasUsed           *uint64      `json:"blobGasUsed"`           // EIP-4844
	ExcessBlobGas         *uint64      `json:"excessBlobGas"`         // EIP-4844
	ParentBeaconBlockRoot *common.Hash `json:"parentBeaconBlockRoot"` // EIP-4788
}

// GenesisAlloc specifies the initial state that is part of the genesis block.
type GenesisAlloc map[common.Address]GenesisAccount

type AuthorityRoundSeal struct {
	/// Seal step.
	Step uint64 `json:"step"`
	/// Seal signature.
	Signature common.Hash `json:"signature"`
}

func (ga *GenesisAlloc) UnmarshalJSON(data []byte) error {
	m := make(map[common2.UnprefixedAddress]GenesisAccount)
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	*ga = make(GenesisAlloc)
	for addr, a := range m {
		(*ga)[common.Address(addr)] = a
	}
	return nil
}

// GenesisAccount is an account in the state of the genesis block.
// Either use "constructor" for deployment code or "code" directly for the final code.
type GenesisAccount struct {
	Constructor []byte                      `json:"constructor,omitempty"` // deployment code
	Code        []byte                      `json:"code,omitempty"`        // final contract code
	Storage     map[common.Hash]common.Hash `json:"storage,omitempty"`
	Balance     *big.Int                    `json:"balance" gencodec:"required"`
	Nonce       uint64                      `json:"nonce,omitempty"`
	PrivateKey  []byte                      `json:"secretKey,omitempty"` // for tests
}

// field type overrides for gencodec
type genesisSpecMarshaling struct {
	Nonce         math.HexOrDecimal64
	Timestamp     math.HexOrDecimal64
	ExtraData     hexutility.Bytes
	GasLimit      math.HexOrDecimal64
	GasUsed       math.HexOrDecimal64
	Number        math.HexOrDecimal64
	Difficulty    *math.HexOrDecimal256
	BaseFee       *math.HexOrDecimal256
	BlobGasUsed   *math.HexOrDecimal64
	ExcessBlobGas *math.HexOrDecimal64
	Alloc         map[common2.UnprefixedAddress]GenesisAccount
}

type genesisAccountMarshaling struct {
	Constructor hexutility.Bytes
	Code        hexutility.Bytes
	Balance     *math.HexOrDecimal256
	Nonce       math.HexOrDecimal64
	Storage     map[storageJSON]storageJSON
	PrivateKey  hexutility.Bytes
}

// storageJSON represents a 256 bit byte array, but allows less than 256 bits when
// unmarshaling from hex.
type storageJSON common.Hash

func (h *storageJSON) UnmarshalText(text []byte) error {
	text = bytes.TrimPrefix(text, []byte("0x"))
	if len(text) > 64 {
		return fmt.Errorf("too many hex characters in storage key/value %q", text)
	}
	offset := len(h) - len(text)/2 // pad on the left
	if _, err := hex.Decode(h[offset:], text); err != nil {
		return fmt.Errorf("invalid hex storage key/value %q", text)
	}
	return nil
}

func (h storageJSON) MarshalText() ([]byte, error) {
	return hexutility.Bytes(h[:]).MarshalText()
}

// GenesisMismatchError is raised when trying to overwrite an existing
// genesis block with an incompatible one.
type GenesisMismatchError struct {
	Stored, New common.Hash
}

func (e *GenesisMismatchError) Error() string {
	config := params.ChainConfigByGenesisHash(e.Stored)
	if config == nil {
		return fmt.Sprintf("database contains incompatible genesis (have %x, new %x)", e.Stored, e.New)
	}
	return fmt.Sprintf("database contains incompatible genesis (try with --chain=%s)", config.ChainName)
}
func (g *Genesis) ConfigOrDefault(genesisHash common.Hash) *chain.Config {
	if g != nil {
		return g.Config
	}

	config := params.ChainConfigByGenesisHash(genesisHash)
	if config != nil {
		return config
	} else {
		return params.AllProtocolChanges
	}
}
