// Copyright 2014 The go-ethereum Authors
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

package types

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon/execution/chain"
)

//go:generate gencodec -type Genesis -field-override genesisSpecMarshaling -out gen_genesis.go
//go:generate gencodec -type GenesisAccount -field-override genesisAccountMarshaling -out gen_genesis_account.go
//go:generate gencodec -type AuRaSeal -out gen_aura_seal.go

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

	AuRaSeal *AuRaSeal `json:"seal"`

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
	RequestsHash          *common.Hash `json:"requestsHash"`          // EIP-7685
}

type AuRaSeal struct {
	AuthorityRound struct {
		Step      math.HexOrDecimal64 `json:"step"`
		Signature hexutil.Bytes       `json:"signature"`
	} `json:"authorityRound"`
}

func NewAuraSeal(step uint64, signature []byte) *AuRaSeal {
	a := AuRaSeal{}
	a.AuthorityRound.Step = math.HexOrDecimal64(step)
	a.AuthorityRound.Signature = append([]byte{}, signature...)
	return &a
}

// GenesisAlloc specifies the initial state that is part of the genesis block.
type GenesisAlloc map[common.Address]GenesisAccount

func (ga *GenesisAlloc) UnmarshalJSON(data []byte) error {
	m := make(map[common.UnprefixedAddress]GenesisAccount)
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	*ga = make(GenesisAlloc)
	for addr, a := range m {
		(*ga)[common.Address(addr)] = a
	}
	return nil
}

func DecodeGenesisAlloc(i interface{}) (GenesisAlloc, error) {
	var alloc GenesisAlloc

	b, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(b, &alloc); err != nil {
		return nil, err
	}

	return alloc, nil
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
	ExtraData     hexutil.Bytes
	GasLimit      math.HexOrDecimal64
	GasUsed       math.HexOrDecimal64
	Number        math.HexOrDecimal64
	Difficulty    *math.HexOrDecimal256
	BaseFee       *math.HexOrDecimal256
	BlobGasUsed   *math.HexOrDecimal64
	ExcessBlobGas *math.HexOrDecimal64
	Alloc         map[common.UnprefixedAddress]GenesisAccount
}

type genesisAccountMarshaling struct {
	Constructor hexutil.Bytes
	Code        hexutil.Bytes
	Balance     *math.HexOrDecimal256
	Nonce       math.HexOrDecimal64
	Storage     map[storageJSON]storageJSON
	PrivateKey  hexutil.Bytes
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
	return hexutil.Bytes(h[:]).MarshalText()
}
