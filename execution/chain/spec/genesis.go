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

package chainspec

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"math/big"

	"github.com/jinzhu/copier"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
)

//go:embed allocs
var allocs embed.FS

func ReadPrealloc(fileSys fs.FS, filename string) types.GenesisAlloc {
	f, err := fileSys.Open(filename)
	if err != nil {
		panic(fmt.Sprintf("Could not open genesis preallocation for %s: %v", filename, err))
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	ga := make(types.GenesisAlloc)
	err = decoder.Decode(&ga)
	if err != nil {
		panic(fmt.Sprintf("Could not parse genesis preallocation for %s: %v", filename, err))
	}
	return ga
}

var (
	// to preserve same pointer in genesis.Config and Spec.Config, init once and reuse configs

	mainnetChainConfig = ReadChainConfig(chainspecs, "chainspecs/mainnet.json")
	holeskyChainConfig = ReadChainConfig(chainspecs, "chainspecs/holesky.json")
	sepoliaChainConfig = ReadChainConfig(chainspecs, "chainspecs/sepolia.json")
	hoodiChainConfig   = ReadChainConfig(chainspecs, "chainspecs/hoodi.json")
	gnosisChainConfig  = ReadChainConfig(chainspecs, "chainspecs/gnosis.json")
	chiadoChainConfig  = ReadChainConfig(chainspecs, "chainspecs/chiado.json")
)

// MainnetGenesisBlock returns the Ethereum main net genesis block.
func MainnetGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     mainnetChainConfig,
		Nonce:      66,
		ExtraData:  hexutil.MustDecode("0x11bbe8db4e347b4e8c937c1c8370e4b5ed33adb3db69cbdb7a38e1e50b1b82fa"),
		GasLimit:   5000,
		Difficulty: big.NewInt(17179869184),
		Alloc:      ReadPrealloc(allocs, "allocs/mainnet.json"),
	}
}

// HoleskyGenesisBlock returns the Holesky main net genesis block.
func HoleskyGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     holeskyChainConfig,
		Nonce:      4660,
		GasLimit:   25000000,
		Difficulty: big.NewInt(1),
		Timestamp:  1695902100,
		Alloc:      ReadPrealloc(allocs, "allocs/holesky.json"),
	}
}

// SepoliaGenesisBlock returns the Sepolia network genesis block.
func SepoliaGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     sepoliaChainConfig,
		Nonce:      0,
		ExtraData:  []byte("Sepolia, Athens, Attica, Greece!"),
		GasLimit:   30000000,
		Difficulty: big.NewInt(131072),
		Timestamp:  1633267481,
		Alloc:      ReadPrealloc(allocs, "allocs/sepolia.json"),
	}
}

// HoodiGenesisBlock returns the Hoodi network genesis block.
func HoodiGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     hoodiChainConfig,
		Nonce:      0x1234,
		ExtraData:  []byte(""),
		GasLimit:   0x2255100, // 36M
		Difficulty: big.NewInt(1),
		Timestamp:  1742212800,
		Alloc:      ReadPrealloc(allocs, "allocs/hoodi.json"),
	}
}

func GnosisGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     gnosisChainConfig,
		Timestamp:  0,
		AuRaSeal:   types.NewAuraSeal(0, common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")),
		GasLimit:   0x989680,
		Difficulty: big.NewInt(0x20000),
		Alloc:      ReadPrealloc(allocs, "allocs/gnosis.json"),
	}
}

func ChiadoGenesisBlock() *types.Genesis {
	return &types.Genesis{
		Config:     chiadoChainConfig,
		Timestamp:  0,
		AuRaSeal:   types.NewAuraSeal(0, common.FromHex("0x0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")),
		GasLimit:   0x989680,
		Difficulty: big.NewInt(0x20000),
		Alloc:      ReadPrealloc(allocs, "allocs/chiado.json"),
	}
}

func TestGenesisBlock() *types.Genesis {
	return &types.Genesis{Config: chain.TestChainConfig}
}

// DeveloperGenesisBlock returns the 'geth --dev' genesis block.
func DeveloperGenesisBlock(period uint64, faucet common.Address) *types.Genesis {
	// Override the default period to the user requested one
	var config chain.Config
	copier.Copy(&config, AllCliqueProtocolChanges)
	config.Clique.Period = period

	// Assemble and return the genesis with the precompiles and faucet pre-funded
	return &types.Genesis{
		Config:     &config,
		ExtraData:  append(append(make([]byte, 32), faucet[:]...), make([]byte, crypto.SignatureLength)...),
		GasLimit:   11500000,
		Difficulty: big.NewInt(1),
		Alloc:      ReadPrealloc(allocs, "allocs/dev.json"),
	}
}
