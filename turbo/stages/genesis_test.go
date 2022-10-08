// Copyright 2017 The go-ethereum Authors
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

package stages_test

import (
	"context"
	"math/big"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/require"
)

func TestDefaultGenesisBlock(t *testing.T) {
	block, _, _ := core.DefaultGenesisBlock().ToBlock()
	if block.Hash() != params.MainnetGenesisHash {
		t.Errorf("wrong mainnet genesis hash, got %v, want %v", block.Hash(), params.MainnetGenesisHash)
	}
	var err error
	block, _, err = core.DefaultRopstenGenesisBlock().ToBlock()
	if err != nil {
		t.Errorf("error: %v", err)
	}
	if block.Hash() != params.RopstenGenesisHash {
		t.Errorf("wrong ropsten genesis hash, got %v, want %v", block.Hash(), params.RopstenGenesisHash)
	}

	block, _, err = core.DefaultSokolGenesisBlock().ToBlock()
	if err != nil {
		t.Errorf("error: %v", err)
	}
	if block.Root() != params.SokolGenesisStateRoot {
		t.Errorf("wrong sokol genesis state root, got %v, want %v", block.Root(), params.SokolGenesisStateRoot)
	}
	if block.Hash() != params.SokolGenesisHash {
		t.Errorf("wrong sokol genesis hash, got %v, want %v", block.Hash(), params.SokolGenesisHash)
	}

	block, _, err = core.DefaultFermionGenesisBlock().ToBlock()
	if err != nil {
		t.Errorf("error: %v", err)
	}
	if block.Root() != params.FermionGenesisStateRoot {
		t.Errorf("wrong fermion genesis state root, got %v, want %v", block.Root(), params.FermionGenesisStateRoot)
	}
	if block.Hash() != params.FermionGenesisHash {
		t.Errorf("wrong fermion genesis hash, got %v, want %v", block.Hash(), params.FermionGenesisHash)
	}

	block, _, err = core.DefaultGnosisGenesisBlock().ToBlock()
	if err != nil {
		t.Errorf("error: %v", err)
	}
	if block.Root() != params.GnosisGenesisStateRoot {
		t.Errorf("wrong Gnosis Chain genesis state root, got %v, want %v", block.Root(), params.GnosisGenesisStateRoot)
	}
	if block.Hash() != params.GnosisGenesisHash {
		t.Errorf("wrong Gnosis Chain genesis hash, got %v, want %v", block.Hash(), params.GnosisGenesisHash)
	}
}

func TestSokolHeaderRLP(t *testing.T) {
	require := require.New(t)
	{ //sokol
		expect := common.FromHex("f9020da00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a0fad4af258fd11939fae0c6c6eec9d340b1caac0b0196fd9a1bc3f489c5bf00b3a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000830200008083663be080808080b8410000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000")
		block, _, err := core.DefaultSokolGenesisBlock().ToBlock()
		require.NoError(err)
		b, err := rlp.EncodeToBytes(block.Header())
		require.NoError(err)
		require.Equal(expect, b)
		h := &types.Header{WithSeal: true}
		err = rlp.DecodeBytes(expect, h)
		require.NoError(err)
		require.Equal(2, len(h.Seal))

		expectSeal2 := rlp.RawValue(common.FromHex("0xb8410000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"))
		require.Equal(rlp.RawValue(common.FromHex("0x80")), h.Seal[0])
		require.Equal(expectSeal2, h.Seal[1])
		enc, err := rlp.EncodeToBytes(h)
		require.NoError(err)
		require.Equal(expect, enc)
	}

	{ // sokol, more seals
		h := &types.Header{WithSeal: true}
		enc := common.FromHex("f9020da00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a0fad4af258fd11939fae0c6c6eec9d340b1caac0b0196fd9a1bc3f489c5bf00b3a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000830200008083663be080808002b8410000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001")
		err := rlp.DecodeBytes(enc, h)
		require.NoError(err)
		require.Equal(2, len(h.Seal))
		require.Equal(rlp.RawValue(common.FromHex("0x2")), h.Seal[0])

		expectSeal2 := rlp.RawValue(common.FromHex("0xb8410000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"))
		require.Equal(expectSeal2, h.Seal[1])

		res, err := rlp.EncodeToBytes(h) // after encode getting source bytes
		require.NoError(err)
		require.Equal(enc, res)
	}

	{ // ethash
		expect := common.FromHex("f901f9a0d405da4e66f1445d455195229624e133f5baafe72b5cf7b3c36c12c8146e98b7a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347948888f1f195afa192cfee860698584c030f4c9db1a05fb2b4bfdef7b314451cb138a534d225c922fc0e5fbe25e451142732c3e25c25a088d2ec6b9860aae1a2c3b299f72b6a5d70d7f7ba4722c78f2c49ba96273c2158a007c6fdfa8eea7e86b81f5b0fc0f78f90cc19f4aa60d323151e0cac660199e9a1b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008302008003832fefba82524d84568e932a80a0a0349d8c3df71f1a48a9df7d03fd5f14aeee7d91332c009ecaff0a71ead405bd88ab4e252a7e8c2a23")
		h := &types.Header{WithSeal: false}
		err := rlp.DecodeBytes(expect, h)
		require.NoError(err)

		res, err := rlp.EncodeToBytes(h) // after encode getting source bytes
		require.NoError(err)
		require.Equal(expect, res)
	}
}

func TestSetupGenesis(t *testing.T) {
	var (
		customghash = common.HexToHash("0x89c99d90b79719238d2645c7642f2c9295246e80775b38cfd162b696817fbd50")
		customg     = core.Genesis{
			Config: &params.ChainConfig{ChainID: big.NewInt(1), HomesteadBlock: big.NewInt(3)},
			Alloc: core.GenesisAlloc{
				{1}: {Balance: big.NewInt(1), Storage: map[common.Hash]common.Hash{{1}: {1}}},
			},
		}
		oldcustomg = customg
	)
	oldcustomg.Config = &params.ChainConfig{ChainID: big.NewInt(1), HomesteadBlock: big.NewInt(2)}
	tests := []struct {
		wantErr    error
		fn         func(kv.RwDB) (*params.ChainConfig, *types.Block, error)
		wantConfig *params.ChainConfig
		name       string
		wantHash   common.Hash
	}{
		{
			name: "genesis without ChainConfig",
			fn: func(db kv.RwDB) (*params.ChainConfig, *types.Block, error) {
				return core.CommitGenesisBlock(db, new(core.Genesis))
			},
			wantErr:    core.ErrGenesisNoConfig,
			wantConfig: params.AllEthashProtocolChanges,
		},
		{
			name: "no block in DB, genesis == nil",
			fn: func(db kv.RwDB) (*params.ChainConfig, *types.Block, error) {
				return core.CommitGenesisBlock(db, nil)
			},
			wantHash:   params.MainnetGenesisHash,
			wantConfig: params.MainnetChainConfig,
		},
		{
			name: "mainnet block in DB, genesis == nil",
			fn: func(db kv.RwDB) (*params.ChainConfig, *types.Block, error) {
				return core.CommitGenesisBlock(db, nil)
			},
			wantHash:   params.MainnetGenesisHash,
			wantConfig: params.MainnetChainConfig,
		},
		{
			name: "custom block in DB, genesis == nil",
			fn: func(db kv.RwDB) (*params.ChainConfig, *types.Block, error) {
				customg.MustCommit(db)
				return core.CommitGenesisBlock(db, nil)
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
		},
		{
			name: "custom block in DB, genesis == ropsten",
			fn: func(db kv.RwDB) (*params.ChainConfig, *types.Block, error) {
				customg.MustCommit(db)
				return core.CommitGenesisBlock(db, core.DefaultRopstenGenesisBlock())
			},
			wantErr:    &core.GenesisMismatchError{Stored: customghash, New: params.RopstenGenesisHash},
			wantHash:   params.RopstenGenesisHash,
			wantConfig: params.RopstenChainConfig,
		},
		{
			name: "compatible config in DB",
			fn: func(db kv.RwDB) (*params.ChainConfig, *types.Block, error) {
				oldcustomg.MustCommit(db)
				return core.CommitGenesisBlock(db, &customg)
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
		},
		{
			name: "incompatible config in DB",
			fn: func(db kv.RwDB) (*params.ChainConfig, *types.Block, error) {
				// Commit the 'old' genesis block with Homestead transition at #2.
				// Advance to block #4, past the homestead transition block of customg.
				key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
				m := stages.MockWithGenesis(t, &oldcustomg, key, false)

				chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 4, nil, false /* intermediateHashes */)
				if err != nil {
					return nil, nil, err
				}
				if err = m.InsertChain(chain); err != nil {
					return nil, nil, err
				}
				// This should return a compatibility error.
				return core.CommitGenesisBlock(m.DB, &customg)
			},
			wantHash:   customghash,
			wantConfig: customg.Config,
			wantErr: &params.ConfigCompatError{
				What:         "Homestead fork block",
				StoredConfig: big.NewInt(2),
				NewConfig:    big.NewInt(3),
				RewindTo:     1,
			},
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			db := memdb.NewTestDB(t)
			config, genesis, err := test.fn(db)
			// Check the return values.
			if !reflect.DeepEqual(err, test.wantErr) {
				spew := spew.ConfigState{DisablePointerAddresses: true, DisableCapacities: true}
				t.Fatalf("%s: returned error %#v, want %#v", test.name, spew.NewFormatter(err), spew.NewFormatter(test.wantErr))
			}
			if !reflect.DeepEqual(config, test.wantConfig) {
				t.Errorf("%s:\nreturned %v\nwant     %v", test.name, config, test.wantConfig)
			}

			if test.wantHash == (common.Hash{}) {
				if genesis != nil {
					t.Fatalf("%s: returned non-nil genesis block, want nil", test.name)
				}
				return
			}

			if genesis.Hash() != test.wantHash {
				t.Errorf("%s: returned hash %s, want %s", test.name, genesis.Hash().Hex(), test.wantHash.Hex())
			} else if err == nil {
				if dbErr := db.View(context.Background(), func(tx kv.Tx) error {
					// Check database content.
					stored := rawdb.ReadBlock(tx, test.wantHash, 0)
					if stored.Hash() != test.wantHash {
						t.Errorf("%s: block in DB has hash %s, want %s", test.name, stored.Hash(), test.wantHash)
					}
					return nil
				}); dbErr != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
