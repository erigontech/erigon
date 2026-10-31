// Copyright 2026 The Erigon Authors
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

package executiontests

import (
	"crypto/ecdsa"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
)

var eip161TouchForks = func() []struct {
	name   string
	config *chain.Config
} {
	prague := chain.TestChainOsakaConfig.Copy()
	prague.OsakaTime = nil
	amsterdam := chain.TestChainOsakaConfig.Copy()
	amsterdam.AmsterdamTime = common.NewUint64(0)
	return []struct {
		name   string
		config *chain.Config
	}{{"prague", prague}, {"amsterdam", amsterdam}}
}()

// generateEIP161TouchBlock builds one block with serial or versioned execution.
// From Amsterdam on GenerateChain always runs versioned execution.
func generateEIP161TouchBlock(t *testing.T, gspec *types.Genesis, key *ecdsa.PrivateKey, versioned bool, fill func(*blockgen.BlockGen)) *blockgen.ChainPack {
	t.Helper()
	dbg.Exec3Parallel = versioned
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
	chainPack, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) { fill(b) })
	require.NoError(t, err)
	return chainPack
}

// importEIP161TouchBlock imports the block in serial and in parallel mode.
func importEIP161TouchBlock(t *testing.T, gspec *types.Genesis, key *ecdsa.PrivateKey, chainPack *blockgen.ChainPack) {
	t.Helper()
	for _, parallel := range []bool{false, true} {
		dbg.Exec3Parallel = parallel
		m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
		require.NoError(t, m.InsertChain(chainPack), "parallel=%v", parallel)
	}
}

func eip161TouchKeys(t *testing.T, n int) ([]*ecdsa.PrivateKey, []common.Address) {
	t.Helper()
	var keys []*ecdsa.PrivateKey
	var addrs []common.Address
	for _, hex := range []string{
		"b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291",
		"49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee",
		"8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a",
	}[:n] {
		key, err := crypto.HexToECDSA(hex)
		require.NoError(t, err)
		keys = append(keys, key)
		addrs = append(addrs, crypto.PubkeyToAddress(key.PublicKey))
	}
	return keys, addrs
}

// callCode returns code that sends value to addr with a CALL.
func callCode(addr common.Address, value byte) []byte {
	code := []byte{byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.PUSH1), value, byte(vm.PUSH20)}
	code = append(code, addr[:]...)
	return append(code, byte(vm.GAS), byte(vm.CALL), byte(vm.STOP))
}

// A transaction touches an empty account, by a zero-value call (also when the
// account is the burnt-fee recipient and the burn is zero) or by paying it a
// zero tip as the coinbase, so EIP-161 removes it at the end of that
// transaction. A later transaction in the
// block, from another sender, uses the account as an EIP-7702 authority, whose
// gas depends on whether the authority exists: it must cost the same as for an
// authority that never existed. The touch comes after a long loop, so the
// parallel executor usually runs the authorization first and must invalidate it.
func TestTouchedEmptyAuthorityIsAbsentForLaterTx(t *testing.T) {
	// This test changes dbg.Exec3Parallel and cannot run in parallel.
	prevParallel, prevWorkers := dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() {
		dbg.Exec3Parallel = prevParallel
		ethconfig.Defaults.Sync.ExecWorkerCount = prevWorkers
	})
	ethconfig.Defaults.Sync.ExecWorkerCount = 4

	keys, senders := eip161TouchKeys(t, 2)
	authorityKey, err := crypto.HexToECDSA("9a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	require.NoError(t, err)
	authority := crypto.PubkeyToAddress(authorityKey.PublicKey)
	// Count down from 30000, then CALL the authority with no value.
	loop := []byte{
		byte(vm.PUSH2), 0x75, 0x30, byte(vm.JUMPDEST), byte(vm.PUSH1), 1, byte(vm.SWAP1), byte(vm.SUB),
		byte(vm.DUP1), byte(vm.PUSH1), 3, byte(vm.JUMPI), byte(vm.POP),
	}
	toucher := common.HexToAddress("0x7007")
	to := common.HexToAddress("0xbeef")

	for _, tc := range []struct {
		name     string
		coinbase common.Address
		tip      uint64
		code     []byte
		burnt    bool
	}{
		{"zero-value call", common.Address{1}, 1, append(loop, callCode(authority, 0)...), false},
		{"zero tip", authority, 0, append(loop, byte(vm.STOP)), false},
		{"zero burn", common.Address{1}, 1, append(loop, callCode(authority, 0)...), true},
	} {
		for _, fork := range eip161TouchForks {
			t.Run(tc.name+"/"+fork.name, func(t *testing.T) {
				config := fork.config
				if tc.burnt {
					config = config.Copy()
					config.BurntContract = map[string]common.Address{"0": authority}
				}
				signer := types.LatestSignerForChainID(config.ChainID)
				auth, err := types.SignAuthorization(authorityKey, *config.ChainID, common.HexToAddress("0xcafe"), 0)
				require.NoError(t, err)
				feeCap := uint256.NewInt(10_000_000_000)

				genesis := func(withEmptyAuthority bool) *types.Genesis {
					alloc := types.GenesisAlloc{toucher: {Code: tc.code}}
					for _, sender := range senders {
						alloc[sender] = types.GenesisAccount{Balance: big.NewInt(1_000_000_000_000_000_000)}
					}
					if withEmptyAuthority {
						alloc[authority] = types.GenesisAccount{Balance: big.NewInt(0)}
					}
					gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}
					if tc.burnt {
						gspec.BaseFee = new(uint256.Int)
					}
					return gspec
				}
				fill := func(b *blockgen.BlockGen) {
					b.SetCoinbase(tc.coinbase)
					touch, err := types.SignTx(&types.DynamicFeeTransaction{
						CommonTx: types.CommonTx{Nonce: b.TxNonce(senders[0]), GasLimit: 1_000_000, To: &toucher},
						ChainID:  *config.ChainID,
						TipCap:   *uint256.NewInt(tc.tip),
						FeeCap:   *feeCap,
					}, *signer, keys[0])
					require.NoError(t, err)
					b.AddTx(touch)
					setCode, err := types.SignTx(&types.SetCodeTransaction{
						DynamicFeeTransaction: types.DynamicFeeTransaction{
							CommonTx: types.CommonTx{Nonce: b.TxNonce(senders[1]), GasLimit: 500_000, To: &to},
							ChainID:  *config.ChainID,
							TipCap:   *uint256.NewInt(tc.tip),
							FeeCap:   *feeCap,
						},
						Authorizations: []types.Authorization{auth},
					}, *signer, keys[1])
					require.NoError(t, err)
					b.AddTx(setCode)
				}

				gspec := genesis(true)
				for _, versioned := range []bool{false, true} {
					want := generateEIP161TouchBlock(t, genesis(false), keys[0], versioned, fill).Receipts[0][1].GasUsed
					chainPack := generateEIP161TouchBlock(t, gspec, keys[0], versioned, fill)
					require.Equal(t, want, chainPack.Receipts[0][1].GasUsed, "authorization gas, versioned=%v", versioned)
					importEIP161TouchBlock(t, gspec, keys[0], chainPack)
				}
			})
		}
	}
}

// A transaction credits an empty account, which must be kept: serial and
// versioned execution must agree. The parallel executor credits fees after the
// transaction's writes are finalized, so a tipped coinbase looks empty there
// first.
func TestCreditedEmptyAccountIsKept(t *testing.T) {
	// This test changes dbg.Exec3Parallel and cannot run in parallel.
	prevParallel := dbg.Exec3Parallel
	t.Cleanup(func() { dbg.Exec3Parallel = prevParallel })

	keys, senders := eip161TouchKeys(t, 1)
	authorityKey, err := crypto.HexToECDSA("9a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	require.NoError(t, err)
	empty := crypto.PubkeyToAddress(authorityKey.PublicKey)
	payer := common.HexToAddress("0xc0de")
	gasPrice := uint256.NewInt(10_000_000_000)

	for _, tc := range []struct {
		name  string
		burnt bool
		fill  func(b *blockgen.BlockGen, config *chain.Config)
	}{
		{"tipped coinbase", false, func(b *blockgen.BlockGen, config *chain.Config) {
			b.SetCoinbase(empty)
			txn, err := types.SignTx(types.NewTransaction(b.TxNonce(senders[0]), empty, uint256.NewInt(0), 100_000, gasPrice, nil), *types.LatestSignerForChainID(config.ChainID), keys[0])
			require.NoError(t, err)
			b.AddTx(txn)
		}},
		{"value call", false, func(b *blockgen.BlockGen, config *chain.Config) {
			b.SetCoinbase(common.Address{1})
			txn, err := types.SignTx(types.NewTransaction(b.TxNonce(senders[0]), payer, uint256.NewInt(0), 200_000, gasPrice, nil), *types.LatestSignerForChainID(config.ChainID), keys[0])
			require.NoError(t, err)
			b.AddTx(txn)
		}},
		{"delegated authority", false, func(b *blockgen.BlockGen, config *chain.Config) {
			b.SetCoinbase(common.Address{1})
			auth, err := types.SignAuthorization(authorityKey, *config.ChainID, common.HexToAddress("0xcafe"), 0)
			require.NoError(t, err)
			txn, err := types.SignTx(&types.SetCodeTransaction{
				DynamicFeeTransaction: types.DynamicFeeTransaction{
					CommonTx: types.CommonTx{Nonce: b.TxNonce(senders[0]), GasLimit: 500_000, To: &empty},
					ChainID:  *config.ChainID,
					TipCap:   *gasPrice,
					FeeCap:   *gasPrice,
				},
				Authorizations: []types.Authorization{auth},
			}, *types.LatestSignerForChainID(config.ChainID), keys[0])
			require.NoError(t, err)
			b.AddTx(txn)
		}},
		{"burnt-fee recipient with a non-zero burn", true, func(b *blockgen.BlockGen, config *chain.Config) {
			b.SetCoinbase(common.Address{1})
			txn, err := types.SignTx(types.NewTransaction(b.TxNonce(senders[0]), empty, uint256.NewInt(0), 100_000, gasPrice, nil), *types.LatestSignerForChainID(config.ChainID), keys[0])
			require.NoError(t, err)
			b.AddTx(txn)
		}},
	} {
		for _, fork := range eip161TouchForks {
			t.Run(tc.name+"/"+fork.name, func(t *testing.T) {
				config := fork.config
				if tc.burnt {
					config = config.Copy()
					config.BurntContract = map[string]common.Address{"0": empty}
				}
				gspec := &types.Genesis{
					Config:   config,
					GasLimit: 30_000_000,
					Alloc: types.GenesisAlloc{
						senders[0]: {Balance: big.NewInt(1_000_000_000_000_000_000)},
						payer:      {Code: callCode(empty, 1), Balance: big.NewInt(1)},
						empty:      {Balance: big.NewInt(0), Storage: map[common.Hash]common.Hash{common.HexToHash("0x01"): common.HexToHash("0x01")}},
					},
				}
				fill := func(b *blockgen.BlockGen) { tc.fill(b, config) }
				serial := generateEIP161TouchBlock(t, gspec, keys[0], false, fill)
				versioned := generateEIP161TouchBlock(t, gspec, keys[0], true, fill)
				require.Equal(t, serial.TopBlock.Root(), versioned.TopBlock.Root())
				importEIP161TouchBlock(t, gspec, keys[0], versioned)
			})
		}
	}
}

// A contract with nonce 0, as deployed before Spurious Dragon, receives value
// through a CALL, which reads whether it is empty, and forwards its whole
// balance. It ends with a zero balance and a zero nonce but keeps its code, so
// it is not removed.
func TestEmptiedContractIsKept(t *testing.T) {
	// This test changes dbg.Exec3Parallel and cannot run in parallel.
	prevParallel := dbg.Exec3Parallel
	t.Cleanup(func() { dbg.Exec3Parallel = prevParallel })

	keys, senders := eip161TouchKeys(t, 1)
	wallet := common.HexToAddress("0x3a11e7")
	payer := common.HexToAddress("0xc0de")
	sink := common.HexToAddress("0x5151")
	forward := []byte{byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.SELFBALANCE), byte(vm.PUSH20)}
	forward = append(forward, sink[:]...)
	forward = append(forward, byte(vm.GAS), byte(vm.CALL), byte(vm.STOP))
	for _, fork := range eip161TouchForks {
		t.Run(fork.name, func(t *testing.T) {
			gspec := &types.Genesis{
				Config:   fork.config,
				GasLimit: 30_000_000,
				Alloc: types.GenesisAlloc{
					senders[0]: {Balance: big.NewInt(1_000_000_000_000_000_000)},
					payer:      {Code: callCode(wallet, 1), Balance: big.NewInt(1)},
					wallet:     {Code: forward},
				},
			}
			fill := func(b *blockgen.BlockGen) {
				b.SetCoinbase(common.Address{1})
				txn, err := types.SignTx(types.NewTransaction(b.TxNonce(senders[0]), payer, uint256.NewInt(0), 300_000, uint256.NewInt(10_000_000_000), nil), *types.LatestSignerForChainID(fork.config.ChainID), keys[0])
				require.NoError(t, err)
				b.AddTx(txn)
			}
			serial := generateEIP161TouchBlock(t, gspec, keys[0], false, fill)
			versioned := generateEIP161TouchBlock(t, gspec, keys[0], true, fill)
			require.Equal(t, serial.TopBlock.Root(), versioned.TopBlock.Root())
			importEIP161TouchBlock(t, gspec, keys[0], versioned)
		})
	}
}

func TestTouchedStorageOnlyAccountIsAbsentForLaterTx(t *testing.T) {
	prevParallel := dbg.Exec3Parallel
	t.Cleanup(func() { dbg.Exec3Parallel = prevParallel })

	keys, senders := eip161TouchKeys(t, 1)
	storageOnly := common.HexToAddress("0x7610")
	gasPrice := uint256.NewInt(10_000_000_000)
	for _, fork := range eip161TouchForks {
		t.Run(fork.name, func(t *testing.T) {
			config := fork.config
			gspec := &types.Genesis{
				Config:   config,
				GasLimit: 30_000_000,
				Alloc: types.GenesisAlloc{
					senders[0]:  {Balance: big.NewInt(1_000_000_000_000_000_000)},
					storageOnly: {Balance: big.NewInt(0), Storage: map[common.Hash]common.Hash{common.HexToHash("0x01"): common.HexToHash("0x09")}},
				},
			}
			fill := func(b *blockgen.BlockGen) {
				for _, value := range []uint64{0, 1} {
					txn, err := types.SignTx(types.NewTransaction(b.TxNonce(senders[0]), storageOnly, uint256.NewInt(value), 300_000, gasPrice, nil), *types.LatestSignerForChainID(config.ChainID), keys[0])
					require.NoError(t, err)
					b.AddTx(txn)
				}
			}
			serial := generateEIP161TouchBlock(t, gspec, keys[0], false, fill)
			for _, receipt := range serial.Receipts[0] {
				require.Equal(t, types.ReceiptStatusSuccessful, receipt.Status)
			}
			versioned := generateEIP161TouchBlock(t, gspec, keys[0], true, fill)
			require.Equal(t, serial.TopBlock.Root(), versioned.TopBlock.Root())
			importEIP161TouchBlock(t, gspec, keys[0], serial)
		})
	}
}
