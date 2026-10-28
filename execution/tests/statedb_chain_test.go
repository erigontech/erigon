// Copyright 2016 The go-ethereum Authors
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

package executiontests

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/abi/bind/backends"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/contracts"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestSelfDestructReceive(t *testing.T) {
	// Configure and generate a sample block chain
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		address = accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))
		funds   = big.NewInt(1000000000)
		gspec   = &types.Genesis{
			Config: &chain.Config{
				ChainID:               uint256.NewInt(1),
				HomesteadBlock:        new(uint64),
				ByzantiumBlock:        new(uint64),
				ConstantinopleBlock:   new(uint64),
				PetersburgBlock:       new(uint64),
				TangerineWhistleBlock: new(uint64),
				SpuriousDragonBlock:   new(uint64),
			},
			Alloc: types.GenesisAlloc{
				address.Value(): {Balance: funds},
			},
		}
		// this code generates a log
		signer = types.LatestSignerForChainID(nil)
	)

	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))

	contractBackend := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
	transactOpts, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
	require.NoError(t, err)

	var contractAddress common.Address
	var selfDestructorContract *contracts.SelfDestructor

	// There are two blocks
	// First block deploys a contract, then makes it self-destruct, and then sends 1 wei to the address of the contract,
	// effectively turning it from contract account to a non-contract account
	// The second block is empty and is only used to force the newly created blockchain object to reload the trie
	// from the database.
	chain, err := m.GenerateChain(2, func(i int, block *blockgen.BlockGen) {
		var txn types.Transaction

		if i == 0 {
			contractAddress, txn, selfDestructorContract, err = contracts.DeploySelfDestructor(transactOpts, contractBackend)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
			txn, err = selfDestructorContract.SelfDestruct(transactOpts)
			if err != nil {
				t.Fatal(err)
			}
			block.AddTx(txn)
			// Send 1 wei to contract after self-destruction
			txn, err = types.SignTx(types.NewTransaction(block.TxNonce(address.Value()), contractAddress, uint256.NewInt(1000), 21000, uint256.NewInt(1), nil), *signer, key)
			block.AddTx(txn)
		}
		contractBackend.Commit()
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	if err := m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		defer st.Close()
		exist, err := st.Exist(address)
		if err != nil {
			return err
		}
		if !exist {
			t.Error("expected account to exist")
		}
		exist, err = st.Exist(accounts.InternAddress(contractAddress))
		if err != nil {
			return err
		}
		if exist {
			t.Error("expected contractAddress to not exist before block 0", contractAddress.String())
		}
		return nil
	}); err != nil {
		panic(err)
	}

	// BLOCK 1
	if err = m.InsertChain(chain.Slice(0, 1)); err != nil {
		t.Fatal(err)
	}

	// BLOCK 2
	if err = m.InsertChain(chain.Slice(1, 2)); err != nil {
		t.Fatal(err)
	}

	if err := m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
		// If we got this far, the newly created blockchain (with empty trie cache) loaded trie from the database
		// and that means that the state of the accounts written in the first block was correct.
		// This test checks that the storage root of the account is properly set to the root of the empty tree
		st := state.New(m.NewStateReader(tx))
		defer st.Close()
		exist, err := st.Exist(address)
		if err != nil {
			t.Error(err)
		}
		if !exist {
			t.Error("expected account to exist")
		}
		exist, err = st.Exist(accounts.InternAddress(contractAddress))
		if err != nil {
			t.Error(err)
		}
		if !exist {
			t.Error("expected contractAddress to exist at the block 2", contractAddress.String())
		}
		code, err := st.GetCode(accounts.InternAddress(contractAddress))
		if err != nil {
			t.Error(err)
		}
		if len(code) != 0 {
			t.Error("expected empty code in contract at block 2", contractAddress.String())
		}
		return nil
	}); err != nil {
		panic(err)
	}
}

// Self-destruct then revive by value transfer in one block, read back as a whole
// account: GetCode resolves separately and can report empty while a stale code
// hash survives on the record. A pre-block deploy leaves no in-block CodeHash
// cell to floor the destruct scan on, so both placements run.
//
// The block is built by the serial or the versioned generator and imported in
// each mode. Clearing experimentalBAL alone leaves the parallel executor running,
// so the driver sets dbg.Exec3Parallel — which rules out t.Parallel.
func TestSelfDestructReceiveAccountRecord(t *testing.T) {
	for _, tc := range []struct {
		name           string
		parallelImport bool
		preBlockDeploy bool
		parallelGen    bool
	}{
		{"serial/same-block-deploy", false, false, false},
		{"parallel/same-block-deploy", true, false, false},
		{"serial/pre-block-deploy", false, true, false},
		{"parallel/pre-block-deploy", true, true, false},
		{"versioned-gen/same-block-deploy", false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prev := dbg.Exec3Parallel
			dbg.Exec3Parallel = tc.parallelGen
			t.Cleanup(func() { dbg.Exec3Parallel = prev })
			var (
				key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
				address = accounts.InternAddress(crypto.PubkeyToAddress(key.PublicKey))
				funds   = big.NewInt(1000000000)
				gspec   = &types.Genesis{
					Config: &chain.Config{
						ChainID:               uint256.NewInt(1),
						HomesteadBlock:        new(uint64),
						ByzantiumBlock:        new(uint64),
						ConstantinopleBlock:   new(uint64),
						PetersburgBlock:       new(uint64),
						TangerineWhistleBlock: new(uint64),
						SpuriousDragonBlock:   new(uint64),
					},
					Alloc: types.GenesisAlloc{address.Value(): {Balance: funds}},
				}
				signer = types.LatestSignerForChainID(nil)
			)

			m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
			contractBackend := backends.NewSimulatedBackendWithConfig(t, gspec.Alloc, gspec.Config, gspec.GasLimit)
			transactOpts, err := bind.NewKeyedTransactorWithChainID(key, m.ChainConfig.ChainID)
			require.NoError(t, err)

			var contractAddress common.Address
			var selfDestructorContract *contracts.SelfDestructor

			deployAt, destructAt := 0, 0
			if tc.preBlockDeploy {
				destructAt = 1
			}

			chain, err := m.GenerateChain(3, func(i int, block *blockgen.BlockGen) {
				var txn types.Transaction
				if i == deployAt {
					contractAddress, txn, selfDestructorContract, err = contracts.DeploySelfDestructor(transactOpts, contractBackend)
					require.NoError(t, err)
					block.AddTx(txn)
				}
				if i == destructAt {
					txn, err = selfDestructorContract.SelfDestruct(transactOpts)
					require.NoError(t, err)
					block.AddTx(txn)

					// A value transfer, not a redeploy: no CREATE writes a nonce or code hash.
					txn, err = types.SignTx(
						types.NewTransaction(block.TxNonce(address.Value()), contractAddress, uint256.NewInt(1000), 21000, uint256.NewInt(1), nil),
						*signer, key,
					)
					require.NoError(t, err)
					block.AddTx(txn)
				}
				contractBackend.Commit()
			})
			require.NoError(t, err)
			dbg.Exec3Parallel = tc.parallelImport
			require.NoError(t, m.InsertChain(chain.Slice(0, 3)))

			require.NoError(t, m.DB.ViewTemporal(context.Background(), func(tx kv.TemporalTx) error {
				st := state.New(m.NewStateReader(tx))
				defer st.Close()
				addr := accounts.InternAddress(contractAddress)

				code, err := st.GetCode(addr)
				require.NoError(t, err)
				require.Empty(t, code, "the destruct removed the code and the transfer did not write any")

				hash, err := st.GetCodeHash(addr)
				require.NoError(t, err)
				require.Equal(t, accounts.EmptyCodeHash, hash, "the code hash must agree with the code")

				nonce, err := st.GetNonce(addr)
				require.NoError(t, err)
				require.Zero(t, nonce, "the transfer revived it as a new account")

				bal, err := st.GetBalance(addr)
				require.NoError(t, err)
				require.Equal(t, uint64(1000), bal.Uint64(), "the transfer that revived it is still credited")
				return nil
			}))
		})
	}
}

func shanghaiConfig() *chain.Config {
	c := chain.TestChainOsakaConfig.Copy()
	c.CancunTime, c.PragueTime, c.OsakaTime = nil, nil, nil
	return c
}

// importSerialThenParallel builds n blocks with serial execution and imports
// them into a serial node. Then, for each worker count, it imports the blocks as
// one batch into a fresh parallel node.
func importSerialThenParallel(t *testing.T, gspec *types.Genesis, key *ecdsa.PrivateKey, n int, workerCounts []int, gen func(i int, b *blockgen.BlockGen)) (*execmoduletester.ExecModuleTester, []*execmoduletester.ExecModuleTester) {
	t.Helper()
	prevParallel, prevWorkers := dbg.Exec3Parallel, ethconfig.Defaults.Sync.ExecWorkerCount
	t.Cleanup(func() {
		dbg.Exec3Parallel = prevParallel
		ethconfig.Defaults.Sync.ExecWorkerCount = prevWorkers
	})
	dbg.Exec3Parallel = false
	serial := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
	chainPack, err := serial.GenerateChain(n, gen)
	require.NoError(t, err)
	require.NoError(t, serial.InsertChain(chainPack))

	var parallels []*execmoduletester.ExecModuleTester
	for _, workers := range workerCounts {
		dbg.Exec3Parallel = true
		ethconfig.Defaults.Sync.ExecWorkerCount = workers
		p := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
		require.NoErrorf(t, p.InsertChain(chainPack), "workers=%d: parallel import", workers)
		parallels = append(parallels, p)
	}
	return serial, parallels
}

func storageValues(t *testing.T, m *execmoduletester.ExecModuleTester, addr common.Address, slots ...uint64) []uint64 {
	t.Helper()
	var out []uint64
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		st := state.New(m.NewStateReader(tx))
		defer st.Close()
		for _, slot := range slots {
			v, err := st.GetState(accounts.InternAddress(addr), accounts.InternKey(common.BigToHash(new(big.Int).SetUint64(slot))))
			if err != nil {
				return err
			}
			out = append(out, v.Uint64())
		}
		return nil
	}))
	return out
}

// domainHistory reads keys as of every txNum of blocks [from, to].
func domainHistory(t *testing.T, m *execmoduletester.ExecModuleTester, domain kv.Domain, keys [][]byte, from, to uint64) []string {
	t.Helper()
	var out []string
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		r := m.BlockReader.TxnumReader()
		lo, err := r.Min(t.Context(), tx, from)
		if err != nil {
			return err
		}
		hi, err := r.Max(t.Context(), tx, to)
		if err != nil {
			return err
		}
		for txNum := lo; txNum <= hi+1; txNum++ {
			for _, k := range keys {
				v, ok, err := tx.GetAsOf(domain, k, txNum)
				if err != nil {
					return err
				}
				out = append(out, fmt.Sprintf("tx%d %x ok=%v %s", txNum-lo, k, ok, hex.EncodeToString(v)))
			}
		}
		return nil
	}))
	return out
}

// A storage slot set and cleared back to its pre-batch value across the blocks
// of one batch must stay cleared, and the per-tx history of storage and of the
// coinbase and sender accounts must match serial execution.
func TestParallelBatchStorageAndHistoryMatchSerial(t *testing.T) {
	toggler := common.HexToAddress("0x7066")
	coinbase := common.HexToAddress("0xc0ffee")
	// SSTORE(cd[64], SLOAD(1)+1); SSTORE(cd[64]+1, SLOAD(2)+1); SSTORE(1, cd[0]); SSTORE(2, cd[32])
	code := common.FromHex("0x600154600101604035556002546001016040356001015560003560015560203560025500")
	config := shanghaiConfig()
	alloc := types.GenesisAlloc{toggler: {Code: code, Storage: map[common.Hash]common.Hash{common.BigToHash(big.NewInt(2)): common.BigToHash(big.NewInt(5))}}}
	keys := revivalKeys(t, alloc, 3)
	gspec := &types.Genesis{Config: config, GasLimit: 30_000_000, Alloc: alloc}
	signer := types.LatestSignerForChainID(config.ChainID)

	// The values each tx writes to slots 1 and 2, per block.
	plan := [][][2]uint64{
		{{1, 0}},
		{{0, 5}},
		{{1, 0}, {0, 5}},
		{{0, 0}},
		{{1, 5}},
		{{0, 0}, {0, 5}, {1, 0}},
		{{0, 5}},
	}
	obs := uint64(1000)
	txi := 0
	gen := func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(coinbase)
		for _, w := range plan[i] {
			from := txi % len(keys)
			txi++
			data := make([]byte, 96)
			new(big.Int).SetUint64(w[0]).FillBytes(data[0:32])
			new(big.Int).SetUint64(w[1]).FillBytes(data[32:64])
			new(big.Int).SetUint64(obs).FillBytes(data[64:96])
			obs += 2
			sender := crypto.PubkeyToAddress(keys[from].PublicKey)
			txn, err := types.SignTx(types.NewTransaction(b.TxNonce(sender), toggler, uint256.NewInt(0), 200_000, uint256.NewInt(10_000_000_000), data), *signer, keys[from])
			require.NoError(t, err)
			b.AddTx(txn)
		}
	}
	serial, parallels := importSerialThenParallel(t, gspec, keys[0], len(plan), []int{1, 4}, gen)

	slots := []uint64{1, 2}
	for s := uint64(1000); s < obs; s++ {
		slots = append(slots, s)
	}
	var storageKeys [][]byte
	for _, slot := range []uint64{1, 2} {
		h := common.BigToHash(new(big.Int).SetUint64(slot))
		storageKeys = append(storageKeys, append(append([]byte{}, toggler[:]...), h[:]...))
	}
	sender := crypto.PubkeyToAddress(keys[0].PublicKey)
	accountKeys := [][]byte{coinbase[:], sender[:]}
	want := storageValues(t, serial, toggler, slots...)
	wantStorageHistory := domainHistory(t, serial, kv.StorageDomain, storageKeys, 1, uint64(len(plan)))
	wantAccountHistory := domainHistory(t, serial, kv.AccountsDomain, accountKeys, 1, uint64(len(plan)))
	for i, p := range parallels {
		require.Equalf(t, want, storageValues(t, p, toggler, slots...), "parallel #%d: storage", i)
		require.Equalf(t, wantStorageHistory, domainHistory(t, p, kv.StorageDomain, storageKeys, 1, uint64(len(plan))), "parallel #%d: per-tx storage history", i)
		require.Equalf(t, wantAccountHistory, domainHistory(t, p, kv.AccountsDomain, accountKeys, 1, uint64(len(plan))), "parallel #%d: per-tx account history", i)
	}
}
