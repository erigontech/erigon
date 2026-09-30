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

package jsonrpc

import (
	"bytes"
	"fmt"
	"math/big"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	eipWitness "github.com/erigontech/erigon/execution/commitment/eip8297/witness"
	pbtengine "github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc"
)

var pbtCorpusStoreRuntime = []byte{0x60, 0x20, 0x35, 0x60, 0x00, 0x35, 0x55, 0x00}

func pbtCorpusDeployCode(runtime []byte) []byte {
	size := len(runtime)
	const prefixLen = 14
	initcode := []byte{0x61, byte(size >> 8), byte(size), 0x60, prefixLen, 0x60, 0x00, 0x39, 0x61, byte(size >> 8), byte(size), 0x60, 0x00, 0xf3}
	return append(initcode, runtime...)
}

func pbtCorpusStoreCalldata(slot common.Hash, value uint64) []byte {
	encoded := uint256.NewInt(value).Bytes32()
	return append(append([]byte{}, slot[:]...), encoded[:]...)
}

func pbtCorpusSlot(value uint64) common.Hash {
	return common.BigToHash(new(big.Int).SetUint64(value))
}

func pbtCorpusBank(t *testing.T) common.Address {
	t.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	return crypto.PubkeyToAddress(key.PublicKey)
}

func pbtCorpusChain(t *testing.T) (*DebugAPIImpl, *execmoduletester.ExecModuleTester) {
	t.Helper()
	bank := pbtCorpusBank(t)
	newAccount := common.HexToAddress("0x7500000000000000000000000000000000000000")
	var storeA, storeB, destroyer, blockhashContract, revertContract common.Address
	to := common.HexToAddress("0x1000000000000000000000000000000000000001")
	delegate := types.CreateAddress(bank, 5)
	authorityKey, err := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
	require.NoError(t, err)
	authority := crypto.PubkeyToAddress(authorityKey.PublicKey)
	setAuth, err := types.SignAuthorization(authorityKey, *chain.AllProtocolChanges.ChainID, delegate, 0)
	require.NoError(t, err)
	clearAuthorityKey, err := crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
	require.NoError(t, err)
	clearAuthority := crypto.PubkeyToAddress(clearAuthorityKey.PublicKey)
	clearSetAuth, err := types.SignAuthorization(clearAuthorityKey, *chain.AllProtocolChanges.ChainID, delegate, 0)
	require.NoError(t, err)
	clearAuth, err := types.SignAuthorization(clearAuthorityKey, *chain.AllProtocolChanges.ChainID, common.Address{}, 1)
	require.NoError(t, err)
	blockhashRuntime := []byte{0x60, 0x00, 0x40, 0x60, 0x00, 0x55, 0x00}
	revertRuntime := []byte{0x5f, 0x5f, 0xfd}
	api, m := pbinWitnessFixtureWithGeneratorN(t, 1000, 11, nil, func(i int, b *blockgen.BlockGen, addTransaction func(common.Address, *uint256.Int, []byte), addContract func(*uint256.Int, []byte), addSigned func(types.Transaction), addTransactionWithChain func(common.Address, *uint256.Int, []byte)) {
		switch i {
		case 0:
			addTransaction(common.HexToAddress("0x7300000000000000000000000000000000000000"), uint256.NewInt(0), nil)
			addTransaction(newAccount, uint256.NewInt(1), nil)
			addTransaction(authority, uint256.NewInt(1), nil)
			addTransaction(clearAuthority, uint256.NewInt(1), nil)
		case 1:
			addContract(uint256.NewInt(0), []byte{0x60, 0x01, 0x60, 0x03, 0x55, 0x60, 0x02, 0x61, 0x01, 0x00, 0x55, 0x60, 0x00, 0xff})
		case 2:
			storeA = types.CreateAddress(bank, b.TxNonce(bank))
			addContract(uint256.NewInt(0), pbtCorpusDeployCode(pbtCorpusStoreRuntime))
		case 3:
			storeB = types.CreateAddress(bank, b.TxNonce(bank))
			addContract(uint256.NewInt(0), pbtCorpusDeployCode(pbtCorpusStoreRuntime))
		case 4:
			for _, slot := range []uint64{3, 4, 64, 65, 255, 256} {
				addTransaction(storeA, uint256.NewInt(0), pbtCorpusStoreCalldata(pbtCorpusSlot(slot), slot+1))
			}
			addTransaction(storeB, uint256.NewInt(0), pbtCorpusStoreCalldata(pbtCorpusSlot(5), 6))
		case 5:
			for _, slot := range []uint64{3, 4, 64, 65, 255, 256} {
				addTransaction(storeA, uint256.NewInt(0), pbtCorpusStoreCalldata(pbtCorpusSlot(slot), 0))
			}
			addTransaction(storeB, uint256.NewInt(0), pbtCorpusStoreCalldata(pbtCorpusSlot(5), 0))
		case 6:
			blockhashContract = types.CreateAddress(bank, b.TxNonce(bank))
			addContract(uint256.NewInt(0), pbtCorpusDeployCode(blockhashRuntime))
			revertContract = types.CreateAddress(bank, b.TxNonce(bank))
			addContract(uint256.NewInt(0), pbtCorpusDeployCode(revertRuntime))
			destroyer = types.CreateAddress(bank, b.TxNonce(bank))
			addContract(uint256.NewInt(0), pbtCorpusDeployCode([]byte{0x5f, 0x36, 0x11, 0x60, 0x09, 0x57, 0x5f, 0x5f, 0xff, 0x5b, 0x60, 0x20, 0x35, 0x5f, 0x55, 0x00}))
		case 7:
			addTransactionWithChain(blockhashContract, uint256.NewInt(0), nil)
			addTransaction(revertContract, uint256.NewInt(0), nil)
			setCodeTarget := to
			addSigned(&types.SetCodeTransaction{DynamicFeeTransaction: types.DynamicFeeTransaction{CommonTx: types.CommonTx{Nonce: b.TxNonce(bank), GasLimit: 500_000, To: &setCodeTarget}, ChainID: *chain.AllProtocolChanges.ChainID, TipCap: *uint256.NewInt(1_000_000_000), FeeCap: *uint256.NewInt(10_000_000_000)}, Authorizations: []types.Authorization{setAuth}})
			addTransaction(authority, uint256.NewInt(0), pbtCorpusStoreCalldata(pbtCorpusSlot(3), 1))
			b.AddWithdrawal(&types.Withdrawal{Index: 0, Validator: 0, Address: newAccount, Amount: 1})
		case 8:
			addTransaction(authority, uint256.NewInt(0), pbtCorpusStoreCalldata(pbtCorpusSlot(3), 2))
			addTransaction(destroyer, uint256.NewInt(0), pbtCorpusStoreCalldata(pbtCorpusSlot(3), 1))
			addTransaction(destroyer, uint256.NewInt(0), pbtCorpusStoreCalldata(pbtCorpusSlot(256), 2))
			b.AddWithdrawal(&types.Withdrawal{Index: 1, Validator: 1, Address: newAccount, Amount: 1})
			b.AddWithdrawal(&types.Withdrawal{Index: 2, Validator: 2, Address: to, Amount: 1})
		case 9:
			addTransaction(destroyer, uint256.NewInt(0), nil)
			setCodeTarget := to
			addSigned(&types.SetCodeTransaction{DynamicFeeTransaction: types.DynamicFeeTransaction{CommonTx: types.CommonTx{Nonce: b.TxNonce(bank), GasLimit: 500_000, To: &setCodeTarget}, ChainID: *chain.AllProtocolChanges.ChainID, TipCap: *uint256.NewInt(1_000_000_000), FeeCap: *uint256.NewInt(10_000_000_000)}, Authorizations: []types.Authorization{clearSetAuth}})
		case 10:
			setCodeTarget := to
			addSigned(&types.SetCodeTransaction{DynamicFeeTransaction: types.DynamicFeeTransaction{CommonTx: types.CommonTx{Nonce: b.TxNonce(bank), GasLimit: 500_000, To: &setCodeTarget}, ChainID: *chain.AllProtocolChanges.ChainID, TipCap: *uint256.NewInt(1_000_000_000), FeeCap: *uint256.NewInt(10_000_000_000)}, Authorizations: []types.Authorization{clearAuth}})
		}
	})
	repairPBinPreForkShadows(t, m, 1000)
	return api, m
}

func pbtCorpusAnchor(t *testing.T, m *execmoduletester.ExecModuleTester, number uint64) common.Hash {
	t.Helper()
	var root common.Hash
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		header := rawdb.ReadHeaderByNumber(tx, number)
		require.NotNil(t, header)
		data, err := rawdb.ReadShadowStateRoot(tx, header.Hash(), number)
		require.NoError(t, err)
		root = common.BytesToHash(data)
		return nil
	}))
	return root
}

func pbtCorpusClone(result *ExecutionWitnessResult) *ExecutionWitnessResult {
	clone := &ExecutionWitnessResult{
		Keys:           make([]hexutil.Bytes, len(result.Keys)),
		State:          make([]hexutil.Bytes, len(result.State)),
		Codes:          make([]hexutil.Bytes, len(result.Codes)),
		Headers:        append([]hexutil.Bytes(nil), result.Headers...),
		headerByNumber: result.headerByNumber,
	}
	for i := range result.Keys {
		clone.Keys[i] = append(hexutil.Bytes(nil), result.Keys[i]...)
		clone.State[i] = append(hexutil.Bytes(nil), result.State[i]...)
	}
	for i := range result.Codes {
		clone.Codes[i] = append(hexutil.Bytes(nil), result.Codes[i]...)
	}
	return clone
}

func pbtCorpusCloneWithout(result *ExecutionWitnessResult, index int) *ExecutionWitnessResult {
	clone := pbtCorpusClone(result)
	clone.Keys = append(clone.Keys[:index], clone.Keys[index+1:]...)
	clone.State = append(clone.State[:index], clone.State[index+1:]...)
	return clone
}

func TestPBinExecutionWitnessCorpus(t *testing.T) {
	api, m := pbtCorpusChain(t)
	pbt := "pbt"
	for number := uint64(1); number <= 11; number++ {
		number := number
		t.Run(fmt.Sprintf("block-%d", number), func(t *testing.T) {
			result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(number)), nil, &pbt)
			require.NoError(t, err)
			require.NotNil(t, result)
			var block *types.Block
			require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
				var err error
				block, err = m.BlockReader.BlockByNumber(t.Context(), tx, number)
				return err
			}))
			require.NotNil(t, block)
			parentRoot := pbtCorpusAnchor(t, m, number-1)
			postRoot := pbtCorpusAnchor(t, m, number)
			require.Equal(t, postRoot, result.pbtPostRoot, "builder post-root for block %d", number)
			require.NoError(t, verifyPBinWitnessAgainstBlock(t.Context(), result, block, parentRoot, postRoot, m.ChainConfig, m.Engine))
			require.NotEmpty(t, result.State)
			if number == 8 || number == 9 {
				stateAfter := pbtStateAfterBlock(t, m, number)
				authorityKey, err := crypto.HexToECDSA("8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a")
				require.NoError(t, err)
				authority := crypto.PubkeyToAddress(authorityKey.PublicKey)
				value, err := stateAfter.GetState(accounts.InternAddress(authority), accounts.InternKey(pbtCorpusSlot(3)))
				require.NoError(t, err)
				if number == 8 {
					require.Equal(t, uint64(1), value.Uint64())
				} else {
					require.Equal(t, uint64(2), value.Uint64())
				}
			}
			if number == 10 || number == 11 {
				stateAfter := pbtStateAfterBlock(t, m, number)
				clearAuthorityKey, err := crypto.HexToECDSA("49a7b37aa6f6645917e7b807e9d1c00d4fa71f18343b0d4122a4d2df64dd6fee")
				require.NoError(t, err)
				clearAuthority := crypto.PubkeyToAddress(clearAuthorityKey.PublicKey)
				codeHash, err := stateAfter.GetCodeHash(accounts.InternAddress(clearAuthority))
				require.NoError(t, err)
				if number == 10 {
					require.False(t, codeHash.IsEmpty(), "delegation must be set before it is cleared")
				} else {
					require.True(t, codeHash.IsEmpty(), "clearing delegation must restore the empty code hash")
				}
			}
			for index := range result.Keys {
				trimmed := pbtCorpusCloneWithout(result, index)
				require.Error(t, verifyPBinWitnessAgainstBlock(t.Context(), trimmed, block, parentRoot, postRoot, m.ChainConfig, m.Engine), "dropping blob %d path %x must fail", index, []byte(result.Keys[index]))

				corrupted := pbtCorpusClone(result)
				corrupted.State[index][len(corrupted.State[index])-1] ^= 1
				require.Error(t, verifyPBinWitnessAgainstBlock(t.Context(), corrupted, block, parentRoot, postRoot, m.ChainConfig, m.Engine), "corrupting blob %d path %x must fail", index, []byte(result.Keys[index]))

				corrupted = pbtCorpusClone(result)
				if len(corrupted.Keys[index]) == 0 {
					corrupted.Keys[index] = hexutil.Bytes{0xff}
				} else {
					corrupted.Keys[index][len(corrupted.Keys[index])-1] ^= 1
				}
				require.Error(t, verifyPBinWitnessAgainstBlock(t.Context(), corrupted, block, parentRoot, postRoot, m.ChainConfig, m.Engine), "corrupting path %d must fail", index)
			}
		})
	}
}

func TestPBinExecutionWitnessProvesOverflowOnlyAccountCodeHashRead(t *testing.T) {
	victim := common.HexToAddress("0x000000000000000000000000000000000000abcde")
	caller := common.HexToAddress("0x000000000000000000000000000000000000abcdf")
	runtime := append([]byte{0x73}, victim[:]...)
	runtime = append(runtime, 0x3f, 0x50, 0x00)
	alloc := types.GenesisAlloc{
		victim: {Storage: map[common.Hash]common.Hash{pbtCorpusSlot(1 << 20): pbtCorpusSlot(9)}},
		caller: {Code: runtime},
	}
	api, m := pbinWitnessFixtureWithGeneratorNAllocNoSystemCalls(t, 1000, 1, func(i int, _ *blockgen.BlockGen, addTransaction func(common.Address, *uint256.Int, []byte), _ func(*uint256.Int, []byte), _ func(types.Transaction), _ func(common.Address, *uint256.Int, []byte)) {
		if i == 0 {
			addTransaction(caller, uint256.NewInt(0), nil)
		}
	}, alloc)
	repairPBinPreForkShadows(t, m, 1000)
	result := pbtPortWitness(t, api, m, 1)
	cache := new(eip8297.DigestCache)
	headerStem := cache.AccountHeaderStem(victim[:])
	stripped := pbtCorpusClone(result)
	removed := false
	for index := range slices.Backward(stripped.Keys) {
		decoded, err := eipWitness.PBinDecodeBlob(stripped.State[index])
		require.NoError(t, err)
		remove := decoded.Leaf != nil && bytes.Equal(decoded.Leaf.Key[:len(decoded.Leaf.Key)-1], headerStem)
		remove = remove || decoded.Group != nil && bytes.Equal(decoded.Group.Stem, headerStem)
		if remove {
			stripped.Keys = append(stripped.Keys[:index], stripped.Keys[index+1:]...)
			stripped.State = append(stripped.State[:index], stripped.State[index+1:]...)
			removed = true
		}
	}
	require.True(t, removed)
	block := pbtPortBlock(t, m, 1)
	parentRoot, postRoot := pbtDualAnchors(t, m, 1, witnessTriePBT)
	require.Error(t, verifyPBinWitnessAgainstBlock(t.Context(), stripped, block, parentRoot, postRoot, m.ChainConfig, m.Engine))
}

func TestPBinExecutionWitnessDeletesPersistedEmptyStorageAccount(t *testing.T) {
	victim := common.HexToAddress("0x7600000000000000000000000000000000000000")
	toucher := common.HexToAddress("0x7700000000000000000000000000000000000000")
	touchCode := []byte{0x60, 0, 0x60, 0, 0x60, 0, 0x60, 0, 0x60, 0, 0x73}
	touchCode = append(touchCode, victim[:]...)
	touchCode = append(touchCode, 0x5a, 0xf1, 0x00)
	alloc := types.GenesisAlloc{victim: {
		Storage: map[common.Hash]common.Hash{
			pbtCorpusSlot(0):   pbtCorpusSlot(1),
			pbtCorpusSlot(256): pbtCorpusSlot(1),
			pbtCorpusSlot(257): pbtCorpusSlot(2),
		},
	}, toucher: {Code: touchCode}}
	api, m := pbinWitnessFixtureWithGeneratorNAllocNoSystemCalls(t, 1000, 1, func(i int, _ *blockgen.BlockGen, addTransaction func(common.Address, *uint256.Int, []byte), _ func(*uint256.Int, []byte), _ func(types.Transaction), _ func(common.Address, *uint256.Int, []byte)) {
		if i == 0 {
			addTransaction(toucher, uint256.NewInt(0), nil)
		}
	}, alloc)
	repairPBinPreForkShadows(t, m, 1000)
	pbt := "pbt"
	parentRoot, postRoot := pbtDualAnchors(t, m, 1, witnessTriePBT)
	result, err := api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHashWithNumber(1), nil, &pbt)
	require.NoError(t, err)
	block := pbtPortBlock(t, m, 1)
	require.NoError(t, verifyPBinWitnessAgainstBlock(t.Context(), result, block, parentRoot, postRoot, m.ChainConfig, m.Engine))

	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	encoded, _, err := tx.GetLatest(kv.AccountsDomain, victim[:], kv.GetLatestOptions{})
	require.NoError(t, err)
	require.Empty(t, encoded, "the persisted empty account must be deleted by the touch")
	for _, slot := range []uint64{0, 256, 257} {
		slotKey := pbtCorpusSlot(slot)
		key := append(append([]byte(nil), victim[:]...), slotKey[:]...)
		storage, _, err := tx.GetLatest(kv.StorageDomain, key, kv.GetLatestOptions{})
		require.NoError(t, err)
		require.Empty(t, storage, "storage slot %d must be deleted with the account", slot)
	}
}

func TestPBinWitnessCorruptCases(t *testing.T) {
	withBinCommitmentDatadir(t)
	f := newPBinStatelessFixture(t)
	t.Run("blob hash", func(t *testing.T) {
		result := cloneExecutionWitnessResult(f.result)
		index := -1
		for i, path := range result.Keys {
			if len(path) == 0 {
				index = i
				break
			}
		}
		require.NotEqual(t, -1, index)
		result.State[index] = append(hexutil.Bytes(nil), result.State[index]...)
		result.State[index][len(result.State[index])-1] ^= 1
		stateless, err := newPBinWitnessStateless(result, f.root)
		if err == nil {
			_, err = stateless.ReadAccountData(accounts.InternAddress(f.address))
		}
		require.ErrorContains(t, err, "hashes to")
	})
	t.Run("wrong path", func(t *testing.T) {
		result := cloneExecutionWitnessResult(f.result)
		index := pbinStatelessNonRootIndex(result)
		result.Keys[index] = hexutil.Bytes{0xff}
		stateless, err := newPBinWitnessStateless(result, f.root)
		require.NoError(t, err)
		_, err = stateless.ReadAccountData(accounts.InternAddress(f.address))
		require.ErrorContains(t, err, "missing node")
	})
	t.Run("missing code", func(t *testing.T) {
		address := common.Address{0x44}
		code := []byte{0x60, 0x00}
		codeHash := crypto.Keccak256Hash(code)
		basic, err := eip8297.EncodeBasicData(0, uint256.NewInt(0), uint64(len(code)))
		require.NoError(t, err)
		encodedHash := eip8297.CodeHashValue(codeHash)
		ctx := newPBinWitnessInputContext()
		entries := []eip8297.Entry{{Key: eip8297.TreeKeyAccount(address[:], eip8297.BasicDataLeafKey), Value: basic[:]}, {Key: eip8297.TreeKeyAccount(address[:], eip8297.CodeHashLeafKey), Value: encodedHash[:]}}
		root, err := pbtengine.NewTrie(ctx).Process(pbinStatelessEntriesToOps(entries))
		require.NoError(t, err)
		paths, blobs, _, err := pbtengine.NewTrie(ctx).Witness(t.Context(), root, eipWitness.PBinDriverInput{Reads: [][]byte{entries[0].Key, entries[1].Key}})
		require.NoError(t, err)
		result := &ExecutionWitnessResult{Keys: make([]hexutil.Bytes, len(paths)), State: make([]hexutil.Bytes, len(blobs))}
		for index := range paths {
			result.Keys[index] = paths[index]
			result.State[index] = blobs[index]
		}
		stateless, err := newPBinWitnessStateless(result, root)
		require.NoError(t, err)
		_, err = stateless.ReadAccountCode(accounts.InternAddress(address))
		require.ErrorContains(t, err, "missing code")
	})
	t.Run("group depth", func(t *testing.T) {
		key := eip8297.TreeKeyAccount([]byte{0x72}, eip8297.BasicDataLeafKey)
		group := eipWitness.PBinGroup{Position: 0, Stem: bytes.Clone(key[:len(key)-1]), Subs: []byte{0, 1}, Values: [][]byte{pbinStatelessValue(1), pbinStatelessValue(2)}}
		blob, err := eipWitness.PBinEncodeGroup(group)
		require.NoError(t, err)
		root, err := eipWitness.PBinHashBlob(blob)
		require.NoError(t, err)
		group.Position = 1
		badBlob, err := eipWitness.PBinEncodeGroup(group)
		require.NoError(t, err)
		badRoot, err := eipWitness.PBinHashBlob(badBlob)
		require.NoError(t, err)
		_, err = eipWitness.NewPBinTree(badRoot, func([]byte) ([]byte, error) { return badBlob, nil })
		require.ErrorContains(t, err, "group position")
		require.NotEqual(t, root, badRoot)
	})
	t.Run("empty root", func(t *testing.T) {
		result := cloneExecutionWitnessResult(f.result)
		rootIndex := -1
		for index, path := range result.Keys {
			if len(path) == 0 {
				rootIndex = index
				break
			}
		}
		require.NotEqual(t, -1, rootIndex)
		result.State[rootIndex] = nil
		_, err := newPBinWitnessStateless(result, f.root)
		require.ErrorContains(t, err, "empty node")
	})
}
