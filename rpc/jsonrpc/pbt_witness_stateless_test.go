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
	"context"
	"sort"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	eipWitness "github.com/erigontech/erigon/execution/commitment/eip8297/witness"
	pbtengine "github.com/erigontech/erigon/execution/commitment/v3/pbt"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

type pbinStatelessFixture struct {
	address common.Address
	slot    common.Hash
	root    common.Hash
	result  *ExecutionWitnessResult
	context *pbinWitnessInputContext
}

func newPBinStatelessFixture(t *testing.T) *pbinStatelessFixture {
	t.Helper()
	withBinCommitmentDatadir(t)
	address := common.HexToAddress("0x6100000000000000000000000000000000000000")
	other := common.HexToAddress("0x6200000000000000000000000000000000000000")
	slot := common.HexToHash("0x80")
	value := uint256.NewInt(7)
	basic, err := eip8297.EncodeBasicData(1, value, 0)
	require.NoError(t, err)
	otherBasic, err := eip8297.EncodeBasicData(0, uint256.NewInt(3), 0)
	require.NoError(t, err)
	emptyCodeHash := eip8297.CodeHashValue(common.Hash{})
	entries := []eip8297.Entry{
		{Key: eip8297.TreeKeyAccount(address[:], eip8297.BasicDataLeafKey), Value: basic[:]},
		{Key: eip8297.TreeKeyAccount(address[:], eip8297.CodeHashLeafKey), Value: emptyCodeHash[:]},
		{Key: eip8297.TreeKeyAccount(other[:], eip8297.BasicDataLeafKey), Value: otherBasic[:]},
		{Key: eip8297.TreeKeyAccount(other[:], eip8297.CodeHashLeafKey), Value: emptyCodeHash[:]},
		{Key: eip8297.TreeKeyStorage(address[:], slot[:]), Value: pbinStatelessValue(9)},
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	ctx := newPBinWitnessInputContext()
	root, err := pbtengine.NewTrie(ctx).Process(pbinStatelessEntriesToOps(entries))
	require.NoError(t, err)
	reads := make([][]byte, 0, len(entries))
	for _, entry := range entries {
		reads = append(reads, entry.Key)
	}
	paths, blobs, _, err := pbtengine.NewTrie(ctx).Witness(context.Background(), root, eipWitness.PBinDriverInput{Reads: reads})
	require.NoError(t, err)
	result := &ExecutionWitnessResult{Keys: make([]hexutil.Bytes, len(paths)), State: make([]hexutil.Bytes, len(blobs))}
	for i := range paths {
		result.Keys[i] = hexutil.Bytes(paths[i])
		result.State[i] = hexutil.Bytes(blobs[i])
	}
	return &pbinStatelessFixture{address: address, slot: slot, root: root, result: result, context: ctx}
}

func pbinStatelessValue(value byte) []byte {
	result := make([]byte, eip8297.ValueLength)
	result[len(result)-1] = value
	return result
}

func pbinStatelessValueArray(value byte) [eip8297.ValueLength]byte {
	var result [eip8297.ValueLength]byte
	result[len(result)-1] = value
	return result
}

func pbinStatelessEntriesToOps(entries []eip8297.Entry) []pbtengine.Op {
	result := make([]pbtengine.Op, len(entries))
	for i, entry := range entries {
		var value [eip8297.ValueLength]byte
		copy(value[:], entry.Value)
		result[i] = pbtengine.Op{Key: entry.Key, Value: value}
	}
	return result
}

func (f *pbinStatelessFixture) stateless(t *testing.T) *pbinWitnessStateless {
	t.Helper()
	stateless, err := newPBinWitnessStateless(f.result, f.root)
	require.NoError(t, err)
	return stateless
}

func TestPBinWitnessStatelessReplaysWrites(t *testing.T) {
	f := newPBinStatelessFixture(t)
	stateless := f.stateless(t)
	address := accounts.InternAddress(f.address)
	account, err := stateless.ReadAccountData(address)
	require.NoError(t, err)
	require.NotNil(t, account)
	account.Balance = *uint256.NewInt(17)
	require.NoError(t, stateless.UpdateAccountData(address, nil, account))
	require.NoError(t, stateless.WriteAccountStorage(address, 0, accounts.InternKey(f.slot), uint256.Int{}, *uint256.NewInt(11)))
	got, err := stateless.Finalize(context.Background())
	require.NoError(t, err)

	post := newPBinWitnessInputContext()
	post.records = clonePBinWitnessInputRecords(f.context.records)
	basic, err := eip8297.EncodeBasicData(1, uint256.NewInt(17), 0)
	require.NoError(t, err)
	ops := []pbtengine.Op{
		{Key: eip8297.TreeKeyAccount(f.address[:], eip8297.BasicDataLeafKey), Value: basic},
		{Key: eip8297.TreeKeyStorage(f.address[:], f.slot[:]), Value: pbinStatelessValueArray(11)},
	}
	want, err := pbtengine.NewTrie(post).Process(ops)
	require.NoError(t, err)
	require.Equal(t, want, got, "the stateless verifier must reproduce the post-state root")
}

func TestPBinWitnessStatelessReplaysWithdrawal(t *testing.T) {
	f := newPBinStatelessFixture(t)
	post := newPBinWitnessInputContext()
	post.records = clonePBinWitnessInputRecords(f.context.records)
	balance := uint256.NewInt(7 + 3*common.GWei)
	basic, err := eip8297.EncodeBasicData(1, balance, 0)
	require.NoError(t, err)
	postRoot, err := pbtengine.NewTrie(post).Process([]pbtengine.Op{{Key: eip8297.TreeKeyAccount(f.address[:], eip8297.BasicDataLeafKey), Value: basic}})
	require.NoError(t, err)
	block := types.NewBlock(&types.Header{Root: postRoot, Number: *uint256.NewInt(1), Difficulty: uint256.Int{}, GasLimit: 30_000_000, Time: 1, BaseFee: uint256.NewInt(7)}, nil, nil, nil, []*types.Withdrawal{{Index: 0, Validator: 0, Address: f.address, Amount: 3}}, nil)
	engine := merge.New(ethash.NewFaker())
	chainConfig := pbinStatelessChainConfig()
	require.NoError(t, verifyPBinWitnessAgainstBlock(context.Background(), f.result, block, f.root, postRoot, chainConfig, engine))
	require.ErrorContains(t, verifyPBinWitnessAgainstBlock(context.Background(), f.result, block, f.root, common.HexToHash("0x01"), chainConfig, engine), "state root mismatch")
}

func pbinStatelessChainConfig() *chain.Config {
	return &chain.Config{
		ChainID:                       uint256.NewInt(1337),
		Rules:                         chain.EtHashRules,
		HomesteadBlock:                common.NewUint64(0),
		TangerineWhistleBlock:         common.NewUint64(0),
		SpuriousDragonBlock:           common.NewUint64(0),
		ByzantiumBlock:                common.NewUint64(0),
		ConstantinopleBlock:           common.NewUint64(0),
		PetersburgBlock:               common.NewUint64(0),
		IstanbulBlock:                 common.NewUint64(0),
		BerlinBlock:                   common.NewUint64(0),
		LondonBlock:                   common.NewUint64(0),
		TerminalTotalDifficulty:       uint256.NewInt(0),
		TerminalTotalDifficultyPassed: true,
		ShanghaiTime:                  common.NewUint64(0),
		Ethash:                        new(chain.EthashConfig),
	}
}

func TestPBinWitnessStatelessMissingBlobErrors(t *testing.T) {
	f := newPBinStatelessFixture(t)
	index := pbinStatelessNonRootIndex(f.result)
	trimmed := cloneExecutionWitnessResult(f.result)
	trimmed.Keys = append(trimmed.Keys[:index], trimmed.Keys[index+1:]...)
	trimmed.State = append(trimmed.State[:index], trimmed.State[index+1:]...)
	stateless, err := newPBinWitnessStateless(trimmed, f.root)
	if err == nil {
		_, err = stateless.ReadAccountData(accounts.InternAddress(f.address))
	}
	require.ErrorContains(t, err, "missing node")
}

func TestPBinWitnessStatelessTamperedBlobErrors(t *testing.T) {
	f := newPBinStatelessFixture(t)
	index := pbinStatelessNonRootIndex(f.result)
	tampered := cloneExecutionWitnessResult(f.result)
	tampered.State[index] = append(hexutil.Bytes(nil), tampered.State[index]...)
	tampered.State[index][len(tampered.State[index])-1] ^= 1
	stateless, err := newPBinWitnessStateless(tampered, f.root)
	if err == nil {
		_, err = stateless.ReadAccountData(accounts.InternAddress(f.address))
	}
	require.ErrorContains(t, err, "hashes to")
}

func TestPBinWitnessStatelessSyntheticSystemAccessIsSuppressed(t *testing.T) {
	f := newPBinStatelessFixture(t)
	rootOnly := &ExecutionWitnessResult{Keys: []hexutil.Bytes{f.result.Keys[0]}, State: []hexutil.Bytes{f.result.State[0]}}
	stateless, err := newPBinWitnessStateless(rootOnly, f.root)
	require.NoError(t, err)
	account, err := stateless.ReadAccountData(params.SystemAddress)
	require.NoError(t, err)
	require.Nil(t, account)
	_, err = stateless.ReadAccountData(accounts.InternAddress(f.address))
	require.ErrorContains(t, err, "missing node")
}

func TestPBinWitnessStatelessGenuineSystemAccessNeedsProof(t *testing.T) {
	f := newPBinStatelessFixture(t)
	system := common.Address(params.SystemAddress.Value())
	basic, err := eip8297.EncodeBasicData(1, uint256.NewInt(2), 0)
	require.NoError(t, err)
	emptyCodeHash := eip8297.CodeHashValue(common.Hash{})
	otherBasic := pbinStatelessAccountBasic(1)
	ctx := newPBinWitnessInputContext()
	entries := []eip8297.Entry{
		{Key: eip8297.TreeKeyAccount(system[:], eip8297.BasicDataLeafKey), Value: basic[:]},
		{Key: eip8297.TreeKeyAccount(system[:], eip8297.CodeHashLeafKey), Value: emptyCodeHash[:]},
		{Key: eip8297.TreeKeyAccount(f.address[:], eip8297.BasicDataLeafKey), Value: otherBasic[:]},
		{Key: eip8297.TreeKeyAccount(f.address[:], eip8297.CodeHashLeafKey), Value: emptyCodeHash[:]},
	}
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	root, err := pbtengine.NewTrie(ctx).Process(pbinStatelessEntriesToOps(entries))
	require.NoError(t, err)
	paths, blobs, _, err := pbtengine.NewTrie(ctx).Witness(context.Background(), root, eipWitness.PBinDriverInput{Reads: [][]byte{eip8297.TreeKeyAccount(system[:], eip8297.BasicDataLeafKey), eip8297.TreeKeyAccount(system[:], eip8297.CodeHashLeafKey)}})
	require.NoError(t, err)
	result := &ExecutionWitnessResult{Keys: make([]hexutil.Bytes, len(paths)), State: make([]hexutil.Bytes, len(blobs))}
	for i := range paths {
		result.Keys[i] = paths[i]
		result.State[i] = blobs[i]
	}
	stateless, err := newPBinWitnessStateless(result, root)
	require.NoError(t, err)
	account, err := stateless.ReadAccountData(params.SystemAddress)
	require.NoError(t, err)
	require.NotNil(t, account, "a genuine system-address read must use its witness proof")
}

func TestPBinWitnessStatelessCreateOverStorageNeedsProof(t *testing.T) {
	f := newPBinStatelessFixture(t)
	rootOnly := &ExecutionWitnessResult{Keys: []hexutil.Bytes{f.result.Keys[0]}, State: []hexutil.Bytes{f.result.State[0]}}
	stateless, err := newPBinWitnessStateless(rootOnly, f.root)
	require.NoError(t, err)
	require.ErrorContains(t, stateless.CreateContract(accounts.InternAddress(f.address)), "missing node", "CREATE over storage must not treat missing storage proof as empty")
}

func TestPBinWitnessStatelessHasStorage(t *testing.T) {
	f := newPBinStatelessFixture(t)
	stateless := f.stateless(t)
	value, present, err := stateless.ReadAccountStorage(accounts.InternAddress(f.address), accounts.InternKey(f.slot))
	require.NoError(t, err)
	require.True(t, present)
	require.Equal(t, uint64(9), value.Uint64())
}

func TestPBinWitnessStatelessCreateOverStorageWipesStorage(t *testing.T) {
	f := newPBinStatelessFixture(t)
	stateless := f.stateless(t)
	address := accounts.InternAddress(f.address)
	require.NoError(t, stateless.CreateContract(address))
	require.NoError(t, stateless.UpdateAccountData(address, nil, &accounts.Account{Balance: *uint256.NewInt(1), CodeHash: accounts.EmptyCodeHash}))
	got, err := stateless.Finalize(context.Background())
	require.NoError(t, err)

	post := newPBinWitnessInputContext()
	post.records = clonePBinWitnessInputRecords(f.context.records)
	cache := new(eip8297.DigestCache)
	headerDrop := pbtengine.Drop(cache.AccountHeaderStem(f.address[:]))
	storageDrop := pbtengine.Drop(cache.AccountStoragePrefix(f.address[:]))
	basic, err := eip8297.EncodeBasicData(0, uint256.NewInt(1), 0)
	require.NoError(t, err)
	ops := []pbtengine.Op{headerDrop, {Key: eip8297.TreeKeyAccount(f.address[:], eip8297.BasicDataLeafKey), Value: basic}, {Key: eip8297.TreeKeyAccount(f.address[:], eip8297.CodeHashLeafKey), Value: eip8297.CodeHashValue(common.Hash{})}, storageDrop}
	want, err := pbtengine.NewTrie(post).Process(ops)
	require.NoError(t, err)
	require.Equal(t, want, got, "CREATE over an existing account must wipe its storage")
}

func TestPBinWitnessStatelessDeleteAndRecreate(t *testing.T) {
	f := newPBinStatelessFixture(t)
	stateless := f.stateless(t)
	address := accounts.InternAddress(f.address)
	require.NoError(t, stateless.DeleteAccount(address, nil))
	require.NoError(t, stateless.UpdateAccountData(address, nil, &accounts.Account{Nonce: 2, CodeHash: accounts.EmptyCodeHash}))
	got, err := stateless.Finalize(context.Background())
	require.NoError(t, err)

	post := newPBinWitnessInputContext()
	post.records = clonePBinWitnessInputRecords(f.context.records)
	cache := new(eip8297.DigestCache)
	ops := []pbtengine.Op{pbtengine.Drop(cache.AccountHeaderStem(f.address[:])), {Key: eip8297.TreeKeyAccount(f.address[:], eip8297.BasicDataLeafKey), Value: pbinStatelessAccountBasic(2)}, {Key: eip8297.TreeKeyAccount(f.address[:], eip8297.CodeHashLeafKey), Value: eip8297.CodeHashValue(common.Hash{})}, pbtengine.Drop(cache.AccountStoragePrefix(f.address[:]))}
	want, err := pbtengine.NewTrie(post).Process(ops)
	require.NoError(t, err)
	require.Equal(t, want, got, "delete and recreate must use the recreated account")
}

func pbinStatelessAccountBasic(nonce uint64) [eip8297.ValueLength]byte {
	value, err := eip8297.EncodeBasicData(nonce, uint256.NewInt(0), 0)
	if err != nil {
		panic(err)
	}
	return value
}

func pbinStatelessNonRootIndex(result *ExecutionWitnessResult) int {
	for i, path := range result.Keys {
		if len(path) != 0 {
			return i
		}
	}
	panic("witness has no non-root node")
}

func cloneExecutionWitnessResult(result *ExecutionWitnessResult) *ExecutionWitnessResult {
	clone := &ExecutionWitnessResult{Keys: make([]hexutil.Bytes, len(result.Keys)), State: make([]hexutil.Bytes, len(result.State)), Codes: make([]hexutil.Bytes, len(result.Codes))}
	for i := range result.Keys {
		clone.Keys[i] = append(hexutil.Bytes(nil), result.Keys[i]...)
		clone.State[i] = append(hexutil.Bytes(nil), result.State[i]...)
	}
	for i := range result.Codes {
		clone.Codes[i] = append(hexutil.Bytes(nil), result.Codes[i]...)
	}
	return clone
}
