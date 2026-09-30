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
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
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

func pbinStatelessWitnessForReads(t *testing.T, f *pbinStatelessFixture, reads ...[]byte) *ExecutionWitnessResult {
	t.Helper()
	paths, blobs, _, err := pbtengine.NewTrie(f.context).Witness(context.Background(), f.root, eipWitness.PBinDriverInput{Reads: reads})
	require.NoError(t, err)
	result := &ExecutionWitnessResult{Keys: make([]hexutil.Bytes, len(paths)), State: make([]hexutil.Bytes, len(blobs))}
	for index := range paths {
		result.Keys[index] = paths[index]
		result.State[index] = blobs[index]
	}
	return result
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
	result := pbinStatelessWitnessForReads(t, f,
		eip8297.TreeKeyAccount(f.address[:], eip8297.BasicDataLeafKey),
		eip8297.TreeKeyAccount(f.address[:], eip8297.CodeHashLeafKey),
	)
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
	require.NoError(t, verifyPBinWitnessAgainstBlock(context.Background(), result, block, f.root, postRoot, chainConfig, engine))
	require.ErrorContains(t, verifyPBinWitnessAgainstBlock(context.Background(), result, block, f.root, common.HexToHash("0x01"), chainConfig, engine), "state root mismatch")
}

func TestPBinWitnessStatelessRejectsUnconsumedNode(t *testing.T) {
	f := newPBinStatelessFixture(t)
	result := pbinStatelessWitnessForReads(t, f,
		eip8297.TreeKeyAccount(f.address[:], eip8297.BasicDataLeafKey),
		eip8297.TreeKeyAccount(f.address[:], eip8297.CodeHashLeafKey),
	)
	result.Keys = append(result.Keys, hexutil.Bytes{0xff})
	result.State = append(result.State, hexutil.Bytes{0x01})
	post := newPBinWitnessInputContext()
	post.records = clonePBinWitnessInputRecords(f.context.records)
	balance := uint256.NewInt(7 + 3*common.GWei)
	basic, err := eip8297.EncodeBasicData(1, balance, 0)
	require.NoError(t, err)
	postRoot, err := pbtengine.NewTrie(post).Process([]pbtengine.Op{{Key: eip8297.TreeKeyAccount(f.address[:], eip8297.BasicDataLeafKey), Value: basic}})
	require.NoError(t, err)
	block := types.NewBlock(&types.Header{Root: postRoot, Number: *uint256.NewInt(1), Difficulty: uint256.Int{}, GasLimit: 30_000_000, Time: 1, BaseFee: uint256.NewInt(7)}, nil, nil, nil, []*types.Withdrawal{{Index: 0, Validator: 0, Address: f.address, Amount: 3}}, nil)
	engine := merge.New(ethash.NewFaker())
	require.ErrorContains(t, verifyPBinWitnessAgainstBlock(context.Background(), result, block, f.root, postRoot, pbinStatelessChainConfig(), engine), "unconsumed node at path ff")
}

func TestPBinWitnessStatelessWithdrawalUsesBasicCodeSize(t *testing.T) {
	address := common.Address{0x63}
	code := []byte{0x60, 0x00, 0x35, 0x60, 0x00}
	basic, err := eip8297.EncodeBasicData(1, uint256.NewInt(7), uint64(len(code)))
	require.NoError(t, err)
	codeHash := eip8297.CodeHashValue(crypto.Keccak256Hash(code))
	entries := []eip8297.Entry{
		{Key: eip8297.TreeKeyAccount(address[:], eip8297.BasicDataLeafKey), Value: basic[:]},
		{Key: eip8297.TreeKeyAccount(address[:], eip8297.CodeHashLeafKey), Value: codeHash[:]},
	}
	ctx := newPBinWitnessInputContext()
	root, err := pbtengine.NewTrie(ctx).Process(pbinStatelessEntriesToOps(entries))
	require.NoError(t, err)
	paths, blobs, _, err := pbtengine.NewTrie(ctx).Witness(context.Background(), root, eipWitness.PBinDriverInput{Reads: [][]byte{entries[0].Key, entries[1].Key}})
	require.NoError(t, err)
	result := &ExecutionWitnessResult{Keys: make([]hexutil.Bytes, len(paths)), State: make([]hexutil.Bytes, len(blobs))}
	for index := range paths {
		result.Keys[index] = paths[index]
		result.State[index] = blobs[index]
	}
	postBasic, err := eip8297.EncodeBasicData(1, uint256.NewInt(7+3*common.GWei), uint64(len(code)))
	require.NoError(t, err)
	postContext := newPBinWitnessInputContext()
	postContext.records = clonePBinWitnessInputRecords(ctx.records)
	postRoot, err := pbtengine.NewTrie(postContext).Process([]pbtengine.Op{{Key: entries[0].Key, Value: postBasic}})
	require.NoError(t, err)
	block := types.NewBlock(&types.Header{Root: postRoot, Number: *uint256.NewInt(1), Difficulty: uint256.Int{}, GasLimit: 30_000_000, Time: 1, BaseFee: uint256.NewInt(7)}, nil, nil, nil, []*types.Withdrawal{{Index: 0, Validator: 0, Address: address, Amount: 3}}, nil)
	engine := merge.New(ethash.NewFaker())
	require.NoError(t, verifyPBinWitnessAgainstBlock(context.Background(), result, block, root, postRoot, pbinStatelessChainConfig(), engine))
}

func TestPBinWitnessStatelessDelegationUsesDesignatorCodeHash(t *testing.T) {
	address := common.Address{0x64}
	target := common.Address{0x65}
	delegation := types.AddressToDelegation(accounts.InternAddress(target))
	basic, err := eip8297.EncodeBasicData(1, uint256.NewInt(7), 0)
	require.NoError(t, err)
	delegationValue := eip8297.EncodeDelegation(delegation)
	entries := []eip8297.Entry{
		{Key: eip8297.TreeKeyAccount(address[:], eip8297.BasicDataLeafKey), Value: basic[:]},
		{Key: eip8297.TreeKeyAccount(address[:], eip8297.DelegationLeafKey), Value: delegationValue[:]},
	}
	ctx := newPBinWitnessInputContext()
	root, err := pbtengine.NewTrie(ctx).Process(pbinStatelessEntriesToOps(entries))
	require.NoError(t, err)
	paths, blobs, _, err := pbtengine.NewTrie(ctx).Witness(context.Background(), root, eipWitness.PBinDriverInput{Reads: [][]byte{entries[0].Key, entries[1].Key}})
	require.NoError(t, err)
	result := &ExecutionWitnessResult{Keys: make([]hexutil.Bytes, len(paths)), State: make([]hexutil.Bytes, len(blobs))}
	for index := range paths {
		result.Keys[index] = paths[index]
		result.State[index] = blobs[index]
	}
	stateless, err := newPBinWitnessStateless(result, root)
	require.NoError(t, err)
	account, err := stateless.ReadAccountData(accounts.InternAddress(address))
	require.NoError(t, err)
	require.Equal(t, accounts.InternCodeHash(crypto.Keccak256Hash(delegation)), account.CodeHash)
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
	stateless.setPBinSystemCallScope(true)
	account, err := stateless.ReadAccountData(params.SystemAddress)
	require.NoError(t, err)
	require.Nil(t, account)
	_, err = stateless.ReadAccountData(accounts.InternAddress(f.address))
	require.ErrorContains(t, err, "missing node")
}

func TestPBinWitnessStatelessGenuineSystemAccessWithoutProofErrors(t *testing.T) {
	f := newPBinStatelessFixture(t)
	rootOnly := &ExecutionWitnessResult{Keys: []hexutil.Bytes{f.result.Keys[0]}, State: []hexutil.Bytes{f.result.State[0]}}
	stateless, err := newPBinWitnessStateless(rootOnly, f.root)
	require.NoError(t, err)
	_, err = stateless.ReadAccountData(params.SystemAddress)
	require.ErrorIs(t, err, commitment.ErrPBinWitnessBlinded)
}

func TestPBinWitnessStatelessTamperedSystemBlobErrors(t *testing.T) {
	f := newPBinStatelessFixture(t)
	system := common.Address(params.SystemAddress.Value())
	result, root := pbinSystemAddressWitness(t, f, system)
	tampered := cloneExecutionWitnessResult(result)
	found := false
	for index, path := range tampered.Keys {
		if len(path) == 0 {
			continue
		}
		candidate := cloneExecutionWitnessResult(result)
		candidate.State[index] = append(hexutil.Bytes(nil), candidate.State[index]...)
		candidate.State[index][len(candidate.State[index])-1] ^= 1
		stateless, err := newPBinWitnessStateless(candidate, root)
		require.NoError(t, err)
		stateless.setPBinSystemCallScope(true)
		_, err = stateless.ReadAccountData(params.SystemAddress)
		if err != nil {
			require.ErrorContains(t, err, "hashes to")
			found = true
			break
		}
	}
	require.True(t, found, "a non-root system-address blob must be checked in system-call scope")
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
	reads := [][]byte{
		eip8297.TreeKeyAccount(f.address[:], eip8297.BasicDataLeafKey),
		eip8297.TreeKeyAccount(f.address[:], eip8297.CodeHashLeafKey),
	}
	paths, blobs, _, err := pbtengine.NewTrie(f.context).Witness(context.Background(), f.root, eipWitness.PBinDriverInput{Reads: reads})
	require.NoError(t, err)
	partial := &ExecutionWitnessResult{Keys: make([]hexutil.Bytes, len(paths)), State: make([]hexutil.Bytes, len(blobs))}
	for index := range paths {
		partial.Keys[index] = paths[index]
		partial.State[index] = blobs[index]
	}
	stateless, err := newPBinWitnessStateless(partial, f.root)
	require.NoError(t, err)
	account, err := stateless.ReadAccountData(accounts.InternAddress(f.address))
	require.NoError(t, err)
	require.NotNil(t, account)
	require.ErrorContains(t, stateless.CreateContract(accounts.InternAddress(f.address)), "missing node", "CREATE over storage must not treat missing storage proof as empty")
}

func TestPBinWitnessStatelessFreshLifecycleSkipsStorageDelete(t *testing.T) {
	f := newPBinStatelessFixture(t)
	fresh := accounts.InternAddress(common.HexToAddress("0x6300000000000000000000000000000000000000"))
	freshAddress := fresh.Value()
	basic := pbinStatelessAccountBasic(0)
	codeHash := eip8297.CodeHashValue(common.Hash{})
	paths, blobs, _, err := pbtengine.NewTrie(f.context).Witness(context.Background(), f.root, eipWitness.PBinDriverInput{Accounts: []eipWitness.PBinAccountUpdate{{Address: freshAddress[:], Values: map[byte][]byte{eip8297.BasicDataLeafKey: basic[:], eip8297.CodeHashLeafKey: codeHash[:]}}}})
	require.NoError(t, err)
	witness := &ExecutionWitnessResult{Keys: make([]hexutil.Bytes, len(paths)), State: make([]hexutil.Bytes, len(blobs))}
	for index := range paths {
		witness.Keys[index] = paths[index]
		witness.State[index] = blobs[index]
	}
	stateless, err := newPBinWitnessStateless(witness, f.root)
	require.NoError(t, err)
	require.NoError(t, stateless.CreateContract(fresh))
	require.NoError(t, stateless.DeleteAccount(fresh, nil))
}

func TestPBinWitnessStatelessEmptyAccessNeedsRoot(t *testing.T) {
	f := newPBinStatelessFixture(t)
	paths, blobs, _, err := pbtengine.NewTrie(f.context).Witness(context.Background(), f.root, eipWitness.PBinDriverInput{})
	require.NoError(t, err)
	result := &ExecutionWitnessResult{Keys: make([]hexutil.Bytes, len(paths)), State: make([]hexutil.Bytes, len(blobs))}
	for index := range paths {
		result.Keys[index] = paths[index]
		result.State[index] = blobs[index]
	}
	header := &types.Header{Root: f.root, Number: *uint256.NewInt(1), Difficulty: uint256.Int{}, GasLimit: 30_000_000, BaseFee: uint256.NewInt(7)}
	block := types.NewBlock(header, nil, nil, nil, nil, nil)
	engine := merge.New(ethash.NewFaker())
	chainConfig := pbinStatelessChainConfig()
	require.NoError(t, verifyPBinWitnessAgainstBlock(context.Background(), result, block, f.root, f.root, chainConfig, engine))
	trimmed := &ExecutionWitnessResult{Keys: nil, State: []hexutil.Bytes{}}
	require.ErrorContains(t, verifyPBinWitnessAgainstBlock(context.Background(), trimmed, block, f.root, f.root, chainConfig, engine), "empty State field")
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
	_, err := stateless.ReadAccountData(address)
	require.NoError(t, err)
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

func pbinSystemAddressWitness(t *testing.T, f *pbinStatelessFixture, system common.Address) (*ExecutionWitnessResult, common.Hash) {
	t.Helper()
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
	return result, root
}
