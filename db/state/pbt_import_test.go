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

package state_test

import (
	"bytes"
	"sort"
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/eip8297/artifact"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/internal/commitmenttest/temporal"
)

func TestImportPBTSnapshotWritesProgressStateAndRoot(t *testing.T) {
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() { require.NoError(t, commitment.SetPBinHashSuite(previousSuite)) })
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))
	db, _ := temporal.Open(t, 8)
	address := common.Address{1}
	slot := [32]byte{1}
	basic, err := eip8297.EncodeBasicData(1, uint256.NewInt(2), 0)
	require.NoError(t, err)
	storageValue := eip8297.EncodeStorageValue([]byte{3})
	codeHashValue := eip8297.CodeHashValue(empty.CodeHash)
	leaves := []eip8297.Entry{
		{Key: eip8297.TreeKeyAccount(address[:], eip8297.BasicDataLeafKey), Value: basic[:]},
		{Key: eip8297.TreeKeyAccount(address[:], eip8297.CodeHashLeafKey), Value: codeHashValue[:]},
		{Key: eip8297.TreeKeyStorage(address[:], slot[:]), Value: storageValue[:]},
	}
	root := eip8297.StateRootWithHash(leaves, eip8297.HashBytes)
	var snapshot bytes.Buffer
	_, err = artifact.WriteSnapshot(&snapshot, root, func(emit func([]byte, []byte) error) error {
		for _, leaf := range leaves {
			if emitErr := emit(leaf.Key, leaf.Value); emitErr != nil {
				return emitErr
			}
		}
		return nil
	})
	require.NoError(t, err)
	var preimages bytes.Buffer
	require.NoError(t, artifact.WritePreimages(&preimages, []artifact.Preimage{{Address: address, Slots: [][32]byte{slot}}}))
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	genesis := common.Hash{9}
	require.NoError(t, rawdb.WriteCanonicalHash(tx, genesis, 0))
	require.NoError(t, rawdb.WriteChainConfig(tx, genesis, &chain.Config{BinaryTrieTime: new(uint64)}))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 1, 1))
	header := &types.Header{Number: *uint256.NewInt(1), Root: root, Time: 0}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	blockHash := header.Hash()
	require.NoError(t, rawdb.WriteCanonicalHash(tx, blockHash, 1))
	var wrongSnapshot bytes.Buffer
	_, err = artifact.WriteSnapshot(&wrongSnapshot, common.Hash{8}, func(emit func([]byte, []byte) error) error {
		for _, leaf := range leaves {
			if emitErr := emit(leaf.Key, leaf.Value); emitErr != nil {
				return emitErr
			}
		}
		return nil
	})
	require.NoError(t, err)
	got, err := dbstate.ImportPBTSnapshot(t.Context(), tx, dbstate.PBTImportOptions{
		Snapshot: bytes.NewReader(snapshot.Bytes()), SnapshotSize: int64(snapshot.Len()),
		Preimages: bytes.NewReader(preimages.Bytes()), PreimageSize: int64(preimages.Len()),
		BlockHash: blockHash, BlockNum: 1, TxNum: 1, Hash: eip8297.HashBytes, Logger: log.New(),
	})
	require.NoError(t, err)
	require.Equal(t, root, got)
	_, err = dbstate.ImportPBTSnapshot(t.Context(), tx, dbstate.PBTImportOptions{
		Snapshot: bytes.NewReader(wrongSnapshot.Bytes()), SnapshotSize: int64(wrongSnapshot.Len()),
		Preimages: bytes.NewReader(preimages.Bytes()), PreimageSize: int64(preimages.Len()),
		BlockHash: blockHash, BlockNum: 1, TxNum: 1, Hash: eip8297.HashBytes, Logger: log.New(),
	})
	require.ErrorContains(t, err, "differs from artifact root", "a changed artifact root must be rejected")
}

func TestImportPBTSnapshotAcceptsAllAccountKindsAndSharedCode(t *testing.T) {
	previousSuite := commitment.PBinHashSuiteName()
	t.Cleanup(func() { require.NoError(t, commitment.SetPBinHashSuite(previousSuite)) })
	require.NoError(t, commitment.SetPBinHashSuite(commitment.PBinHashKeccak))
	sharedCode := bytes.Repeat([]byte{1}, eip8297.ChunkDataLen+1)
	zeroCode := make([]byte, eip8297.ChunkDataLen)
	delegation := append(append([]byte(nil), eip8297.DelegationMarker[:]...), bytes.Repeat([]byte{7}, 20)...)
	address1 := common.Address{1}
	address2 := common.Address{2}
	address3 := common.Address{3}
	address4 := common.Address{4}
	address5 := common.Address{5}
	states := []eip8297.State{
		{Address: address1[:], Nonce: 1, Balance: *uint256.NewInt(2), Slots: map[string][]byte{string([]byte{1}): {3}, string([]byte{0x80}): {4}}},
		{Address: address2[:], Code: sharedCode},
		{Address: address3[:], Code: sharedCode},
		{Address: address4[:], Code: zeroCode},
		{Address: address5[:], Code: delegation},
	}
	entries := eip8297.EmbedState([][]eip8297.State{states})
	sort.Slice(entries, func(i, j int) bool { return bytes.Compare(entries[i].Key, entries[j].Key) < 0 })
	root := eip8297.StateRootWithHash(entries, eip8297.HashBytes)
	var snapshot bytes.Buffer
	_, err := artifact.WriteSnapshot(&snapshot, root, func(emit func([]byte, []byte) error) error {
		for _, entry := range entries {
			if err := emit(entry.Key, entry.Value); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)
	records := make([]artifact.Preimage, 0, len(states))
	for _, item := range states {
		record := artifact.Preimage{Address: common.BytesToAddress(item.Address)}
		for slot := range item.Slots {
			slotBytes := eip8297.RightAlign32([]byte(slot))
			record.Slots = append(record.Slots, slotBytes)
		}
		sort.Slice(record.Slots, func(i, j int) bool {
			a := keccak.Sum256(record.Slots[i][:])
			b := keccak.Sum256(record.Slots[j][:])
			return bytes.Compare(a[:], b[:]) < 0
		})
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool {
		a := keccak.Sum256(records[i].Address[:])
		b := keccak.Sum256(records[j].Address[:])
		return bytes.Compare(a[:], b[:]) < 0
	})
	var preimages bytes.Buffer
	require.NoError(t, artifact.WritePreimages(&preimages, records))
	db, _ := temporal.Open(t, 8)
	tx, err := db.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	genesis := common.Hash{9}
	require.NoError(t, rawdb.WriteCanonicalHash(tx, genesis, 0))
	require.NoError(t, rawdb.WriteChainConfig(tx, genesis, &chain.Config{BinaryTrieTime: new(uint64)}))
	require.NoError(t, rawdbv3.TxNums.Append(tx, 1, 1))
	header := &types.Header{Number: *uint256.NewInt(1), Root: root, Time: 0}
	require.NoError(t, rawdb.WriteHeader(tx, header))
	blockHash := header.Hash()
	require.NoError(t, rawdb.WriteCanonicalHash(tx, blockHash, 1))
	got, err := dbstate.ImportPBTSnapshot(t.Context(), tx, dbstate.PBTImportOptions{
		Snapshot: bytes.NewReader(snapshot.Bytes()), SnapshotSize: int64(snapshot.Len()),
		Preimages: bytes.NewReader(preimages.Bytes()), PreimageSize: int64(preimages.Len()),
		BlockHash: blockHash, BlockNum: 1, TxNum: 1, Hash: eip8297.HashBytes, Logger: log.New(),
	})
	require.NoError(t, err)
	require.Equal(t, root, got)
}
