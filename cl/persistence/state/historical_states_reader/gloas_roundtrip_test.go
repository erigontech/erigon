// Copyright 2024 The Erigon Authors
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

package historical_states_reader

import (
	"bytes"
	"context"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// compressSSZ encodes an SSZ-encodable object and zstd-compresses the result.
// This matches the storage format used by the beacon state antiquary for
// per-slot GLOAS fields.
func compressSSZ(t *testing.T, obj interface {
	EncodeSSZ(buf []byte) ([]byte, error)
}) []byte {
	t.Helper()
	sszData, err := obj.EncodeSSZ(nil)
	require.NoError(t, err)
	return compressRawSSZ(t, sszData)
}

// compressRawSSZ zstd-compresses raw bytes (already SSZ-encoded).
func compressRawSSZ(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	enc, err := zstd.NewWriter(&buf)
	require.NoError(t, err)
	_, err = enc.Write(raw)
	require.NoError(t, err)
	require.NoError(t, enc.Close())
	return buf.Bytes()
}

// compressZeros returns a valid zstd-compressed stream of n zero bytes.
func compressZeros(t *testing.T, n int) []byte {
	t.Helper()
	return compressRawSSZ(t, make([]byte, n))
}

// mockGetValFn builds a GetValFn backed by an in-memory map[table][key] -> value.
func mockGetValFn(data map[string]map[string][]byte) state_accessors.GetValFn {
	return func(table string, key []byte) ([]byte, error) {
		if t, ok := data[table]; ok {
			if v, ok2 := t[string(key)]; ok2 {
				return v, nil
			}
		}
		return nil, nil
	}
}

// ---------------------------------------------------------------------------
// Unit tests for readCompressedSSZ / ReadQueueSSZ helpers
// ---------------------------------------------------------------------------

// TestReadCompressedSSZ_ExecutionPayloadBid round-trips an ExecutionPayloadBid
// through compress -> store -> readCompressedSSZ, including BlobKzgCommitments.
func TestReadCompressedSSZ_ExecutionPayloadBid(t *testing.T) {
	// Build a bid with non-empty BlobKzgCommitments to exercise the variable-length field.
	commitments := solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48)
	c1 := &cltypes.KZGCommitment{}
	copy(c1[:], bytes.Repeat([]byte{0xAA}, 48))
	c2 := &cltypes.KZGCommitment{}
	copy(c2[:], bytes.Repeat([]byte{0xBB}, 48))
	commitments.Append(c1)
	commitments.Append(c2)

	bid := &cltypes.ExecutionPayloadBid{
		ParentBlockHash:       common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111"),
		ParentBlockRoot:       common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222"),
		BlockHash:             common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333"),
		PrevRandao:            common.HexToHash("0x4444444444444444444444444444444444444444444444444444444444444444"),
		FeeRecipient:          common.HexToAddress("0x6666666666666666666666666666666666666666"),
		GasLimit:              30_000_000,
		BuilderIndex:          7,
		Slot:                  1000,
		Value:                 5_000_000_000,
		ExecutionPayment:      1_000_000_000,
		BlobKzgCommitments:    *commitments,
		ExecutionRequestsRoot: common.HexToHash("0x5555555555555555555555555555555555555555555555555555555555555555"),
	}

	slot := uint64(1000)
	key := string(base_encoding.Encode64ToBytes4(slot))
	compressed := compressSSZ(t, bid)

	getter := mockGetValFn(map[string]map[string][]byte{
		kv.LatestExecutionPayloadBidTable: {key: compressed},
	})

	got := new(cltypes.ExecutionPayloadBid)
	require.NoError(t, readCompressedSSZ(getter, slot, kv.LatestExecutionPayloadBidTable, got, int(clparams.GloasVersion)))

	require.Equal(t, bid.ParentBlockHash, got.ParentBlockHash)
	require.Equal(t, bid.ParentBlockRoot, got.ParentBlockRoot)
	require.Equal(t, bid.BlockHash, got.BlockHash)
	require.Equal(t, bid.PrevRandao, got.PrevRandao)
	require.Equal(t, bid.FeeRecipient, got.FeeRecipient)
	require.Equal(t, bid.GasLimit, got.GasLimit)
	require.Equal(t, bid.BuilderIndex, got.BuilderIndex)
	require.Equal(t, bid.Slot, got.Slot)
	require.Equal(t, bid.Value, got.Value)
	require.Equal(t, bid.ExecutionPayment, got.ExecutionPayment)
	require.Equal(t, bid.ExecutionRequestsRoot, got.ExecutionRequestsRoot)
	// BlobKzgCommitments: the only variable-length field — verify count and content.
	require.Equal(t, 2, got.BlobKzgCommitments.Len())
	gotC0 := got.BlobKzgCommitments.Get(0)
	gotC1 := got.BlobKzgCommitments.Get(1)
	require.Equal(t, c1[:], gotC0[:])
	require.Equal(t, c2[:], gotC1[:])
}

// TestReadCompressedSSZ_MissingData verifies that readCompressedSSZ returns
// ErrMissingGloasData when the table contains no entry for the requested slot,
// rather than silently producing a zero-valued object.
func TestReadCompressedSSZ_MissingData(t *testing.T) {
	slot := uint64(500)
	getter := mockGetValFn(map[string]map[string][]byte{})

	bid := new(cltypes.ExecutionPayloadBid)
	err := readCompressedSSZ(getter, slot, kv.LatestExecutionPayloadBidTable, bid, int(clparams.GloasVersion))
	require.ErrorIs(t, err, ErrMissingGloasData)
	require.ErrorContains(t, err, kv.LatestExecutionPayloadBidTable)
	require.ErrorContains(t, err, "500")
}

// TestReadRequiredQueueSSZ_MissingDump verifies that ReadRequiredQueueSSZ
// returns ErrMissingGloasData when the dump table has no entry for the
// target dump slot, rather than silently returning an empty list.
func TestReadRequiredQueueSSZ_MissingDump(t *testing.T) {
	// Use a slot that is NOT dump-aligned so the function computes a dump slot
	// and looks it up.
	slot := uint64(clparams.SlotsPerDump + 10) // dump slot = SlotsPerDump, query slot = SlotsPerDump+10
	getter := mockGetValFn(map[string]map[string][]byte{})

	builders := solid.NewStaticListSSZ[*cltypes.Builder](1024, new(cltypes.Builder).EncodingSizeSSZ())
	err := ReadRequiredQueueSSZ(getter, slot, kv.BuildersDump, kv.Builders, builders)
	require.ErrorIs(t, err, ErrMissingGloasData)
	require.ErrorContains(t, err, kv.BuildersDump)

	// Same for BuilderPendingWithdrawals
	bpw := solid.NewStaticListSSZ[*cltypes.BuilderPendingWithdrawal](1024, new(cltypes.BuilderPendingWithdrawal).EncodingSizeSSZ())
	err = ReadRequiredQueueSSZ(getter, slot, kv.BuilderPendingWithdrawalsDump, kv.BuilderPendingWithdrawals, bpw)
	require.ErrorIs(t, err, ErrMissingGloasData)
	require.ErrorContains(t, err, kv.BuilderPendingWithdrawalsDump)

	// Same for PayloadExpectedWithdrawals
	pew := solid.NewStaticListSSZ[*cltypes.Withdrawal](16, new(cltypes.Withdrawal).EncodingSizeSSZ())
	err = ReadRequiredQueueSSZ(getter, slot, kv.PayloadExpectedWithdrawalsDump, kv.PayloadExpectedWithdrawals, pew)
	require.ErrorIs(t, err, ErrMissingGloasData)
	require.ErrorContains(t, err, kv.PayloadExpectedWithdrawalsDump)
}

// TestReadRequiredQueueSSZ_WithData verifies that ReadRequiredQueueSSZ
// succeeds and returns correct data when the dump table is populated.
func TestReadRequiredQueueSSZ_WithData(t *testing.T) {
	builders := solid.NewStaticListSSZ[*cltypes.Builder](1024, new(cltypes.Builder).EncodingSizeSSZ())
	builders.Append(&cltypes.Builder{
		Pubkey: common.Bytes48{0x01}, Balance: 100,
	})

	sszData, err := builders.EncodeSSZ(nil)
	require.NoError(t, err)
	compressed := compressRawSSZ(t, sszData)

	slot := uint64(clparams.SlotsPerDump) // dump-aligned
	dumpKey := string(base_encoding.Encode64ToBytes4(slot))

	getter := mockGetValFn(map[string]map[string][]byte{
		kv.BuildersDump: {dumpKey: compressed},
	})

	got := solid.NewStaticListSSZ[*cltypes.Builder](1024, new(cltypes.Builder).EncodingSizeSSZ())
	require.NoError(t, ReadRequiredQueueSSZ(getter, slot, kv.BuildersDump, kv.Builders, got))
	require.Equal(t, 1, got.Len())
	require.Equal(t, common.Bytes48{0x01}, got.Get(0).Pubkey)
}

// TestReadCompressedSSZ_BuilderPendingPayments round-trips a BuilderPendingPayments
// VectorSSZ through compress -> readCompressedSSZ.
func TestReadCompressedSSZ_BuilderPendingPayments(t *testing.T) {
	slotsPerEpoch := uint64(32)
	payments := solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](int(2 * slotsPerEpoch))
	// Populate a couple of entries.
	payments.Set(0, &cltypes.BuilderPendingPayment{
		Weight: 100,
		Withdrawal: &cltypes.BuilderPendingWithdrawal{
			FeeRecipient: common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
			Amount:       500,
			BuilderIndex: 3,
		},
	})
	payments.Set(1, &cltypes.BuilderPendingPayment{
		Weight: 200,
		Withdrawal: &cltypes.BuilderPendingWithdrawal{
			FeeRecipient: common.HexToAddress("0xcccccccccccccccccccccccccccccccccccccccc"),
			Amount:       1000,
			BuilderIndex: 5,
		},
	})

	sszData, err := payments.EncodeSSZ(nil)
	require.NoError(t, err)
	compressed := compressRawSSZ(t, sszData)

	slot := uint64(2000)
	key := string(base_encoding.Encode64ToBytes4(slot))

	getter := mockGetValFn(map[string]map[string][]byte{
		kv.BuilderPendingPaymentsTable: {key: compressed},
	})

	got := solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](int(2 * slotsPerEpoch))
	require.NoError(t, readCompressedSSZ(getter, slot, kv.BuilderPendingPaymentsTable, got, int(clparams.GloasVersion)))

	require.Equal(t, payments.Length(), got.Length())
	p0 := got.Get(0)
	require.Equal(t, uint64(100), p0.Weight)
	require.Equal(t, uint64(500), p0.Withdrawal.Amount)
	require.Equal(t, uint64(3), p0.Withdrawal.BuilderIndex)

	p1 := got.Get(1)
	require.Equal(t, uint64(200), p1.Weight)
	require.Equal(t, uint64(1000), p1.Withdrawal.Amount)
	require.Equal(t, uint64(5), p1.Withdrawal.BuilderIndex)
}

// TestReadQueueSSZ_BuildersDump tests ReadQueueSSZ for the Builders list by
// writing a dump at a SlotsPerDump-aligned slot and reading it back.
func TestReadQueueSSZ_BuildersDump(t *testing.T) {
	// Create a list with two builders
	builders := solid.NewStaticListSSZ[*cltypes.Builder](1024, new(cltypes.Builder).EncodingSizeSSZ())
	builders.Append(&cltypes.Builder{
		Pubkey:            common.Bytes48{0x01, 0x02, 0x03},
		Version:           1,
		ExecutionAddress:  common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		Balance:           1_000_000,
		DepositEpoch:      10,
		WithdrawableEpoch: 100,
	})
	builders.Append(&cltypes.Builder{
		Pubkey:            common.Bytes48{0x04, 0x05, 0x06},
		Version:           2,
		ExecutionAddress:  common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		Balance:           2_000_000,
		DepositEpoch:      20,
		WithdrawableEpoch: 200,
	})

	// Serialize and compress the dump
	sszData, err := builders.EncodeSSZ(nil)
	require.NoError(t, err)
	compressed := compressRawSSZ(t, sszData)

	// Place at a SlotsPerDump-aligned slot
	slot := uint64(clparams.SlotsPerDump) // = 1536
	dumpKey := string(base_encoding.Encode64ToBytes4(slot))

	getter := mockGetValFn(map[string]map[string][]byte{
		kv.BuildersDump: {dumpKey: compressed},
	})

	got := solid.NewStaticListSSZ[*cltypes.Builder](1024, new(cltypes.Builder).EncodingSizeSSZ())
	require.NoError(t, ReadQueueSSZ(getter, slot, kv.BuildersDump, kv.Builders, got))

	require.Equal(t, 2, got.Len())

	b0 := got.Get(0)
	require.Equal(t, common.Bytes48{0x01, 0x02, 0x03}, b0.Pubkey)
	require.Equal(t, uint8(1), b0.Version)
	require.Equal(t, uint64(1_000_000), b0.Balance)
	require.Equal(t, uint64(10), b0.DepositEpoch)
	require.Equal(t, uint64(100), b0.WithdrawableEpoch)

	b1 := got.Get(1)
	require.Equal(t, common.Bytes48{0x04, 0x05, 0x06}, b1.Pubkey)
	require.Equal(t, uint8(2), b1.Version)
	require.Equal(t, uint64(2_000_000), b1.Balance)
}

// TestReadQueueSSZ_BuilderPendingWithdrawals_DumpOnly tests ReadQueueSSZ for
// BuilderPendingWithdrawals using a dump snapshot with no diffs.
func TestReadQueueSSZ_BuilderPendingWithdrawals_DumpOnly(t *testing.T) {
	list := solid.NewStaticListSSZ[*cltypes.BuilderPendingWithdrawal](
		1024, new(cltypes.BuilderPendingWithdrawal).EncodingSizeSSZ(),
	)
	list.Append(&cltypes.BuilderPendingWithdrawal{
		FeeRecipient: common.HexToAddress("0xdddddddddddddddddddddddddddddddddddddd"),
		Amount:       777,
		BuilderIndex: 42,
	})

	sszData, err := list.EncodeSSZ(nil)
	require.NoError(t, err)
	compressed := compressRawSSZ(t, sszData)

	slot := uint64(0)
	dumpKey := string(base_encoding.Encode64ToBytes4(slot))

	getter := mockGetValFn(map[string]map[string][]byte{
		kv.BuilderPendingWithdrawalsDump: {dumpKey: compressed},
	})

	got := solid.NewStaticListSSZ[*cltypes.BuilderPendingWithdrawal](
		1024, new(cltypes.BuilderPendingWithdrawal).EncodingSizeSSZ(),
	)
	require.NoError(t, ReadQueueSSZ(getter, slot, kv.BuilderPendingWithdrawalsDump, kv.BuilderPendingWithdrawals, got))

	require.Equal(t, 1, got.Len())
	w0 := got.Get(0)
	require.Equal(t, uint64(777), w0.Amount)
	require.Equal(t, uint64(42), w0.BuilderIndex)
}

// TestReadQueueSSZ_EmptyDumpNoDiffs ensures ReadQueueSSZ returns an empty list
// when neither dump nor diffs are present.
func TestReadQueueSSZ_EmptyDumpNoDiffs(t *testing.T) {
	getter := mockGetValFn(map[string]map[string][]byte{})
	slot := uint64(100)

	got := solid.NewStaticListSSZ[*cltypes.Builder](1024, new(cltypes.Builder).EncodingSizeSSZ())
	require.NoError(t, ReadQueueSSZ(getter, slot, kv.BuildersDump, kv.Builders, got))
	require.Equal(t, 0, got.Len())
}

// TestReadCompressedSSZ_ExecutionPayloadAvailability tests round-trip for the
// execution_payload_availability bit-vector (GLOAS field).
func TestReadCompressedSSZ_ExecutionPayloadAvailability(t *testing.T) {
	slotsPerHistoricalRoot := int(clparams.MainnetBeaconConfig.SlotsPerHistoricalRoot)
	bv := solid.NewBitVector(slotsPerHistoricalRoot)
	// Set a few bits
	bv.SetBitAt(0, true)
	bv.SetBitAt(42, true)
	bv.SetBitAt(slotsPerHistoricalRoot-1, true)

	sszData, err := bv.EncodeSSZ(nil)
	require.NoError(t, err)
	compressed := compressRawSSZ(t, sszData)

	slot := uint64(3000)
	key := string(base_encoding.Encode64ToBytes4(slot))

	getter := mockGetValFn(map[string]map[string][]byte{
		kv.ExecutionPayloadAvailabilityTable: {key: compressed},
	})

	got := solid.NewBitVector(slotsPerHistoricalRoot)
	require.NoError(t, readCompressedSSZ(getter, slot, kv.ExecutionPayloadAvailabilityTable, got, int(clparams.GloasVersion)))

	require.True(t, got.GetBitAt(0))
	require.True(t, got.GetBitAt(42))
	require.True(t, got.GetBitAt(slotsPerHistoricalRoot-1))
	require.False(t, got.GetBitAt(1))
	require.False(t, got.GetBitAt(100))
}

// TestReadQueueSSZ_BuildersDumpWithDiffs tests ReadQueueSSZ applying incremental
// diffs on top of a dump to verify the queue diff mechanism works for Builders.
func TestReadQueueSSZ_BuildersDumpWithDiffs(t *testing.T) {
	// Start with one builder at the dump slot
	initialBuilders := solid.NewStaticListSSZ[*cltypes.Builder](1024, new(cltypes.Builder).EncodingSizeSSZ())
	initialBuilders.Append(&cltypes.Builder{
		Pubkey:            common.Bytes48{0x01},
		Version:           1,
		ExecutionAddress:  common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		Balance:           100,
		DepositEpoch:      1,
		WithdrawableEpoch: 10,
	})

	sszDump, err := initialBuilders.EncodeSSZ(nil)
	require.NoError(t, err)
	compressedDump := compressRawSSZ(t, sszDump)

	// Create a diff that adds a second builder
	newBuilder := &cltypes.Builder{
		Pubkey:            common.Bytes48{0x02},
		Version:           2,
		ExecutionAddress:  common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
		Balance:           200,
		DepositEpoch:      2,
		WithdrawableEpoch: 20,
	}

	// Use the real SSZQueueEncoder to produce a correct diff
	encoder := base_encoding.NewSSZQueueEncoder[*cltypes.Builder](func(a, b *cltypes.Builder) bool {
		return a.Pubkey == b.Pubkey && a.Balance == b.Balance
	})
	encoder.Initialize(initialBuilders)

	updatedBuilders := solid.NewStaticListSSZ[*cltypes.Builder](1024, new(cltypes.Builder).EncodingSizeSSZ())
	updatedBuilders.Append(initialBuilders.Get(0))
	updatedBuilders.Append(newBuilder)

	var diffBuf bytes.Buffer
	require.NoError(t, encoder.WriteDiff(&diffBuf, updatedBuilders))

	// The dump is at slot 0, diff is at slot 1, we query slot 1
	dumpSlot := uint64(0)
	diffSlot := uint64(1)
	dumpKey := string(base_encoding.Encode64ToBytes4(dumpSlot))
	diffKey := string(base_encoding.Encode64ToBytes4(diffSlot))

	getter := mockGetValFn(map[string]map[string][]byte{
		kv.BuildersDump: {dumpKey: compressedDump},
		kv.Builders:     {diffKey: diffBuf.Bytes()},
	})

	got := solid.NewStaticListSSZ[*cltypes.Builder](1024, new(cltypes.Builder).EncodingSizeSSZ())
	require.NoError(t, ReadQueueSSZ(getter, diffSlot, kv.BuildersDump, kv.Builders, got))

	require.Equal(t, 2, got.Len())

	b0 := got.Get(0)
	require.Equal(t, common.Bytes48{0x01}, b0.Pubkey)
	require.Equal(t, uint64(100), b0.Balance)

	b1 := got.Get(1)
	require.Equal(t, common.Bytes48{0x02}, b1.Pubkey)
	require.Equal(t, uint64(200), b1.Balance)
	require.Equal(t, uint8(2), b1.Version)
}

// ---------------------------------------------------------------------------
// Integration test: ReadHistoricalState() end-to-end for GLOAS
// ---------------------------------------------------------------------------

// TestReadHistoricalState_GloasFieldsReconstruction populates a memdb with the
// minimal data needed for ReadHistoricalState() to succeed at a GLOAS-version
// slot, then verifies that all 9 GLOAS fields are correctly reconstructed.
//
// This is the key integration test: it calls ReadHistoricalState() end-to-end,
// proving the version gate, table names, type construction, and setter wiring
// are all correct.
func TestReadHistoricalState_GloasFieldsReconstruction(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	// Use slot at an epoch boundary to simplify balance reconstruction
	// (no per-slot diff needed when slot % SlotsPerEpoch == 0).
	slot := cfg.SlotsPerEpoch // = 32

	// ---- Genesis state (mainnet has real validators) ----
	genesisState, err := initial_state.GetGenesisState(chainspec.MainnetChainID)
	require.NoError(t, err)
	numValidators := uint64(genesisState.ValidatorLength())

	// ---- Build the expected GLOAS test data ----
	expectedLatestBlockHash := common.HexToHash("0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789")
	expectedNextWithdrawalBuilderIndex := uint64(42)

	// ExecutionPayloadBid
	expectedBid := &cltypes.ExecutionPayloadBid{
		ParentBlockHash:       common.HexToHash("0x1100000000000000000000000000000000000000000000000000000000000011"),
		BlockHash:             common.HexToHash("0x2200000000000000000000000000000000000000000000000000000000000022"),
		BuilderIndex:          7,
		Slot:                  slot,
		Value:                 5_000_000_000,
		BlobKzgCommitments:    *solid.NewStaticListSSZ[*cltypes.KZGCommitment](cltypes.MaxBlobsCommittmentsPerBlock, 48),
		ExecutionRequestsRoot: common.HexToHash("0x3300000000000000000000000000000000000000000000000000000000000033"),
	}

	// Builders list
	expectedBuilders := solid.NewStaticListSSZ[*cltypes.Builder](int(cfg.BuilderRegistryLimit), new(cltypes.Builder).EncodingSizeSSZ())
	expectedBuilders.Append(&cltypes.Builder{
		Pubkey: common.Bytes48{0xAA}, Version: 1, Balance: 999,
		ExecutionAddress: common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
	})

	// BuilderPendingWithdrawals list
	expectedBPW := solid.NewStaticListSSZ[*cltypes.BuilderPendingWithdrawal](
		int(cfg.BuilderPendingWithdrawalsLimit), new(cltypes.BuilderPendingWithdrawal).EncodingSizeSSZ())
	expectedBPW.Append(&cltypes.BuilderPendingWithdrawal{
		FeeRecipient: common.HexToAddress("0xdddddddddddddddddddddddddddddddddddddddd"),
		Amount:       777, BuilderIndex: 3,
	})

	// PayloadExpectedWithdrawals list
	expectedPEW := solid.NewStaticListSSZ[*cltypes.Withdrawal](
		int(cfg.MaxWithdrawalsPerPayload), new(cltypes.Withdrawal).EncodingSizeSSZ())

	// ExecutionPayloadAvailability bit-vector — seed two non-zero bits
	expectedEPA := solid.NewBitVector(int(cfg.SlotsPerHistoricalRoot))
	expectedEPA.SetBitAt(7, true)
	expectedEPA.SetBitAt(100, true)

	// BuilderPendingPayments vector — seed a non-zero entry at index 3
	expectedBPP := solid.NewVectorSSZ[*cltypes.BuilderPendingPayment](int(2 * cfg.SlotsPerEpoch))
	expectedBPP.Set(3, &cltypes.BuilderPendingPayment{
		Weight: 12345,
		Withdrawal: &cltypes.BuilderPendingWithdrawal{
			FeeRecipient: common.HexToAddress("0xff00ff00ff00ff00ff00ff00ff00ff00ff00ff00"),
			Amount:       9876,
			BuilderIndex: 11,
		},
	})

	// PtcWindow vector of vectors — seed a non-zero entry at outer=2, inner=5
	expectedPtcWindow := solid.NewUint64VectorOfVectors(int((2+cfg.MinSeedLookahead)*cfg.SlotsPerEpoch), int(cfg.PtcSize))
	expectedPtcWindow.Get(2).Set(5, 42)

	// ---- Build SlotData (GLOAS version, use real numValidators from genesis) ----
	slotData := &state_accessors.SlotData{
		Version:                      clparams.GloasVersion,
		ValidatorLength:              numValidators,
		Eth1Data:                     genesisState.Eth1Data(),
		Eth1DepositIndex:             genesisState.Eth1DepositIndex(),
		Fork:                         genesisState.Fork(),
		NextWithdrawalIndex:          0,
		NextWithdrawalValidatorIndex: 0,
		DepositRequestsStartIndex:    0,
		NextWithdrawalBuilderIndex:   expectedNextWithdrawalBuilderIndex,
		LatestBlockHash:              expectedLatestBlockHash,
	}

	// ---- Build EpochData (GLOAS requires >= FuluVersion, so ProposerLookahead included) ----
	epochData := &state_accessors.EpochData{
		TotalActiveBalance:          1_000_000,
		JustificationBits:           &cltypes.JustificationBits{},
		CurrentJustifiedCheckpoint:  genesisState.CurrentJustifiedCheckpoint(),
		PreviousJustifiedCheckpoint: genesisState.PreviousJustifiedCheckpoint(),
		FinalizedCheckpoint:         genesisState.FinalizedCheckpoint(),
		HistoricalSummariesLength:   0,
		HistoricalRootsLength:       0,
		ProposerLookahead:           solid.NewUint64VectorSSZ(int((1 + cfg.MinSeedLookahead) * cfg.SlotsPerEpoch)),
		BeaconConfig:                cfg,
		Version:                     clparams.GloasVersion,
	}

	// ---- Serialize SlotData / EpochData ----
	var slotDataBuf bytes.Buffer
	require.NoError(t, slotData.WriteTo(&slotDataBuf))

	var epochDataBuf bytes.Buffer
	require.NoError(t, epochData.WriteTo(&epochDataBuf))

	slotKey := base_encoding.Encode64ToBytes4(slot)
	roundedSlot := cfg.RoundSlotToEpoch(slot) // = slot for epoch boundary
	roundedKey := base_encoding.Encode64ToBytes4(roundedSlot)
	genesisKey := base_encoding.Encode64ToBytes4(0)

	// ---- Create a block at the target slot ----
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = slot
	block.Block.ProposerIndex = 0
	block.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()

	mockReader := tests.NewMockBlockReader()
	mockReader.U[slot] = block
	// Also provide blocks for participation loop (beginSlot..slot).
	// The reader returns nil for missing slots, which is handled gracefully.

	// ---- Prepare compressed data for DB tables ----
	// Balance dump at genesis (slot 0): real genesis has numValidators balances.
	genesisBalances := make([]byte, numValidators*8)
	compressedBalances := compressRawSSZ(t, genesisBalances)

	// Effective balance dump at genesis
	compressedEffBalances := compressRawSSZ(t, genesisBalances)

	// Slashings dump (EpochsPerSlashingsVector * 8 bytes)
	compressedSlashings := compressZeros(t, int(cfg.EpochsPerSlashingsVector)*8)

	// Inactivity scores dump (numValidators * 8 bytes)
	compressedInactivity := compressZeros(t, int(numValidators)*8)

	// IntraRandaoMixes: 32 zero bytes
	intraRandaoMix := make([]byte, 32)

	// GLOAS compressed fields
	compressedBid := compressSSZ(t, expectedBid)
	compressedEPA := compressSSZ(t, expectedEPA)
	compressedBPP := compressSSZ(t, expectedBPP)
	compressedPtcWindow := compressSSZ(t, expectedPtcWindow)

	// GLOAS queue dumps
	buildersSSZ, err := expectedBuilders.EncodeSSZ(nil)
	require.NoError(t, err)
	compressedBuilders := compressRawSSZ(t, buildersSSZ)

	bpwSSZ, err := expectedBPW.EncodeSSZ(nil)
	require.NoError(t, err)
	compressedBPW := compressRawSSZ(t, bpwSSZ)

	pewSSZ, err := expectedPEW.EncodeSSZ(nil)
	require.NoError(t, err)
	compressedPEW := compressRawSSZ(t, pewSSZ)

	// ---- Populate memdb ----
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(context.Background())
	require.NoError(t, err)
	defer tx.Rollback()

	// Block roots and state roots: ReadHistoricalState needs 32 entries
	// (one per slot from 0 to slot-1) for readHistoryHashVector.
	zeroHash := make([]byte, 32)
	for i := uint64(0); i < slot; i++ {
		k := base_encoding.Encode64ToBytes4(i)
		require.NoError(t, tx.Put(kv.BlockRoot, k, zeroHash))
		require.NoError(t, tx.Put(kv.StateRoot, k, zeroHash))
	}

	// State processing progress
	require.NoError(t, tx.Put(kv.StatesProcessingProgress, kv.StatesProcessingKey, slotKey))

	// SlotData
	require.NoError(t, tx.Put(kv.SlotData, slotKey, slotDataBuf.Bytes()))

	// EpochData
	require.NoError(t, tx.Put(kv.EpochData, roundedKey, epochDataBuf.Bytes()))

	// Balance dump at genesis
	require.NoError(t, tx.Put(kv.BalancesDump, genesisKey, compressedBalances))

	// Effective balance dump at genesis
	require.NoError(t, tx.Put(kv.EffectiveBalancesDump, genesisKey, compressedEffBalances))

	// IntraRandaoMixes
	require.NoError(t, tx.Put(kv.IntraRandaoMixes, slotKey, intraRandaoMix))

	// Slashings dump at genesis
	require.NoError(t, tx.Put(kv.ValidatorSlashings, genesisKey, compressedSlashings))

	// Inactivity scores dump at genesis
	require.NoError(t, tx.Put(kv.InactivityScores, genesisKey, compressedInactivity))

	// GLOAS per-slot compressed fields
	require.NoError(t, tx.Put(kv.LatestExecutionPayloadBidTable, slotKey, compressedBid))
	require.NoError(t, tx.Put(kv.ExecutionPayloadAvailabilityTable, slotKey, compressedEPA))
	require.NoError(t, tx.Put(kv.BuilderPendingPaymentsTable, slotKey, compressedBPP))
	require.NoError(t, tx.Put(kv.PtcWindowTable, slotKey, compressedPtcWindow))

	// GLOAS queue dumps at genesis (slot 0)
	require.NoError(t, tx.Put(kv.BuildersDump, genesisKey, compressedBuilders))
	require.NoError(t, tx.Put(kv.BuilderPendingWithdrawalsDump, genesisKey, compressedBPW))
	require.NoError(t, tx.Put(kv.PayloadExpectedWithdrawalsDump, genesisKey, compressedPEW))

	require.NoError(t, tx.Commit())

	// ---- Set up HistoricalStatesReader ----
	vt := state_accessors.NewStaticValidatorTable()
	vt.SetSlot(slot) // validator table must cover the target slot

	sn := synced_data.NewSyncedDataManager(cfg, true)
	require.NoError(t, sn.OnHeadState(genesisState))

	hr := NewHistoricalStatesReader(cfg, mockReader, vt, genesisState, nil, sn)

	// ---- Call ReadHistoricalState ----
	roTx, err := db.BeginRo(context.Background())
	require.NoError(t, err)
	defer roTx.Rollback()

	reconstructed, err := hr.ReadHistoricalState(context.Background(), roTx, slot)
	require.NoError(t, err)
	require.NotNil(t, reconstructed, "ReadHistoricalState must not return nil for a GLOAS slot")

	// ---- Verify version ----
	require.Equal(t, clparams.GloasVersion, reconstructed.Version())

	// ---- Verify all 9 GLOAS fields ----

	// 1. NextWithdrawalBuilderIndex (from SlotData)
	require.Equal(t, expectedNextWithdrawalBuilderIndex, reconstructed.GetNextWithdrawalBuilderIndex(),
		"NextWithdrawalBuilderIndex mismatch")

	// 2. LatestBlockHash (from SlotData)
	require.Equal(t, expectedLatestBlockHash, reconstructed.GetLatestBlockHash(),
		"LatestBlockHash mismatch")

	// 3. LatestExecutionPayloadBid (from kv.LatestExecutionPayloadBidTable)
	gotBid := reconstructed.GetLatestExecutionPayloadBid()
	require.NotNil(t, gotBid, "LatestExecutionPayloadBid must not be nil")
	require.Equal(t, expectedBid.ParentBlockHash, gotBid.ParentBlockHash)
	require.Equal(t, expectedBid.BlockHash, gotBid.BlockHash)
	require.Equal(t, expectedBid.BuilderIndex, gotBid.BuilderIndex)
	require.Equal(t, expectedBid.Slot, gotBid.Slot)
	require.Equal(t, expectedBid.Value, gotBid.Value)
	require.Equal(t, expectedBid.ExecutionRequestsRoot, gotBid.ExecutionRequestsRoot)

	// 4. Builders (from kv.BuildersDump/kv.Builders queue)
	gotBuilders := reconstructed.GetBuilders()
	require.NotNil(t, gotBuilders, "Builders must not be nil")
	require.Equal(t, 1, gotBuilders.Len(), "expected 1 builder")
	require.Equal(t, common.Bytes48{0xAA}, gotBuilders.Get(0).Pubkey)
	require.Equal(t, uint64(999), gotBuilders.Get(0).Balance)

	// 5. BuilderPendingWithdrawals (from kv.BuilderPendingWithdrawalsDump/kv.BuilderPendingWithdrawals queue)
	gotBPW := reconstructed.GetBuilderPendingWithdrawals()
	require.NotNil(t, gotBPW, "BuilderPendingWithdrawals must not be nil")
	require.Equal(t, 1, gotBPW.Len(), "expected 1 builder pending withdrawal")
	require.Equal(t, uint64(777), gotBPW.Get(0).Amount)
	require.Equal(t, uint64(3), gotBPW.Get(0).BuilderIndex)

	// 6. PayloadExpectedWithdrawals (from kv.PayloadExpectedWithdrawalsDump/kv.PayloadExpectedWithdrawals queue)
	gotPEW := reconstructed.GetPayloadExpectedWithdrawals()
	require.NotNil(t, gotPEW, "PayloadExpectedWithdrawals must not be nil")
	require.Equal(t, 0, gotPEW.Len(), "expected empty payload expected withdrawals")

	// 7. ExecutionPayloadAvailability (from kv.ExecutionPayloadAvailabilityTable)
	// We seeded bits 7 and 100 as true; verify content, not just shape.
	gotEPA := reconstructed.GetExecutionPayloadAvailability()
	require.NotNil(t, gotEPA, "ExecutionPayloadAvailability must not be nil")
	require.True(t, gotEPA.GetBitAt(7), "ExecutionPayloadAvailability bit 7 should be set")
	require.True(t, gotEPA.GetBitAt(100), "ExecutionPayloadAvailability bit 100 should be set")
	require.False(t, gotEPA.GetBitAt(0), "ExecutionPayloadAvailability bit 0 should be unset")
	require.False(t, gotEPA.GetBitAt(50), "ExecutionPayloadAvailability bit 50 should be unset")

	// 8. BuilderPendingPayments (from kv.BuilderPendingPaymentsTable)
	// We seeded a non-zero entry at index 3; verify content.
	gotBPPState := reconstructed.GetBuilderPendingPayments()
	require.NotNil(t, gotBPPState, "BuilderPendingPayments must not be nil")
	require.Equal(t, int(2*cfg.SlotsPerEpoch), gotBPPState.Length())
	gotPayment := gotBPPState.Get(3)
	require.Equal(t, uint64(12345), gotPayment.Weight, "BuilderPendingPayments[3].Weight mismatch")
	require.Equal(t, uint64(9876), gotPayment.Withdrawal.Amount, "BuilderPendingPayments[3].Withdrawal.Amount mismatch")
	require.Equal(t, uint64(11), gotPayment.Withdrawal.BuilderIndex, "BuilderPendingPayments[3].Withdrawal.BuilderIndex mismatch")
	// Verify a zero-valued slot is still zero (proves we didn't corrupt adjacent entries).
	require.Equal(t, uint64(0), gotBPPState.Get(0).Weight, "BuilderPendingPayments[0] should be zero")

	// 9. PtcWindow (from kv.PtcWindowTable)
	// We seeded outer=2, inner=5 as value 42; verify content.
	gotPtcWindow := reconstructed.GetPtcWindow()
	require.NotNil(t, gotPtcWindow, "PtcWindow must not be nil")
	require.Equal(t, int((2+cfg.MinSeedLookahead)*cfg.SlotsPerEpoch), gotPtcWindow.Length())
	require.Equal(t, uint64(42), gotPtcWindow.Get(2).Get(5), "PtcWindow[2][5] should be 42")
	require.Equal(t, uint64(0), gotPtcWindow.Get(0).Get(0), "PtcWindow[0][0] should be zero")
}
