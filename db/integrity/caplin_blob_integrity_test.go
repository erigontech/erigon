// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package integrity

import (
	"bytes"
	"math"
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestCheckCaplinBlobSidecarsRejectsMissingSidecar(t *testing.T) {
	const slot = uint64(7)
	const limit = snaptype.CaplinMergeLimit

	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	block.Block.Slot = slot
	block.Block.Body.ExecutionPayload.BlockHash[0] = 1
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	root, err := block.SignedBeaconBlockHeader().Header.HashSSZ()
	require.NoError(t, err)

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(current uint64) []byte {
		if current != slot {
			return nil
		}
		return encodeBeaconBlockSnapshot(t, block)
	})
	writeCaplinIntegritySegment(t, dirs, snaptype.BlobSidecars, 0, limit, func(uint64) []byte { return nil })

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, root)
	}))

	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	err = CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "slot 7")
	require.ErrorContains(t, err, "expected 1 sidecars, got 0")
	err = CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, false, log.New())
	require.ErrorContains(t, err, "expected 1 sidecars, got 0")
}

func TestCheckCaplinBlobSidecarsRejectsUnavailableSnapshots(t *testing.T) {
	err := CheckCaplinBlobSidecars(t.Context(), nil, nil, &clparams.MainnetBeaconConfig, true, log.New())
	require.ErrorContains(t, err, "caplin snapshots are unavailable")
}

func TestCheckCaplinBlobSidecarsRejectsMissingCanonicalBlock(t *testing.T) {
	const slot = uint64(7)
	cfg := denebBlobIntegrityConfig()

	err := checkCaplinBlobIntegrityFixture(t, &cfg, slot, nil, nil, common.Hash{1}, nil)
	require.ErrorContains(t, err, "slot 7")
	require.ErrorContains(t, err, "canonical block is missing")
}

func TestCheckCaplinBlobSidecarsRejectsWrongIndex(t *testing.T) {
	const slot = uint64(7)
	cfg := denebBlobIntegrityConfig()
	block, sidecar, canonicalRoot := validBlobIntegrityData(t, &cfg, slot)
	sidecar.Index = 1

	err := checkCaplinBlobIntegrityFixture(t, &cfg, slot, block, []*cltypes.BlobSidecar{sidecar}, canonicalRoot, nil)
	require.ErrorContains(t, err, "slot 7")
	require.ErrorContains(t, err, "expected index 0, got 1")
}

func TestCheckCaplinBlobSidecarsRejectsWrongHeaderSignature(t *testing.T) {
	const slot = uint64(7)
	cfg := denebBlobIntegrityConfig()
	block, sidecar, canonicalRoot := validBlobIntegrityData(t, &cfg, slot)
	sidecar.SignedBlockHeader.Signature[0] = 1

	err := checkCaplinBlobIntegrityFixture(t, &cfg, slot, block, []*cltypes.BlobSidecar{sidecar}, canonicalRoot, nil)
	require.ErrorContains(t, err, "slot 7")
	require.ErrorContains(t, err, "header signature")
}

func TestCheckCaplinBlobSidecarsRejectsNonCanonicalRoot(t *testing.T) {
	const slot = uint64(7)
	cfg := denebBlobIntegrityConfig()
	block, sidecar, canonicalRoot := validBlobIntegrityData(t, &cfg, slot)
	sidecar.SignedBlockHeader.Header.ProposerIndex++

	err := checkCaplinBlobIntegrityFixture(t, &cfg, slot, block, []*cltypes.BlobSidecar{sidecar}, canonicalRoot, nil)
	require.EqualError(t, err, "blob snapshot slot 7 index 0: sidecar block root does not match canonical root")
}

func TestCheckCaplinBlobSidecarsRejectsInvalidKZGProof(t *testing.T) {
	const slot = uint64(7)
	cfg := denebBlobIntegrityConfig()
	block, sidecar, canonicalRoot := validBlobIntegrityData(t, &cfg, slot)
	sidecar.KzgProof[0] ^= 0xff

	err := checkCaplinBlobIntegrityFixture(t, &cfg, slot, block, []*cltypes.BlobSidecar{sidecar}, canonicalRoot, nil)
	require.ErrorContains(t, err, "slot 7")
	require.ErrorContains(t, err, "KZG")
}

func TestCheckCaplinBlobSidecarsAcceptsValidSidecar(t *testing.T) {
	const slot = snaptype.CaplinMergeLimit - 1
	cfg := denebBlobIntegrityConfig()
	block, sidecar, canonicalRoot := validBlobIntegrityDataWithTransactions(t, &cfg, slot, [][]byte{{1, 2, 3}})

	require.NoError(t, checkCaplinBlobIntegrityFixture(t, &cfg, slot, block, []*cltypes.BlobSidecar{sidecar}, canonicalRoot, nil))
}

func TestCheckCaplinBlobSidecarsRejectsMismatchedBeaconSnapshot(t *testing.T) {
	const slot = uint64(7)
	cfg := denebBlobIntegrityConfig()
	canonicalBlock, sidecar, canonicalRoot := validBlobIntegrityData(t, &cfg, slot)
	otherBlock := *canonicalBlock
	otherMessage := *canonicalBlock.Block
	otherMessage.ProposerIndex++
	otherBlock.Block = &otherMessage

	err := checkCaplinBlobIntegrityFixture(t, &cfg, slot, &otherBlock, []*cltypes.BlobSidecar{sidecar}, canonicalRoot, nil)
	require.ErrorContains(t, err, "slot 7")
	require.ErrorContains(t, err, "beacon snapshot root")
}

func TestCheckCaplinBlobSidecarsRejectsMissingBlobSnapshotRange(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(uint64) []byte { return nil })
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, limit, 2*limit, func(uint64) []byte { return nil })

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	err := CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "missing blob snapshot coverage")
	require.ErrorContains(t, err, "0-20000")
}

func TestCheckCaplinBlobSidecarsRejectsMissingTerminalBlobSnapshotRange(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(uint64) []byte { return nil })
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, limit, 2*limit, func(uint64) []byte { return nil })
	writeCaplinIntegritySegment(t, dirs, snaptype.BlobSidecars, 0, limit, func(uint64) []byte { return nil })

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	err := CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "missing blob snapshot coverage")
	require.ErrorContains(t, err, "10000-20000")
}

func TestCheckCaplinBlobSidecarsRejectsBeaconGapBeforeTail(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(uint64) []byte { return nil })
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 2*limit, 3*limit, func(uint64) []byte { return nil })

	db := memdb.NewTestDB(t, dbcfg.CaplinDB)
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 2 * limit / cfg.SlotsPerEpoch
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	err := CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "beacon snapshot coverage missing 10000-20000")
}

func TestCheckCaplinBlobSidecarsRejectsBeaconGapBeforeTailCrossingDeneb(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(uint64) []byte { return nil })
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 2*limit, 4*limit, func(uint64) []byte { return nil })

	db := memdb.NewTestDB(t, dbcfg.CaplinDB)
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 3*limit/cfg.SlotsPerEpoch + 1
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	err := CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "beacon snapshot coverage missing 10000-20000")
}

func TestCheckCaplinBlobSidecarsIgnoresBeaconGapBeforePreDenebTail(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(uint64) []byte { return nil })
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 2*limit, 3*limit, func(uint64) []byte { return nil })

	db := memdb.NewTestDB(t, dbcfg.CaplinDB)
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 3*limit/cfg.SlotsPerEpoch + 1
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	require.NoError(t, CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New()))
}

func TestCheckCaplinBlobSidecarsRejectsMissingBlobSnapshotIndex(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(uint64) []byte { return nil })
	writeCaplinIntegritySegmentData(t, dirs, snaptype.BlobSidecars, 0, limit, func(uint64) []byte { return nil })

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())
	err := CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "blob snapshot segment 0-10000: index is missing")
}

func TestCheckCaplinBlobSidecarsRejectsMissingBeaconSnapshotIndexAfterDeneb(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegmentData(t, dirs, snaptype.BeaconBlocks, 0, limit, func(uint64) []byte { return nil })

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())
	err := CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "beacon snapshot segment 0-10000: index is missing")
}

func TestCheckCaplinBlobSidecarsIgnoresMissingPreDenebBeaconIndex(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegmentData(t, dirs, snaptype.BeaconBlocks, 0, limit, func(uint64) []byte { return nil })

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = limit/cfg.SlotsPerEpoch + 1
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())
	require.NoError(t, CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New()))
}

func TestCheckCaplinBlobSidecarsRejectsMalformedRecord(t *testing.T) {
	const slot = uint64(7)
	const limit = snaptype.CaplinMergeLimit

	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	block.Block.Slot = slot
	block.Block.Body.ExecutionPayload.BlockHash[0] = 1
	root, err := block.SignedBeaconBlockHeader().Header.HashSSZ()
	require.NoError(t, err)

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(current uint64) []byte {
		if current == slot {
			return encodeBeaconBlockSnapshot(t, block)
		}
		return nil
	})
	writeCaplinIntegritySegment(t, dirs, snaptype.BlobSidecars, 0, limit, func(current uint64) []byte {
		if current == slot {
			return []byte{1}
		}
		return nil
	})

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, root)
	}))
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())
	err = CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "slot 7")
	require.ErrorContains(t, err, "invalid sidecar list length")
}

func TestCheckCaplinBlobSidecarsRejectsZeroCountFromWrongBeaconBlock(t *testing.T) {
	const slot = uint64(7)
	cfg := denebBlobIntegrityConfig()
	_, _, canonicalRoot := validBlobIntegrityData(t, &cfg, slot)
	wrongBlock := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	wrongBlock.Block.Slot = slot
	wrongBlock.Block.Body.ExecutionPayload.BlockHash[0] = 1

	err := checkCaplinBlobIntegrityFixture(t, &cfg, slot, wrongBlock, nil, canonicalRoot, nil)
	require.ErrorContains(t, err, "slot 7")
	require.ErrorContains(t, err, "beacon snapshot root")
}

func TestCheckCaplinBlobSidecarsAcceptsZeroBlobBlock(t *testing.T) {
	const slot = uint64(7)
	cfg := denebBlobIntegrityConfig()
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	block.Block.Slot = slot
	block.Block.Body.ExecutionPayload.BlockHash[0] = 1
	canonicalRoot, err := block.SignedBeaconBlockHeader().Header.HashSSZ()
	require.NoError(t, err)

	require.NoError(t, checkCaplinBlobIntegrityFixture(t, &cfg, slot, block, nil, canonicalRoot, nil))
}

func TestCheckCaplinBlobSidecarsAcceptsPreDenebEmptySlotAndBlock(t *testing.T) {
	const slot = uint64(31)
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 1
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.CapellaVersion)
	block.Block.Slot = slot
	block.Block.Body.ExecutionPayload.BlockHash[0] = 1
	canonicalRoot, err := block.SignedBeaconBlockHeader().Header.HashSSZ()
	require.NoError(t, err)

	require.NoError(t, checkCaplinBlobIntegrityFixture(t, &cfg, slot, block, nil, canonicalRoot, nil))
}

func TestCheckCaplinBlobSidecarsAcceptsFuluSidecar(t *testing.T) {
	const slot = uint64(7)
	cfg := denebBlobIntegrityConfig()
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	block, sidecar, canonicalRoot := validBlobIntegrityDataForVersion(t, &cfg, slot, clparams.FuluVersion)

	require.NoError(t, checkCaplinBlobIntegrityFixture(t, &cfg, slot, block, []*cltypes.BlobSidecar{sidecar}, canonicalRoot, nil))
}

func TestCheckCaplinBlobSidecarsRejectsInvalidCommitmentInclusionProof(t *testing.T) {
	const slot = uint64(7)
	cfg := denebBlobIntegrityConfig()
	block, sidecar, canonicalRoot := validBlobIntegrityData(t, &cfg, slot)
	sidecar.CommitmentInclusionProof.Set(0, common.Hash{1})

	err := checkCaplinBlobIntegrityFixture(t, &cfg, slot, block, []*cltypes.BlobSidecar{sidecar}, canonicalRoot, nil)
	require.ErrorContains(t, err, "KZG/proof verification failed")
}

func TestCheckCaplinBlobSidecarsRejectsExtraSidecarAtEmptySlot(t *testing.T) {
	const slot = uint64(7)
	cfg := denebBlobIntegrityConfig()
	_, sidecar, _ := validBlobIntegrityData(t, &cfg, slot)

	err := checkCaplinBlobIntegrityFixture(t, &cfg, slot, nil, []*cltypes.BlobSidecar{sidecar}, common.Hash{}, nil)
	require.EqualError(t, err, "blob snapshot slot 7: beacon block is missing but found 1 sidecars")
}

func TestCheckCaplinBlobSidecarsRejectsBeaconBodyRootMismatch(t *testing.T) {
	const slot = uint64(7)
	cfg := denebBlobIntegrityConfig()
	block, sidecar, _ := validBlobIntegrityData(t, &cfg, slot)
	header := block.SignedBeaconBlockHeader()
	header.Header.BodyRoot[0] ^= 0xff
	canonicalRoot, err := header.Header.HashSSZ()
	require.NoError(t, err)
	encodedBlock := encodeBeaconBlockSnapshotWithBodyRoot(t, block, header.Header.BodyRoot)

	err = checkCaplinBlobIntegrityFixture(t, &cfg, slot, block, []*cltypes.BlobSidecar{sidecar}, canonicalRoot, encodedBlock)
	require.ErrorContains(t, err, "body root does not match decoded body")
}

func TestCheckCaplinBlobSidecarsRejectsGloasCommitmentMismatch(t *testing.T) {
	const slot = uint64(7)
	cfg := denebBlobIntegrityConfig()
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	block, sidecar, canonicalRoot := gloasBlobIntegrityData(t, &cfg, slot)
	sidecar.KzgCommitment[0] ^= 0xff

	err := checkCaplinBlobIntegrityFixture(t, &cfg, slot, block, []*cltypes.BlobSidecar{sidecar}, canonicalRoot, nil)
	require.ErrorContains(t, err, "sidecar commitment does not match beacon block")
}

func TestCheckCaplinBlobSidecarsRejectsGloasBodyRootMismatch(t *testing.T) {
	const slot = uint64(7)
	cfg := denebBlobIntegrityConfig()
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	block, sidecar, _ := gloasBlobIntegrityData(t, &cfg, slot)
	header := block.SignedBeaconBlockHeader()
	header.Header.BodyRoot[0] ^= 0xff
	canonicalRoot, err := header.Header.HashSSZ()
	require.NoError(t, err)
	encodedBlock := encodeBeaconBlockSnapshotWithBodyRoot(t, block, header.Header.BodyRoot)

	err = checkCaplinBlobIntegrityFixture(t, &cfg, slot, block, []*cltypes.BlobSidecar{sidecar}, canonicalRoot, encodedBlock)
	require.ErrorContains(t, err, "body root does not match decoded body")
}

func TestCheckCaplinBlobSidecarsRejectsBlobSegmentWithExtraWord(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit
	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(uint64) []byte { return nil })
	writeCaplinIntegritySegmentWithExtraWord(t, dirs, snaptype.BlobSidecars, 0, limit)

	cfg := denebBlobIntegrityConfig()
	db := memdb.NewTestDB(t, dbcfg.CaplinDB)
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	err := CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "expected 10000 words, got 10001")
}

func TestCaplinBlobSidecarsIsAvailableIntegrityCheck(t *testing.T) {
	require.Contains(t, AllChecks, Check("CaplinBlobSidecars"))
	require.NotContains(t, FastChecks, Check("CaplinBlobSidecars"))
	require.Contains(t, SlowChecks, Check("CaplinBlobSidecars"))
}

func denebBlobIntegrityConfig() clparams.BeaconChainConfig {
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = math.MaxUint64
	cfg.FuluForkEpoch = math.MaxUint64
	cfg.GloasForkEpoch = math.MaxUint64
	return cfg
}

func checkCaplinBlobIntegrityFixture(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64, block *cltypes.SignedBeaconBlock, sidecars []*cltypes.BlobSidecar, canonicalRoot common.Hash, encodedBlock []byte) error {
	t.Helper()
	const limit = snaptype.CaplinMergeLimit
	if encodedBlock == nil && block != nil {
		encodedBlock = encodeBeaconBlockSnapshot(t, block)
	}
	encodedSidecars := make([]byte, 0)
	for _, sidecar := range sidecars {
		encoded, err := sidecar.EncodeSSZ(nil)
		require.NoError(t, err)
		encodedSidecars = append(encodedSidecars, encoded...)
	}

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(current uint64) []byte {
		if current == slot {
			return encodedBlock
		}
		return nil
	})
	writeCaplinIntegritySegment(t, dirs, snaptype.BlobSidecars, 0, limit, func(current uint64) []byte {
		if current == slot {
			return encodedSidecars
		}
		return nil
	})

	db := memdb.NewTestDB(t, dbcfg.CaplinDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, canonicalRoot)
	}))
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())
	return CheckCaplinBlobSidecars(t.Context(), db, snapshots, cfg, true, log.New())
}

func validBlobIntegrityData(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64) (*cltypes.SignedBeaconBlock, *cltypes.BlobSidecar, common.Hash) {
	t.Helper()
	return validBlobIntegrityDataWithTransactions(t, cfg, slot, nil)
}

func validBlobIntegrityDataForVersion(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64, stateVersion clparams.StateVersion) (*cltypes.SignedBeaconBlock, *cltypes.BlobSidecar, common.Hash) {
	t.Helper()
	return validBlobIntegrityDataForVersionWithTransactions(t, cfg, slot, stateVersion, nil)
}

func validBlobIntegrityDataWithTransactions(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64, transactions [][]byte) (*cltypes.SignedBeaconBlock, *cltypes.BlobSidecar, common.Hash) {
	t.Helper()
	return validBlobIntegrityDataForVersionWithTransactions(t, cfg, slot, clparams.DenebVersion, transactions)
}

func validBlobIntegrityDataForVersionWithTransactions(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64, stateVersion clparams.StateVersion, transactions [][]byte) (*cltypes.SignedBeaconBlock, *cltypes.BlobSidecar, common.Hash) {
	t.Helper()
	blob := goethkzg.Blob{}
	commitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof(&blob, commitment, 0)
	require.NoError(t, err)

	block := cltypes.NewSignedBeaconBlock(cfg, stateVersion)
	block.Block.Slot = slot
	block.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	block.Block.Body.ExecutionPayload.Transactions = solid.NewTransactionsSSZFromTransactions(transactions)
	block.Block.Body.ExecutionPayload.BlockHash[0] = 1
	block.GetBlobKzgCommitments().Append((*cltypes.KZGCommitment)(&commitment))
	branch, err := block.Block.Body.KzgCommitmentMerkleProof(0)
	require.NoError(t, err)
	inclusionProof := solid.NewHashVector(len(branch))
	for i, node := range branch {
		inclusionProof.Set(i, common.Hash(node))
	}
	header := block.SignedBeaconBlockHeader()
	root, err := header.Header.HashSSZ()
	require.NoError(t, err)
	sidecar := cltypes.NewBlobSidecar(0, (*cltypes.Blob)(&blob), common.Bytes48(commitment), common.Bytes48(proof), header, inclusionProof)
	return block, sidecar, root
}

func gloasBlobIntegrityData(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64) (*cltypes.SignedBeaconBlock, *cltypes.BlobSidecar, common.Hash) {
	t.Helper()
	blob := goethkzg.Blob{}
	commitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof(&blob, commitment, 0)
	require.NoError(t, err)
	block := cltypes.NewSignedBeaconBlock(cfg, clparams.GloasVersion)
	block.Block.Slot = slot
	block.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
	block.GetBlobKzgCommitments().Append((*cltypes.KZGCommitment)(&commitment))
	header := block.SignedBeaconBlockHeader()
	root, err := header.Header.HashSSZ()
	require.NoError(t, err)
	sidecar := cltypes.NewBlobSidecar(0, (*cltypes.Blob)(&blob), common.Bytes48(commitment), common.Bytes48(proof), header, solid.NewHashVector(cltypes.CommitmentBranchSize))
	return block, sidecar, root
}

func writeCaplinIntegritySegment(t *testing.T, dirs datadir.Dirs, snapshotType snaptype.Type, from, to uint64, word func(uint64) []byte) {
	t.Helper()
	info := writeCaplinIntegritySegmentData(t, dirs, snapshotType, from, to, word)
	require.NoError(t, snapshotsync.BeaconSimpleIdx(t.Context(), info, 1, dirs.Tmp, &background.Progress{}, log.LvlCrit, log.New()))
}

func writeCaplinIntegritySegmentData(t *testing.T, dirs datadir.Dirs, snapshotType snaptype.Type, from, to uint64, word func(uint64) []byte) snaptype.FileInfo {
	t.Helper()
	name := snapshotType.FileName(version.ZeroVersion, from, to)
	info, _, ok := snaptype.ParseFileName(dirs.Snap, name)
	require.True(t, ok)

	compressor, err := seg.NewCompressor(t.Context(), "test "+snapshotType.Name(), info.Path, dirs.Tmp, seg.DefaultCfg, log.LvlCrit, log.New())
	require.NoError(t, err)
	defer compressor.Close()
	for slot := from; slot < to; slot++ {
		require.NoError(t, compressor.AddWord(word(slot)))
	}
	require.NoError(t, compressor.Compress())
	return info
}

func writeCaplinIntegritySegmentWithExtraWord(t *testing.T, dirs datadir.Dirs, snapshotType snaptype.Type, from, to uint64) {
	t.Helper()
	name := snapshotType.FileName(version.ZeroVersion, from, to)
	info, _, ok := snaptype.ParseFileName(dirs.Snap, name)
	require.True(t, ok)
	compressor, err := seg.NewCompressor(t.Context(), "test "+snapshotType.Name(), info.Path, dirs.Tmp, seg.DefaultCfg, log.LvlCrit, log.New())
	require.NoError(t, err)
	defer compressor.Close()
	for slot := from; slot <= to; slot++ {
		require.NoError(t, compressor.AddWord(nil))
	}
	require.NoError(t, compressor.Compress())
	require.NoError(t, snapshotsync.BeaconSimpleIdx(t.Context(), info, 1, dirs.Tmp, &background.Progress{}, log.LvlCrit, log.New()))
}

func encodeBeaconBlockSnapshot(t *testing.T, block *cltypes.SignedBeaconBlock) []byte {
	t.Helper()
	var encoded bytes.Buffer
	writer, err := zstd.NewWriter(&encoded)
	require.NoError(t, err)
	_, err = snapshot_format.WriteBlockForSnapshot(writer, block, nil)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return encoded.Bytes()
}

func encodeBeaconBlockSnapshotWithBodyRoot(t *testing.T, block *cltypes.SignedBeaconBlock, bodyRoot common.Hash) []byte {
	t.Helper()
	var raw bytes.Buffer
	_, err := snapshot_format.WriteBlockForSnapshot(&raw, block, nil)
	require.NoError(t, err)
	copy(raw.Bytes()[1:33], bodyRoot[:])

	var encoded bytes.Buffer
	writer, err := zstd.NewWriter(&encoded)
	require.NoError(t, err)
	_, err = writer.Write(raw.Bytes())
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	return encoded.Bytes()
}
