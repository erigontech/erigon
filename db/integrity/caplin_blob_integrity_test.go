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
	"os"
	"path/filepath"
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
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
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

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
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
	limit := uint64(snaptype.CaplinMergeLimit)
	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(uint64) []byte { return nil })
	writeCaplinIntegritySegment(t, dirs, snaptype.BlobSidecars, 0, limit, func(uint64) []byte { return nil })

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, 7, common.Hash{1})
	}))

	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	err := CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "slot 7")
	require.ErrorContains(t, err, "canonical block is missing")
}

func TestCheckCaplinBlobSidecarsRejectsWrongIndex(t *testing.T) {
	const slot = uint64(7)
	const limit = snaptype.CaplinMergeLimit

	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	block.Block.Slot = slot
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	header := block.SignedBeaconBlockHeader()
	root, err := header.Header.HashSSZ()
	require.NoError(t, err)
	sidecar := &cltypes.BlobSidecar{
		Index:                    1,
		SignedBlockHeader:        header,
		CommitmentInclusionProof: solid.NewHashVector(cltypes.CommitmentBranchSize),
	}
	encodedSidecar, err := sidecar.EncodeSSZ(nil)
	require.NoError(t, err)

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(current uint64) []byte {
		if current != slot {
			return nil
		}
		return encodeBeaconBlockSnapshot(t, block)
	})
	writeCaplinIntegritySegment(t, dirs, snaptype.BlobSidecars, 0, limit, func(current uint64) []byte {
		if current == slot {
			return encodedSidecar
		}
		return nil
	})

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, root)
	}))

	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	err = CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "slot 7")
	require.ErrorContains(t, err, "expected index 0, got 1")
}

func TestCheckCaplinBlobSidecarsRejectsWrongHeaderSignature(t *testing.T) {
	const slot = uint64(7)
	const limit = snaptype.CaplinMergeLimit

	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	block, sidecar, canonicalRoot := validBlobIntegrityData(t, &cfg, slot)
	sidecar.SignedBlockHeader.Signature[0] = 1
	encodedSidecar, err := sidecar.EncodeSSZ(nil)
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
			return encodedSidecar
		}
		return nil
	})

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, canonicalRoot)
	}))
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	err = CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "slot 7")
	require.ErrorContains(t, err, "header signature")
}

func TestCheckCaplinBlobSidecarsRejectsNonCanonicalRoot(t *testing.T) {
	const slot = uint64(7)
	const limit = snaptype.CaplinMergeLimit

	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	block.Block.Slot = slot
	block.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	header := block.SignedBeaconBlockHeader()
	canonicalRoot, err := header.Header.HashSSZ()
	require.NoError(t, err)
	canonicalRoot[0] ^= 0xff
	sidecar := &cltypes.BlobSidecar{
		Index:                    0,
		SignedBlockHeader:        header,
		CommitmentInclusionProof: solid.NewHashVector(cltypes.CommitmentBranchSize),
	}
	encodedSidecar, err := sidecar.EncodeSSZ(nil)
	require.NoError(t, err)

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(current uint64) []byte {
		if current != slot {
			return nil
		}
		return encodeBeaconBlockSnapshot(t, block)
	})
	writeCaplinIntegritySegment(t, dirs, snaptype.BlobSidecars, 0, limit, func(current uint64) []byte {
		if current == slot {
			return encodedSidecar
		}
		return nil
	})

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, canonicalRoot)
	}))

	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	err = CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "slot 7")
	require.ErrorContains(t, err, "canonical root")
}

func TestCheckCaplinBlobSidecarsRejectsInvalidKZGProof(t *testing.T) {
	slot := uint64(7)
	limit := uint64(snaptype.CaplinMergeLimit)

	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	block, sidecar, canonicalRoot := validBlobIntegrityData(t, &cfg, slot)
	sidecar.KzgProof[0] ^= 0xff
	encodedSidecar, err := sidecar.EncodeSSZ(nil)
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
			return encodedSidecar
		}
		return nil
	})

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, canonicalRoot)
	}))

	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	err = CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "slot 7")
	require.ErrorContains(t, err, "KZG")
}

func TestCheckCaplinBlobSidecarsAcceptsValidSidecar(t *testing.T) {
	limit := uint64(snaptype.CaplinMergeLimit)
	slot := limit - 1

	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	block, sidecar, canonicalRoot := validBlobIntegrityData(t, &cfg, slot)
	encodedSidecar, err := sidecar.EncodeSSZ(nil)
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
			return encodedSidecar
		}
		return nil
	})

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, canonicalRoot)
	}))

	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())
	require.NoError(t, CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New()))
}

func TestCheckCaplinBlobSidecarsRejectsMismatchedBeaconSnapshot(t *testing.T) {
	slot := uint64(7)
	limit := uint64(snaptype.CaplinMergeLimit)

	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	canonicalBlock, sidecar, canonicalRoot := validBlobIntegrityData(t, &cfg, slot)
	otherBlock := *canonicalBlock
	otherMessage := *canonicalBlock.Block
	otherMessage.ProposerIndex++
	otherBlock.Block = &otherMessage
	encodedSidecar, err := sidecar.EncodeSSZ(nil)
	require.NoError(t, err)

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(current uint64) []byte {
		if current == slot {
			return encodeBeaconBlockSnapshot(t, &otherBlock)
		}
		return nil
	})
	writeCaplinIntegritySegment(t, dirs, snaptype.BlobSidecars, 0, limit, func(current uint64) []byte {
		if current == slot {
			return encodedSidecar
		}
		return nil
	})

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, canonicalRoot)
	}))

	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	err = CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "slot 7")
	require.ErrorContains(t, err, "beacon snapshot root")
}

func TestCheckCaplinBlobSidecarsRejectsMissingBlobSnapshotRange(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(uint64) []byte { return nil })
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, limit, 2*limit, func(uint64) []byte { return nil })

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	err := CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "missing blob snapshot coverage")
	require.ErrorContains(t, err, "0-10000")
}

func TestCheckCaplinBlobSidecarsRejectsMissingBlobSnapshotIndex(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(uint64) []byte { return nil })
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, limit, 2*limit, func(uint64) []byte { return nil })
	writeCaplinIntegritySegmentData(t, dirs, snaptype.BlobSidecars, 0, limit, func(uint64) []byte { return nil })

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())
	err := CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "missing blob snapshot coverage")
}

func TestCheckCaplinBlobSidecarsRejectsCorruptBlobSnapshotIndex(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(uint64) []byte { return nil })
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, limit, 2*limit, func(uint64) []byte { return nil })
	info := writeCaplinIntegritySegmentData(t, dirs, snaptype.BlobSidecars, 0, limit, func(uint64) []byte { return nil })
	require.NoError(t, snapshotsync.BeaconSimpleIdx(t.Context(), info, 1, dirs.Tmp, &background.Progress{}, log.LvlCrit, log.New()))
	indexPath := filepath.Join(info.Dir(), info.Type.IdxFileName(info.Version, info.From, info.To))
	require.FileExists(t, indexPath)
	require.NoError(t, os.WriteFile(indexPath, []byte("corrupt"), 0o644))

	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	err := snapshots.OpenFolder()
	require.ErrorContains(t, err, "incomplete file")
}

func TestCheckCaplinBlobSidecarsRejectsMalformedRecord(t *testing.T) {
	const slot = uint64(7)
	const limit = snaptype.CaplinMergeLimit

	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	block.Block.Slot = slot
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

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
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
	const limit = snaptype.CaplinMergeLimit

	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	canonicalBlock := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	canonicalBlock.Block.Slot = slot
	canonicalBlock.GetBlobKzgCommitments().Append(&cltypes.KZGCommitment{})
	canonicalRoot, err := canonicalBlock.SignedBeaconBlockHeader().Header.HashSSZ()
	require.NoError(t, err)

	wrongBlock := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	wrongBlock.Block.Slot = slot
	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(current uint64) []byte {
		if current == slot {
			return encodeBeaconBlockSnapshot(t, wrongBlock)
		}
		return nil
	})
	writeCaplinIntegritySegment(t, dirs, snaptype.BlobSidecars, 0, limit, func(uint64) []byte { return nil })

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, canonicalRoot)
	}))

	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	err = CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New())
	require.ErrorContains(t, err, "slot 7")
	require.ErrorContains(t, err, "beacon snapshot root")
}

func TestCheckCaplinBlobSidecarsAcceptsZeroBlobBlock(t *testing.T) {
	const slot = uint64(7)
	const limit = snaptype.CaplinMergeLimit

	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	block.Block.Slot = slot
	root, err := block.SignedBeaconBlockHeader().Header.HashSSZ()
	require.NoError(t, err)

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(current uint64) []byte {
		if current == slot {
			return encodeBeaconBlockSnapshot(t, block)
		}
		return nil
	})
	writeCaplinIntegritySegment(t, dirs, snaptype.BlobSidecars, 0, limit, func(uint64) []byte { return nil })

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, root)
	}))

	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())
	require.NoError(t, CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New()))
}

func TestCheckCaplinBlobSidecarsAcceptsPreDenebEmptySlotAndBlock(t *testing.T) {
	const blockSlot = uint64(31)
	const limit = snaptype.CaplinMergeLimit

	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 1
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.CapellaVersion)
	block.Block.Slot = blockSlot
	root, err := block.SignedBeaconBlockHeader().Header.HashSSZ()
	require.NoError(t, err)

	dirs := datadir.New(t.TempDir())
	writeCaplinIntegritySegment(t, dirs, snaptype.BeaconBlocks, 0, limit, func(current uint64) []byte {
		if current == blockSlot {
			return encodeBeaconBlockSnapshot(t, block)
		}
		return nil
	})
	writeCaplinIntegritySegment(t, dirs, snaptype.BlobSidecars, 0, limit, func(uint64) []byte { return nil })

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, blockSlot, root)
	}))
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())
	require.NoError(t, CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New()))
}

func TestCheckCaplinBlobSidecarsAcceptsFuluSidecar(t *testing.T) {
	const slot = uint64(7)
	const limit = snaptype.CaplinMergeLimit

	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = math.MaxUint64
	block, sidecar, canonicalRoot := validBlobIntegrityDataForVersion(t, &cfg, slot, clparams.FuluVersion)
	encodedSidecar, err := sidecar.EncodeSSZ(nil)
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
			return encodedSidecar
		}
		return nil
	})

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(t.Context(), tx, slot, canonicalRoot)
	}))
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: "mainnet"}, &cfg, dirs, log.New())
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())
	require.NoError(t, CheckCaplinBlobSidecars(t.Context(), db, snapshots, &cfg, true, log.New()))
}

func TestCaplinBlobSidecarsIsAvailableIntegrityCheck(t *testing.T) {
	require.Contains(t, AllChecks, Check("CaplinBlobSidecars"))
	require.NotContains(t, FastChecks, Check("CaplinBlobSidecars"))
	require.Contains(t, SlowChecks, Check("CaplinBlobSidecars"))
}

func validBlobIntegrityData(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64) (*cltypes.SignedBeaconBlock, *cltypes.BlobSidecar, common.Hash) {
	t.Helper()
	return validBlobIntegrityDataForVersion(t, cfg, slot, clparams.DenebVersion)
}

func validBlobIntegrityDataForVersion(t *testing.T, cfg *clparams.BeaconChainConfig, slot uint64, stateVersion clparams.StateVersion) (*cltypes.SignedBeaconBlock, *cltypes.BlobSidecar, common.Hash) {
	t.Helper()
	blob := goethkzg.Blob{}
	commitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof(&blob, commitment, 0)
	require.NoError(t, err)

	block := cltypes.NewSignedBeaconBlock(cfg, stateVersion)
	block.Block.Slot = slot
	block.Block.Body.SyncAggregate = cltypes.NewSyncAggregate()
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
