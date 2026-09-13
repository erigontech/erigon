// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"bytes"
	"context"
	"errors"
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
)

type recordedColumnWrite struct {
	root   common.Hash
	index  int64
	column *cltypes.DataColumnSidecar
}

type recordingColumnWriter struct {
	writes []recordedColumnWrite
}

func (w *recordingColumnWriter) WriteColumnSidecars(_ context.Context, root common.Hash, index int64, column *cltypes.DataColumnSidecar) error {
	w.writes = append(w.writes, recordedColumnWrite{root: root, index: index, column: column})
	return nil
}

type recordingColumnPublisher struct {
	topics []string
	data   [][]byte
}

type failOnceColumnWriter struct {
	failedAt int64
	failed   bool
	writes   []int64
}

func (w *failOnceColumnWriter) WriteColumnSidecars(_ context.Context, _ common.Hash, index int64, _ *cltypes.DataColumnSidecar) error {
	w.writes = append(w.writes, index)
	if index == w.failedAt && !w.failed {
		w.failed = true
		return errors.New("write failed")
	}
	return nil
}

type failOnceColumnPublisher struct {
	failedAt string
	failed   bool
	topics   []string
}

func (p *failOnceColumnPublisher) Publish(_ context.Context, topic string, _ []byte) error {
	p.topics = append(p.topics, topic)
	if topic == p.failedAt && !p.failed {
		p.failed = true
		return errors.New("publish failed")
	}
	return nil
}

func (p *recordingColumnPublisher) Publish(_ context.Context, topic string, data []byte) error {
	p.topics = append(p.topics, topic)
	p.data = append(p.data, bytes.Clone(data))
	return nil
}

func TestBlobDataPreparerStoresAndPublishesGloasColumns(t *testing.T) {
	cfg := gloasCoordinatorConfig()
	cfg.NumberOfColumns = peerdasutils.CELLS_PER_EXT_BLOB
	bundle := validBlobDataBundle(t)
	writer := new(recordingColumnWriter)
	publisher := new(recordingColumnPublisher)
	preparer := newBlobDataPreparer(&cfg, writer, publisher)
	blockRoot := common.HexToHash("0xa0")

	prepared, err := preparer.Prepare(t.Context(), 64, blockRoot, bundle)
	require.NoError(t, err)
	require.NoError(t, prepared.Store(t.Context()))
	require.NoError(t, prepared.Publish(t.Context()))

	require.Len(t, writer.writes, int(cfg.NumberOfColumns))
	require.Len(t, publisher.data, int(cfg.NumberOfColumns))
	commitments, err := buildBlobCommitments(&cfg, 64, bundle)
	require.NoError(t, err)
	for i, write := range writer.writes {
		require.Equal(t, blockRoot, write.root)
		require.Equal(t, int64(i), write.index)
		require.Equal(t, uint64(64), write.column.Slot)
		require.Equal(t, blockRoot, write.column.BeaconBlockRoot)
		require.True(t, das.VerifyDataColumnSidecarKZGProofsWithCommitments(write.column, commitments))

		encoded, err := write.column.EncodeSSZ(nil)
		require.NoError(t, err)
		require.Equal(t, encoded, publisher.data[i])
		decoded := cltypes.NewDataColumnSidecarWithVersionAndConfig(clparams.GloasVersion, &cfg)
		require.NoError(t, decoded.DecodeSSZ(publisher.data[i], int(clparams.GloasVersion)))
		require.Equal(t, uint64(i), decoded.Index)
		require.Equal(t, gossip.TopicNameDataColumnSidecar(uint64(i)%cfg.DataColumnSidecarSubnetCount), publisher.topics[i])
	}
}

func TestBlobDataPreparerRejectsZeroSubnetCount(t *testing.T) {
	cfg := gloasCoordinatorConfig()
	cfg.NumberOfColumns = peerdasutils.CELLS_PER_EXT_BLOB
	cfg.DataColumnSidecarSubnetCount = 0
	preparer := newBlobDataPreparer(&cfg, new(recordingColumnWriter), new(recordingColumnPublisher))

	prepared, err := preparer.Prepare(t.Context(), 64, common.HexToHash("0xa0"), validBlobDataBundle(t))

	require.ErrorContains(t, err, "subnet count")
	require.Nil(t, prepared)
}

func TestBlobDataPreparerRejectsInvalidProofBeforeSideEffects(t *testing.T) {
	cfg := gloasCoordinatorConfig()
	cfg.NumberOfColumns = peerdasutils.CELLS_PER_EXT_BLOB
	bundle := validBlobDataBundle(t)
	bundle.Proofs[0][0] ^= 0xff
	writer := new(recordingColumnWriter)
	publisher := new(recordingColumnPublisher)
	preparer := newBlobDataPreparer(&cfg, writer, publisher)

	prepared, err := preparer.Prepare(t.Context(), 64, common.HexToHash("0xa0"), bundle)

	require.ErrorContains(t, err, "invalid KZG proof")
	require.Nil(t, prepared)
	require.Empty(t, writer.writes)
	require.Empty(t, publisher.data)
}

func TestBlobDataPreparerEmptyBundleHasNoSideEffects(t *testing.T) {
	cfg := gloasCoordinatorConfig()
	writer := new(recordingColumnWriter)
	publisher := new(recordingColumnPublisher)
	preparer := newBlobDataPreparer(&cfg, writer, publisher)
	bundle := &eladapter.BlobsBundle{Commitments: [][]byte{}, Proofs: [][]byte{}, Blobs: [][]byte{}}

	prepared, err := preparer.Prepare(t.Context(), 64, common.HexToHash("0xa0"), bundle)
	require.NoError(t, err)
	require.NoError(t, prepared.Store(t.Context()))
	require.NoError(t, prepared.Publish(t.Context()))
	require.Empty(t, writer.writes)
	require.Empty(t, publisher.data)
}

func TestPreparedBlobDataRetriesFromFirstUnconfirmedColumn(t *testing.T) {
	writer := &failOnceColumnWriter{failedAt: 1}
	publisher := &failOnceColumnPublisher{failedAt: "column-1"}
	prepared := &preparedBlobData{
		writer:    writer,
		publisher: publisher,
		columns: []preparedDataColumn{
			{index: 0, sidecar: new(cltypes.DataColumnSidecar), topic: "column-0"},
			{index: 1, sidecar: new(cltypes.DataColumnSidecar), topic: "column-1"},
			{index: 2, sidecar: new(cltypes.DataColumnSidecar), topic: "column-2"},
		},
	}

	require.Error(t, prepared.Store(t.Context()))
	require.NoError(t, prepared.Store(t.Context()))
	require.Equal(t, []int64{0, 1, 1, 2}, writer.writes)
	require.Error(t, prepared.Publish(t.Context()))
	require.NoError(t, prepared.Publish(t.Context()))
	require.Equal(t, []string{"column-0", "column-1", "column-1", "column-2"}, publisher.topics)
}

func BenchmarkBlobDataPreparerPrepareOneBlob(b *testing.B) {
	cfg := gloasCoordinatorConfig()
	cfg.NumberOfColumns = peerdasutils.CELLS_PER_EXT_BLOB
	bundle := validBlobDataBundle(b)
	preparer := newBlobDataPreparer(&cfg, new(recordingColumnWriter), new(recordingColumnPublisher))
	blockRoot := common.HexToHash("0xa0")

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := preparer.Prepare(b.Context(), 64, blockRoot, bundle); err != nil {
			b.Fatal(err)
		}
	}
}

func validBlobDataBundle(t testing.TB) *eladapter.BlobsBundle {
	t.Helper()
	blob := new(goethkzg.Blob)
	blob[0] = 1
	commitment, err := kzg.Ctx().BlobToKZGCommitment(blob, 0)
	require.NoError(t, err)
	_, proofs, err := peerdasutils.ComputeCellsAndKZGProofs(blob[:])
	require.NoError(t, err)
	bundle := &eladapter.BlobsBundle{
		Commitments: [][]byte{bytes.Clone(commitment[:])},
		Proofs:      make([][]byte, len(proofs)),
		Blobs:       [][]byte{bytes.Clone(blob[:])},
	}
	for i := range proofs {
		bundle.Proofs[i] = bytes.Clone(proofs[i][:])
	}
	return bundle
}
