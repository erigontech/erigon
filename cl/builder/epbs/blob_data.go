// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package epbs

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/common"
)

type DataColumnWriter interface {
	WriteColumnSidecars(context.Context, common.Hash, int64, *cltypes.DataColumnSidecar) error
}

type blobDataPreparer struct {
	beaconCfg *clparams.BeaconChainConfig
	writer    DataColumnWriter
	publisher GossipPublisher
}

type preparedDataColumn struct {
	root    common.Hash
	index   int64
	sidecar *cltypes.DataColumnSidecar
	topic   string
	encoded []byte
}

type preparedBlobData struct {
	writer    DataColumnWriter
	publisher GossipPublisher
	columns   []preparedDataColumn
	storeNext int
	pubNext   int
}

func newBlobDataPreparer(beaconCfg *clparams.BeaconChainConfig, writer DataColumnWriter, publisher GossipPublisher) BlobDataPreparer {
	return &blobDataPreparer{beaconCfg: beaconCfg, writer: writer, publisher: publisher}
}

func (p *blobDataPreparer) Prepare(
	ctx context.Context,
	slot uint64,
	blockRoot common.Hash,
	bundle *eladapter.BlobsBundle,
) (PreparedBlobData, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p == nil || p.beaconCfg == nil || isNilDependency(p.writer) || isNilDependency(p.publisher) {
		return nil, errors.New("epbs/blob-data: missing dependency")
	}
	if p.beaconCfg.DataColumnSidecarSubnetCount == 0 {
		return nil, errors.New("epbs/blob-data: data column subnet count must be positive")
	}
	commitments, err := buildBlobCommitments(p.beaconCfg, slot, bundle)
	if err != nil {
		return nil, fmt.Errorf("epbs/blob-data: %w", err)
	}
	if commitments.Len() == 0 {
		return &preparedBlobData{writer: p.writer, publisher: p.publisher}, nil
	}
	if bundle == nil || p.beaconCfg.NumberOfColumns == 0 || uint64(len(bundle.Blobs)) > math.MaxUint64/p.beaconCfg.NumberOfColumns {
		return nil, errors.New("epbs/blob-data: invalid blob bundle")
	}
	cellsAndProofs := make([]peerdasutils.CellsAndKZGProofs, len(bundle.Blobs))
	for blobIndex := range bundle.Blobs {
		blob := new(cltypes.Blob)
		copy(blob[:], bundle.Blobs[blobIndex])
		cells, err := das.ComputeCells(blob)
		if err != nil {
			return nil, fmt.Errorf("epbs/blob-data: compute cells for blob %d: %w", blobIndex, err)
		}
		proofs := make([]cltypes.KZGProof, p.beaconCfg.NumberOfColumns)
		proofOffset := uint64(blobIndex) * p.beaconCfg.NumberOfColumns
		for columnIndex := range p.beaconCfg.NumberOfColumns {
			copy(proofs[columnIndex][:], bundle.Proofs[proofOffset+columnIndex])
		}
		cellsAndProofs[blobIndex] = peerdasutils.CellsAndKZGProofs{Blobs: cells, Proofs: proofs}
	}
	columns, err := peerdasutils.GetDataColumnSidecarsGloasWithConfig(p.beaconCfg, slot, blockRoot, cellsAndProofs)
	if err != nil {
		return nil, fmt.Errorf("epbs/blob-data: build columns: %w", err)
	}
	prepared := &preparedBlobData{
		writer: p.writer, publisher: p.publisher,
		columns: make([]preparedDataColumn, len(columns)),
	}
	if !das.VerifyDataColumnSidecarsKZGProofsWithCommitments(columns, commitments) {
		return nil, errors.New("epbs/blob-data: invalid KZG proof")
	}
	for i, column := range columns {
		encoded, err := column.EncodeSSZ(nil)
		if err != nil {
			return nil, fmt.Errorf("epbs/blob-data: encode column %d: %w", column.Index, err)
		}
		prepared.columns[i] = preparedDataColumn{
			root: blockRoot, index: int64(column.Index), sidecar: column,
			topic:   gossip.TopicNameDataColumnSidecar(column.Index % p.beaconCfg.DataColumnSidecarSubnetCount),
			encoded: encoded,
		}
	}
	return prepared, nil
}

func (p *preparedBlobData) Store(ctx context.Context) error {
	for i := p.storeNext; i < len(p.columns); i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		column := &p.columns[i]
		if err := p.writer.WriteColumnSidecars(ctx, column.root, column.index, column.sidecar); err != nil {
			return fmt.Errorf("epbs/blob-data: store column %d: %w", column.index, err)
		}
		p.storeNext = i + 1
	}
	return nil
}

func (p *preparedBlobData) Publish(ctx context.Context) error {
	for i := p.pubNext; i < len(p.columns); i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		column := &p.columns[i]
		if err := p.publisher.Publish(ctx, column.topic, column.encoded); err != nil {
			return fmt.Errorf("epbs/blob-data: publish column %d: %w", column.index, err)
		}
		p.pubNext = i + 1
	}
	return nil
}
