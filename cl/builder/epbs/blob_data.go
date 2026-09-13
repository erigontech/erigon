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
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/common"
)

type DataColumnWriter interface {
	WriteColumnSidecars(context.Context, common.Hash, int64, *cltypes.DataColumnSidecar) error
}

type blobDataPreparer struct {
	beaconCfg  *clparams.BeaconChainConfig
	writer     DataColumnWriter
	publisher  GossipPublisher
	storeSlots chan struct{}
}

type preparedDataColumn struct {
	root    common.Hash
	index   int64
	sidecar *cltypes.DataColumnSidecar
	topic   string
	encoded []byte
}

type preparedBlobData struct {
	writer      DataColumnWriter
	publisher   GossipPublisher
	columns     []preparedDataColumn
	stored      []bool
	published   []bool
	storeSlots  chan struct{}
	storeFlight <-chan error
}

func newBlobDataPreparer(beaconCfg *clparams.BeaconChainConfig, writer DataColumnWriter, publisher GossipPublisher) BlobDataPreparer {
	return &blobDataPreparer{
		beaconCfg:  beaconCfg,
		writer:     writer,
		publisher:  publisher,
		storeSlots: make(chan struct{}, maxConcurrentReveals+1),
	}
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
		return &preparedBlobData{writer: p.writer, publisher: p.publisher, storeSlots: p.storeSlots}, nil
	}
	columns, err := buildBlobDataColumns(ctx, p.beaconCfg, slot, blockRoot, bundle, commitments)
	if err != nil {
		return nil, err
	}
	prepared := &preparedBlobData{
		writer: p.writer, publisher: p.publisher,
		columns:    make([]preparedDataColumn, len(columns)),
		stored:     make([]bool, len(columns)),
		published:  make([]bool, len(columns)),
		storeSlots: p.storeSlots,
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

func buildBlobDataColumns(
	ctx context.Context,
	beaconCfg *clparams.BeaconChainConfig,
	slot uint64,
	blockRoot common.Hash,
	bundle *eladapter.BlobsBundle,
	commitments *solid.ListSSZ[*cltypes.KZGCommitment],
) ([]*cltypes.DataColumnSidecar, error) {
	if bundle == nil || beaconCfg == nil || beaconCfg.NumberOfColumns == 0 || uint64(len(bundle.Blobs)) > math.MaxUint64/beaconCfg.NumberOfColumns {
		return nil, errors.New("epbs/blob-data: invalid blob bundle")
	}
	cellsAndProofs := make([]peerdasutils.CellsAndKZGProofs, len(bundle.Blobs))
	for blobIndex := range bundle.Blobs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		blob := new(cltypes.Blob)
		copy(blob[:], bundle.Blobs[blobIndex])
		cells, err := das.ComputeCells(blob)
		if err != nil {
			return nil, fmt.Errorf("epbs/blob-data: compute cells for blob %d: %w", blobIndex, err)
		}
		proofs := make([]cltypes.KZGProof, beaconCfg.NumberOfColumns)
		proofOffset := uint64(blobIndex) * beaconCfg.NumberOfColumns
		for columnIndex := range beaconCfg.NumberOfColumns {
			copy(proofs[columnIndex][:], bundle.Proofs[proofOffset+columnIndex])
		}
		cellsAndProofs[blobIndex] = peerdasutils.CellsAndKZGProofs{Blobs: cells, Proofs: proofs}
	}
	columns, err := peerdasutils.GetDataColumnSidecarsGloasWithConfig(beaconCfg, slot, blockRoot, cellsAndProofs)
	if err != nil {
		return nil, fmt.Errorf("epbs/blob-data: build columns: %w", err)
	}
	if !das.VerifyDataColumnSidecarsKZGProofsWithCommitments(columns, commitments) {
		return nil, errors.New("epbs/blob-data: invalid KZG proof")
	}
	return columns, nil
}

func (p *preparedBlobData) Store(ctx context.Context) error {
	if p.storeFlight == nil {
		if p.storeSlots == nil {
			p.storeSlots = make(chan struct{}, 1)
		}
		select {
		case p.storeSlots <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		finished := make(chan error, 1)
		p.storeFlight = finished
		go func() {
			defer func() { <-p.storeSlots }()
			finished <- p.store(ctx)
		}()
	}
	select {
	case err := <-p.storeFlight:
		p.storeFlight = nil
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *preparedBlobData) store(ctx context.Context) error {
	if len(p.stored) != len(p.columns) {
		p.stored = make([]bool, len(p.columns))
	}
	var result error
	for i := range p.columns {
		if p.stored[i] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		column := &p.columns[i]
		if err := p.writer.WriteColumnSidecars(ctx, column.root, column.index, column.sidecar); err != nil {
			result = errors.Join(result, fmt.Errorf("epbs/blob-data: store column %d: %w", column.index, err))
			continue
		}
		p.stored[i] = true
	}
	return result
}

func (p *preparedBlobData) Publish(ctx context.Context) error {
	if len(p.published) != len(p.columns) {
		p.published = make([]bool, len(p.columns))
	}
	var result error
	for i := range p.columns {
		if p.published[i] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		column := &p.columns[i]
		if err := p.publisher.Publish(ctx, column.topic, column.encoded); err != nil {
			result = errors.Join(result, fmt.Errorf("epbs/blob-data: publish column %d: %w", column.index, err))
			continue
		}
		p.published[i] = true
	}
	return result
}
