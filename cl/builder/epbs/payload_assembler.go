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

	"github.com/erigontech/erigon/cl/builder/epbs/eladapter"
	"github.com/erigontech/erigon/execution/builder"
)

var ErrBlobPayloadUnsupported = errors.New("epbs/runtime: blob payloads are not supported")

type bloblessPayloadAssembler struct {
	assembler PayloadAssembler
}

func newBloblessPayloadAssembler(assembler PayloadAssembler) PayloadAssembler {
	return &bloblessPayloadAssembler{assembler: assembler}
}

func (a *bloblessPayloadAssembler) AssemblePayload(ctx context.Context, parameters *builder.Parameters) (uint64, error) {
	return a.assembler.AssemblePayload(ctx, parameters)
}

func (a *bloblessPayloadAssembler) GetPayload(ctx context.Context, payloadID uint64) (*eladapter.AssembledPayload, error) {
	payload, err := a.assembler.GetPayload(ctx, payloadID)
	if err != nil || payload == nil || payload.BlobsBundle == nil {
		return payload, err
	}
	bundle := payload.BlobsBundle
	if len(bundle.Blobs) != 0 || len(bundle.Commitments) != 0 || len(bundle.Proofs) != 0 {
		return nil, fmt.Errorf("%w: payload %d", ErrBlobPayloadUnsupported, payloadID)
	}
	return payload, nil
}
