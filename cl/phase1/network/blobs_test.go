// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package network

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

type blockingBlobPeerClient struct {
	calls     atomic.Int64
	inFlight  atomic.Int64
	maxFlight atomic.Int64
}

type failingBlobPeerClient struct{ calls atomic.Int64 }

func (*failingBlobPeerClient) Peers() (uint64, error) { return 1, nil }

func (c *failingBlobPeerClient) SendBlobsSidecarByIdentifierReq(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	c.calls.Add(1)
	return nil, "peer", errors.New("resource unavailable")
}

func (*blockingBlobPeerClient) Peers() (uint64, error) { return 1, nil }

func (c *blockingBlobPeerClient) SendBlobsSidecarByIdentifierReq(ctx context.Context, _ *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error) {
	c.calls.Add(1)
	inFlight := c.inFlight.Add(1)
	defer c.inFlight.Add(-1)
	for {
		maxFlight := c.maxFlight.Load()
		if inFlight <= maxFlight || c.maxFlight.CompareAndSwap(maxFlight, inFlight) {
			break
		}
	}
	<-ctx.Done()
	return nil, "peer", ctx.Err()
}

func TestRequestBlobsFranticallyBoundsConcurrentRequests(t *testing.T) {
	client := &blockingBlobPeerClient{}
	req := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 40)
	req.Append(&cltypes.BlobIdentifier{})
	ctx, cancel := context.WithTimeout(t.Context(), 350*time.Millisecond)
	defer cancel()

	_, err := RequestBlobsFrantically(ctx, client, req)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.LessOrEqual(t, client.maxFlight.Load(), int64(2))
	require.LessOrEqual(t, client.calls.Load(), int64(2))
}

func TestRequestBlobsFranticallyBacksOffAfterFailures(t *testing.T) {
	client := &failingBlobPeerClient{}
	req := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 40)
	req.Append(&cltypes.BlobIdentifier{})
	ctx, cancel := context.WithTimeout(t.Context(), 750*time.Millisecond)
	defer cancel()

	_, err := RequestBlobsFrantically(ctx, client, req)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.LessOrEqual(t, client.calls.Load(), int64(4))
}
