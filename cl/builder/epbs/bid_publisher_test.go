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
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes"
)

type capturingBidProcessor struct {
	processed chan []byte
}

func (p *capturingBidProcessor) ProcessMessage(_ context.Context, _ *uint64, bid *cltypes.SignedExecutionPayloadBid) error {
	encoded, err := bid.EncodeSSZ(nil)
	if err != nil {
		return err
	}
	p.processed <- encoded
	return nil
}

type cancelingBidPublisher struct {
	cancel    context.CancelFunc
	err       error
	published chan []byte
}

func (p *cancelingBidPublisher) Publish(_ context.Context, _ string, data []byte) error {
	p.published <- append([]byte(nil), data...)
	p.cancel()
	return p.err
}

func TestCoordinatorReturnsNilAndReleasesPayloadWhenLocalBidRejected(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	rejected := errors.New("invalid builder signature")
	processor := &retryingBidProcessor{errors: []error{rejected}}
	gossipPublisher := &observedBidPublisher{published: make(chan []byte, 1)}
	coordinator := NewCoordinator(
		&config,
		new(coordinatorSigner),
		FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payloadID: 1, payload: assembled},
		newValidatedBidPublisher(processor, gossipPublisher, time.Millisecond),
		1,
	)

	signedBid, err := coordinator.RunSlot(t.Context(), input)
	require.ErrorIs(t, err, rejected)
	require.Nil(t, signedBid)
	_, retained, lookupErr := coordinator.Payload(payloadIdentity(input, assembled))
	require.NoError(t, lookupErr)
	require.False(t, retained)
	select {
	case <-gossipPublisher.published:
		t.Fatal("locally rejected bid was gossiped")
	default:
	}

	signedBid, err = coordinator.RunSlot(t.Context(), input)
	require.NoError(t, err)
	require.NotNil(t, signedBid)
	select {
	case <-gossipPublisher.published:
	case <-time.After(time.Second):
		t.Fatal("released auction could not be retried")
	}
}

func TestCoordinatorKeepsBidAfterLocalAcceptanceAndAmbiguousGossipFailure(t *testing.T) {
	config := gloasCoordinatorConfig()
	input := validCoordinatorSlotInput(config)
	assembled := validCoordinatorPayload(&config, input, big.NewInt(2_000_000_000))
	processor := &capturingBidProcessor{processed: make(chan []byte, 1)}
	ctx, cancel := context.WithCancel(t.Context())
	ambiguous := errors.New("delivery outcome unknown")
	gossipPublisher := &cancelingBidPublisher{
		cancel: cancel, err: ambiguous, published: make(chan []byte, 1),
	}
	coordinator := NewCoordinator(
		&config,
		new(coordinatorSigner),
		FixedMarginStrategy{Margin: 1},
		&coordinatorAssembler{payloadID: 1, payload: assembled},
		newValidatedBidPublisher(processor, gossipPublisher, time.Millisecond),
		1,
	)

	signedBid, err := coordinator.RunSlot(ctx, input)
	require.ErrorIs(t, err, ambiguous)
	require.NotNil(t, signedBid)
	locallyProcessed := <-processor.processed
	gossiped := <-gossipPublisher.published
	require.True(t, bytes.Equal(locallyProcessed, gossiped))
	encodedBid, encodeErr := signedBid.EncodeSSZ(nil)
	require.NoError(t, encodeErr)
	require.True(t, bytes.Equal(encodedBid, gossiped))
	_, retained, lookupErr := coordinator.Payload(payloadIdentity(input, assembled))
	require.NoError(t, lookupErr)
	require.True(t, retained)

	secondBid, secondErr := coordinator.RunSlot(t.Context(), input)
	require.ErrorIs(t, secondErr, ErrAuctionAlreadyTracked)
	require.Nil(t, secondBid)
}
