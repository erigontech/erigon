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
	"fmt"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
)

type BidProcessor interface {
	ProcessMessage(context.Context, *uint64, *cltypes.SignedExecutionPayloadBid) error
}

type validatedBidPublisher struct {
	processor     BidProcessor
	publisher     GossipPublisher
	retryInterval time.Duration
}

func newValidatedBidPublisher(processor BidProcessor, publisher GossipPublisher, retryInterval time.Duration) GossipPublisher {
	return &validatedBidPublisher{processor: processor, publisher: publisher, retryInterval: retryInterval}
}

func (p *validatedBidPublisher) Publish(ctx context.Context, topic string, data []byte) error {
	if topic != gossip.TopicNameExecutionPayloadBid {
		return fmt.Errorf("epbs/bid publisher: unexpected topic %q", topic)
	}
	owned := bytes.Clone(data)
	bid := &cltypes.SignedExecutionPayloadBid{}
	if err := bid.DecodeSSZStrict(owned, int(clparams.GloasVersion)); err != nil {
		return fmt.Errorf("epbs/bid publisher: decode bid: %w", err)
	}
	if err := p.processor.ProcessMessage(ctx, nil, bid); err != nil {
		return fmt.Errorf("epbs/bid publisher: process local bid: %w", err)
	}
	var publishErr error
	for {
		if err := ctx.Err(); err != nil {
			return errors.Join(err, publishErr)
		}
		publishErr = p.publisher.Publish(ctx, topic, owned)
		if publishErr == nil {
			return nil
		}
		timer := time.NewTimer(p.retryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(ctx.Err(), publishErr)
		case <-timer.C:
		}
	}
}
