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

package libsentry

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/gointerfaces/grpcutil"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type (
	MessageStreamFactory  func(context.Context, sentryproto.SentryClient) (grpc.ClientStream, error)
	StatusDataFactory     func(context.Context) (*sentryproto.StatusData, error)
	MessageFactory[T any] func() T
	MessageHandler[T any] func(context.Context, T, sentryproto.SentryClient) error
)

func ReconnectAndPumpStreamLoop[TMessage interface{}](
	ctx context.Context,
	sentryClient sentryproto.SentryClient,
	statusDataFactory StatusDataFactory,
	streamName string,
	streamFactory MessageStreamFactory,
	messageFactory MessageFactory[TMessage],
	handleInboundMessage MessageHandler[TMessage],
	wg *sync.WaitGroup,
	logger log.Logger,
) {
	for ctx.Err() == nil {
		if _, err := sentryClient.HandShake(ctx, &emptypb.Empty{}, grpc.WaitForReady(true)); err != nil {
			if errors.Is(err, context.Canceled) {
				continue
			}
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}
			logger.Warn("HandShake error, sentry not ready yet", "stream", streamName, "err", err)
			time.Sleep(time.Second)
			continue
		}

		statusData, err := statusDataFactory(ctx)

		if err != nil {
			logger.Error("SentryReconnectAndPumpStreamLoop: statusDataFactory error", "stream", streamName, "err", err)
			time.Sleep(time.Second)
			continue
		}

		if _, err := sentryClient.SetStatus(ctx, statusData); err != nil {
			if errors.Is(err, context.Canceled) {
				continue
			}
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}
			logger.Warn("Status error, sentry not ready yet", "stream", streamName, "err", err)
			time.Sleep(time.Second)
			continue
		}

		if err := pumpStreamLoop(ctx, sentryClient, streamName, streamFactory, messageFactory, handleInboundMessage, wg, logger); err != nil {
			if errors.Is(err, context.Canceled) {
				continue
			}
			if IsPeerNotFoundErr(err) {
				continue
			}
			if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
				time.Sleep(3 * time.Second)
				continue
			}
			logger.Warn("pumpStreamLoop failure", "stream", streamName, "err", err)
			continue
		}
	}
}

// pumpStreamLoop is normally run in a separate go-routine.
// It only exists until there are no more messages
// to be received (end of process, or interruption, or end of test).
// wg is used only in tests to avoid using waits, which is brittle. For non-test code wg == nil.
func pumpStreamLoop[TMessage interface{}](
	ctx context.Context,
	sentry sentryproto.SentryClient,
	streamName string,
	streamFactory MessageStreamFactory,
	messageFactory MessageFactory[TMessage],
	handleInboundMessage MessageHandler[TMessage],
	wg *sync.WaitGroup,
	logger log.Logger,
) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if disconnectedMarker, ok := sentry.(interface{ MarkDisconnected() }); ok {
		defer disconnectedMarker.MarkDisconnected()
	}

	// need to read all messages from Sentry as fast as we can, then:
	// - can group them or process in batch
	// - can have slow processing
	reqs := make(chan TMessage, 256)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case req := <-reqs:
				if err := handleInboundMessage(ctx, req, sentry); err != nil {
					logger.Debug("Handling incoming message", "stream", streamName, "err", err)
				}
				if wg != nil {
					wg.Done()
				}
			}
		}
	}()

	stream, err := streamFactory(ctx, sentry)
	if err != nil {
		return err
	}

	for ctx.Err() == nil {
		req := messageFactory()
		err := stream.RecvMsg(req)
		if err != nil {
			return err
		}

		select {
		case reqs <- req:
		case <-ctx.Done():
		}
	}

	return ctx.Err()
}

func IsPeerNotFoundErr(err error) bool {
	return strings.Contains(err.Error(), "peer not found")
}
