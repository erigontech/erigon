// Copyright 2026 The Erigon Authors
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

package direct

import (
	"context"
	"errors"
	"io"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

// recvUntilErrServer mimics the private API filter aggregators: it reads filter requests until Recv fails.
type recvUntilErrServer struct {
	remoteproto.UnimplementedETHBACKENDServer
}

func (recvUntilErrServer) SubscribeLogs(s grpc.BidiStreamingServer[remoteproto.LogsFilterRequest, remoteproto.SubscribeLogsReply]) error {
	for {
		if _, err := s.Recv(); err != nil {
			return err
		}
	}
}

func (recvUntilErrServer) SubscribeReceipts(s grpc.BidiStreamingServer[remoteproto.ReceiptsFilterRequest, remoteproto.SubscribeReceiptsReply]) error {
	for {
		if _, err := s.Recv(); err != nil {
			return err
		}
	}
}

func TestSubscribeLogsSendAfterServerExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		stream, err := NewEthBackendClientDirect(recvUntilErrServer{}).SubscribeLogs(ctx)
		require.NoError(t, err)
		cancel()
		client := stream.(*SubscribeLogsStreamC)
		for range client.chRecv {
		}
		synctest.Wait()
		for range cap(client.chSend) + 1 {
			require.Error(t, stream.Send(&remoteproto.LogsFilterRequest{}))
		}
	})
}

func TestSubscribeReceiptsSendAfterServerExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		stream, err := NewEthBackendClientDirect(recvUntilErrServer{}).SubscribeReceipts(ctx)
		require.NoError(t, err)
		cancel()
		client := stream.(*SubscribeReceiptsStreamC)
		for range client.chRecv {
		}
		synctest.Wait()
		for range cap(client.chSend) + 1 {
			require.Error(t, stream.Send(&remoteproto.ReceiptsFilterRequest{}))
		}
	})
}

func TestSubscribeLogsStreamSendCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		replies := make(chan *subscribeLogsReply, 1)
		replies <- &subscribeLogsReply{}
		stream := &SubscribeLogsStreamS{ctx: ctx, chSend: replies}
		done := make(chan error, 1)
		go func() { done <- stream.Send(&remoteproto.SubscribeLogsReply{}) }()
		synctest.Wait()

		cancel()
		synctest.Wait()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		default:
			<-replies
			<-done
			t.Fatal("Send remained blocked after cancellation")
		}
	})
}

func TestSubscribeLogsStreamErrCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		replies := make(chan *subscribeLogsReply, 1)
		replies <- &subscribeLogsReply{}
		stream := &SubscribeLogsStreamS{ctx: ctx, chSend: replies}
		done := make(chan struct{})
		go func() {
			stream.Err(errors.New("server failed"))
			close(done)
		}()
		synctest.Wait()

		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			<-replies
			<-done
			t.Fatal("Err remained blocked after cancellation")
		}
	})
}

func TestSubscribeReceiptsStreamSendCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		replies := make(chan *subscribeReceiptsReply, 1)
		replies <- &subscribeReceiptsReply{}
		stream := &SubscribeReceiptsStreamS{ctx: ctx, chSend: replies}
		done := make(chan error, 1)
		go func() { done <- stream.Send(&remoteproto.SubscribeReceiptsReply{}) }()
		synctest.Wait()

		cancel()
		synctest.Wait()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		default:
			<-replies
			<-done
			t.Fatal("Send remained blocked after cancellation")
		}
	})
}

func TestSubscribeReceiptsStreamErrCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		replies := make(chan *subscribeReceiptsReply, 1)
		replies <- &subscribeReceiptsReply{}
		stream := &SubscribeReceiptsStreamS{ctx: ctx, chSend: replies}
		done := make(chan struct{})
		go func() {
			stream.Err(errors.New("server failed"))
			close(done)
		}()
		synctest.Wait()

		cancel()
		synctest.Wait()
		select {
		case <-done:
		default:
			<-replies
			<-done
			t.Fatal("Err remained blocked after cancellation")
		}
	})
}

func TestSubscribeBlockedClientSendEndsWhenHandlerExits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		handlerDone := make(chan struct{})
		logsReqs := make(chan *subscribeLogsRequest, 1)
		logsReqs <- &subscribeLogsRequest{}
		logs := &SubscribeLogsStreamC{chSend: logsReqs, done: handlerDone, ctx: t.Context()}
		receiptsReqs := make(chan *subscribeReceiptsRequest, 1)
		receiptsReqs <- &subscribeReceiptsRequest{}
		receipts := &SubscribeReceiptsStreamC{chSend: receiptsReqs, done: handlerDone, ctx: t.Context()}
		errs := make(chan error, 2)
		go func() { errs <- logs.Send(&remoteproto.LogsFilterRequest{}) }()
		go func() { errs <- receipts.Send(&remoteproto.ReceiptsFilterRequest{}) }()
		synctest.Wait()
		require.Empty(t, errs)

		close(handlerDone)
		synctest.Wait()
		require.Len(t, errs, 2, "Send remained blocked after the handler exited")
		require.ErrorIs(t, <-errs, io.EOF)
		require.ErrorIs(t, <-errs, io.EOF)
		<-logsReqs
		<-receiptsReqs
	})
}

// failingServer returns from each subscription at once, while the caller's context stays active.
type failingServer struct {
	remoteproto.UnimplementedETHBACKENDServer
}

func (failingServer) SubscribeLogs(grpc.BidiStreamingServer[remoteproto.LogsFilterRequest, remoteproto.SubscribeLogsReply]) error {
	return errors.New("handler failed")
}

func (failingServer) SubscribeReceipts(grpc.BidiStreamingServer[remoteproto.ReceiptsFilterRequest, remoteproto.SubscribeReceiptsReply]) error {
	return errors.New("handler failed")
}

func TestSubscribeSendAfterHandlerErrorFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, err := NewEthBackendClientDirect(failingServer{}).SubscribeLogs(t.Context())
		require.NoError(t, err)
		for range logs.(*SubscribeLogsStreamC).chRecv {
		}
		receipts, err := NewEthBackendClientDirect(failingServer{}).SubscribeReceipts(t.Context())
		require.NoError(t, err)
		for range receipts.(*SubscribeReceiptsStreamC).chRecv {
		}
		synctest.Wait()
		require.Error(t, logs.Send(&remoteproto.LogsFilterRequest{}))
		require.Error(t, receipts.Send(&remoteproto.ReceiptsFilterRequest{}))
	})
}
