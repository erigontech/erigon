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
	"strings"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
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
		<-client.done
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
		<-client.done
		for range cap(client.chSend) + 1 {
			require.Error(t, stream.Send(&remoteproto.ReceiptsFilterRequest{}))
		}
	})
}

func TestSubscribeStreamSendCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		replies := make(chan *subscribeReply, 1)
		replies <- &subscribeReply{}
		stream := &SubscribeStreamS{ctx: ctx, ch: replies}
		done := make(chan error, 1)
		go func() { done <- stream.Send(&remoteproto.SubscribeReply{}) }()
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

func TestSubscribeStreamErrCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		replies := make(chan *subscribeReply, 1)
		replies <- &subscribeReply{}
		stream := &SubscribeStreamS{ctx: ctx, ch: replies}
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

func TestSubscribeSendAfterHandlerErrorFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		logs, err := NewEthBackendClientDirect(remoteproto.UnimplementedETHBACKENDServer{}).SubscribeLogs(t.Context())
		require.NoError(t, err)
		<-logs.(*SubscribeLogsStreamC).done
		receipts, err := NewEthBackendClientDirect(remoteproto.UnimplementedETHBACKENDServer{}).SubscribeReceipts(t.Context())
		require.NoError(t, err)
		<-receipts.(*SubscribeReceiptsStreamC).done
		require.Error(t, logs.Send(&remoteproto.LogsFilterRequest{}))
		require.Error(t, receipts.Send(&remoteproto.ReceiptsFilterRequest{}))
	})
}

// Every in-process server stream returns from Send and Err once the caller's context ends,
// even when its reply buffer is full; Send then reports the cancel.
func TestDirectServerStreamsReturnOnCancel(t *testing.T) {
	failed := errors.New("server failed")
	type stream func(ctx context.Context) (call func() error, drain func())
	for name, open := range map[string]stream{
		"sentinel gossip Send": func(ctx context.Context) (func() error, func()) {
			ch := make(chan *gossipReply, 1)
			ch <- &gossipReply{}
			s := &SentinelSubscribeGossipS{ctx: ctx, ch: ch}
			return func() error { return s.Send(&sentinelproto.GossipData{}) }, func() { <-ch }
		},
		"sentinel gossip Err": func(ctx context.Context) (func() error, func()) {
			ch := make(chan *gossipReply, 1)
			ch <- &gossipReply{}
			s := &SentinelSubscribeGossipS{ctx: ctx, ch: ch}
			return func() error { s.Err(failed); return nil }, func() { <-ch }
		},
		"mining pending block Send": func(ctx context.Context) (func() error, func()) {
			ch := make(chan *onPendigBlockReply, 1)
			ch <- &onPendigBlockReply{}
			s := &MiningOnPendingBlockS{ctx: ctx, ch: ch}
			return func() error { return s.Send(&txpoolproto.OnPendingBlockReply{}) }, func() { <-ch }
		},
		"mining pending block Err": func(ctx context.Context) (func() error, func()) {
			ch := make(chan *onPendigBlockReply, 1)
			ch <- &onPendigBlockReply{}
			s := &MiningOnPendingBlockS{ctx: ctx, ch: ch}
			return func() error { s.Err(failed); return nil }, func() { <-ch }
		},
		"mining mined block Send": func(ctx context.Context) (func() error, func()) {
			ch := make(chan *onMinedBlockReply, 1)
			ch <- &onMinedBlockReply{}
			s := &MiningOnMinedBlockS{ctx: ctx, ch: ch}
			return func() error { return s.Send(&txpoolproto.OnMinedBlockReply{}) }, func() { <-ch }
		},
		"mining mined block Err": func(ctx context.Context) (func() error, func()) {
			ch := make(chan *onMinedBlockReply, 1)
			ch <- &onMinedBlockReply{}
			s := &MiningOnMinedBlockS{ctx: ctx, ch: ch}
			return func() error { s.Err(failed); return nil }, func() { <-ch }
		},
		"mining pending logs Send": func(ctx context.Context) (func() error, func()) {
			ch := make(chan *onPendingLogsReply, 1)
			ch <- &onPendingLogsReply{}
			s := &MiningOnPendingLogsS{ctx: ctx, ch: ch}
			return func() error { return s.Send(&txpoolproto.OnPendingLogsReply{}) }, func() { <-ch }
		},
		"mining pending logs Err": func(ctx context.Context) (func() error, func()) {
			ch := make(chan *onPendingLogsReply, 1)
			ch <- &onPendingLogsReply{}
			s := &MiningOnPendingLogsS{ctx: ctx, ch: ch}
			return func() error { s.Err(failed); return nil }, func() { <-ch }
		},
		"txpool OnAdd Send": func(ctx context.Context) (func() error, func()) {
			ch := make(chan *onAddReply, 1)
			ch <- &onAddReply{}
			s := &TxPoolOnAddS{ctx: ctx, ch: ch}
			return func() error { return s.Send(&txpoolproto.OnAddReply{}) }, func() { <-ch }
		},
		"txpool OnAdd Err": func(ctx context.Context) (func() error, func()) {
			ch := make(chan *onAddReply, 1)
			ch <- &onAddReply{}
			s := &TxPoolOnAddS{ctx: ctx, ch: ch}
			return func() error { s.Err(failed); return nil }, func() { <-ch }
		},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				call, drain := open(ctx)
				done := make(chan error, 1)
				go func() { done <- call() }()
				synctest.Wait()
				select {
				case <-done:
					t.Fatal("returned before the cancel, with its buffer full")
				default:
				}

				cancel()
				synctest.Wait()
				select {
				case err := <-done:
					if strings.HasSuffix(name, "Send") {
						require.ErrorIs(t, err, context.Canceled)
					}
				default:
					drain()
					<-done
					t.Fatal("remained blocked after cancellation")
				}
			})
		})
	}
}

// Once the caller's context ended, a client stream reports the cancel, not a clean end.
func TestDirectClientStreamsReportTheCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	gossip := make(chan *gossipReply)
	close(gossip)
	_, err := (&SentinelSubscribeGossipC{ctx: ctx, ch: gossip}).Recv()
	require.ErrorIs(t, err, context.Canceled)
	pending := make(chan *onPendigBlockReply)
	close(pending)
	_, err = (&MiningOnPendingBlockC{ctx: ctx, ch: pending}).Recv()
	require.ErrorIs(t, err, context.Canceled)
	mined := make(chan *onMinedBlockReply)
	close(mined)
	_, err = (&MiningOnMinedBlockC{ctx: ctx, ch: mined}).Recv()
	require.ErrorIs(t, err, context.Canceled)
	logs := make(chan *onPendingLogsReply)
	close(logs)
	_, err = (&MiningOnPendingLogsC{ctx: ctx, ch: logs}).Recv()
	require.ErrorIs(t, err, context.Canceled)
	added := make(chan *onAddReply)
	close(added)
	_, err = (&TxPoolOnAddC{ctx: ctx, ch: added}).Recv()
	require.ErrorIs(t, err, context.Canceled)
}

// A send after the cancel reports it and queues nothing, even with room in the buffer.
func TestDirectServerStreamSendAfterCancelQueuesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	ch := make(chan *gossipReply, 1)
	require.ErrorIs(t, (&SentinelSubscribeGossipS{ctx: ctx, ch: ch}).Send(&sentinelproto.GossipData{}), context.Canceled)
	require.Empty(t, ch)
}
