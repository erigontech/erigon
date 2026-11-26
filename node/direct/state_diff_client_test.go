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
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

func TestStateDiffStreamSendCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		replies := make(chan *stateDiffReply, 1)
		replies <- &stateDiffReply{}
		stream := &StateDiffStreamS{ctx: ctx, ch: replies}
		done := make(chan error, 1)
		go func() { done <- stream.Send(&remoteproto.StateChangeBatch{}) }()
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

func TestStateDiffStreamErrCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		replies := make(chan *stateDiffReply, 1)
		replies <- &stateDiffReply{}
		stream := &StateDiffStreamS{ctx: ctx, ch: replies}
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

func TestStateDiffStreamRecvCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		replies := make(chan *stateDiffReply)
		stream := &StateDiffStreamC{ctx: ctx, ch: replies}
		done := make(chan error, 1)
		go func() {
			_, err := stream.Recv()
			done <- err
		}()
		synctest.Wait()

		cancel()
		synctest.Wait()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		default:
			close(replies)
			<-done
			t.Fatal("Recv remained blocked after cancellation")
		}
	})
}
