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

package cli

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

type failingStateChangesClient struct {
	subscribe func(context.Context) (remoteproto.KV_StateChangesClient, error)
}

func (c failingStateChangesClient) StateChanges(ctx context.Context, _ *remoteproto.StateChangeRequest, _ ...grpc.CallOption) (remoteproto.KV_StateChangesClient, error) {
	return c.subscribe(ctx)
}

func TestStateChangesRetryBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var attempts []time.Time
		client := failingStateChangesClient{subscribe: func(context.Context) (remoteproto.KV_StateChangesClient, error) {
			attempts = append(attempts, time.Now())
			if len(attempts) == 2 {
				cancel()
			}
			return nil, errors.New("snapshot read failed")
		}}
		subscribeToStateChangesLoop(ctx, client, kvcache.New(kvcache.DefaultCoherentConfig))
		<-ctx.Done()
		synctest.Wait()
		require.Len(t, attempts, 2)
		require.Equal(t, 3*time.Second, attempts[1].Sub(attempts[0]))
	})
}
