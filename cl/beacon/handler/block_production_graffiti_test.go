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

package handler

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/execution/engineapi/engine_types"
)

func graffitiText(g common.Hash) string {
	return string(bytes.TrimRight(g[:], "\x00"))
}

type rpcError struct{ code int }

func (e rpcError) Error() string  { return "rpc error" }
func (e rpcError) ErrorCode() int { return e.code }

func TestGraffitiCommitPrefix(t *testing.T) {
	require.Equal(t, "a53e", graffitiCommitPrefix("0xa53e9545"))
	require.Equal(t, "a53e", graffitiCommitPrefix("a53e9545"))
	require.Equal(t, "ab00", graffitiCommitPrefix("ab"))
	require.Equal(t, "0000", graffitiCommitPrefix(""))
}

func TestGraffitiClientCode(t *testing.T) {
	require.Equal(t, "GE", graffitiClientCode("GE"))
	require.Equal(t, "GE", graffitiClientCode("GETH"))
	require.Equal(t, "N", graffitiClientCode("N"))
}

func TestFetchExecutionClientVersion(t *testing.T) {
	t.Run("available is cached", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		engine := execution_client.NewMockExecutionEngine(ctrl)
		engine.EXPECT().GetClientVersionV1(gomock.Any(), gomock.Any()).
			Return([]engine_types.ClientVersionV1{{Code: "GE", Commit: "0xc3d4e5f6"}}, nil).
			Times(1)

		a := &ApiHandler{engine: engine, version: "1.2.3"}
		a.fetchExecutionClientVersion()
		got := a.elClientVersion.Load()
		require.NotNil(t, got)
		require.Equal(t, "GE", got.Code)
	})

	t.Run("method-not-found is cached as unavailable", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		engine := execution_client.NewMockExecutionEngine(ctrl)
		engine.EXPECT().GetClientVersionV1(gomock.Any(), gomock.Any()).
			Return(nil, rpcError{code: -32601}).
			Times(1)

		a := &ApiHandler{engine: engine, version: "1.2.3"}
		a.fetchExecutionClientVersion()
		require.Same(t, elClientVersionUnavailable, a.elClientVersion.Load())
	})

	t.Run("empty version list is cached as unavailable", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		engine := execution_client.NewMockExecutionEngine(ctrl)
		engine.EXPECT().GetClientVersionV1(gomock.Any(), gomock.Any()).
			Return([]engine_types.ClientVersionV1{}, nil).
			Times(1)

		a := &ApiHandler{engine: engine, version: "1.2.3"}
		a.fetchExecutionClientVersion()
		require.Same(t, elClientVersionUnavailable, a.elClientVersion.Load())
	})

	t.Run("transient error is not cached and a later fetch can succeed", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		engine := execution_client.NewMockExecutionEngine(ctrl)
		gomock.InOrder(
			engine.EXPECT().GetClientVersionV1(gomock.Any(), gomock.Any()).
				Return(nil, errors.New("timeout")),
			engine.EXPECT().GetClientVersionV1(gomock.Any(), gomock.Any()).
				Return([]engine_types.ClientVersionV1{{Code: "GE", Commit: "0xc3d4e5f6"}}, nil),
		)

		a := &ApiHandler{engine: engine, version: "1.2.3"}
		a.fetchExecutionClientVersion()
		require.Nil(t, a.elClientVersion.Load())
		a.fetchExecutionClientVersion()
		got := a.elClientVersion.Load()
		require.NotNil(t, got)
		require.Equal(t, "GE", got.Code)
	})
}

func TestDefaultGraffiti(t *testing.T) {
	clCommit := graffitiCommitPrefix(version.GitCommit)

	t.Run("cached execution client version yields full graffiti", func(t *testing.T) {
		a := &ApiHandler{version: "1.2.3"}
		a.elClientVersion.Store(&engine_types.ClientVersionV1{Code: "GE", Commit: "0xc3d4e5f6"})
		require.Equal(t, "GEc3d4"+caplinClientCode+clCommit, graffitiText(a.defaultGraffiti()))
	})

	t.Run("over-long execution client code is clamped to two bytes", func(t *testing.T) {
		a := &ApiHandler{version: "1.2.3"}
		a.elClientVersion.Store(&engine_types.ClientVersionV1{Code: "GETH", Commit: "0xc3d4e5f6"})
		require.Equal(t, "GEc3d4"+caplinClientCode+clCommit, graffitiText(a.defaultGraffiti()))
	})

	t.Run("cached-unavailable yields consensus-only", func(t *testing.T) {
		a := &ApiHandler{version: "1.2.3"}
		a.elClientVersion.Store(elClientVersionUnavailable)
		require.Equal(t, caplinClientCode+clCommit, graffitiText(a.defaultGraffiti()))
	})

	t.Run("no engine yields consensus-only", func(t *testing.T) {
		a := &ApiHandler{version: "1.2.3"}
		require.Equal(t, caplinClientCode+clCommit, graffitiText(a.defaultGraffiti()))
	})

	t.Run("cold cache does not block and fills asynchronously", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		engine := execution_client.NewMockExecutionEngine(ctrl)
		release := make(chan struct{})
		engine.EXPECT().GetClientVersionV1(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, *engine_types.ClientVersionV1) ([]engine_types.ClientVersionV1, error) {
				<-release
				return []engine_types.ClientVersionV1{{Code: "GE", Commit: "0xc3d4e5f6"}}, nil
			}).
			Times(1)

		a := &ApiHandler{engine: engine, version: "1.2.3"}
		// Returns immediately with consensus-only graffiti while the engine call is still blocked.
		require.Equal(t, caplinClientCode+clCommit, graffitiText(a.defaultGraffiti()))
		close(release)
		require.Eventually(t, func() bool {
			return graffitiText(a.defaultGraffiti()) == "GEc3d4"+caplinClientCode+clCommit
		}, time.Second, time.Millisecond)
	})

	t.Run("concurrent cold-cache proposals trigger at most one fetch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		engine := execution_client.NewMockExecutionEngine(ctrl)
		release := make(chan struct{})
		engine.EXPECT().GetClientVersionV1(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, *engine_types.ClientVersionV1) ([]engine_types.ClientVersionV1, error) {
				<-release
				return []engine_types.ClientVersionV1{{Code: "GE", Commit: "0xc3d4e5f6"}}, nil
			}).
			Times(1)

		a := &ApiHandler{engine: engine, version: "1.2.3"}
		var wg sync.WaitGroup
		for range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = a.defaultGraffiti()
			}()
		}
		wg.Wait() // returns while the engine call is still blocked, proving proposals never block on it
		close(release)
		require.Eventually(t, func() bool {
			return graffitiText(a.defaultGraffiti()) == "GEc3d4"+caplinClientCode+clCommit
		}, time.Second, time.Millisecond)
	})
}
