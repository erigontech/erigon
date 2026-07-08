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
	"testing"

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

func TestGraffitiCommitPrefix(t *testing.T) {
	require.Equal(t, "a53e", graffitiCommitPrefix("0xa53e9545"))
	require.Equal(t, "a53e", graffitiCommitPrefix("a53e9545"))
	require.Equal(t, "ab00", graffitiCommitPrefix("ab"))
	require.Equal(t, "0000", graffitiCommitPrefix(""))
}

func TestDefaultGraffiti(t *testing.T) {
	clCommit := graffitiCommitPrefix(version.GitCommit)

	t.Run("execution client version available", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		engine := execution_client.NewMockExecutionEngine(ctrl)
		engine.EXPECT().GetClientVersionV1(gomock.Any(), gomock.Any()).
			Return([]engine_types.ClientVersionV1{{Code: "GE", Commit: "0xc3d4e5f6"}}, nil)

		a := &ApiHandler{engine: engine, version: "1.2.3"}
		require.Equal(t, "GEc3d4"+caplinClientCode+clCommit, graffitiText(a.defaultGraffiti(context.Background())))
	})

	t.Run("execution client version unavailable falls back to consensus-only and is cached", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		engine := execution_client.NewMockExecutionEngine(ctrl)
		engine.EXPECT().GetClientVersionV1(gomock.Any(), gomock.Any()).
			Return(nil, errors.New("not supported")).
			Times(1)

		a := &ApiHandler{engine: engine, version: "1.2.3"}
		first := graffitiText(a.defaultGraffiti(context.Background()))
		second := graffitiText(a.defaultGraffiti(context.Background()))
		require.Equal(t, caplinClientCode+clCommit, first)
		require.Equal(t, first, second)
	})

	t.Run("no engine falls back to consensus-only", func(t *testing.T) {
		a := &ApiHandler{version: "1.2.3"}
		require.Equal(t, caplinClientCode+clCommit, graffitiText(a.defaultGraffiti(context.Background())))
	})

	t.Run("execution client version is cached across calls", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		engine := execution_client.NewMockExecutionEngine(ctrl)
		engine.EXPECT().GetClientVersionV1(gomock.Any(), gomock.Any()).
			Return([]engine_types.ClientVersionV1{{Code: "GE", Commit: "0xc3d4e5f6"}}, nil).
			Times(1)

		a := &ApiHandler{engine: engine, version: "1.2.3"}
		first := graffitiText(a.defaultGraffiti(context.Background()))
		second := graffitiText(a.defaultGraffiti(context.Background()))
		require.Equal(t, first, second)
		require.Equal(t, "GEc3d4"+caplinClientCode+clCommit, second)
	})
}
