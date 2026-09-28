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
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beacon_router_configuration"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
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
			Return([]engine_types.ClientVersionV1{{Code: "EG", Commit: "0xc3d4e5f6"}}, nil).
			Times(1)

		a := &ApiHandler{engine: engine, version: "1.2.3"}
		a.fetchExecutionClientVersion()
		got := a.elClientVersion.Load()
		require.NotNil(t, got)
		require.Equal(t, "EG", got.Code)
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
				Return([]engine_types.ClientVersionV1{{Code: "EG", Commit: "0xc3d4e5f6"}}, nil),
		)

		a := &ApiHandler{engine: engine, version: "1.2.3"}
		a.fetchExecutionClientVersion()
		require.Nil(t, a.elClientVersion.Load())
		a.fetchExecutionClientVersion()
		got := a.elClientVersion.Load()
		require.NotNil(t, got)
		require.Equal(t, "EG", got.Code)
	})
}

func TestDefaultGraffiti(t *testing.T) {
	clCommit := graffitiCommitPrefix(version.GitCommit)

	t.Run("cached execution client version yields full graffiti", func(t *testing.T) {
		a := &ApiHandler{version: "1.2.3"}
		a.elClientVersion.Store(&engine_types.ClientVersionV1{Code: "EG", Commit: "0xc3d4e5f6"})
		require.Equal(t, "EGc3d4"+caplinClientCode+clCommit, graffitiText(a.defaultGraffiti()))
	})

	t.Run("over-long execution client code is clamped to two bytes", func(t *testing.T) {
		a := &ApiHandler{version: "1.2.3"}
		a.elClientVersion.Store(&engine_types.ClientVersionV1{Code: "EGXX", Commit: "0xc3d4e5f6"})
		require.Equal(t, "EGc3d4"+caplinClientCode+clCommit, graffitiText(a.defaultGraffiti()))
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
				return []engine_types.ClientVersionV1{{Code: "EG", Commit: "0xc3d4e5f6"}}, nil
			}).
			Times(1)

		a := &ApiHandler{engine: engine, version: "1.2.3"}
		// Returns immediately with consensus-only graffiti while the engine call is still blocked.
		require.Equal(t, caplinClientCode+clCommit, graffitiText(a.defaultGraffiti()))
		close(release)
		require.Eventually(t, func() bool {
			return graffitiText(a.defaultGraffiti()) == "EGc3d4"+caplinClientCode+clCommit
		}, time.Second, time.Millisecond)
	})

	t.Run("concurrent cold-cache proposals trigger at most one fetch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		engine := execution_client.NewMockExecutionEngine(ctrl)
		release := make(chan struct{})
		engine.EXPECT().GetClientVersionV1(gomock.Any(), gomock.Any()).
			DoAndReturn(func(context.Context, *engine_types.ClientVersionV1) ([]engine_types.ClientVersionV1, error) {
				<-release
				return []engine_types.ClientVersionV1{{Code: "EG", Commit: "0xc3d4e5f6"}}, nil
			}).
			Times(1)

		a := &ApiHandler{engine: engine, version: "1.2.3"}
		var wg sync.WaitGroup
		for range 16 {
			wg.Go(func() {
				_ = a.defaultGraffiti()
			})
		}
		wg.Wait() // returns while the engine call is still blocked, proving proposals never block on it
		close(release)
		require.Eventually(t, func() bool {
			return graffitiText(a.defaultGraffiti()) == "EGc3d4"+caplinClientCode+clCommit
		}, time.Second, time.Millisecond)
	})

	// A proposal that reads the cache as empty and only then wins the fetch slot must
	// not fetch again: the fetch it raced against may have populated the cache and
	// released the slot in between.
	t.Run("a proposal racing a completing fetch does not fetch again", func(t *testing.T) {
		for range 200 {
			ctrl := gomock.NewController(t)
			engine := execution_client.NewMockExecutionEngine(ctrl)
			engine.EXPECT().GetClientVersionV1(gomock.Any(), gomock.Any()).
				Return([]engine_types.ClientVersionV1{{Code: "EG", Commit: "0xc3d4e5f6"}}, nil).
				Times(1)

			a := &ApiHandler{engine: engine, version: "1.2.3"}
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					for range 50 {
						_ = a.defaultGraffiti()
					}
				})
			}
			wg.Wait()
			// Let the fetch goroutine finish before the mock controller is checked.
			require.Eventually(t, func() bool {
				return a.elClientVersion.Load() != nil && !a.elClientVersionFetching.Load()
			}, 200*time.Millisecond, time.Millisecond)
		}
	})
}

func TestCombinedGraffiti(t *testing.T) {
	clCommit := graffitiCommitPrefix(version.GitCommit)

	withELVersion := func() *ApiHandler {
		a := &ApiHandler{version: "1.2.3"}
		a.elClientVersion.Store(&engine_types.ClientVersionV1{Code: "EG", Commit: "0xc3d4e5f6"})
		return a
	}

	t.Run("short custom graffiti is prefixed with the full EL+CL segment", func(t *testing.T) {
		a := withELVersion()
		custom := graffitiFromString("pool.eth")
		got := graffitiText(a.combinedGraffiti(custom))
		require.Equal(t, "EGc3d4"+caplinClientCode+clCommit+" pool.eth", got)
	})

	t.Run("custom graffiti is truncated to fit the 32-byte field", func(t *testing.T) {
		a := withELVersion()
		segment := "EGc3d4" + caplinClientCode + clCommit // 12 bytes
		var zero common.Hash
		available := len(zero) - len(segment) - 1 // 19 bytes for custom text (1 more for the separator)
		long := strings.Repeat("x", available+5)
		custom := graffitiFromString(long)

		got := graffitiText(a.combinedGraffiti(custom))

		require.Equal(t, segment+" "+long[:available], got)
		require.LessOrEqual(t, len(got), len(zero))
	})

	t.Run("empty custom graffiti yields the identification segment with no trailing separator", func(t *testing.T) {
		a := withELVersion()
		got := graffitiText(a.combinedGraffiti(common.Hash{}))
		require.Equal(t, "EGc3d4"+caplinClientCode+clCommit, got)
	})

	t.Run("consensus-only segment leaves more room for custom graffiti", func(t *testing.T) {
		a := &ApiHandler{version: "1.2.3"}     // no cached EL version: consensus-only segment
		segment := caplinClientCode + clCommit // 6 bytes
		var zero common.Hash
		available := len(zero) - len(segment) - 1 // 25 bytes for custom text (1 more for the separator)
		long := strings.Repeat("y", available+3)
		custom := graffitiFromString(long)

		got := graffitiText(a.combinedGraffiti(custom))

		require.Equal(t, segment+" "+long[:available], got)
	})

	t.Run("truncation backs off to a UTF-8 rune boundary", func(t *testing.T) {
		a := withELVersion()
		segment := "EGc3d4" + caplinClientCode + clCommit // 12 bytes
		var zero common.Hash
		available := len(zero) - len(segment) - 1
		require.Equal(t, 19, available, "test below assumes this exact byte budget")
		// "ab" (2 bytes) + five 4-byte runes: byte 19 of 22 lands inside the last rune.
		long := "ab" + strings.Repeat("🎉", 5)
		custom := graffitiFromString(long)

		text := graffitiText(a.combinedGraffiti(custom))

		require.True(t, utf8.ValidString(text), "truncated graffiti must be valid UTF-8: %q", text)
		require.Equal(t, segment+" ab"+strings.Repeat("🎉", 4), text)
	})

	t.Run("short caller graffiti decoded by graffitiFromHex is not silently dropped", func(t *testing.T) {
		a := withELVersion()
		custom := graffitiFromHex("0x01") // right-padded: content at byte 0, not byte 31
		got := graffitiText(a.combinedGraffiti(custom))
		require.Equal(t, "EGc3d4"+caplinClientCode+clCommit+" \x01", got)
	})
}

func TestGraffitiFromHex(t *testing.T) {
	t.Run("a full 32-byte value matches common.HexToHash", func(t *testing.T) {
		full := "0x" + strings.Repeat("ab", 32)
		require.Equal(t, common.HexToHash(full), graffitiFromHex(full))
	})

	t.Run("a short value is zero-padded on the right, not the left", func(t *testing.T) {
		var want common.Hash
		want[0] = 0x01
		require.Equal(t, want, graffitiFromHex("0x01"))
		// common.HexToHash treats short input as a right-aligned number instead,
		// placing the byte at the end: exactly the mismatch this helper avoids.
		require.NotEqual(t, common.HexToHash("0x01"), graffitiFromHex("0x01"))
	})
}

func TestRequestGraffiti(t *testing.T) {
	clCommit := graffitiCommitPrefix(version.GitCommit)

	newHandler := func(preserveGraffiti bool) *ApiHandler {
		return &ApiHandler{
			version:   "1.2.3",
			routerCfg: &beacon_router_configuration.RouterConfiguration{PreserveGraffiti: preserveGraffiti},
		}
	}

	t.Run("no custom graffiti uses the default regardless of the flag", func(t *testing.T) {
		for _, force := range []bool{false, true} {
			a := newHandler(force)
			got := graffitiText(a.requestGraffiti(false, common.Hash{}))
			require.Equal(t, caplinClientCode+clCommit, got)
		}
	})

	t.Run("custom graffiti is combined by default", func(t *testing.T) {
		a := newHandler(false)
		custom := graffitiFromString("pool.eth")
		got := graffitiText(a.requestGraffiti(true, custom))
		require.Equal(t, caplinClientCode+clCommit+" pool.eth", got)
	})

	t.Run("PreserveGraffiti opts out of combining and returns the caller's graffiti verbatim", func(t *testing.T) {
		a := newHandler(true)
		custom := graffitiFromString("pool.eth")
		got := a.requestGraffiti(true, custom)
		require.Equal(t, custom, got)
	})
}

func TestLogGraffitiIdentificationOnce(t *testing.T) {
	getLogs := captureAllProductionLogs(t)
	a := &ApiHandler{version: "1.2.3", logger: log.Root()}

	a.logGraffitiIdentificationOnce()
	a.logGraffitiIdentificationOnce()
	a.logGraffitiIdentificationOnce()

	require.Equal(t, 1, strings.Count(getLogs(), "Default graffiti updated"))
}

func TestLogGraffitiIdentification(t *testing.T) {
	getLogs := captureAllProductionLogs(t)

	ctrl := gomock.NewController(t)
	engine := execution_client.NewMockExecutionEngine(ctrl)
	release := make(chan struct{})
	engine.EXPECT().GetClientVersionV1(gomock.Any(), gomock.Any()).
		DoAndReturn(func(context.Context, *engine_types.ClientVersionV1) ([]engine_types.ClientVersionV1, error) {
			<-release
			return []engine_types.ClientVersionV1{{Code: "EG", Commit: "0xc3d4e5f6"}}, nil
		}).
		Times(1)

	a := &ApiHandler{engine: engine, version: "1.2.3", logger: log.Root()}
	a.LogGraffitiIdentification()

	// The startup log fires synchronously, before the (async) execution client lookup
	// could possibly have resolved.
	require.Contains(t, getLogs(), "Default graffiti")
	require.NotContains(t, getLogs(), "Default graffiti updated")

	close(release)
	require.Eventually(t, func() bool {
		return strings.Contains(getLogs(), "Default graffiti updated")
	}, time.Second, time.Millisecond)

	require.Equal(t, 1, strings.Count(getLogs(), "Default graffiti updated"),
		"the resolution log must fire exactly once, not once per call site or per retry")
}
