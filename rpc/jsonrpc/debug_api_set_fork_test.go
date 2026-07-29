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

package jsonrpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/rpc/rpchelper"
)

// forkControllerBackend embeds a rpchelper.ApiBackend and implements
// ForkController with a canned response — lets DebugAPIImpl exercise
// the type-assert-and-delegate path with a stub.
type forkControllerBackend struct {
	rpchelper.ApiBackend
	got         string
	gotUCAN     string
	returns     *rpchelper.SetForkResult
	err         error
}

func (b *forkControllerBackend) SetFork(_ context.Context, chainName, authorityUCAN string) (*rpchelper.SetForkResult, error) {
	b.got = chainName
	b.gotUCAN = authorityUCAN
	return b.returns, b.err
}

// TestDebugSetFork_UnavailableWhenBackendLacksForkController: the RPC
// must fail loud with an actionable error when the backend has no
// SetFork surface (e.g. standalone rpcdaemon that reaches the node
// via gRPC). Silently returning nil would hide the deployment gap
// from the operator.
func TestDebugSetFork_UnavailableWhenBackendLacksForkController(t *testing.T) {
	t.Parallel()

	api := &DebugAPIImpl{ethBackend: bareBackend{}}
	result, err := api.SetFork(context.Background(), "target-chain", "any-ucan")
	require.Nil(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not available in this deployment")
}

// TestDebugSetFork_DelegatesToForkController: when the backend does
// implement ForkController, the RPC forwards the target chain name
// and returns the controller's payload unchanged.
func TestDebugSetFork_DelegatesToForkController(t *testing.T) {
	t.Parallel()

	fc := &forkControllerBackend{
		returns: &rpchelper.SetForkResult{
			FromChain:       "hoodi",
			ToChain:         "hoodi-fork-42",
			UnwoundFrom:     100,
			UnwoundTo:       80,
			RestartRequired: true,
			Message:         "restart with --chain=hoodi-fork-42",
		},
	}
	api := &DebugAPIImpl{ethBackend: fc}
	result, err := api.SetFork(context.Background(), "hoodi-fork-42", "b64-ucan-blob")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "hoodi-fork-42", fc.got, "RPC must forward chainName")
	require.Equal(t, "b64-ucan-blob", fc.gotUCAN, "RPC must forward authorityUCAN unchanged")
	require.Equal(t, "hoodi", result.FromChain)
	require.Equal(t, "hoodi-fork-42", result.ToChain)
	require.Equal(t, uint64(100), result.UnwoundFrom)
	require.Equal(t, uint64(80), result.UnwoundTo)
	require.True(t, result.RestartRequired)
}

// TestDebugSetFork_PropagatesControllerError: a validation failure
// in the controller (unknown chain, no parent relationship, etc.)
// surfaces through the RPC unwrapped so the operator can act on it.
func TestDebugSetFork_PropagatesControllerError(t *testing.T) {
	t.Parallel()

	fc := &forkControllerBackend{err: errors.New("target chain has no direct parent relationship with current")}
	api := &DebugAPIImpl{ethBackend: fc}
	result, err := api.SetFork(context.Background(), "some-fork", "b64-ucan-blob")
	require.Nil(t, result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no direct parent relationship")
}

// bareBackend embeds rpchelper.ApiBackend without implementing
// ForkController — exercises the type-assert-fail branch.
type bareBackend struct{ rpchelper.ApiBackend }
