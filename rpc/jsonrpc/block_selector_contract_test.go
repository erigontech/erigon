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
	"io"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// TestBlockHashSelectorContracts pins, per endpoint family, the response to an
// unknown block hash and to a known hash that is not canonical.
func TestBlockHashSelectorContracts(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	side := writeNonCanonicalTestBlock(t, m).Hash()
	unknown := common.Hash{0x77}
	eth := newEthApiForTest(base, m.DB, nil, nil)
	debugAPI := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})
	traceAPI := NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})

	endpoints := []struct {
		name string
		call func(hash common.Hash) (any, error)
	}{
		{"eth_getLogs", func(hash common.Hash) (any, error) {
			return logsOf(eth.GetLogs(m.Ctx, filters.FilterCriteria{BlockHash: &hash}))
		}},
		{"debug_accountAt", func(hash common.Hash) (any, error) {
			return debugAPI.AccountAt(m.Ctx, hash, 0, m.Address)
		}},
		{"debug_storageRangeAt", func(hash common.Hash) (any, error) {
			return debugAPI.StorageRangeAt(m.Ctx, hash, 0, m.Address, nil, 1)
		}},
		{"debug_accountRange", func(hash common.Hash) (any, error) {
			return debugAPI.AccountRange(m.Ctx, rpc.BlockNumberOrHashWithHash(hash, false), m.Address[:], 1, true, true, nil)
		}},
		{"trace_call", func(hash common.Hash) (any, error) {
			sel := rpc.BlockNumberOrHashWithHash(hash, false)
			return traceAPI.Call(m.Ctx, TraceCallParam{}, []string{TraceTypeTrace}, &sel, nil)
		}},
		{"debug_traceCall", func(hash common.Hash) (any, error) {
			sel := rpc.BlockNumberOrHashWithHash(hash, false)
			return nil, debugAPI.TraceCall(m.Ctx, ethapi.CallArgs{}, &sel, nil, jsonstream.New(io.Discard))
		}},
		{"eth_getProof", func(hash common.Hash) (any, error) {
			sel := rpc.BlockNumberOrHashWithHash(hash, false)
			return eth.GetProof(m.Ctx, m.Address, nil, &sel)
		}},
		{"eth_getWitness", func(hash common.Hash) (any, error) {
			return eth.GetWitness(m.Ctx, rpc.BlockNumberOrHashWithHash(hash, false))
		}},
		{"debug_executionWitness", func(hash common.Hash) (any, error) {
			tx, err := m.DB.BeginTemporalRo(m.Ctx)
			require.NoError(t, err)
			defer tx.Rollback()
			return debugAPI.resolveWitnessBlock(m.Ctx, tx, rpc.BlockNumberOrHashWithHash(hash, false))
		}},
		{"eth_simulateV1", func(hash common.Hash) (any, error) {
			return eth.SimulateV1(m.Ctx, SimulationRequest{BlockStateCalls: []SimulatedBlock{{}}}, rpc.BlockNumberOrHashWithHash(hash, false))
		}},
	}

	for _, e := range endpoints {
		t.Run(e.name+"/unknown hash", func(t *testing.T) {
			_, err := e.call(unknown)
			switch e.name {
			case "eth_getLogs":
				require.ErrorIs(t, err, errBlockHashNotFound)
			case "debug_accountAt", "debug_storageRangeAt":
				require.NoError(t, err, "an unknown hash is an empty result, not an error")
			case "trace_call":
				require.ErrorAs(t, err, new(*rpc.ResourceNotFoundError))
			default:
				require.ErrorAs(t, err, &rpc.BlockNotFoundErr{})
			}
		})
		t.Run(e.name+"/non-canonical hash", func(t *testing.T) {
			_, err := e.call(side)
			switch e.name {
			case "eth_getLogs":
				require.ErrorIs(t, err, errBlockHashNotFound)
			case "debug_accountAt":
				require.EqualError(t, err, "block hash is not canonical")
			case "trace_call":
				var custom *rpc.CustomError
				require.ErrorAs(t, err, &custom)
				require.Equal(t, 4444, custom.Code)
			default:
				require.ErrorAs(t, err, &rpchelper.NonCanonicalHashError{})
			}
		})
	}
}

// TestPendingSelectorContracts covers the pending tag on the families that no
// other test pins: logs accept it as the newest block, debug state rejects it.
func TestPendingSelectorContracts(t *testing.T) {
	base, m, _ := newOverlayAheadTestAPI(t)
	pending := big.NewInt(rpc.PendingBlockNumber.Int64())

	t.Run("eth_getLogs", func(t *testing.T) {
		eth := newEthApiForTest(base, m.DB, nil, nil)
		_, err := logsOf(eth.GetLogs(m.Ctx, filters.FilterCriteria{FromBlock: pending, ToBlock: pending}))
		require.NoError(t, err)
	})

	t.Run("debug_accountRange", func(t *testing.T) {
		debugAPI := NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})
		_, err := debugAPI.AccountRange(m.Ctx, rpc.BlockNumberOrHashWithNumber(rpc.PendingBlockNumber), m.Address[:], 1, true, true, nil)
		require.EqualError(t, err, "accountRange for pending block not supported")
	})
}
