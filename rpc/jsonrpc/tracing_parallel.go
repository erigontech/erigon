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
	"bytes"
	"context"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/state"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/transactions"
)

// traceBlockParallel traces every transaction of block on the history state before it, so
// the transactions do not wait for each other. The objects are written in block order as
// they complete.
func (api *DebugAPIImpl) traceBlockParallel(ctx context.Context, block *types.Block, baseTxNum uint64, config *tracersConfig.TraceConfig,
	chainConfig *chain.Config, rules *chain.Rules, signer *types.Signer, stream *jsonstream.Stream,
) error {
	txns := block.Transactions()
	header := block.HeaderNoCopy()
	engine := api.engine()

	outs := make([]bytes.Buffer, len(txns))
	usages := make([]mdgas.TxnGasUsage, len(txns))
	done := make([]chan struct{}, len(txns))
	for i := range done {
		done[i] = make(chan struct{})
	}

	workers := debugTraceBlockWorkers(len(txns))
	ctx, cancel := context.WithCancel(ctx)
	replay, err := replayTxnsParallel(ctx, api.db, api._blockReader, header.Number.Uint64(), block.Hash(), baseTxNum, len(txns), workers, 2*workers, func(ctx context.Context, workerTx kv.TemporalTx) (func(int) error, error) {
		getHeader := func(hash common.Hash, n uint64) (*types.Header, error) {
			return api._blockReader.HeaderByNumber(ctx, workerTx, n)
		}
		blockCtx := protocol.NewEVMBlockContext(header, protocol.GetHashFn(header, getHeader), engine, accounts.NilAddress, chainConfig)
		return func(txnIndex int) error {
			txNum := baseTxNum + uint64(txnIndex)
			firstLogIndex, err := rawtemporaldb.FirstLogIndex(workerTx, txNum, txnIndex)
			if err != nil {
				return err
			}
			ibs := state.New(state.NewHistoryReaderV3(workerTx, txNum))
			defer ibs.Close()
			transactions.VersionedCallState(ibs)
			api.shareCodeByHash(ibs)
			ibs.ResumeLogIndexAt(firstLogIndex)
			ibs.SetTxContext(blockCtx.BlockNumber, txnIndex)

			s := jsonstream.New(&outs[txnIndex])
			usages[txnIndex] = api.writeTxnTrace(ctx, s, block, txnIndex, ibs, blockCtx, rules, signer, config, chainConfig, nil)
			if err := s.Flush(); err != nil {
				return err
			}
			close(done[txnIndex])
			return nil
		}, nil
	})
	if err != nil {
		cancel()
		return err
	}
	defer func() {
		cancel()
		_ = replay.Wait()
	}()

	stream.WriteArrayStart()
	for i := range txns {
		select {
		case <-done[i]:
		case <-replay.ctx.Done():
			if err := replay.Wait(); err != nil {
				return err
			}
			return ctx.Err()
		}
		stream.WriteRawBytes(outs[i].Bytes())
		outs[i] = bytes.Buffer{}
		replay.written()
		if err := stream.Flush(); err != nil {
			return err
		}
	}
	if err := replay.Wait(); err != nil {
		return err
	}
	var gasUsed protocol.GasUsed
	for _, usage := range usages {
		gasUsed.BlockExecution += usage.BlockExecutionGasUsed
		gasUsed.BlockState += usage.BlockStateGasUsed
	}
	assertTracedBlockGas(block, config, &gasUsed)
	stream.WriteArrayEnd()
	return nil
}

// debugTraceBlockWorkers gives a quarter of the cores to one block: more workers per request
// compete for the same cores and caches, so they cost CPU without lowering latency.
func debugTraceBlockWorkers(txnCount int) int {
	return replayWorkerCount(replayCPUs()/4, txnCount)
}
