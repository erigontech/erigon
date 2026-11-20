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
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// removedReader reads the keys of keys in domain as deleted.
type removedReader struct {
	commitmentdb.StateReader
	domain kv.Domain
	keys   map[string]struct{}
}

func (r *removedReader) Read(d kv.Domain, plainKey []byte, stepSize uint64) ([]byte, kv.Step, error) {
	if _, ok := r.keys[string(plainKey)]; ok && d == r.domain {
		return nil, 0, nil
	}
	return r.StateReader.Read(d, plainKey, stepSize)
}

func (r *removedReader) Clone(tx kv.TemporalTx) commitmentdb.StateReader {
	return &removedReader{StateReader: r.StateReader.Clone(tx), domain: r.domain, keys: r.keys}
}

func (r *removedReader) CloneForWorker(workerCtx context.Context, tx kv.TemporalTx) commitmentdb.StateReader {
	return &removedReader{StateReader: r.StateReader.CloneForWorker(workerCtx, tx), domain: r.domain, keys: r.keys}
}

// removalClosureSiblings returns the sibling paths the trie collapses onto
// when every accessed account, then separately every accessed slot, present
// in the parent state is removed. A prover computing state roots inside the
// block, not only at its end, needs them for collapses that the block's net
// change hides: a slot cleared and refilled, or an account removed and
// recreated. The passes are separate because removing an account drops its
// storage.
func removalClosureSiblings(
	ctx context.Context,
	tx kv.TemporalTx,
	hc *headCaptureSource,
	firstTxNumInBlock, endTxNum, blockNum, parentNum uint64,
	accessed *accessedState,
) ([][]byte, error) {
	pre := commitmentdb.NewHistoryStateReader(tx, firstTxNumInBlock)
	present := func(d kv.Domain, key []byte, stepSize uint64) (bool, error) {
		enc, _, err := pre.Read(d, key, stepSize)
		return len(enc) > 0, err
	}
	var paths [][]byte
	seen := map[string]struct{}{}
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		domains, err := newSnapshotCommitmentDomains(ctx, tx, log.New())
		if err != nil {
			return nil, err
		}
		sdCtx := domains.GetCommitmentContext()
		removed := map[string]struct{}{}
		sdCtx.SetStateReader(&removedReader{StateReader: collapseReaderFor(hc, tx, firstTxNumInBlock, endTxNum), domain: domain, keys: removed})
		_, seekBlockNum, err := domains.SeekCommitment(ctx, tx)
		if err == nil && seekBlockNum != parentNum {
			err = fmt.Errorf("commitment trie for block %d is at block %d instead of parent %d", blockNum, seekBlockNum, parentNum)
		}
		if err != nil {
			domains.Close()
			return nil, err
		}
		// Touching a key absent before and after corrupts the trie's maps.
		touch := func(d kv.Domain, key []byte) error {
			ok, err := present(d, key, domains.StepSize())
			if err == nil && ok {
				removed[string(key)] = struct{}{}
				sdCtx.TouchKey(d, string(key), nil)
			}
			return err
		}
		if domain == kv.AccountsDomain {
			for addr := range accessed.Addresses {
				if err != nil {
					break
				}
				err = touch(domain, bytes.Clone(addr[:]))
			}
		} else {
			for addr, slots := range accessed.Storage {
				for slot := range slots {
					if err != nil {
						break
					}
					err = touch(domain, append(bytes.Clone(addr[:]), slot[:]...))
				}
			}
		}
		if err == nil && len(removed) > 0 {
			sdCtx.SetCollapseTracer(func(hashedKeyPath, _ []byte) {
				if _, ok := seen[string(hashedKeyPath)]; !ok {
					seen[string(hashedKeyPath)] = struct{}{}
					paths = append(paths, bytes.Clone(hashedKeyPath))
				}
			})
			_, err = sdCtx.ComputeCommitment(ctx, tx, false, blockNum, firstTxNumInBlock, "debug_executionWitness_removal_closure", nil)
			sdCtx.SetCollapseTracer(nil)
		}
		domains.Close()
		if err != nil {
			return nil, fmt.Errorf("removal closure of %s: %w", domain, err)
		}
	}
	return paths, nil
}

// HeadWitnesses builds the execution witnesses of the blocks an in-process
// producer commits, without commitment history: a WitnessPin taken while a
// block's parent is the head supplies the parent's commitment. Its witnesses
// are the legacy debug_executionWitness shape with the removal closure.
type HeadWitnesses struct {
	api *DebugAPIImpl
}

// NewHeadWitnesses returns the witness builder of the chain in db.
func NewHeadWitnesses(db kv.TemporalRoDB, blockReader dbservices.FullBlockReader, engine rules.Engine) *HeadWitnesses {
	api := NewPrivateDebugAPI(NewBaseApi(nil, nil, blockReader, engine, nil), db, nil, rpccfg.DefaultDebugApiConfig())
	api.removalClosure = true
	return &HeadWitnesses{api: api}
}

// WitnessPin is a read-only snapshot of the state at the committed head.
type WitnessPin struct {
	pin *rollingPin
}

// Number and Hash identify the pinned head.
func (p *WitnessPin) Number() uint64    { return p.pin.num }
func (p *WitnessPin) Hash() common.Hash { return p.pin.hash }

// Close releases the snapshot.
func (p *WitnessPin) Close() { p.pin.close() }

// Pin snapshots the committed head; it must be closed.
func (w *HeadWitnesses) Pin(ctx context.Context) (*WitnessPin, error) {
	pin, err := openRollingPin(ctx, w.api.db)
	if err != nil {
		return nil, err
	}
	if pin == nil {
		// Genesis is committed without stage progress.
		tx, err := w.api.db.BeginTemporalRo(ctx) //nolint:gocritic
		if err != nil {
			return nil, err
		}
		hash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		pin = &rollingPin{tx: tx, hash: hash}
	}
	return &WitnessPin{pin: pin}, nil
}

// Build builds the witness of the committed block hash, a child of the head
// pin snapshotted.
func (w *HeadWitnesses) Build(ctx context.Context, pin *WitnessPin, hash common.Hash) (*ExecutionWitnessResult, error) {
	tx, err := w.api.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	info, err := w.api.resolveWitnessBlock(ctx, tx, rpc.BlockNumberOrHashWithHash(hash, false))
	if err != nil {
		return nil, err
	}
	if info.Block.ParentHash() != pin.pin.hash {
		return nil, fmt.Errorf("block %d (%x) is not a child of the pinned head %d (%x)", info.BlockNum, hash, pin.pin.num, pin.pin.hash)
	}
	return w.api.buildWitnessResultHeadCapture(ctx, tx, pin.pin, nil, info, witnessModeLegacy)
}
