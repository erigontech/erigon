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

package exec

import (
	"bytes"
	"context"
	"slices"
	"sync/atomic"

	keccak "github.com/erigontech/fastkeccak"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

const staticWarmBatch = 8

type staticSlot struct {
	addr common.Address
	slot common.Hash
}

func compareStaticSlots(a, b staticSlot) int {
	if c := a.addr.Cmp(b.addr); c != 0 {
		return c
	}
	return bytes.Compare(a.slot[:], b.slot[:])
}

func staticKeys(header *types.Header, body *types.Body, senders []accounts.Address) ([]common.Address, []staticSlot) {
	txns := body.Transactions
	addrs := make([]common.Address, 1, 2*len(txns)+len(body.Withdrawals)+1)
	addrs[0] = header.Coinbase
	var slots []staticSlot
	for i, txn := range txns {
		if len(senders) == len(txns) {
			addrs = append(addrs, senders[i].Value())
		}
		if to := txn.GetTo(); to != nil {
			addrs = append(addrs, *to)
		}
		for _, entry := range txn.GetAccessList() {
			addrs = append(addrs, entry.Address)
			for _, slot := range entry.StorageKeys {
				slots = append(slots, staticSlot{addr: entry.Address, slot: slot})
			}
		}
	}
	for _, w := range body.Withdrawals {
		addrs = append(addrs, w.Address)
	}
	slices.SortFunc(addrs, common.Address.Cmp)
	slices.SortFunc(slots, compareStaticSlots)
	return slices.Compact(addrs), slices.Compact(slots)
}

func staticTouches(addrs []common.Address, slots []staticSlot) []PathTouch {
	touches := make([]PathTouch, 0, len(addrs)+len(slots))
	for i := range addrs {
		touches = append(touches, PathTouch{Account: keccak.Sum256(addrs[i][:])})
	}
	for i := range slots {
		touches = append(touches, PathTouch{Account: keccak.Sum256(slots[i].addr[:]), Slot: keccak.Sum256(slots[i].slot[:]), Storage: true})
	}
	slices.SortFunc(touches, ComparePathTouch)
	return touches
}

func (bra *BlockReadAheader) warmStatic(ctx context.Context, p *BranchPrefetcher, header *types.Header, body *types.Body, recovery *SendersRecovery, workers int) error {
	var senders []accounts.Address
	if recovery != nil {
		var err error
		if senders, err = recovery.Wait(ctx); err != nil {
			return ctx.Err()
		}
	}
	addrs, slots := staticKeys(header, body, senders)
	p.Add(staticTouches(addrs, slots)...)
	total := len(addrs) + len(slots)
	var next atomic.Int64
	wg, ctx := errgroup.WithContext(ctx)
	for range min(workers, branchPrefetchTxs-branchPrefetchWorkers, (total+staticWarmBatch-1)/staticWarmBatch) {
		tx := p.borrow()
		if tx == nil {
			break
		}
		wg.Go(func() error {
			defer p.giveBack(tx)
			reader := state.NewReaderV3(readAheadGetter(tx, bra.stateCache, true))
			for from := int(next.Add(staticWarmBatch)) - staticWarmBatch; from < total; from = int(next.Add(staticWarmBatch)) - staticWarmBatch {
				if err := ctx.Err(); err != nil {
					return err
				}
				for i := from; i < min(from+staticWarmBatch, total); i++ {
					if i >= len(addrs) {
						s := slots[i-len(addrs)]
						_, _, _ = reader.ReadAccountStorageByPlainKey(accounts.InternAddress(s.addr), s.slot)
						continue
					}
					addr := accounts.InternAddress(addrs[i])
					if acc, err := reader.ReadAccountData(addr); err == nil && acc != nil && !acc.CodeHash.IsEmpty() {
						_, _ = reader.ReadAccountCode(addr)
					}
				}
			}
			return nil
		})
	}
	return wg.Wait()
}
