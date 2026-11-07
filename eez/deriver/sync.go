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
package deriver

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol"
)

// ErrSyncBlock reports a settlement whose Sync block the DA payload cannot
// supply.
var ErrSyncBlock = errors.New("settlement does not match its DA payload")

// SyncBlock is the content of a settlement's Sync block, the last block of
// its range.
type SyncBlock struct {
	// Txs is the block's transactions: each applied outbound entry's load
	// and user transaction, each applied delivery, then the remaining pure
	// transactions.
	Txs [][]byte
	// Outbound are the applied outbound entries, whose calls the block must
	// be seen to make.
	Outbound []protocol.ExecutionEntry
}

// BuildSyncBlock rebuilds the Sync block of settlement s from the payload,
// with system nonces counting from the system address nonce at the parent.
// Only the entries L1 applied contribute; each outbound entry pairs with the
// Sync-block user transaction at its own ordinal. A resumed settlement
// appends after its predecessor's system transactions, so the skipped
// prefix counts towards the nonces.
func BuildSyncBlock(p *Payload, s *protocol.Settlement, ctx protocol.SyncTxContext, nonce uint64) (*SyncBlock, error) {
	var outbound, inbound []protocol.ExecutionEntry
	for i := range p.Actions {
		e, err := protocol.EntryFromAction(&p.Actions[i], ctx.RollupID)
		if err != nil {
			return nil, fmt.Errorf("%w: action %d: %w", ErrSyncBlock, i, err)
		}
		if e.ProxyEntryHash == (common.Hash{}) {
			outbound = append(outbound, *e)
		} else {
			inbound = append(inbound, *e)
		}
	}
	userTxs := p.BlockTxs(int(p.Blocks()) - 1)
	if len(userTxs) < len(outbound) {
		return nil, fmt.Errorf("%w: %d outbound entries, %d Sync-block transactions", ErrSyncBlock, len(outbound), len(userTxs))
	}

	var applied []protocol.OutboundSettlement
	var deliveries []protocol.ExecutionEntry
	for _, a := range s.Applied {
		switch a.Role {
		case protocol.RoleOutbound:
			if a.Ordinal >= len(outbound) {
				return nil, fmt.Errorf("%w: entry %d is outbound %d of %d", ErrSyncBlock, a.Index, a.Ordinal, len(outbound))
			}
			applied = append(applied, protocol.OutboundSettlement{Entry: &outbound[a.Ordinal], UserTx: userTxs[a.Ordinal]})
		case protocol.RoleInbound:
			if a.Ordinal >= len(inbound) {
				return nil, fmt.Errorf("%w: entry %d is inbound %d of %d", ErrSyncBlock, a.Index, a.Ordinal, len(inbound))
			}
			deliveries = append(deliveries, inbound[a.Ordinal])
		}
	}

	var skipOut, skipIn int
	if len(s.Applied) > 0 {
		switch first := s.Applied[0]; first.Role {
		case protocol.RoleOutbound:
			skipOut = first.Ordinal
		case protocol.RoleInbound:
			skipOut, skipIn = len(outbound), first.Ordinal
		}
	}
	if skipOut > 0 || skipIn > 0 {
		prefix := make([]protocol.OutboundSettlement, skipOut)
		for i := range prefix {
			prefix[i] = protocol.OutboundSettlement{Entry: &outbound[i], UserTx: userTxs[i]}
		}
		pairs, err := protocol.BuildSyncPairs(prefix, inbound[:skipIn], ctx, nonce)
		if err != nil {
			return nil, fmt.Errorf("%w: skipped prefix: %w", ErrSyncBlock, err)
		}
		nonce += uint64(len(pairs))
	}
	pairs, err := protocol.BuildSyncPairs(applied, deliveries, ctx, nonce)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrSyncBlock, err)
	}
	sb := &SyncBlock{Txs: append(protocol.SyncBlockTxs(pairs), userTxs[len(outbound):]...)}
	for _, o := range applied {
		sb.Outbound = append(sb.Outbound, *o.Entry)
	}
	return sb, nil
}

// ResumeTxs returns the transactions to rebuild a Sync block with when a
// resumed settlement appends newContent to it, or false when the local block
// is already the settled one. A copy of newContent the block already ends
// with is replaced rather than appended twice.
func ResumeTxs(localHash, settled common.Hash, existing, newContent [][]byte) ([][]byte, bool) {
	if localHash == settled {
		return nil, false
	}
	base := existing
	if n := len(newContent); n > 0 && len(existing) >= n &&
		slices.EqualFunc(existing[len(existing)-n:], newContent, bytes.Equal) {
		base = existing[:len(existing)-n]
	}
	return append(slices.Clone(base), newContent...), true
}

// ErrResume reports a resumed settlement whose Sync block is neither the
// one it starts from nor the one it settles.
var ErrResume = errors.New("resumed Sync block matches neither the entry nor the settled state")

// ResumeNativeTxs is ResumeTxs under rollupX v0.1, where the gas limit of a
// native transaction depends on its position. The local block is the
// settled one when it ends with newContent, gas limits aside; otherwise it
// must be the block the settlement starts from, and newContent is appended
// to it.
func ResumeNativeTxs(localHash common.Hash, s *protocol.Settlement, existing, newContent [][]byte) ([][]byte, bool, error) {
	if len(newContent) > 0 && localHash == s.FinalState {
		start := len(existing) - len(newContent)
		if start < 0 {
			return nil, false, fmt.Errorf("%w: settled suffix of %d transactions in a block of %d", ErrResume, len(newContent), len(existing))
		}
		for i, tx := range newContent {
			same, err := sameUpToGas(existing[start+i], tx)
			if err != nil {
				return nil, false, err
			}
			if !same {
				return nil, false, fmt.Errorf("%w: settled suffix transaction %d differs", ErrResume, i)
			}
		}
		return nil, false, nil
	}
	if localHash != s.EntryState {
		return nil, false, fmt.Errorf("%w: local %x, entry %x, settled %x", ErrResume, localHash, s.EntryState, s.FinalState)
	}
	return append(slices.Clone(existing), newContent...), true, nil
}

// sameUpToGas reports whether a and b encode the same transaction, the gas
// limits of native transactions aside.
func sameUpToGas(a, b []byte) (bool, error) {
	if bytes.Equal(a, b) {
		return true, nil
	}
	if len(a) == 0 || len(b) == 0 || a[0] != protocol.NativeTxType || b[0] != protocol.NativeTxType {
		return false, nil
	}
	x, err := protocol.DecodeNativeTx(a)
	if err != nil {
		return false, err
	}
	y, err := protocol.DecodeNativeTx(b)
	if err != nil {
		return false, err
	}
	x.GasLimit, y.GasLimit = 0, 0
	ex, _ := x.MarshalBinary()
	ey, _ := y.MarshalBinary()
	return bytes.Equal(ex, ey), nil
}
