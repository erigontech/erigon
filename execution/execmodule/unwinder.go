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

package execmodule

import (
	"context"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
)

// Unwinder is the seam between SetHead's existing exec-stage unwind
// pipeline and the storage-layer admin unwind primitive (production:
// *node/components/storage.Provider). Defined here as a narrow
// interface so execution/execmodule has no direct dependency on the
// storage package; the production wiring in node/eth/backend.go uses
// a thin adapter to satisfy this interface.
//
// SetHead calls Unwinder *after* its existing RunUnwind pass has
// rolled the temporal db and aggregator state to toBlock. The
// Unwinder is responsible for the storage-layer sub-ops that the
// exec-stage path cannot handle on its own — commitment-entry write
// at an arbitrary block, and (once implemented) snapshot file trim
// past toBlock.
//
// Nil-valued unwinder: SetHead skips the call entirely, preserving
// the pre-Provider.Unwind behavior. This is the case for the
// execmoduletester harness, which does not stand up a storage
// Provider; production wires the real adapter via backend.go.
type Unwinder interface {
	Unwind(ctx context.Context, toBlock uint64, args UnwindArgs) error
}

// UnwindArgs carries the inputs SetHead supplies to the Unwinder.
// Mirrors storage.UnwindOpts shape; the adapter at the wiring point
// translates between the two so neither package leaks into the other.
type UnwindArgs struct {
	// BlockAligned is true on chains configured with
	// --snap.block-aligned-boundaries. The storage-layer admin
	// unwind is only safe to run in aligned mode; non-aligned
	// callers fall back to the existing CanUnwindToBlockNum guard.
	BlockAligned bool

	// TxNum is the last txNum at toBlock — caller computes via
	// rawdbv3.TxNums.Max(ctx, tx, toBlock).
	TxNum uint64

	// TrieState is the encoded patricia trie state at toBlock,
	// produced by HexPatriciaHashed.EncodeCurrentState(nil) (or the
	// concurrent variant's RootTrie().EncodeCurrentState(nil)). The
	// caller positions the trie at toBlock before encoding.
	TrieState []byte

	// Domains is the SharedDomains handle whose CommitmentDomain
	// the commitment-entry write goes through.
	// *db/state/execctx.SharedDomains satisfies
	// commitmentdb.CommitmentStateWriter structurally.
	Domains commitmentdb.CommitmentStateWriter

	// Tx is the temporal transaction the commitment-entry write is
	// performed inside. SetHead owns its lifecycle.
	Tx kv.TemporalTx
}
