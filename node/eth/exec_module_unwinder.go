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

package eth

import (
	"context"

	"github.com/erigontech/erigon/execution/execmodule"
	storagecomp "github.com/erigontech/erigon/node/components/storage"
)

// providerUnwinderAdapter bridges execmodule.Unwinder (defined in
// execution/execmodule with no knowledge of the storage package) and
// *storagecomp.Provider (which has the real Unwind implementation but
// lives in a different layer). The adapter translates the
// execmodule.UnwindArgs struct to storagecomp.UnwindOpts; the two
// types intentionally mirror each other field-for-field so the
// translation is mechanical.
//
// Lives in node/eth (the wiring point) so neither execmodule nor
// storagecomp gains a dependency on the other.
type providerUnwinderAdapter struct {
	p *storagecomp.Provider
}

func newProviderUnwinderAdapter(p *storagecomp.Provider) execmodule.Unwinder {
	if p == nil {
		return nil
	}
	return providerUnwinderAdapter{p: p}
}

func (a providerUnwinderAdapter) BlockAligned() bool { return a.p.BlockAligned() }

func (a providerUnwinderAdapter) Unwind(ctx context.Context, toBlock uint64, args execmodule.UnwindArgs) error {
	return a.p.Unwind(ctx, toBlock, storagecomp.UnwindOpts{
		Tx:     args.Tx,
		Engine: args.Engine,
	})
}

func (a providerUnwinderAdapter) FinalizeUnwind() error { return a.p.FinalizeUnwind() }

func (a providerUnwinderAdapter) AbortUnwind() { a.p.AbortUnwind() }
