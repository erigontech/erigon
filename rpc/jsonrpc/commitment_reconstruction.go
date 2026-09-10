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
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
)

func commitmentReconstructionView(tx kv.TemporalTx) kv.TemporalTx {
	if agg, ok := tx.AggTx().(*dbstate.AggregatorRoTx); ok {
		return commitmentReconstructionTx{TemporalTx: tx, agg: commitmentReconstructionAgg{agg}}
	}
	return tx
}

// Reconstruction writes stay in private memory; the source transaction is read-only.
type commitmentReconstructionTx struct {
	kv.TemporalTx
	agg commitmentReconstructionAgg
}

func (tx commitmentReconstructionTx) AggTx() any { return &tx.agg }

type commitmentReconstructionAgg struct {
	*dbstate.AggregatorRoTx
}

func (*commitmentReconstructionAgg) IsDomainFrozen(kv.Domain) (uint64, bool) {
	return 0, false
}
