// Copyright 2024 The Erigon Authors
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

package wrap

import (
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/state"
)

func NewTxContainer(Tx kv.RwTx, doms *state.SharedDomains) TxContainer {
	txContainer := TxContainer{
		Tx:   Tx,
		Doms: doms,
		Ttx:  nil,
	}
	if ttx, ok := Tx.(kv.TemporalTx); ok {
		txContainer.Ttx = ttx
	}
	return txContainer
}

type TxContainer struct {
	Tx   kv.RwTx
	Ttx  kv.TemporalTx
	Doms *state.SharedDomains
}
