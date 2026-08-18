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

package execctxapi

import (
	"context"

	"github.com/erigontech/erigon/db/kv"
)

// StateGetter provides execution-aware reads over temporal state.
type StateGetter interface {
	kv.TemporalGetter
	GetLatestContext(ctx context.Context, name kv.Domain, k []byte) ([]byte, kv.Step, error)
	GetCode(addr []byte, txNum uint64) ([]byte, bool, error)
	GetCodeSize(addr []byte, txNum uint64) (int, bool, error)
}
