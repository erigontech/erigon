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

import "github.com/erigontech/erigon/db/kv"

type StateGetterOptions struct {
	metrics kv.GetLatestMetrics
}

type StateGetterOption func(StateGetterOptions) StateGetterOptions

func WithStateGetterMetrics(metrics kv.GetLatestMetrics) StateGetterOption {
	return func(opts StateGetterOptions) StateGetterOptions {
		opts.metrics = metrics
		return opts
	}
}

func ApplyStateGetterOptions(opts ...StateGetterOption) StateGetterOptions {
	var cfg StateGetterOptions
	for i := range opts {
		cfg = opts[i](cfg)
	}
	return cfg
}

func (opts StateGetterOptions) Metrics() kv.GetLatestMetrics {
	return opts.metrics
}

// StateGetter provides execution-aware reads over temporal state.
type StateGetter interface {
	kv.TemporalGetter
	// GetCode reports whether the address has non-empty code. It is read-only;
	// writes must use GetLatest to resolve the previous CodeDomain value.
	GetCode(addr []byte, txNum uint64) ([]byte, bool, error)
	// GetCodeSize reports whether the address has non-empty code.
	GetCodeSize(addr []byte, txNum uint64) (int, bool, error)
}
