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
package metrics

import "sync"

// MaxGauge exports the largest value observed since process start.
type MaxGauge struct {
	Gauge
	mu  sync.Mutex
	max float64
}

func NewMaxGauge(name string) *MaxGauge {
	return &MaxGauge{Gauge: GetOrCreateGauge(name)}
}

func (m *MaxGauge) Observe(v float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if v > m.max {
		m.max = v
		m.Set(v)
	}
}
