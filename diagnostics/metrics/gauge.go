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

package metrics

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type Gauge interface {
	prometheus.Gauge
	ValueGetter
	SetUint32(v uint32)
	SetUint64(v uint64)
	SetInt(v int)
}

type gauge struct {
	prometheus.Gauge
}

// GetValue returns native float64 value stored by this gauge
func (g *gauge) GetValue() float64 {
	var m dto.Metric
	if err := g.Write(&m); err != nil {
		panic(fmt.Errorf("calling GetValue with invalid metric: %w", err))
	}

	return m.GetGauge().GetValue()
}

// GetValueUint64 returns native float64 value stored by this gauge cast to
// an uint64 value for convenience
func (g *gauge) GetValueUint64() uint64 {
	return uint64(g.GetValue())
}

// SetUint32 sets gauge using an uint32 value. Note under the hood this
// is a cast to float64 which is the native type of prometheus gauges.
//
// This is a convenience function for better UX.
func (g *gauge) SetUint32(v uint32) {
	g.Set(float64(v))
}

// SetUint64 sets gauge using an uint64 value. Note under the hood this
// is a cast to float64 which is the native type of prometheus gauges.
//
// This is a convenience function for better UX which is safe for uints up
// to 2^53 (mantissa bits).
//
// This is fine for all usages in our codebase, and it is
// unlikely we will hit issues with this.
//
// If, however there is a new requirement that requires accuracy for more than
// 2^53 we can implement our own simple uintGauge that satisfies the Gauge
// interface.
func (g *gauge) SetUint64(v uint64) {
	g.Set(float64(v))
}

// SetInt sets gauge using an int value. Note under the hood this
// is a cast to float64 which is the native type of prometheus gauges.
//
// This is a convenience function for better UX which is safe for uints up
// to 2^53 (mantissa bits).
//
// This is fine for all usages in our codebase, and it is
// unlikely we will hit issues with this.
//
// If, however there is a new requirement that requires accuracy for more than
// 2^53 we can implement our own simple intGauge that satisfies the Gauge
// interface.
func (g *gauge) SetInt(v int) {
	g.Set(float64(v))
}
