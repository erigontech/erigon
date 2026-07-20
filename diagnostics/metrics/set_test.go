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

import (
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestSetGetOrCreateReturnsExisting(t *testing.T) {
	s := NewSet()

	h, err := s.GetOrCreateHistogram("h")
	require.NoError(t, err)
	h2, err := s.GetOrCreateHistogram("h")
	require.NoError(t, err)
	require.Same(t, h, h2)

	c, err := s.GetOrCreateCounter("c")
	require.NoError(t, err)
	c2, err := s.GetOrCreateCounter("c")
	require.NoError(t, err)
	require.Same(t, c, c2)

	g, err := s.GetOrCreateGauge("g")
	require.NoError(t, err)
	g2, err := s.GetOrCreateGauge("g")
	require.NoError(t, err)
	require.Same(t, g, g2)

	sm, err := s.GetOrCreateSummary("sm")
	require.NoError(t, err)
	sm2, err := s.GetOrCreateSummary("sm")
	require.NoError(t, err)
	require.Same(t, sm, sm2)
}

func TestSetGetOrCreateWrongType(t *testing.T) {
	s := NewSet()

	_, err := s.GetOrCreateCounter("m")
	require.NoError(t, err)

	_, err = s.GetOrCreateGauge("m")
	require.ErrorContains(t, err, `metric "m" isn't a Gauge. It is`)

	_, err = s.GetOrCreateHistogram("m")
	require.ErrorContains(t, err, `metric "m" isn't a Histogram. It is`)

	_, err = s.GetOrCreateSummary("m")
	require.ErrorContains(t, err, `metric "m" isn't a Summary. It is`)
}

func TestSetGetOrCreateInvalidName(t *testing.T) {
	s := NewSet()

	_, err := s.GetOrCreateCounter("m{unclosed")
	require.ErrorContains(t, err, `invalid metric name "m{unclosed"`)

	_, err = s.GetOrCreateGaugeVec("m{unclosed", []string{"l"})
	require.ErrorContains(t, err, `invalid metric name "m{unclosed"`)
}

func TestSetGetOrCreateGaugeVecReturnsExisting(t *testing.T) {
	s := NewSet()

	gv, err := s.GetOrCreateGaugeVec("gv", []string{"a"})
	require.NoError(t, err)
	gv2, err := s.GetOrCreateGaugeVec("gv", []string{"b"})
	require.NoError(t, err)
	require.Same(t, gv, gv2)
}

func TestSetGetOrCreateGaugeVecWrongType(t *testing.T) {
	s := NewSet()

	cv := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "v"}, []string{"l"})
	s.mu.Lock()
	nmv := &namedMetricVec{name: "v", metric: cv}
	s.vecs["v"] = nmv
	s.av = append(s.av, nmv)
	s.mu.Unlock()

	_, err := s.GetOrCreateGaugeVec("v", []string{"l"})
	require.ErrorContains(t, err, `metric "v" isn't a GaugeVec. It is *prometheus.CounterVec`)
}

func TestSetGaugeVecDescribedOnce(t *testing.T) {
	s := NewSet()

	_, err := s.GetOrCreateGaugeVec("gv", []string{"a"})
	require.NoError(t, err)

	ch := make(chan *prometheus.Desc, 16)
	s.Describe(ch)
	close(ch)
	count := 0
	for range ch {
		count++
	}
	require.Equal(t, 1, count)
}

func TestSetGetOrCreateConcurrent(t *testing.T) {
	s := NewSet()

	const workers = 16
	counters := make([]prometheus.Counter, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			counters[i], errs[i] = s.GetOrCreateCounter("c")
		})
	}
	wg.Wait()

	for i := range workers {
		require.NoError(t, errs[i])
		require.Same(t, counters[0], counters[i])
	}
}
