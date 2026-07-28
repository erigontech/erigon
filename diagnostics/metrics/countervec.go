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

import "github.com/prometheus/client_golang/prometheus"

// Since we wrapped prometheus.Counter into a custom interface, we need to wrap prometheus.CounterVec into a custom struct
type CounterVec struct {
	*prometheus.CounterVec
}

func (cv *CounterVec) Collect(ch chan<- prometheus.Metric) {
	cv.MetricVec.Collect(ch)
}

func (cv *CounterVec) CurryWith(labels prometheus.Labels) (*CounterVec, error) {
	cv2, err := cv.CounterVec.CurryWith(labels)
	return &CounterVec{cv2}, err
}

func (cv *CounterVec) Delete(labels prometheus.Labels) bool {
	return cv.CounterVec.MetricVec.Delete(labels)
}

func (cv *CounterVec) DeleteLabelValues(lvs ...string) bool {
	return cv.CounterVec.MetricVec.DeleteLabelValues(lvs...)
}

func (cv *CounterVec) DeletePartialMatch(labels prometheus.Labels) int {
	return cv.CounterVec.MetricVec.DeletePartialMatch(labels)
}

func (cv *CounterVec) Describe(ch chan<- *prometheus.Desc) {
	cv.CounterVec.MetricVec.Describe(ch)
}

func (cv *CounterVec) GetMetricWith(labels prometheus.Labels) (Counter, error) {
	c, err := cv.CounterVec.GetMetricWith(labels)
	return &counter{c}, err
}

func (cv *CounterVec) GetMetricWithLabelValues(lvs ...string) (Counter, error) {
	c, err := cv.CounterVec.GetMetricWithLabelValues(lvs...)
	return &counter{c}, err
}

func (cv *CounterVec) MustCurryWith(labels prometheus.Labels) *CounterVec {
	return &CounterVec{cv.CounterVec.MustCurryWith(labels)}
}

func (cv *CounterVec) Reset() {
	cv.CounterVec.MetricVec.Reset()
}

func (cv *CounterVec) With(labels prometheus.Labels) Counter {
	return &counter{cv.CounterVec.With(labels)}
}

func (cv *CounterVec) WithLabelValues(lvs ...string) Counter {
	return &counter{cv.CounterVec.WithLabelValues(lvs...)}
}
