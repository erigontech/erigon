// Copyright 2020 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package rpc

import (
	"fmt"

	metrics2 "github.com/VictoriaMetrics/metrics"
	"github.com/ledgerwatch/erigon/metrics"
)

var (
	rpcMetricsLabels   = map[bool]map[string]string{}
	rpcRequestGauge    = metrics.GetOrCreateCounter("rpc_total")
	failedReqeustGauge = metrics.GetOrCreateCounter("rpc_failure")
)

func preAllocateRPCMetricLabels(methods MethodList) {
	successMap, ok := rpcMetricsLabels[true]
	if !ok {
		successMap = make(map[string]string)
	}

	failureMap, ok := rpcMetricsLabels[false]
	if !ok {
		failureMap = make(map[string]string)
	}

	for method := range methods {
		successMap[method] = createRPCMetricsLabel(method, true)
		failureMap[method] = createRPCMetricsLabel(method, false)
	}

	rpcMetricsLabels[true] = successMap
	rpcMetricsLabels[false] = failureMap
}

func createRPCMetricsLabel(method string, valid bool) string {
	status := "failure"
	if valid {
		status = "success"
	}

	return fmt.Sprintf(`rpc_duration_seconds{method="%s",success="%s"}`, method, status)

}

func newRPCServingTimerMS(method string, valid bool) *metrics2.Summary {
	label, ok := rpcMetricsLabels[valid][method]
	if !ok {
		label = createRPCMetricsLabel(method, valid)
	}

	return metrics.GetOrCreateSummary(label)
}
