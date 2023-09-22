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
	"reflect"
	"strings"

	metrics2 "github.com/VictoriaMetrics/metrics"
	"github.com/ledgerwatch/erigon/metrics"
)

var (
	rpcMetricsLabels   = map[bool]map[string]string{}
	rpcRequestGauge    = metrics.GetOrCreateCounter("rpc_total")
	failedReqeustGauge = metrics.GetOrCreateCounter("rpc_failure")
)

// PreAllocateRPCMetricLabels pre-allocates labels for all rpc methods inside API List
func PreAllocateRPCMetricLabels(apiList []API) {
	methods := getRPCMethodNames(apiList)

	successMap, ok := rpcMetricsLabels[true]
	if !ok {
		successMap = make(map[string]string)
	}

	failureMap, ok := rpcMetricsLabels[false]
	if !ok {
		failureMap = make(map[string]string)
	}

	for _, method := range methods {
		successMap[method] = createRPCMetricsLabel(method, true)
		failureMap[method] = createRPCMetricsLabel(method, false)
	}

	rpcMetricsLabels[true] = successMap
	rpcMetricsLabels[false] = failureMap
}

func getRPCMethodNames(apiList []API) (methods []string) {
	for _, api := range apiList {
		apiType := reflect.TypeOf(api.Service)

		for i := 0; i < apiType.NumMethod(); i++ {
			method := apiType.Method(i)
			rpcMethod := fmt.Sprintf("%s_%s", api.Namespace, pascalToCamel(method.Name))
			methods = append(methods, rpcMethod)
		}
	}

	return
}

func pascalToCamel(input string) string {
	if input == "" || strings.ToLower(input[0:1]) == input[0:1] {
		return input
	}

	return strings.ToLower(input[0:1]) + input[1:]
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
