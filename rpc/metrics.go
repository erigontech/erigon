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
	rpcRequestGauge    = metrics.GetOrCreateCounter("rpc_total")
	failedReqeustGauge = metrics.GetOrCreateCounter("rpc_failure")
)

func newRPCServingTimerMS(method string, valid bool) *metrics2.Summary {
	if valid {
		return metrics.GetOrCreateSummary(fmt.Sprintf(`rpc_duration_seconds{method="%s",success="success"}`, method))
	}
	return metrics.GetOrCreateSummary(fmt.Sprintf(`rpc_duration_seconds{method="%s",success="failure"}`, method))
}
