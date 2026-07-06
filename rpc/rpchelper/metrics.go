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

package rpchelper

import (
	"github.com/erigontech/erigon/diagnostics/metrics"
)

const (
	filterLabelName   = "filter"
	clientLabelName   = "client"
	protocolLabelName = "protocol"
)

var (
	activeSubscriptionsGauge         = metrics.GetOrCreateGaugeVec("subscriptions_active", []string{filterLabelName, protocolLabelName}, "Current number of active subscriptions")
	createdSubscriptionsCounter      = metrics.GetOrCreateCounterVec("subscriptions_created_total", []string{filterLabelName, protocolLabelName}, "Total number of subscriptions created")
	unsubscribedSubscriptionsCounter = metrics.GetOrCreateCounterVec("subscriptions_unsubscribed_total", []string{filterLabelName, protocolLabelName}, "Total number of subscriptions removed by client unsubscribe or timeout eviction")
	reapedSubscriptionsCounter       = metrics.GetOrCreateCounterVec("subscriptions_reaped_total", []string{filterLabelName}, "Total number of idle subscriptions evicted by timeout")

	activeSubscriptionsLogsAllAddressesGauge = metrics.GetOrCreateGauge("subscriptions_logs_all_addresses")
	activeSubscriptionsLogsAllTopicsGauge    = metrics.GetOrCreateGauge("subscriptions_logs_all_topics")
	activeSubscriptionsLogsAddressesGauge    = metrics.GetOrCreateGauge("subscriptions_logs_addresses")
	activeSubscriptionsLogsTopicsGauge       = metrics.GetOrCreateGauge("subscriptions_logs_topics")
	activeSubscriptionsLogsClientGauge       = metrics.GetOrCreateGaugeVec("subscriptions_logs_client", []string{clientLabelName}, "Current number of subscriptions by client")
)
