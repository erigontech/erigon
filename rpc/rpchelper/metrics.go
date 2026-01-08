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

// Protocol types for metrics labeling
const (
	ProtocolHTTP = "http"
	ProtocolWS   = "ws"
)

var (
	// activeSubscriptionsGauge tracks current active subscriptions by filter type and protocol
	activeSubscriptionsGauge = metrics.GetOrCreateGaugeVec(
		"subscriptions_active",
		[]string{filterLabelName, protocolLabelName},
		"Current number of active subscriptions",
	)

	activeSubscriptionsLogsAllAddressesGauge = metrics.GetOrCreateGauge("subscriptions_logs_all_addresses")
	activeSubscriptionsLogsAllTopicsGauge    = metrics.GetOrCreateGauge("subscriptions_logs_all_topics")
	activeSubscriptionsLogsAddressesGauge    = metrics.GetOrCreateGauge("subscriptions_logs_addresses")
	activeSubscriptionsLogsTopicsGauge       = metrics.GetOrCreateGauge("subscriptions_logs_topics")
	activeSubscriptionsLogsClientGauge       = metrics.GetOrCreateGaugeVec("subscriptions_logs_client", []string{clientLabelName}, "Current number of subscriptions by client")
)

// getSubscriptionCounter returns a counter for subscription lifecycle events.
// pattern: subscriptions_{event}_total{{filter="{filterType}",protocol="{protocol}"}}
func getSubscriptionCounter(event, filterType, protocol string) metrics.Counter {
	return metrics.GetOrCreateCounter(
		`subscriptions_` + event + `_total{filter="` + filterType + `",protocol="` + protocol + `"}`,
	)
}

// getReapedCounter returns a counter for reaped (timeout-evicted) subscriptions.
func getReapedCounter(filterType string) metrics.Counter {
	return metrics.GetOrCreateCounter(
		`subscriptions_reaped_total{filter="` + filterType + `"}`,
	)
}
