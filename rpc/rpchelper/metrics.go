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
	"github.com/erigontech/erigon-lib/metrics"
)

const (
	filterLabelName = "filter"
	clientLabelName = "client"
)

var (
	activeSubscriptionsGauge                 = metrics.GetOrCreateGaugeVec("subscriptions", []string{filterLabelName}, "Current number of subscriptions")
	activeSubscriptionsLogsAllAddressesGauge = metrics.GetOrCreateGauge("subscriptions_logs_all_addresses")
	activeSubscriptionsLogsAllTopicsGauge    = metrics.GetOrCreateGauge("subscriptions_logs_all_topics")
	activeSubscriptionsLogsAddressesGauge    = metrics.GetOrCreateGauge("subscriptions_logs_addresses")
	activeSubscriptionsLogsTopicsGauge       = metrics.GetOrCreateGauge("subscriptions_logs_topics")
	activeSubscriptionsLogsClientGauge       = metrics.GetOrCreateGaugeVec("subscriptions_logs_client", []string{clientLabelName}, "Current number of subscriptions by client")
)
