package rpchelper

import (
	"github.com/ledgerwatch/erigon-lib/metrics"
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
