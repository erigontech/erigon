package erigon_lib

import "github.com/ledgerwatch/erigon-lib/metrics"

var (
	HeaderDelaySummary         = metrics.NewSummary(`block_production_delays{type="header"}`)
	BodyDelaySummary           = metrics.NewSummary(`block_production_delays{type="body"}`)
	ExecutionStartDelaySummary = metrics.NewSummary(`block_production_delays{type="execution_start"}`)
	ExecutionEndDelaySummary   = metrics.NewSummary(`block_production_delays{type="execution_end"}`)
	ProductionDelaySummary     = metrics.NewSummary(`block_production_delays{type="production"}`)
)
