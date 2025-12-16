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

package state

import (
	"fmt"
	"os"
	"strings"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

var pruneBuild = "new_prune"

func withBuild(metric string) string {
	if strings.Contains(metric, "{") {
		return strings.Replace(metric, "{", fmt.Sprintf("{build=%q,", pruneBuild), 1)
	}
	return fmt.Sprintf(`%s{build=%q}`, metric, pruneBuild)
}

var (
	//LatestStateReadWarm          = metrics.GetOrCreateSummary(`latest_state_read{type="warm",found="yes"}`)  //nolint
	//LatestStateReadWarmNotFound  = metrics.GetOrCreateSummary(`latest_state_read{type="warm",found="no"}`)   //nolint
	//LatestStateReadGrind         = metrics.GetOrCreateSummary(`latest_state_read{type="grind",found="yes"}`) //nolint
	//LatestStateReadGrindNotFound = metrics.GetOrCreateSummary(`latest_state_read{type="grind",found="no"}`)  //nolint
	//LatestStateReadCold          = metrics.GetOrCreateSummary(`latest_state_read{type="cold",found="yes"}`)  //nolint
	//LatestStateReadColdNotFound  = metrics.GetOrCreateSummary(`latest_state_read{type="cold",found="no"}`)   //nolint
	build          = os.Getenv("ERIGON_PRUNE_BUILD")
	mxPruneTookAgg = metrics.GetOrCreateSummary(
		withBuild(`prune_seconds{type="state"}`),
	)

	mxPrunableDAcc  = metrics.GetOrCreateGauge(withBuild(`domain_prunable{type="domain",table="account"}`))
	mxPrunableDSto  = metrics.GetOrCreateGauge(withBuild(`domain_prunable{type="domain",table="storage"}`))
	mxPrunableDCode = metrics.GetOrCreateGauge(withBuild(`domain_prunable{type="domain",table="code"}`))
	mxPrunableDComm = metrics.GetOrCreateGauge(withBuild(`domain_prunable{type="domain",table="commitment"}`))

	mxPrunableHAcc  = metrics.GetOrCreateGauge(withBuild(`domain_prunable{type="history",table="account"}`))
	mxPrunableHSto  = metrics.GetOrCreateGauge(withBuild(`domain_prunable{type="history",table="storage"}`))
	mxPrunableHCode = metrics.GetOrCreateGauge(withBuild(`domain_prunable{type="history",table="code"}`))
	mxPrunableHComm = metrics.GetOrCreateGauge(withBuild(`domain_prunable{type="history",table="commitment"}`))

	mxUnwindTook = metrics.GetOrCreateHistogram(
		withBuild(`domain_unwind_took{type="domain"}`),
	)

	mxRunningUnwind        = metrics.GetOrCreateGauge(withBuild("domain_running_unwind"))
	mxRunningMerges        = metrics.GetOrCreateGauge(withBuild("domain_running_merges"))
	mxRunningFilesBuilding = metrics.GetOrCreateGauge(withBuild("domain_running_files_building"))

	mxCollateTook        = metrics.GetOrCreateHistogram(withBuild(`domain_collate_took{type="domain"}`))
	mxCollateTookHistory = metrics.GetOrCreateHistogram(withBuild(`domain_collate_took{type="history"}`))
	mxCollateTookIndex   = metrics.GetOrCreateHistogram(withBuild(`domain_collate_took{type="index"}`))

	mxPruneTookDomain  = metrics.GetOrCreateHistogram(withBuild(`domain_prune_took{type="domain"}`))
	mxPruneTookHistory = metrics.GetOrCreateHistogram(withBuild(`domain_prune_took{type="history"}`))
	mxPruneTookIndex   = metrics.GetOrCreateHistogram(withBuild(`domain_prune_took{type="index"}`))

	mxPruneInProgress = metrics.GetOrCreateGauge(withBuild("domain_pruning_progress"))

	mxCollationSize     = metrics.GetOrCreateGauge(withBuild("domain_collation_size"))
	mxCollationSizeHist = metrics.GetOrCreateGauge(withBuild("domain_collation_hist_size"))

	mxPruneSizeDomain  = metrics.GetOrCreateCounter(withBuild(`domain_prune_size{type="domain"}`))
	mxPruneSizeHistory = metrics.GetOrCreateCounter(withBuild(`domain_prune_size{type="history"}`))
	mxPruneSizeIndex   = metrics.GetOrCreateCounter(withBuild(`domain_prune_size{type="index"}`))

	mxDupsPruneSizeIndex = metrics.GetOrCreateCounter(
		withBuild(`domain_dups_prune_size{type="index"}`),
	)

	mxBuildTook = metrics.GetOrCreateSummary(
		withBuild("domain_build_files_took"),
	)

	mxStepTook = metrics.GetOrCreateSummary(
		withBuild("domain_step_took"),
	)
)

var (
	branchKeyDerefSpent = []metrics.Summary{
		metrics.GetOrCreateSummary(`branch_key_deref{level="L0"}`),
		metrics.GetOrCreateSummary(`branch_key_deref{level="L1"}`),
		metrics.GetOrCreateSummary(`branch_key_deref{level="L2"}`),
		metrics.GetOrCreateSummary(`branch_key_deref{level="L3"}`),
		metrics.GetOrCreateSummary(`branch_key_deref{level="L4"}`),
		metrics.GetOrCreateSummary(`branch_key_deref{level="recent"}`),
	}
)

var (
	mxsKVGet = [kv.DomainLen][]metrics.Summary{
		kv.AccountsDomain: {
			metrics.GetOrCreateSummary(`kv_get{level="L0",domain="account"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L1",domain="account"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L2",domain="account"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L3",domain="account"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L4",domain="account"}`),
			metrics.GetOrCreateSummary(`kv_get{level="recent",domain="account"}`),
		},
		kv.StorageDomain: {
			metrics.GetOrCreateSummary(`kv_get{level="L0",domain="storage"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L1",domain="storage"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L2",domain="storage"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L3",domain="storage"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L4",domain="storage"}`),
			metrics.GetOrCreateSummary(`kv_get{level="recent",domain="storage"}`),
		},
		kv.CodeDomain: {
			metrics.GetOrCreateSummary(`kv_get{level="L0",domain="code"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L1",domain="code"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L2",domain="code"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L3",domain="code"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L4",domain="code"}`),
			metrics.GetOrCreateSummary(`kv_get{level="recent",domain="code"}`),
		},
		kv.CommitmentDomain: {
			metrics.GetOrCreateSummary(`kv_get{level="L0",domain="commitment"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L1",domain="commitment"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L2",domain="commitment"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L3",domain="commitment"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L4",domain="commitment"}`),
			metrics.GetOrCreateSummary(`kv_get{level="recent",domain="commitment"}`),
		},
		kv.ReceiptDomain: {
			metrics.GetOrCreateSummary(`kv_get{level="L0",domain="receipt"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L1",domain="receipt"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L2",domain="receipt"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L3",domain="receipt"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L4",domain="receipt"}`),
			metrics.GetOrCreateSummary(`kv_get{level="recent",domain="receipt"}`),
		},
		kv.RCacheDomain: {
			metrics.GetOrCreateSummary(`kv_get{level="L0",domain="rcache"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L1",domain="rcache"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L2",domain="rcache"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L3",domain="rcache"}`),
			metrics.GetOrCreateSummary(`kv_get{level="L4",domain="rcache"}`),
			metrics.GetOrCreateSummary(`kv_get{level="recent",domain="rcache"}`),
		},
	}
)
