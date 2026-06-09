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

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

var (
	//LatestStateReadWarm          = metrics.GetOrCreateSummary(`latest_state_read{type="warm",found="yes"}`)  //nolint
	//LatestStateReadWarmNotFound  = metrics.GetOrCreateSummary(`latest_state_read{type="warm",found="no"}`)   //nolint
	//LatestStateReadGrind         = metrics.GetOrCreateSummary(`latest_state_read{type="grind",found="yes"}`) //nolint
	//LatestStateReadGrindNotFound = metrics.GetOrCreateSummary(`latest_state_read{type="grind",found="no"}`)  //nolint
	//LatestStateReadCold          = metrics.GetOrCreateSummary(`latest_state_read{type="cold",found="yes"}`)  //nolint
	//LatestStateReadColdNotFound  = metrics.GetOrCreateSummary(`latest_state_read{type="cold",found="no"}`)   //nolint
	mxPruneTookAgg         = metrics.GetOrCreateSummary(`prune_seconds{type="state"}`)
	mxPrunableDAcc         = metrics.GetOrCreateGauge(`domain_prunable{type="domain",table="account"}`)
	mxPrunableDSto         = metrics.GetOrCreateGauge(`domain_prunable{type="domain",table="storage"}`)
	mxPrunableDCode        = metrics.GetOrCreateGauge(`domain_prunable{type="domain",table="code"}`)
	mxPrunableDComm        = metrics.GetOrCreateGauge(`domain_prunable{type="domain",table="commitment"}`)
	mxPrunableHAcc         = metrics.GetOrCreateGauge(`domain_prunable{type="history",table="account"}`)
	mxPrunableHSto         = metrics.GetOrCreateGauge(`domain_prunable{type="history",table="storage"}`)
	mxPrunableHCode        = metrics.GetOrCreateGauge(`domain_prunable{type="history",table="code"}`)
	mxPrunableHComm        = metrics.GetOrCreateGauge(`domain_prunable{type="history",table="commitment"}`)
	mxUnwindTook           = metrics.GetOrCreateHistogram(`domain_unwind_took{type="domain"}`)
	mxRunningUnwind        = metrics.GetOrCreateGauge("domain_running_unwind")
	mxRunningMerges        = metrics.GetOrCreateGauge("domain_running_merges")
	mxRunningFilesBuilding = metrics.GetOrCreateGauge("domain_running_files_building")
	mxCollateTook          = metrics.GetOrCreateHistogram(`domain_collate_took{type="domain"}`)
	mxCollateTookHistory   = metrics.GetOrCreateHistogram(`domain_collate_took{type="history"}`)
	mxCollateTookIndex     = metrics.GetOrCreateHistogram(`domain_collate_took{type="index"}`)
	mxPruneTookDomain      = metrics.GetOrCreateHistogram(`domain_prune_took{type="domain"}`)
	mxPruneTookHistory     = metrics.GetOrCreateHistogram(`domain_prune_took{type="history"}`)
	mxPruneTookIndex       = metrics.GetOrCreateHistogram(`domain_prune_took{type="index"}`)
	mxPruneInProgress      = metrics.GetOrCreateGauge("domain_pruning_progress")
	mxCollationSize        = metrics.GetOrCreateGauge("domain_collation_size")
	mxCollationSizeHist    = metrics.GetOrCreateGauge("domain_collation_hist_size")
	mxPruneSizeDomain      = metrics.GetOrCreateCounter(`domain_prune_size{type="domain"}`)
	mxPruneSizeHistory     = metrics.GetOrCreateCounter(`domain_prune_size{type="history"}`)
	mxPruneSizeIndex       = metrics.GetOrCreateCounter(`domain_prune_size{type="index"}`)
	mxDupsPruneSizeIndex   = metrics.GetOrCreateCounter(`domain_dups_prune_size{type="index"}`)
	mxBuildTook            = metrics.GetOrCreateSummary("domain_build_files_took")
	mxStepTook             = metrics.GetOrCreateSummary("domain_step_took")
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

	// Per (domain, level) split of a state read into its three phases:
	//   kvei_get  - existence (bloom) filter probe
	//   btnav_get - BTree navigation (pivot search + in-file key compares)
	//   kvval_get - value fetch from the .kv
	mxsKVEI  = newLevelledSummaries("kvei_get")
	mxsBtNav = newLevelledSummaries("btnav_get")
	mxsKVVal = newLevelledSummaries("kvval_get")
)

var kvLevelNames = []string{"L0", "L1", "L2", "L3", "L4", "recent"}

func newLevelledSummaries(metric string) [kv.DomainLen][]metrics.Summary {
	domNames := map[kv.Domain]string{
		kv.AccountsDomain:   "account",
		kv.StorageDomain:    "storage",
		kv.CodeDomain:       "code",
		kv.CommitmentDomain: "commitment",
		kv.ReceiptDomain:    "receipt",
		kv.RCacheDomain:     "rcache",
	}
	var out [kv.DomainLen][]metrics.Summary
	for d, dn := range domNames {
		arr := make([]metrics.Summary, len(kvLevelNames))
		for li, lv := range kvLevelNames {
			arr[li] = metrics.GetOrCreateSummary(fmt.Sprintf("%s{level=%q,domain=%q}", metric, lv, dn))
		}
		out[d] = arr
	}
	return out
}
