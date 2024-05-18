/*
   Copyright 2024 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package state

import "github.com/ledgerwatch/erigon-lib/metrics"

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
	mxUnwindSharedTook     = metrics.GetOrCreateHistogram(`domain_unwind_took{type="shared"}`)
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
	mxBuildTook            = metrics.GetOrCreateSummary("domain_build_files_took")
	mxStepTook             = metrics.GetOrCreateSummary("domain_step_took")
	mxFlushTook            = metrics.GetOrCreateSummary("domain_flush_took")
	mxCommitmentRunning    = metrics.GetOrCreateGauge("domain_running_commitment")
	mxCommitmentTook       = metrics.GetOrCreateSummary("domain_commitment_took")
)
