// Copyright 2026 The Erigon Authors
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

package jsonrpc

import "github.com/erigontech/erigon/diagnostics/metrics"

var (
	witnessCacheHitCounter             = metrics.GetOrCreateCounter("witness_cache_hit_total")
	witnessCacheMissCounter            = metrics.GetOrCreateCounter("witness_cache_miss_total")
	witnessCacheBuildOKCounter         = metrics.GetOrCreateCounter("witness_cache_build_ok_total")
	witnessCacheBuildFailVerifyCounter = metrics.GetOrCreateCounter("witness_cache_build_fail_verify_total")
	witnessCacheBuildFailOtherCounter  = metrics.GetOrCreateCounter("witness_cache_build_fail_other_total")
	witnessCacheCoalesceDropCounter    = metrics.GetOrCreateCounter("witness_cache_coalesce_drop_total")

	witnessCacheEntriesResidentGauge = metrics.GetOrCreateGauge("witness_cache_entries_resident")

	witnessCacheBuildDuration = metrics.GetOrCreateHistogram("witness_cache_build_duration_seconds")
)
