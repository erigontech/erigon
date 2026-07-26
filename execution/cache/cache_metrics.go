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

package cache

import "github.com/erigontech/erigon/diagnostics/metrics"

var (
	mxAccountCacheHitRate  = metrics.NewGauge(`state_cache_hit_rate{domain="account"}`)
	mxAccountCacheEntries  = metrics.NewGauge(`state_cache_entries{domain="account"}`)
	mxAccountCacheSizeMB   = metrics.NewGauge(`state_cache_size_mb{domain="account"}`)
	mxAccountCacheUsagePct = metrics.NewGauge(`state_cache_usage_pct{domain="account"}`)

	mxStorageCacheHitRate  = metrics.NewGauge(`state_cache_hit_rate{domain="storage"}`)
	mxStorageCacheEntries  = metrics.NewGauge(`state_cache_entries{domain="storage"}`)
	mxStorageCacheSizeMB   = metrics.NewGauge(`state_cache_size_mb{domain="storage"}`)
	mxStorageCacheUsagePct = metrics.NewGauge(`state_cache_usage_pct{domain="storage"}`)

	mxCodeAddrCacheHitRate  = metrics.NewGauge(`state_cache_hit_rate{domain="code_addr"}`)
	mxCodeAddrCacheEntries  = metrics.NewGauge(`state_cache_entries{domain="code_addr"}`)
	mxCodeAddrCacheSizeMB   = metrics.NewGauge(`state_cache_size_mb{domain="code_addr"}`)
	mxCodeAddrCacheUsagePct = metrics.NewGauge(`state_cache_usage_pct{domain="code_addr"}`)

	mxCodeCacheHitRate  = metrics.NewGauge(`state_cache_hit_rate{domain="code"}`)
	mxCodeCacheEntries  = metrics.NewGauge(`state_cache_entries{domain="code"}`)
	mxCodeCacheSizeMB   = metrics.NewGauge(`state_cache_size_mb{domain="code"}`)
	mxCodeCacheUsagePct = metrics.NewGauge(`state_cache_usage_pct{domain="code"}`)
)
