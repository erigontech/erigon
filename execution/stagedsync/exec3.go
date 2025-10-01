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

package stagedsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/cmp"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/estimate"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/metrics"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/exec"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/rawdbhelpers"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/exec3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/turbo/shards"
)

var (
	mxExecStepsInDB    = metrics.NewGauge(`exec_steps_in_db`) //nolint
	mxExecRepeats      = metrics.NewGauge(`exec_repeats`)     //nolint
	mxExecTriggers     = metrics.NewGauge(`exec_triggers`)    //nolint
	mxExecTransactions = metrics.NewGauge(`exec_txns`)
	mxExecTxnPerBlock  = metrics.NewGauge(`exec_txns_per_block`)
	mxExecGasPerTxn    = metrics.NewGauge(`exec_gas_per_transaction`)
	mxExecBlocks       = metrics.NewGauge("exec_blocks")
	mxExecCPUs         = metrics.NewGauge("exec_cpus")
	mxExecMGasSec      = metrics.NewGauge(`exec_mgas_sec`)
	mxTaskMgasSec      = metrics.NewGauge(`exec_task_mgas_sec`)

	mxExecBlockDuration = metrics.NewGauge("exec_block_dur")

	mxExecTxnDuration             = metrics.NewGauge("exec_txn_dur")
	mxExecTxnExecDuration         = metrics.NewGauge("exec_txn_exec_dur")
	mxExecTxnReadDuration         = metrics.NewGauge("exec_txn_read_dur")
	mxExecTxnAccountReadDuration  = metrics.NewGauge("exec_txn_account_read_dur")
	mxExecTxnStoreageReadDuration = metrics.NewGauge("exec_txn_storage_read_dur")
	mxExecTxnCodeReadDuration     = metrics.NewGauge("exec_txn_code_read_dur")

	mxExecReadRate        = metrics.NewGauge("exec_read_rate")
	mxExecAccountReadRate = metrics.NewGauge("exec_account_read_rate")
	mxExecStorageReadRate = metrics.NewGauge("exec_storage_read_rate")
	mxExecCodeReadRate    = metrics.NewGauge("exec_code_read_rate")
	mxExecWriteRate       = metrics.NewGauge("exec_write_rate")

	mxExecDomainReads             = metrics.NewGauge(`exec_domain_read_rate{domain="all"}`)
	mxExecDomainReadDuration      = metrics.NewGauge(`exec_domain_read_dur{domain="all"}`)
	mxExecDomainCacheReads        = metrics.NewGauge(`exec_domain_cache_read_rate{domain="all"}`)
	mxExecDomainCacheReadDuration = metrics.NewGauge(`exec_domain_cache_read_dur{domain="all"}`)
	mxExecDomainPutRate           = metrics.NewGauge(`exec_domain_cache_put_rate{domain="all"}`)
	mxExecDomainPutSize           = metrics.NewGauge(`exec_domain_cache_put_size{domain="all"}`)
	mxExecDomainDbReads           = metrics.NewGauge(`exec_domain_db_read_rate{domain="all"}`)
	mxExecDomainDbReadDuration    = metrics.NewGauge(`exec_domain_db_read_dur{domain="all"}`)
	mxExecDomainFileReads         = metrics.NewGauge(`exec_domain_file_read_rate{domain="all"}`)
	mxExecDomainFileReadDuration  = metrics.NewGauge(`exec_domain_file_read_dur{domain="all"}`)

	mxExecAccountDomainReads             = metrics.NewGauge(`exec_domain_read_rate{domain="account"}`)
	mxExecAccountDomainReadDuration      = metrics.NewGauge(`exec_domain_read_dur{domain="account"}`)
	mxExecAccountDomainCacheReads        = metrics.NewGauge(`exec_domain_cache_read_rate{domain="account"}`)
	mxExecAccountDomainCacheReadDuration = metrics.NewGauge(`exec_domain_cache_read_dur{domain="account"}`)
	mxExecAccountDomainPutRate           = metrics.NewGauge(`exec_domain_cache_put_rate{domain="account"}`)
	mxExecAccountDomainPutSize           = metrics.NewGauge(`exec_domain_cache_put_size{domain="account"}`)
	mxExecAccountDomainDbReads           = metrics.NewGauge(`exec_domain_db_read_rate{domain="account"}`)
	mxExecAccountDomainDbReadDuration    = metrics.NewGauge(`exec_domain_db_read_dur{domain="account"}`)
	mxExecAccountDomainFileReads         = metrics.NewGauge(`exec_domain_file_read_rate{domain="account"}`)
	mxExecAccountDomainFileReadDuration  = metrics.NewGauge(`exec_domain_file_read_dur{domain="account"}`)

	mxExecStorageDomainReads             = metrics.NewGauge(`exec_domain_read_rate{domain="storage"}`)
	mxExecStorageDomainReadDuration      = metrics.NewGauge(`exec_domain_read_dur{domain="storage"}`)
	mxExexStorageDomainCacheReads        = metrics.NewGauge(`exec_domain_cache_read_rate{domain="storage"}`)
	mxExecStorageDomainCacheReadDuration = metrics.NewGauge(`exec_domain_cache_read_dur{domain="storage"}`)
	mxExecStorageDomainPutRate           = metrics.NewGauge(`exec_domain_cache_put_rate{domain="storage"}`)
	mxExecStorageDomainPutSize           = metrics.NewGauge(`exec_domain_cache_put_size{domain="storage"}`)
	mxExecStorageDomainDbReads           = metrics.NewGauge(`exec_domain_db_read_rate{domain="storage"}`)
	mxExecStorageDomainDbReadDuration    = metrics.NewGauge(`exec_domain_db_read_dur{domain="storage"}`)
	mxExecStorageDomainFileReads         = metrics.NewGauge(`exec_domain_file_read_rate{domain="storage"}`)
	mxExecStorageDomainFileReadDuration  = metrics.NewGauge(`exec_domain_file_read_dur{domain="storage"}`)

	mxExecCodeDomainReads             = metrics.NewGauge(`exec_domain_read_rate{domain="code"}`)
	mxExecCodeDomainReadDuration      = metrics.NewGauge(`exec_domain_read_dur{domain="code"}`)
	mxExexCodeDomainCacheReads        = metrics.NewGauge(`exec_domain_cache_read_rate{domain="code"}`)
	mxExecCodeDomainCacheReadDuration = metrics.NewGauge(`exec_domain_cache_read_dur{domain="code"}`)
	mxExecCodeDomainPutRate           = metrics.NewGauge(`exec_domain_cache_put_rate{domain="code"}`)
	mxExecCodeDomainPutSize           = metrics.NewGauge(`exec_domain_cache_put_size{domain="code"}`)
	mxExecCodeDomainDbReads           = metrics.NewGauge(`exec_domain_db_read_rate{domain="code"}`)
	mxExecCodeDomainDbReadDuration    = metrics.NewGauge(`exec_domain_db_read_dur{domain="code"}`)
	mxExecCodeDomainFileReads         = metrics.NewGauge(`exec_domain_file_read_rate{domain="code"}`)
	mxExecCodeDomainFileReadDuration  = metrics.NewGauge(`exec_domain_file_read_dur{domain="code"}`)

	mxCommitmentTransactions            = metrics.NewGauge(`commit_txns`)
	mxCommitmentBlocks                  = metrics.NewGauge("commit_blocks")
	mxCommitmentMGasSec                 = metrics.NewGauge(`commit_mgas_sec`)
	mxCommitmentBlockDuration           = metrics.NewGauge("commit_block_dur")
	mxCommitmentReadRate                = metrics.NewGauge("commit_read_rate")
	mxCommitmentAccountReadRate         = metrics.NewGauge("commit_account_read_rate")
	mxCommitmentStorageReadRate         = metrics.NewGauge("commit_storage_read_rate")
	mxCommitmentBranchReadRate          = metrics.NewGauge("commit_branch_read_rate")
	mxCommitmentBrancgWriteRate         = metrics.NewGauge("commit_branch_write_rate")
	mxCommitmentKeyRate                 = metrics.NewGauge("commit_key_rate")
	mxCommitmentAccountKeyRate          = metrics.NewGauge("commit_account_key_rate")
	mxCommitmentStorageKeyRate          = metrics.NewGauge("commit_storage_key_rate")
	mxCommitmentFoldRate                = metrics.NewGauge("commit_fold_rate")
	mxCommitmentUnfoldRate              = metrics.NewGauge("commit_unfold_rate")
	mxCommitmentDomainReads             = metrics.NewGauge(`exec_domain_read_rate{domain="commitment"}`)
	mxCommitmentDomainReadDuration      = metrics.NewGauge(`exec_domain_read_dur{domain="commitment"}`)
	mxCommitmentDomainCacheReads        = metrics.NewGauge(`exec_domain_cache_read_rate{domain="commitment"}`)
	mxCommitmentDomainCacheReadDuration = metrics.NewGauge(`exec_domain_cache_read_dur{domain="commitment"}`)
	mxCommitmentDomainPutRate           = metrics.NewGauge(`exec_domain_cache_put_rate{domain="commitment"}`)
	mxCommitmentDomainPutSize           = metrics.NewGauge(`exec_domain_cache_put_size{domain="commitment"}`)
	mxCommitmentDomainDbReads           = metrics.NewGauge(`exec_domain_db_read_rate{domain="commitment"}`)
	mxCommitmentDomainDbReadDuration    = metrics.NewGauge(`exec_domain_db_read_dur{domain="commitment"}`)
	mxCommitmentDomainFileReads         = metrics.NewGauge(`exec_domain_file_read_rate{domain="commitment"}`)
	mxCommitmentDomainFileReadDuration  = metrics.NewGauge(`exec_domain_file_read_dur{domain="commitment"}`)
)

var ErrWrongTrieRoot = fmt.Errorf("%w: wrong trie root", consensus.ErrInvalidBlock)

const (
	maxUnwindJumpAllowance = 1000 // Maximum number of blocks we are allowed to unwind
)

type gaugeResetTask struct {
	*time.Timer
	sync.Mutex
	ctx     context.Context
	gauges  []metrics.Gauge
	stopped bool
}

func (g *gaugeResetTask) run(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				g.Lock()
				defer g.Unlock()
				g.reset()
				g.stopped = true
				return
			case <-g.C:
				g.Lock()
				defer g.Unlock()
				g.reset()
				g.stopped = true
				return
			}
		}
	}()
}

func (g *gaugeResetTask) reset() {
	for _, gauge := range g.gauges {
		gauge.Set(0)
	}
}

var execResetTask = gaugeResetTask{}
var commitResetTask = gaugeResetTask{}
var domainResetTask = gaugeResetTask{}

// enough time to alow the sampler to scrape
const resetDelay = 60 * time.Second

func resetExecGauges(ctx context.Context) {
	execResetTask.Lock()
	defer execResetTask.Unlock()
	if execResetTask.Timer != nil {
		if execResetTask.stopped {
			execResetTask.Timer = time.NewTimer(resetDelay)
		} else {
			execResetTask.Reset(resetDelay)
		}
	} else {
		execResetTask.Timer = time.NewTimer(resetDelay)
		execResetTask.ctx = ctx
		execResetTask.gauges = []metrics.Gauge{
			mxExecStepsInDB, mxExecRepeats, mxExecTriggers, mxExecTransactions,
			mxExecTxnPerBlock, mxExecGasPerTxn, mxExecBlocks, mxExecCPUs,
			mxExecMGasSec, mxTaskMgasSec, mxExecBlockDuration, mxExecTxnDuration,
			mxExecTxnExecDuration, mxExecTxnReadDuration, mxExecTxnAccountReadDuration, mxExecTxnStoreageReadDuration,
			mxExecTxnCodeReadDuration, mxExecReadRate, mxExecAccountReadRate, mxExecStorageReadRate,
			mxExecCodeReadRate, mxExecWriteRate}
		execResetTask.run(ctx)
	}
}

func resetCommitmentGauges(ctx context.Context) {
	commitResetTask.Lock()
	defer commitResetTask.Unlock()
	if commitResetTask.Timer != nil {
		if commitResetTask.stopped {
			commitResetTask.Timer = time.NewTimer(resetDelay)
		} else {
			commitResetTask.Reset(resetDelay)
		}
	} else {
		commitResetTask.Timer = time.NewTimer(resetDelay)
		commitResetTask.ctx = ctx
		commitResetTask.gauges = []metrics.Gauge{
			mxCommitmentTransactions, mxCommitmentBlocks, mxCommitmentMGasSec, mxCommitmentBlockDuration,
		}
		commitResetTask.run(ctx)
	}
}

func resetDomainGauges(ctx context.Context) {
	domainResetTask.Lock()
	defer domainResetTask.Unlock()
	if domainResetTask.Timer != nil {
		if domainResetTask.stopped {
			domainResetTask.Timer = time.NewTimer(resetDelay)
		} else {
			domainResetTask.Reset(resetDelay)
		}
	} else {
		domainResetTask.Timer = time.NewTimer(resetDelay)
		domainResetTask.ctx = ctx
		domainResetTask.gauges = []metrics.Gauge{
			mxExecDomainReads, mxExecDomainReadDuration,
			mxExecDomainCacheReads, mxExecDomainCacheReadDuration, mxExecDomainDbReads, mxExecDomainDbReadDuration,
			mxExecDomainFileReads, mxExecDomainFileReadDuration, mxExecAccountDomainReads, mxExecAccountDomainReadDuration,
			mxExecAccountDomainCacheReads, mxExecAccountDomainCacheReadDuration, mxExecAccountDomainDbReads, mxExecAccountDomainDbReadDuration,
			mxExecAccountDomainFileReads, mxExecAccountDomainFileReadDuration, mxExecStorageDomainReads, mxExecStorageDomainReadDuration,
			mxExexStorageDomainCacheReads, mxExecStorageDomainCacheReadDuration, mxExecStorageDomainDbReads, mxExecStorageDomainDbReadDuration,
			mxExecStorageDomainFileReads, mxExecStorageDomainFileReadDuration, mxExecCodeDomainReads, mxExecCodeDomainReadDuration,
			mxExexCodeDomainCacheReads, mxExecCodeDomainCacheReadDuration, mxExecCodeDomainDbReads, mxExecCodeDomainDbReadDuration,
			mxExecCodeDomainFileReads, mxExecCodeDomainFileReadDuration, mxExecDomainPutRate, mxExecDomainPutSize,
			mxExecAccountDomainPutRate, mxExecAccountDomainPutSize, mxExecStorageDomainPutRate, mxExecStorageDomainPutSize,
			mxExecCodeDomainPutRate, mxExecCodeDomainPutSize, mxCommitmentReadRate, mxCommitmentAccountReadRate,
			mxCommitmentStorageReadRate, mxCommitmentBranchReadRate, mxCommitmentBrancgWriteRate,
			mxCommitmentKeyRate, mxCommitmentAccountKeyRate, mxCommitmentStorageKeyRate, mxCommitmentFoldRate,
			mxCommitmentUnfoldRate, mxCommitmentDomainReads, mxCommitmentDomainReadDuration, mxCommitmentDomainCacheReads,
			mxCommitmentDomainCacheReadDuration, mxCommitmentDomainDbReads, mxCommitmentDomainDbReadDuration, mxCommitmentDomainFileReads,
			mxCommitmentDomainFileReadDuration, mxCommitmentDomainPutRate, mxCommitmentDomainPutSize,
		}
		domainResetTask.run(ctx)
	}
}

func updateExecDomainMetrics(metrics *dbstate.DomainMetrics, prevMetrics *dbstate.DomainMetrics, interval time.Duration) *dbstate.DomainMetrics {
	metrics.RLock()
	defer metrics.RUnlock()

	if prevMetrics == nil {
		prevMetrics = &dbstate.DomainMetrics{
			Domains: map[kv.Domain]*dbstate.DomainIOMetrics{},
		}
	}

	seconds := interval.Seconds()

	cacheReads := metrics.CacheReadCount - prevMetrics.CacheReadCount
	cacheDuration := metrics.CacheReadDuration - prevMetrics.CacheReadDuration
	dbReads := metrics.DbReadCount - prevMetrics.DbReadCount
	dbDuration := metrics.DbReadDuration - prevMetrics.DbReadDuration
	fileReads := metrics.FileReadCount - prevMetrics.FileReadCount
	fileDuration := metrics.FileReadDuration - prevMetrics.FileReadDuration
	cachePutCount := metrics.CachePutCount - prevMetrics.CachePutCount
	cachePutSize := metrics.CachePutSize - prevMetrics.CachePutSize

	mxExecDomainReads.Set(float64(cacheReads+dbReads+fileReads) / seconds)
	mxExecDomainReadDuration.Set(float64(cacheDuration+dbDuration+fileDuration) / float64(cacheReads+dbReads+fileReads))
	mxExecDomainCacheReads.Set(float64(cacheReads) / seconds)
	mxExecDomainCacheReadDuration.Set(float64(cacheDuration) / float64(cacheReads))
	mxExecDomainPutRate.Set(float64(cachePutCount) / seconds)
	mxExecDomainPutSize.Set(float64(cachePutSize))
	mxExecDomainDbReads.Set(float64(dbReads) / seconds)
	mxExecDomainDbReadDuration.Set(float64(dbDuration) / float64(dbReads))
	mxExecDomainFileReads.Set(float64(fileReads) / seconds)
	mxExecDomainFileReadDuration.Set(float64(fileDuration) / float64(fileReads))

	prevMetrics.DomainIOMetrics = metrics.DomainIOMetrics

	if accountMetrics, ok := metrics.Domains[kv.AccountsDomain]; ok {
		var prevAccountMetrics dbstate.DomainIOMetrics

		if prev, ok := prevMetrics.Domains[kv.AccountsDomain]; ok {
			prevAccountMetrics = *prev
		}

		cacheReads := accountMetrics.CacheReadCount - prevAccountMetrics.CacheReadCount
		cacheDuration := accountMetrics.CacheReadDuration - prevAccountMetrics.CacheReadDuration
		dbReads := accountMetrics.DbReadCount - prevAccountMetrics.DbReadCount
		dbDuration := accountMetrics.DbReadDuration - prevAccountMetrics.DbReadDuration
		fileReads := accountMetrics.FileReadCount - prevAccountMetrics.FileReadCount
		fileDuration := accountMetrics.FileReadDuration - prevAccountMetrics.FileReadDuration
		cachePutCount := accountMetrics.CachePutCount - prevAccountMetrics.CachePutCount
		cachePutSize := accountMetrics.CachePutSize - prevAccountMetrics.CachePutSize

		mxExecAccountDomainReads.Set(float64(cacheReads+dbReads+fileReads) / seconds)
		mxExecAccountDomainReadDuration.Set(float64(cacheDuration+dbDuration+fileDuration) / float64(cacheReads+dbReads+fileReads))
		mxExecAccountDomainCacheReads.Set(float64(cacheReads) / seconds)
		mxExecAccountDomainCacheReadDuration.Set(float64(cacheDuration) / float64(cacheReads))
		mxExecAccountDomainPutRate.Set(float64(cachePutCount))
		mxExecAccountDomainPutSize.Set(float64(cachePutSize))
		mxExecAccountDomainDbReads.Set(float64(dbReads) / seconds)
		mxExecAccountDomainDbReadDuration.Set(float64(dbDuration) / float64(dbReads))
		mxExecAccountDomainFileReads.Set(float64(fileReads) / seconds)
		mxExecAccountDomainFileReadDuration.Set(float64(fileDuration) / float64(fileReads))

		prevAccountMetrics = *accountMetrics
		prevMetrics.Domains[kv.AccountsDomain] = &prevAccountMetrics
	}

	if storageMetrics, ok := metrics.Domains[kv.StorageDomain]; ok {
		var prevStorageMetrics dbstate.DomainIOMetrics

		if prev, ok := prevMetrics.Domains[kv.StorageDomain]; ok {
			prevStorageMetrics = *prev
		}

		cacheReads := storageMetrics.CacheReadCount - prevStorageMetrics.CacheReadCount
		cacheDuration := storageMetrics.CacheReadDuration - prevStorageMetrics.CacheReadDuration
		dbReads := storageMetrics.DbReadCount - prevStorageMetrics.DbReadCount
		dbDuration := storageMetrics.DbReadDuration - prevStorageMetrics.DbReadDuration
		fileReads := storageMetrics.FileReadCount - prevStorageMetrics.FileReadCount
		fileDuration := storageMetrics.FileReadDuration - prevStorageMetrics.FileReadDuration
		cachePutCount := storageMetrics.CachePutCount - prevStorageMetrics.CachePutCount
		cachePutSize := storageMetrics.CachePutSize - prevStorageMetrics.CachePutSize

		mxExecStorageDomainReads.Set(float64(cacheReads+dbReads+fileReads) / seconds)
		mxExecStorageDomainReadDuration.Set(float64(cacheDuration+dbDuration+fileDuration) / float64(cacheReads+dbReads+fileReads))
		mxExexStorageDomainCacheReads.Set(float64(cacheReads) / seconds)
		mxExecStorageDomainCacheReadDuration.Set(float64(cacheDuration) / float64(cacheReads))
		mxExecStorageDomainPutRate.Set(float64(cachePutCount))
		mxExecStorageDomainPutSize.Set(float64(cachePutSize))
		mxExecStorageDomainDbReads.Set(float64(dbReads) / seconds)
		mxExecStorageDomainDbReadDuration.Set(float64(dbDuration) / float64(dbReads))
		mxExecStorageDomainFileReads.Set(float64(fileReads) / seconds)
		mxExecStorageDomainFileReadDuration.Set(float64(fileDuration) / float64(fileReads))

		prevStorageMetrics = *storageMetrics
		prevMetrics.Domains[kv.StorageDomain] = &prevStorageMetrics
	}

	if codeMetrics, ok := metrics.Domains[kv.CodeDomain]; ok {
		var prevCodeMetrics dbstate.DomainIOMetrics

		if prev, ok := prevMetrics.Domains[kv.CodeDomain]; ok {
			prevCodeMetrics = *prev
		}

		cacheReads := codeMetrics.CacheReadCount - prevCodeMetrics.CacheReadCount
		cacheDuration := codeMetrics.CacheReadDuration - prevCodeMetrics.CacheReadDuration
		dbReads := codeMetrics.DbReadCount - prevCodeMetrics.DbReadCount
		dbDuration := codeMetrics.DbReadDuration - prevCodeMetrics.DbReadDuration
		fileReads := codeMetrics.FileReadCount - prevCodeMetrics.FileReadCount
		fileDuration := codeMetrics.FileReadDuration - prevCodeMetrics.FileReadDuration
		cachePutCount := codeMetrics.CachePutCount - prevCodeMetrics.CachePutCount
		cachePutSize := codeMetrics.CachePutSize - prevCodeMetrics.CachePutSize

		mxExecCodeDomainReads.Set(float64(cacheReads+dbReads+fileReads) / seconds)
		mxExecCodeDomainReadDuration.Set(float64(cacheDuration+dbDuration+fileDuration) / float64(cacheReads+dbReads+fileReads))
		mxExexCodeDomainCacheReads.Set(float64(cacheReads) / seconds)
		mxExecCodeDomainCacheReadDuration.Set(float64(cacheDuration) / float64(cacheReads))
		mxExecCodeDomainPutRate.Set(float64(cachePutCount))
		mxExecCodeDomainPutSize.Set(float64(cachePutSize))
		mxExecCodeDomainDbReads.Set(float64(dbReads) / seconds)
		mxExecCodeDomainDbReadDuration.Set(float64(dbDuration) / float64(dbReads))
		mxExecCodeDomainFileReads.Set(float64(fileReads) / seconds)
		mxExecCodeDomainFileReadDuration.Set(float64(fileDuration) / float64(fileReads))

		prevCodeMetrics = *codeMetrics
		prevMetrics.Domains[kv.CodeDomain] = &prevCodeMetrics
	}

	if commitmentMetrics, ok := metrics.Domains[kv.CommitmentDomain]; ok {
		var prevCommitmentMetrics dbstate.DomainIOMetrics

		if prev, ok := prevMetrics.Domains[kv.CommitmentDomain]; ok {
			prevCommitmentMetrics = *prev
		}

		cacheReads := commitmentMetrics.CacheReadCount - prevCommitmentMetrics.CacheReadCount
		cacheDuration := commitmentMetrics.CacheReadDuration - prevCommitmentMetrics.CacheReadDuration
		dbReads := commitmentMetrics.DbReadCount - prevCommitmentMetrics.DbReadCount
		dbDuration := commitmentMetrics.DbReadDuration - prevCommitmentMetrics.DbReadDuration
		fileReads := commitmentMetrics.FileReadCount - prevCommitmentMetrics.FileReadCount
		fileDuration := commitmentMetrics.FileReadDuration - prevCommitmentMetrics.FileReadDuration
		cachePutCount := commitmentMetrics.CachePutCount - prevCommitmentMetrics.CachePutCount
		cachePutSize := commitmentMetrics.CachePutSize - prevCommitmentMetrics.CachePutSize

		mxCommitmentDomainReads.Set(float64(cacheReads+dbReads+fileReads) / seconds)
		mxCommitmentDomainReadDuration.Set(float64(cacheDuration+dbDuration+fileDuration) / float64(cacheReads+dbReads+fileReads))
		mxCommitmentDomainCacheReads.Set(float64(cacheReads) / seconds)
		mxCommitmentDomainCacheReadDuration.Set(float64(cacheDuration) / float64(cacheReads))
		mxCommitmentDomainPutRate.Set(float64(cachePutCount))
		mxCommitmentDomainPutSize.Set(float64(cachePutSize))
		mxCommitmentDomainDbReads.Set(float64(dbReads) / seconds)
		mxCommitmentDomainDbReadDuration.Set(float64(dbDuration) / float64(dbReads))
		mxCommitmentDomainFileReads.Set(float64(fileReads) / seconds)
		mxCommitmentDomainFileReadDuration.Set(float64(fileDuration) / float64(fileReads))

		prevCommitmentMetrics = *commitmentMetrics
		prevMetrics.Domains[kv.CommitmentDomain] = &prevCommitmentMetrics
	}

	return prevMetrics
}

func NewProgress(initialBlockNum, initialTxNum, commitThreshold uint64, updateMetrics bool, logPrefix string, logger log.Logger) *Progress {
	now := time.Now()
	return &Progress{
		initialTime:           now,
		initialTxNum:          initialTxNum,
		initialBlockNum:       initialBlockNum,
		prevExecTime:          now,
		prevExecutedBlockNum:  initialBlockNum,
		prevExecutedTxNum:     initialTxNum,
		prevCommitTime:        now,
		prevCommittedBlockNum: initialBlockNum,
		prevCommittedTxNum:    initialTxNum,
		commitThreshold:       commitThreshold,
		logPrefix:             logPrefix,
		logger:                logger}
}

type Progress struct {
	initialTime                    time.Time
	initialTxNum                   uint64
	initialBlockNum                uint64
	prevExecTime                   time.Time
	prevExecutedBlockNum           uint64
	prevExecutedTxNum              uint64
	prevExecutedGas                int64
	prevExecCount                  uint64
	prevActivations                int64
	prevTaskDuration               time.Duration
	prevTaskReadDuration           time.Duration
	prevAccountReadDuration        time.Duration
	prevStorageReadDuration        time.Duration
	prevCodeReadDuration           time.Duration
	prevTaskReadCount              int64
	prevTaskGas                    int64
	prevBlockCount                 int64
	prevBlockDuration              time.Duration
	prevAbortCount                 uint64
	prevInvalidCount               uint64
	prevReadCount                  int64
	prevAccountReadCount           int64
	prevStorageReadCount           int64
	prevCodeReadCount              int64
	prevWriteCount                 uint64
	prevCommitTime                 time.Time
	prevCommittedBlockNum          uint64
	prevCommittedTxNum             uint64
	prevCommittedGas               int64
	prevCommitmentKeyCount         uint64
	prevCommitmentAccountKeyCount  uint64
	prevCommitmentStorageKeyCount  uint64
	prevCommitmentAccountReadCount uint64
	prevCommitmentStorageReadCount uint64
	prevBranchReadCount            uint64
	prevBranchWriteCount           uint64
	commitThreshold                uint64
	prevDomainMetrics              *dbstate.DomainMetrics
	logPrefix                      string
	logger                         log.Logger
}

func (p *Progress) LogExecuted(rs *state.StateV3, ex executor) {
	currentTime := time.Now()
	interval := currentTime.Sub(p.prevExecTime)
	seconds := interval.Seconds()

	var suffix string
	var execVals []interface{}
	var te *txExecutor

	switch ex := ex.(type) {
	case *parallelExecutor:
		te = &ex.txExecutor
		suffix = " parallel"
	case *serialExecutor:
		te = &ex.txExecutor
		suffix = " serial"
	}

	taskGas := te.taskExecMetrics.GasUsed.Total.Load()
	taskDur := time.Duration(te.taskExecMetrics.Duration.Load())
	taskReadDur := time.Duration(te.taskExecMetrics.ReadDuration.Load())
	accountReadDur := time.Duration(te.taskExecMetrics.AccountReadDuration.Load())
	storageReadDur := time.Duration(te.taskExecMetrics.StorageReadDuration.Load())
	codeReadDur := time.Duration(te.taskExecMetrics.CodeReadDuration.Load())
	activations := te.taskExecMetrics.Active.Total.Load()
	accountReadCount := te.taskExecMetrics.AccountReadCount.Load()
	storageReadCount := te.taskExecMetrics.StorageReadCount.Load()
	codeReadCount := te.taskExecMetrics.CodeReadCount.Load()

	curTaskGas := taskGas - p.prevTaskGas
	curTaskDur := taskDur - p.prevTaskDuration
	curTaskReadDur := taskReadDur - p.prevTaskReadDuration
	curAccountReadDur := accountReadDur - p.prevAccountReadDuration
	curStorageReadDur := storageReadDur - p.prevStorageReadDuration
	curCodeReadDur := codeReadDur - p.prevCodeReadDuration
	curAccountReadCount := accountReadCount - p.prevAccountReadCount
	curStorageReadCount := storageReadCount - p.prevStorageReadCount
	curCodeReadCount := codeReadCount - p.prevCodeReadCount
	curActivations := activations - p.prevActivations

	p.prevTaskGas = taskGas
	p.prevTaskDuration = taskDur
	p.prevTaskReadDuration = taskReadDur
	p.prevAccountReadDuration = accountReadDur
	p.prevStorageReadDuration = storageReadDur
	p.prevCodeReadDuration = codeReadDur
	p.prevActivations = activations
	p.prevAccountReadCount = accountReadCount
	p.prevStorageReadCount = storageReadCount
	p.prevCodeReadCount = codeReadCount

	var readRatio float64
	var execRatio float64
	var avgTaskGasPerSec int64
	var avgTaskDur time.Duration
	var avgReadDur time.Duration
	var avgExecDur time.Duration
	var avgAccountReadDur time.Duration
	var avgStorageReadDur time.Duration
	var avgCodeReadDur time.Duration
	var avgTaskGas int64

	if curActivations > 0 {
		avgTaskDur = curTaskDur / time.Duration(curActivations)
		avgReadDur = curTaskReadDur / time.Duration(curActivations)
		avgExecDur = avgTaskDur - avgReadDur
		avgAccountReadDur = curAccountReadDur / time.Duration(curActivations)
		avgStorageReadDur = curStorageReadDur / time.Duration(curActivations)
		avgCodeReadDur = curCodeReadDur / time.Duration(curActivations)

		mxExecTxnDuration.Set(float64(avgTaskDur))
		mxExecTxnExecDuration.Set(float64(avgExecDur))
		mxExecTxnReadDuration.Set(float64(avgReadDur))
		mxExecTxnAccountReadDuration.Set(float64(avgAccountReadDur))
		mxExecTxnStoreageReadDuration.Set(float64(avgStorageReadDur))
		mxExecTxnCodeReadDuration.Set(float64(avgCodeReadDur))

		if avgTaskDur > 0 {
			readRatio = 100.0 * float64(avgReadDur) / float64(avgTaskDur)
			execRatio = 100.0 * float64(avgExecDur) / float64(avgTaskDur)
		}

		avgTaskGas = curTaskGas / curActivations
		avgTaskGasPerSec = int64(float64(avgTaskGas) / seconds)
	}

	curTaskGasPerSec := int64(float64(curTaskGas) / seconds)

	uncommitedGas := uint64(te.executedGas.Load() - te.committedGas)
	sizeEstimate := rs.SizeEstimate()

	switch ex.(type) {
	case *parallelExecutor:
		execCount := uint64(te.execCount.Load())
		abortCount := uint64(te.abortCount.Load())
		invalidCount := uint64(te.invalidCount.Load())
		readCount := te.readCount.Load()
		writeCount := uint64(te.writeCount.Load())

		// not sure why this happens but sometime we read more from disk than from memory
		storageReadCount := te.taskExecMetrics.ReadCount.Load()
		if storageReadCount > readCount {
			readCount = storageReadCount
		}

		execDiff := execCount - p.prevExecCount

		var repeats = max(int(execDiff)-max(int(te.lastExecutedTxNum.Load())-int(p.prevExecutedTxNum), 0), 0)
		var repeatRatio float64

		if repeats > 0 {
			repeatRatio = 100.0 * float64(repeats) / float64(execDiff)
		}

		blockCount := te.blockExecMetrics.BlockCount.Load()
		blockExecDur := time.Duration(te.blockExecMetrics.Duration.Load())

		curBlockCount := blockCount - p.prevBlockCount
		curBlockExecDur := blockExecDur - p.prevBlockDuration

		p.prevBlockCount = blockCount
		p.prevBlockDuration = blockExecDur

		var avgBlockDur time.Duration

		if curBlockCount > 0 {
			avgBlockDur = curBlockExecDur / time.Duration(curBlockCount)
		}

		curReadCount := int64(readCount - p.prevReadCount)
		curWriteCount := int64(writeCount - p.prevWriteCount)

		curReadRate := uint64(float64(curReadCount) / seconds)
		curWriteRate := uint64(float64(curWriteCount) / seconds)

		mxExecReadRate.SetUint64(curReadRate)
		mxExecWriteRate.SetUint64(curWriteRate)
		mxExecAccountReadRate.Set(float64(curAccountReadCount) / seconds)
		mxExecStorageReadRate.Set(float64(curStorageReadCount) / seconds)
		mxExecCodeReadRate.Set(float64(curCodeReadCount) / seconds)

		mxExecGasPerTxn.Set(float64(avgTaskGas))
		mxTaskMgasSec.Set(float64(curTaskGasPerSec / 1e6))
		mxExecCPUs.Set(float64(curTaskDur) / float64(interval))
		mxExecBlockDuration.Set(float64(avgBlockDur))

		execVals = []interface{}{
			"exec", common.PrettyCounter(execDiff),
			"repeat%", fmt.Sprintf("%.2f", repeatRatio),
			"abort", common.PrettyCounter(abortCount - p.prevAbortCount),
			"invalid", common.PrettyCounter(invalidCount - p.prevInvalidCount),
			"tgas/s", common.PrettyCounter(curTaskGasPerSec),
			"tcpus", fmt.Sprintf("%.1f", float64(curTaskDur)/float64(interval)),
			"tdur", common.Round(avgTaskDur, 0).String(),
			"exec", fmt.Sprintf("%v(%.2f%%)", common.Round(avgExecDur, 0), execRatio),
			"read", fmt.Sprintf("%v(%.2f%%),a=%v,s=%v,c=%v", common.Round(avgReadDur, 0), readRatio, common.Round(avgAccountReadDur, 0), common.Round(avgStorageReadDur, 0), common.Round(avgCodeReadDur, 0)),
			"bdur", common.Round(avgBlockDur, 0),
			"rd", fmt.Sprintf("%s,a=%s,s=%s,c=%s", common.PrettyCounter(curReadCount), common.PrettyCounter(curAccountReadCount),
				common.PrettyCounter(curStorageReadCount), common.PrettyCounter(curCodeReadCount)),
			"wrt", common.PrettyCounter(curWriteCount),
			"rd/s", common.PrettyCounter(curReadRate),
			"wrt/s", common.PrettyCounter(curWriteRate),
			"buf", fmt.Sprintf("%s/%s", common.ByteCount(uint64(sizeEstimate)), common.ByteCount(p.commitThreshold)),
		}

		mxExecRepeats.SetInt(repeats)
		mxExecTriggers.SetInt(int(execCount))

		p.prevExecCount = execCount
		p.prevAbortCount = abortCount
		p.prevInvalidCount = invalidCount
		p.prevReadCount = readCount
		p.prevWriteCount = writeCount
	case *serialExecutor:
		readCount := te.taskExecMetrics.ReadCount.Load()
		curReadCount := readCount - p.prevReadCount
		p.prevReadCount = readCount

		execVals = []interface{}{
			"tgas/s", fmt.Sprintf("%s(%s)", common.PrettyCounter(curTaskGasPerSec), common.PrettyCounter(avgTaskGasPerSec)),
			"aratio", fmt.Sprintf("%.1f", float64(curTaskDur)/float64(interval)),
			"tdur", common.Round(avgTaskDur, 0),
			"trdur", fmt.Sprintf("%v(%.2f%%)", common.Round(avgReadDur, 0), readRatio),
			"rd", common.PrettyCounter(curReadCount),
			"rd/s", common.PrettyCounter(uint64(float64(curReadCount) / seconds)),
			"buf", fmt.Sprintf("%s/%s", common.ByteCount(uint64(sizeEstimate)), common.ByteCount(p.commitThreshold)),
		}
	}

	executedGasSec := uint64(float64(te.executedGas.Load()-p.prevExecutedGas) / seconds)

	if executedGas := te.executedGas.Load(); executedGas > 0 {
		mxExecMGasSec.Set((float64(executedGasSec) / 1e6))
	}

	var executedTxSec uint64

	if uint64(te.lastExecutedTxNum.Load()) > p.prevExecutedTxNum {
		executedTxSec = uint64(float64(uint64(te.lastExecutedTxNum.Load())-p.prevExecutedTxNum) / seconds)
	}
	executedDiffBlocks := max(te.lastExecutedBlockNum.Load()-int64(p.prevExecutedBlockNum), 0)
	executedDiffTxs := uint64(max(te.lastExecutedTxNum.Load()-int64(p.prevExecutedTxNum), 0))

	mxExecBlocks.Add(float64(executedDiffBlocks))
	mxExecTransactions.Set(float64(executedDiffTxs) / seconds)
	mxExecTxnPerBlock.Set(float64(executedDiffBlocks) / float64(executedDiffTxs))

	p.log("executed", suffix, te, rs, interval, uint64(te.lastExecutedBlockNum.Load()), executedDiffBlocks,
		executedDiffTxs, executedTxSec, executedGasSec, uncommitedGas, 0, execVals)

	p.prevDomainMetrics = updateExecDomainMetrics(te.doms.Metrics(), p.prevDomainMetrics, interval)

	p.prevExecTime = currentTime

	if te.lastExecutedBlockNum.Load() > 0 {
		p.prevExecutedTxNum = uint64(te.lastExecutedTxNum.Load())
		p.prevExecutedGas = te.executedGas.Load()
		p.prevExecutedBlockNum = uint64(te.lastExecutedBlockNum.Load())
	}
}

func (p *Progress) LogCommitted(rs *state.StateV3, ex executor, commitStart time.Time, stepsInDb float64, lastProgress commitment.CommitProgress) {
	var te *txExecutor
	var suffix string

	switch ex := ex.(type) {
	case *parallelExecutor:
		te = &ex.txExecutor
		suffix = " parallel"

	case *serialExecutor:
		te = &ex.txExecutor
		suffix = " serial"
	}

	if p.prevCommitTime.Before(commitStart) {
		p.prevCommitTime = commitStart
	}

	currentTime := time.Now()
	interval := currentTime.Sub(p.prevCommitTime)

	committedGasSec := uint64(float64(te.committedGas-p.prevCommittedGas) / interval.Seconds())
	var committedTxSec uint64
	if te.lastCommittedTxNum > p.prevCommittedTxNum {
		committedTxSec = uint64(float64(te.lastCommittedTxNum-p.prevCommittedTxNum) / interval.Seconds())
	}
	committedDiffBlocks := max(int64(te.lastCommittedBlockNum)-int64(p.prevCommittedBlockNum), 0)

	var commitedBlockDur time.Duration

	if committedDiffBlocks > 0 {
		commitedBlockDur = interval / time.Duration(committedDiffBlocks)
	}

	lastProgress.Metrics.RLock()
	accountKeyCount := lastProgress.Metrics.AddressKeys
	storageKeyCount := lastProgress.Metrics.StorageKeys
	keyCount := accountKeyCount + storageKeyCount
	accountReadCount := lastProgress.Metrics.LoadAccount
	storageReadCount := lastProgress.Metrics.LoadStorage
	branchReadCount := lastProgress.Metrics.LoadBranch
	branchWriteCount := lastProgress.Metrics.UpdateBranch
	lastProgress.Metrics.RUnlock()

	curKeyCount := int64(keyCount - p.prevCommitmentKeyCount)
	curAccountKeyCount := int64(accountKeyCount - p.prevCommitmentAccountKeyCount)
	curStorageKeyCount := int64(storageKeyCount - p.prevCommitmentStorageKeyCount)

	mxCommitmentKeyRate.Set(float64(curKeyCount) / interval.Seconds())
	mxCommitmentAccountKeyRate.Set(float64(curAccountKeyCount) / interval.Seconds())
	mxCommitmentStorageKeyRate.Set(float64(curStorageKeyCount) / interval.Seconds())

	curAccountReadCount := int64(accountReadCount - p.prevCommitmentAccountReadCount)
	curStorageReadCount := int64(storageReadCount - p.prevCommitmentStorageReadCount)
	curBranchReadCount := int64(branchReadCount - p.prevBranchReadCount)
	curBranchWriteCount := int64(branchWriteCount - p.prevBranchWriteCount)

	curReadCount := curAccountReadCount + curStorageReadCount + curBranchReadCount
	curReadRate := uint64(float64(curReadCount) / interval.Seconds())
	curBranchWriteRate := uint64(float64(curBranchWriteCount) / interval.Seconds())

	mxCommitmentReadRate.SetUint64(curReadRate)
	mxCommitmentAccountReadRate.Set(float64(curAccountReadCount) / interval.Seconds())
	mxCommitmentStorageReadRate.Set(float64(curStorageReadCount) / interval.Seconds())
	mxCommitmentBranchReadRate.Set(float64(curBranchReadCount) / interval.Seconds())
	mxCommitmentBrancgWriteRate.SetUint64(curBranchWriteRate)

	mxCommitmentTransactions.Set(float64(committedTxSec))
	mxCommitmentMGasSec.Set(float64(committedGasSec / 1e6))
	mxCommitmentBlockDuration.Set(float64(commitedBlockDur))

	commitVals := []any{
		"bdur", common.Round(commitedBlockDur, 0),
		"progress", fmt.Sprintf("%s/%s", common.PrettyCounter(lastProgress.KeyIndex), common.PrettyCounter(lastProgress.UpdateCount)),
		"buf", common.ByteCount(uint64(rs.Domains().Metrics().CachePutSize + rs.Domains().Metrics().CacheGetSize)),
	}

	p.log("committed", suffix, te, rs, interval, te.lastCommittedBlockNum, committedDiffBlocks,
		te.lastCommittedTxNum-p.prevCommittedTxNum, committedTxSec, committedGasSec, 0, stepsInDb, commitVals)

	p.prevDomainMetrics = updateExecDomainMetrics(te.doms.Metrics(), p.prevDomainMetrics, interval)

	p.prevCommitTime = currentTime

	if te.lastCommittedTxNum > 0 {
		p.prevCommittedTxNum = te.lastCommittedTxNum
		p.prevCommittedGas = te.committedGas
		p.prevCommittedBlockNum = te.lastCommittedBlockNum
	}
}

func (p *Progress) LogComplete(rs *state.StateV3, ex executor, stepsInDb float64) {
	interval := time.Since(p.initialTime)
	var te *txExecutor
	var suffix string

	switch ex := ex.(type) {
	case *parallelExecutor:
		te = &ex.txExecutor
		suffix = " parallel"

	case *serialExecutor:
		te = &ex.txExecutor
		suffix = " serial"
	}

	gas := te.committedGas

	if gas == 0 {
		gas = te.executedGas.Load()
	}

	lastTxNum := te.lastCommittedTxNum

	if lastTxNum == 0 {
		lastTxNum = uint64(te.lastExecutedTxNum.Load())
	}

	lastBlockNum := te.lastCommittedBlockNum

	if lastBlockNum == 0 {
		lastBlockNum = uint64(te.lastExecutedBlockNum.Load())
	}

	gasSec := uint64(float64(gas) / interval.Seconds())
	var txSec uint64
	if lastTxNum > p.initialTxNum {
		txSec = uint64((float64(lastTxNum) - float64(p.initialTxNum)) / interval.Seconds())
	}
	diffBlocks := max(int64(lastBlockNum)-int64(p.initialBlockNum), 0)

	p.log("done", suffix, te, rs, interval, lastBlockNum, diffBlocks, lastTxNum-p.initialTxNum, txSec, gasSec, 0, stepsInDb, nil)
}

func (p *Progress) log(mode string, suffix string, te *txExecutor, rs *state.StateV3, interval time.Duration,
	blk uint64, blks int64, txs uint64, txsSec uint64, gasSec uint64, uncommitedGas uint64, stepsInDb float64, extraVals []interface{}) {

	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	mxExecStepsInDB.Set(stepsInDb * 100)

	if len(suffix) > 0 {
		suffix += " "
	}

	vals := []interface{}{
		"blk", blk,
		"blks", blks,
		"blk/s", common.PrettyCounter(float64(blks) / interval.Seconds()),
		"txs", common.PrettyCounter(txs),
		"tx/s", common.PrettyCounter(txsSec),
		"gas/s", common.PrettyCounter(gasSec),
	}

	if len(extraVals) > 0 {
		vals = append(vals, extraVals...)
	}

	if stepsInDb > 0 {
		vals = append(vals, []interface{}{
			"stepsInDB", fmt.Sprintf("%.2f", stepsInDb),
			"step", fmt.Sprintf("%.1f", float64(te.lastCommittedTxNum)/float64(config3.DefaultStepSize)),
		}...)
	}

	if uncommitedGas > 0 {
		vals = append(vals, []interface{}{
			"ucgas", common.PrettyCounter(uncommitedGas),
		}...)
	}

	vals = append(vals, []interface{}{
		"alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys),
		"inMem", te.inMemExec,
	}...)

	p.logger.Info(fmt.Sprintf("[%s]%s%s", p.logPrefix, suffix, mode), vals...)
}

// Cases:
//  1. Snapshots > ExecutionStage: snapshots can have half-block data `10.4`. Get right txNum from SharedDomains (after SeekCommitment)
//  2. ExecutionStage > Snapshots: no half-block data possible. Rely on DB.
func restoreTxNum(ctx context.Context, cfg *ExecuteBlockCfg, applyTx kv.Tx, doms *dbstate.SharedDomains, maxBlockNum uint64) (
	inputTxNum uint64, maxTxNum uint64, offsetFromBlockBeginning uint64, err error) {

	txNumsReader := cfg.blockReader.TxnumReader(ctx)

	inputTxNum = doms.TxNum()

	if nothing, err := nothingToExec(applyTx, txNumsReader, inputTxNum); err != nil {
		return 0, 0, 0, err
	} else if nothing {
		return 0, 0, 0, err
	}

	maxTxNum, err = txNumsReader.Max(applyTx, maxBlockNum)
	if err != nil {
		return 0, 0, 0, err
	}

	blockNum, ok, err := txNumsReader.FindBlockNum(applyTx, doms.TxNum())
	if err != nil {
		return 0, 0, 0, err
	}
	if !ok {
		lb, lt, _ := txNumsReader.Last(applyTx)
		fb, ft, _ := txNumsReader.First(applyTx)
		return 0, 0, 0, fmt.Errorf("seems broken TxNums index not filled. can't find blockNum of txNum=%d; in db: (%d-%d, %d-%d)", inputTxNum, fb, lb, ft, lt)
	}
	{
		max, _ := txNumsReader.Max(applyTx, blockNum)
		if doms.TxNum() == max {
			blockNum++
		}
	}

	min, err := txNumsReader.Min(applyTx, blockNum)
	if err != nil {
		return 0, 0, 0, err
	}

	if doms.TxNum() > min {
		// if stopped in the middle of the block: start from beginning of block.
		// first part will be executed in HistoryExecution mode
		offsetFromBlockBeginning = doms.TxNum() - min
	}

	inputTxNum = min

	//_max, _ := txNumsReader.Max(applyTx, blockNum)
	//fmt.Printf("[commitment] found domain.txn %d, inputTxn %d, offset %d. DB found block %d {%d, %d}\n", doms.TxNum(), inputTxNum, offsetFromBlockBeginning, blockNum, _min, _max)
	doms.SetBlockNum(blockNum)
	doms.SetTxNum(inputTxNum)
	return inputTxNum, maxTxNum, offsetFromBlockBeginning, nil
}

func nothingToExec(applyTx kv.Tx, txNumsReader rawdbv3.TxNumsReader, inputTxNum uint64) (bool, error) {
	_, lastTxNum, err := txNumsReader.Last(applyTx)
	if err != nil {
		return false, err
	}
	return lastTxNum == inputTxNum, nil
}

func ExecV3(ctx context.Context,
	execStage *StageState, u Unwinder, workerCount int, cfg ExecuteBlockCfg, txc wrap.TxContainer,
	parallel bool, //nolint
	maxBlockNum uint64,
	logger log.Logger,
	hooks *tracing.Hooks,
	initialCycle bool,
	isMining bool,
) (execErr error) {
	inMemExec := txc.Doms != nil

	blockReader := cfg.blockReader
	chainConfig := cfg.chainConfig
	totalGasUsed := uint64(0)

	useExternalTx := txc.Tx != nil
	var applyTx kv.TemporalRwTx

	if useExternalTx {
		var ok bool
		applyTx, ok = txc.Tx.(kv.TemporalRwTx)
		if !ok {
			applyTx, ok = txc.Ttx.(kv.TemporalRwTx)

			if !ok {
				return errors.New("txc.Tx is not a temporal tx")
			}
		}
	} else {
		var err error
		temporalDb, ok := cfg.db.(kv.TemporalRwDB)
		if !ok {
			return errors.New("cfg.db is not a temporal db")
		}
		applyTx, err = temporalDb.BeginTemporalRw(ctx) //nolint
		if err != nil {
			return err
		}
		defer func() { // need callback - because tx may be committed
			applyTx.Rollback()
		}()
	}

	agg := cfg.db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	if !inMemExec && !isMining {
		agg.SetCollateAndBuildWorkers(min(2, estimate.StateV3Collate.Workers()))
		agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	} else {
		agg.SetCompressWorkers(1)
		agg.SetCollateAndBuildWorkers(1)
	}

	var err error
	var doms *dbstate.SharedDomains
	if inMemExec {
		doms = txc.Doms
	} else {
		var err error
		doms, err = dbstate.NewSharedDomains(applyTx, log.New())
		// if we are behind the commitment, we can't execute anything
		// this can heppen if progress in domain is higher than progress in blocks
		if errors.Is(err, commitmentdb.ErrBehindCommitment) {
			return nil
		}
		if err != nil {
			return err
		}
		defer doms.Close()
	}

	var (
		stageProgress = execStage.BlockNumber
		outputTxNum   = atomic.Uint64{}
		blockNum      = doms.BlockNum()
	)

	if maxBlockNum < blockNum {
		return nil
	}

	outputTxNum.Store(doms.TxNum())
	agg.BuildFilesInBackground(outputTxNum.Load())

	var (
		inputTxNum               uint64
		offsetFromBlockBeginning uint64
		maxTxNum                 uint64
	)

	if applyTx != nil {
		if inputTxNum, maxTxNum, offsetFromBlockBeginning, err = restoreTxNum(ctx, &cfg, applyTx, doms, maxBlockNum); err != nil {
			return err
		}
	} else {
		if err := cfg.db.View(ctx, func(tx kv.Tx) (err error) {
			inputTxNum, maxTxNum, offsetFromBlockBeginning, err = restoreTxNum(ctx, &cfg, tx, doms, maxBlockNum)
			return err
		}); err != nil {
			return err
		}
	}

	if maxTxNum == 0 {
		return nil
	}

	shouldReportToTxPool := cfg.notifications != nil && !isMining && maxBlockNum <= blockNum+64
	var accumulator *shards.Accumulator
	if shouldReportToTxPool {
		accumulator = cfg.notifications.Accumulator
		if accumulator == nil {
			accumulator = shards.NewAccumulator()
		}
	}
	rs := state.NewStateV3Buffered(state.NewStateV3(doms, cfg.syncCfg, logger))

	commitThreshold := cfg.batchSize.Bytes()

	logInterval := 20 * time.Second
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()
	flushEvery := time.NewTicker(2 * time.Second)
	defer flushEvery.Stop()
	defer resetExecGauges(ctx)
	defer resetCommitmentGauges(ctx)
	defer resetDomainGauges(ctx)

	var executor executor
	var executorContext context.Context
	var executorCancel context.CancelFunc

	if parallel {
		pe := &parallelExecutor{
			txExecutor: txExecutor{
				cfg:                   cfg,
				rs:                    rs,
				doms:                  doms,
				agg:                   agg,
				isMining:              isMining,
				inMemExec:             inMemExec,
				logger:                logger,
				logPrefix:             execStage.LogPrefix(),
				progress:              NewProgress(blockNum, outputTxNum.Load(), commitThreshold, false, execStage.LogPrefix(), logger),
				enableChaosMonkey:     execStage.CurrentSyncCycle.IsInitialCycle,
				hooks:                 hooks,
				lastCommittedTxNum:    doms.TxNum(),
				lastCommittedBlockNum: blockNum,
			},
			workerCount: workerCount,
		}

		executorContext, executorCancel = pe.run(ctx)

		defer executorCancel()

		executor = pe
	} else {
		se := &serialExecutor{
			txExecutor: txExecutor{
				cfg:                   cfg,
				rs:                    rs,
				doms:                  doms,
				agg:                   agg,
				u:                     u,
				isMining:              isMining,
				inMemExec:             inMemExec,
				applyTx:               applyTx,
				logger:                logger,
				logPrefix:             execStage.LogPrefix(),
				progress:              NewProgress(blockNum, outputTxNum.Load(), commitThreshold, false, execStage.LogPrefix(), logger),
				enableChaosMonkey:     execStage.CurrentSyncCycle.IsInitialCycle,
				hooks:                 hooks,
				lastCommittedTxNum:    doms.TxNum(),
				lastCommittedBlockNum: blockNum,
			},
		}

		executor = se
	}

	executor.resetWorkers(ctx, rs, applyTx)

	stepsInDb := rawdbhelpers.IdxStepsCountV3(applyTx)
	defer func() {
		executor.LogComplete(stepsInDb)
	}()

	computeCommitmentDuration := time.Duration(0)
	blockNum = executor.domains().BlockNum()

	if maxBlockNum < blockNum {
		return nil
	}

	agg.BuildFilesInBackground(outputTxNum.Load())

	var uncommitedGas uint64
	var b *types.Block

	var readAhead chan uint64
	// snapshots are often stored on chaper drives. don't expect low-read-latency and manually read-ahead.
	// can't use OS-level ReadAhead - because Data >> RAM
	// it also warmsup state a bit - by touching senders/coninbase accounts and code
	if !execStage.CurrentSyncCycle.IsInitialCycle {
		var clean func()

		readAhead, clean = exec3.BlocksReadAhead(ctx, 2, cfg.db, cfg.engine, cfg.blockReader)
		defer clean()
	}

	startBlockNum := blockNum
	blockLimit := uint64(cfg.syncCfg.LoopBlockLimit)

	if blockLimit > 0 && min(blockNum+blockLimit, maxBlockNum) > blockNum+16 || maxBlockNum > blockNum+16 {
		execType := "serial"
		if parallel {
			execType = "parallel"
		}

		log.Info(fmt.Sprintf("[%s] %s starting", execStage.LogPrefix(), execType),
			"from", blockNum, "to", min(blockNum+blockLimit, maxBlockNum), "fromTxNum", doms.TxNum(), "initialBlockTxOffset", offsetFromBlockBeginning, "initialCycle", initialCycle, "useExternalTx", useExternalTx, "inMem", inMemExec)
	}

	if !parallel {
		se := executor.(*serialExecutor)

		execErr = func() error {
			havePartialBlock := false

			for ; blockNum <= maxBlockNum; blockNum++ {
				shouldGenerateChangesets := shouldGenerateChangeSets(cfg, blockNum, maxBlockNum, initialCycle)
				changeSet := &changeset.StateChangeSet{}
				if shouldGenerateChangesets && blockNum > 0 {
					executor.domains().SetChangesetAccumulator(changeSet)
				}

				select {
				case readAhead <- blockNum:
				default:
				}

				b, err = exec3.BlockWithSenders(ctx, cfg.db, applyTx, blockReader, blockNum)
				if err != nil {
					return err
				}
				if b == nil {
					// TODO: panic here and see that overall process deadlock
					return fmt.Errorf("nil block %d", blockNum)
				}

				txs := b.Transactions()
				header := b.HeaderNoCopy()
				totalGasUsed += b.GasUsed()
				getHashFnMutex := sync.Mutex{}

				blockContext := core.NewEVMBlockContext(header, core.GetHashFn(header, func(hash common.Hash, number uint64) (*types.Header, error) {
					getHashFnMutex.Lock()
					defer getHashFnMutex.Unlock()
					return executor.getHeader(ctx, hash, number)
				}), cfg.engine, cfg.author, chainConfig)

				if accumulator != nil {
					txs, err := blockReader.RawTransactions(context.Background(), applyTx, b.NumberU64(), b.NumberU64())
					if err != nil {
						return err
					}
					accumulator.StartChange(header, txs, false)
				}

				var txTasks []exec.Task

				for txIndex := -1; txIndex <= len(txs); txIndex++ {
					// Do not oversend, wait for the result heap to go under certain size
					txTask := &exec.TxTask{
						TxNum:           inputTxNum,
						TxIndex:         txIndex,
						Header:          header,
						Uncles:          b.Uncles(),
						Txs:             txs,
						EvmBlockContext: blockContext,
						Withdrawals:     b.Withdrawals(),

						// use history reader instead of state reader to catch up to the tx where we left off
						HistoryExecution: offsetFromBlockBeginning > 0 && txIndex < int(offsetFromBlockBeginning),
						Trace:            dbg.TraceTx(blockNum, txIndex),
						Hooks:            hooks,
						Logger:           logger,
					}

					if txTask.TxNum > 0 && txTask.TxNum <= outputTxNum.Load() {
						havePartialBlock = true
						inputTxNum++
						continue
					}

					txTasks = append(txTasks, txTask)
					stageProgress = blockNum
					inputTxNum++
				}

				continueLoop, err := se.execute(ctx, txTasks, execStage.CurrentSyncCycle.IsInitialCycle, false)

				if err != nil {
					return err
				}

				uncommitedGas = uint64(se.executedGas.Load() - int64(se.committedGas))

				if !continueLoop {
					return nil
				}

				if !dbg.BatchCommitments || shouldGenerateChangesets {
					start := time.Now()
					if dbg.TraceBlock(blockNum) {
						se.doms.SetTrace(true, false)
					}
					rh, err := executor.domains().ComputeCommitment(ctx, applyTx, true, blockNum, inputTxNum, execStage.LogPrefix(), nil)
					se.doms.SetTrace(false, false)

					if err != nil {
						return err
					}

					computeCommitmentDuration += time.Since(start)
					executor.domains().SavePastChangesetAccumulator(b.Hash(), blockNum, changeSet)
					if !inMemExec {
						if err := changeset.WriteDiffSet(applyTx, blockNum, b.Hash(), changeSet); err != nil {
							return err
						}
					}
					executor.domains().SetChangesetAccumulator(nil)

					if !isMining && !bytes.Equal(rh, header.Root.Bytes()) {
						logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", execStage.LogPrefix(), header.Number.Uint64(), rh, header.Root.Bytes(), header.Hash()))
						return fmt.Errorf("%w, block=%d", ErrWrongTrieRoot, blockNum)
					}
				}

				if dbg.StopAfterBlock > 0 && blockNum == dbg.StopAfterBlock {
					panic(fmt.Sprintf("stopping: block %d complete", blockNum))
					//return fmt.Errorf("stopping: block %d complete", blockNum)
				}

				if offsetFromBlockBeginning > 0 {
					// after history execution no offset will be required
					offsetFromBlockBeginning = 0
				}

				select {
				case <-logEvery.C:
					if inMemExec || isMining {
						break
					}

					executor.LogExecuted()

					//TODO: https://github.com/erigontech/erigon/issues/10724
					//if executor.tx().(dbstate.HasAggTx).AggTx().(*dbstate.AggregatorRoTx).CanPrune(executor.tx(), outputTxNum.Load()) {
					//	//small prune cause MDBX_TXN_FULL
					//	if _, err := executor.tx().(dbstate.HasAggTx).AggTx().(*dbstate.AggregatorRoTx).PruneSmallBatches(ctx, 10*time.Hour, executor.tx()); err != nil {
					//		return err
					//	}
					//}

					isBatchFull := executor.readState().SizeEstimate() >= commitThreshold
					canPrune := dbstate.AggTx(applyTx).CanPrune(applyTx, outputTxNum.Load())
					needCalcRoot := isBatchFull || havePartialBlock || canPrune
					// If we have a partial first block it may not be validated, then we should compute root hash ASAP for fail-fast

					// this will only happen for the first executed block
					havePartialBlock = false

					if !needCalcRoot {
						break
					}

					resetExecGauges(ctx)

					var (
						commitStart   = time.Now()
						pruneDuration time.Duration
					)

					se := executor.(*serialExecutor)

					ok, times, err := flushAndCheckCommitmentV3(ctx, b.HeaderNoCopy(), applyTx, executor.domains(), cfg, execStage, stageProgress, parallel, logger, u, inMemExec)
					if err != nil {
						return err
					} else if !ok {
						return nil
					}

					resetCommitmentGauges(ctx)

					computeCommitmentDuration += times.ComputeCommitment
					flushDuration := times.Flush

					se.txExecutor.lastCommittedBlockNum = b.NumberU64()
					se.txExecutor.lastCommittedTxNum = inputTxNum

					timeStart := time.Now()

					pruneTimeout := 250 * time.Millisecond
					if initialCycle {
						pruneTimeout = 10 * time.Hour

						if err = applyTx.GreedyPruneHistory(ctx, kv.CommitmentDomain); err != nil {
							return err
						}
					}

					if _, err := applyTx.PruneSmallBatches(ctx, pruneTimeout); err != nil {
						return err
					}

					pruneDuration = time.Since(timeStart)

					stepsInDb = rawdbhelpers.IdxStepsCountV3(applyTx)

					var commitDuration time.Duration
					applyTx, commitDuration, err = executor.(*serialExecutor).commit(ctx, execStage, applyTx, nil, useExternalTx)
					if err != nil {
						return err
					}
					// on chain-tip: if batch is full then stop execution - to allow stages commit
					if !initialCycle {
						if isBatchFull {
							return &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block batch is full"}
						}

						if canPrune {
							return &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block batch can be pruned"}
						}
					}

					if !useExternalTx {
						executor.LogCommitted(commitStart, 0, 0, uncommitedGas, stepsInDb, commitment.CommitProgress{})
					}

					uncommitedGas = 0

					logger.Info("Committed", "time", time.Since(commitStart),
						"block", executor.domains().BlockNum(), "txNum", executor.domains().TxNum(),
						"step", fmt.Sprintf("%.1f", float64(executor.domains().TxNum())/float64(agg.StepSize())),
						"flush", flushDuration, "compute commitment", computeCommitmentDuration, "tx.commit", commitDuration, "prune", pruneDuration)
				default:
				}

				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				if blockLimit > 0 && blockNum-startBlockNum+1 >= blockLimit {
					return &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block limit reached"}
				}
			}

			return nil
		}()

		if u != nil && !u.HasUnwindPoint() {
			if b != nil {
				if errors.Is(execErr, ErrWrongTrieRoot) {
					execErr = handleIncorrectRootHashError(
						b.NumberU64(), b.Hash(), b.ParentHash(), applyTx, cfg, execStage, maxBlockNum, logger, u)
				} else {
					_, _, err = flushAndCheckCommitmentV3(ctx, b.HeaderNoCopy(), applyTx, executor.domains(), cfg, execStage, stageProgress, parallel, logger, u, inMemExec)
					if err != nil {
						return err
					}

					se.txExecutor.lastCommittedBlockNum = b.NumberU64()
					committedTransactions := inputTxNum - se.txExecutor.lastCommittedTxNum
					se.txExecutor.lastCommittedTxNum = inputTxNum

					commitStart := time.Now()
					stepsInDb = rawdbhelpers.IdxStepsCountV3(applyTx)
					applyTx, _, err = se.commit(ctx, execStage, applyTx, nil, useExternalTx)
					if err != nil {
						return err
					}

					if !useExternalTx {
						executor.LogCommitted(commitStart, 0, committedTransactions, uncommitedGas, stepsInDb, commitment.CommitProgress{})
					}

					uncommitedGas = 0
				}
			} else {
				fmt.Printf("[dbg] mmmm... do we need action here????\n")
			}
		}
	} else {
		pe := executor.(*parallelExecutor)

		var asyncTxChan mdbx.TxApplyChan
		var asyncTx kv.Tx

		switch applyTx := applyTx.(type) {
		case *temporal.RwTx:
			temporalTx := applyTx.AsyncClone(mdbx.NewAsyncRwTx(applyTx.RwTx, 1000))
			asyncTxChan = temporalTx.ApplyChan()
			asyncTx = temporalTx
		default:
			return fmt.Errorf("expected *temporal.RwTx: got %T", applyTx)
		}

		applyResults := make(chan applyResult, 100_000)

		maxExecBlockNum := maxBlockNum
		if blockLimit > 0 && blockNum+uint64(blockLimit) < maxBlockNum {
			maxExecBlockNum = blockNum + blockLimit - 1
		}

		if err := executor.executeBlocks(executorContext, asyncTx, blockNum, maxExecBlockNum, readAhead, applyResults); err != nil {
			return err
		}

		var lastExecutedLog time.Time
		var lastCommitedLog time.Time
		var lastBlockResult blockResult
		var uncommittedBlocks int64
		var uncommittedTransactions uint64
		var uncommittedGas int64
		var flushPending bool

		execErr = func() error {
			defer func() {
				if rec := recover(); rec != nil {
					pe.logger.Warn("["+execStage.LogPrefix()+"] rw panic", "rec", rec, "stack", dbg.Stack())
				} else if err != nil && !errors.Is(err, context.Canceled) {
					pe.logger.Warn("["+execStage.LogPrefix()+"] rw exit", "err", err)
				} else {
					pe.logger.Debug("[" + execStage.LogPrefix() + "] rw exit")
				}
			}()

			shouldGenerateChangesets := shouldGenerateChangeSets(cfg, blockNum, maxBlockNum, initialCycle)
			changeSet := &changeset.StateChangeSet{}
			if shouldGenerateChangesets && blockNum > 0 {
				executor.domains().SetChangesetAccumulator(changeSet)
			}

			blockUpdateCount := 0
			blockApplyCount := 0

			for {
				select {
				case request := <-asyncTxChan:
					request.Apply()
				case applyResult := <-applyResults:
					switch applyResult := applyResult.(type) {
					case *txResult:
						uncommittedGas += applyResult.gasUsed
						uncommittedTransactions++
						pe.rs.SetTxNum(applyResult.blockNum, applyResult.txNum)
						if dbg.TraceApply && dbg.TraceBlock(applyResult.blockNum) {
							pe.rs.SetTrace(true)
							fmt.Println(applyResult.blockNum, "apply", applyResult.txNum, applyResult.stateUpdates.UpdateCount())
						}
						blockUpdateCount += applyResult.stateUpdates.UpdateCount()
						err := pe.rs.ApplyTxState(ctx, applyTx, applyResult.blockNum, applyResult.txNum, applyResult.stateUpdates,
							nil, applyResult.receipt, applyResult.logs, applyResult.traceFroms, applyResult.traceTos,
							pe.cfg.chainConfig, applyResult.rules, false)
						blockApplyCount += applyResult.stateUpdates.UpdateCount()
						pe.rs.SetTrace(false)
						if err != nil {
							return err
						}
					case *blockResult:
						if applyResult.BlockNum > 0 && !applyResult.isPartial { //Disable check for genesis. Maybe need somehow improve it in future - to satisfy TestExecutionSpec
							checkReceipts := !cfg.vmConfig.StatelessExec &&
								cfg.chainConfig.IsByzantium(applyResult.BlockNum) &&
								!cfg.vmConfig.NoReceipts && !isMining

							b, err = blockReader.BlockByHash(ctx, applyTx, applyResult.BlockHash)

							if err != nil {
								return fmt.Errorf("can't retrieve block %d: for post validation: %w", applyResult.BlockNum, err)
							}

							if b.NumberU64() != applyResult.BlockNum {
								return fmt.Errorf("block numbers don't match expected: %d: got: %d for hash %x", applyResult.BlockNum, b.NumberU64(), applyResult.BlockHash)
							}

							if blockUpdateCount != applyResult.ApplyCount {
								return fmt.Errorf("block %d: applyCount mismatch: got: %d expected %d", applyResult.BlockNum, blockUpdateCount, applyResult.ApplyCount)
							}

							if err := core.BlockPostValidation(applyResult.GasUsed, applyResult.BlobGasUsed, checkReceipts, applyResult.Receipts,
								b.HeaderNoCopy(), pe.isMining, b.Transactions(), pe.cfg.chainConfig, pe.logger); err != nil {
								dumpTxIODebug(applyResult.BlockNum, applyResult.TxIO)
								return fmt.Errorf("%w, block=%d, %v", consensus.ErrInvalidBlock, applyResult.BlockNum, err) //same as in stage_exec.go
							}

							if !isMining && !applyResult.isPartial && !execStage.CurrentSyncCycle.IsInitialCycle {
								cfg.notifications.RecentLogs.Add(applyResult.Receipts)
							}
						}

						if applyResult.BlockNum > lastBlockResult.BlockNum {
							uncommittedBlocks++
							pe.doms.SetTxNum(applyResult.lastTxNum)
							pe.doms.SetBlockNum(applyResult.BlockNum)
							lastBlockResult = *applyResult
						}

						flushPending = pe.rs.SizeEstimate() > pe.cfg.batchSize.Bytes()

						if !dbg.DiscardCommitment() {
							if !dbg.BatchCommitments || shouldGenerateChangesets || lastBlockResult.BlockNum == maxExecBlockNum ||
								(flushPending && lastBlockResult.BlockNum > pe.lastCommittedBlockNum()) {

								resetExecGauges(ctx)

								if dbg.TraceApply && dbg.TraceBlock(applyResult.BlockNum) {
									fmt.Println(applyResult.BlockNum, "applied count", blockApplyCount, "last tx", applyResult.lastTxNum)
								}

								var trace bool
								if dbg.TraceBlock(applyResult.BlockNum) {
									fmt.Println(applyResult.BlockNum, "Commitment")
									trace = true
								}
								pe.doms.SetTrace(trace, !dbg.BatchCommitments)

								commitProgress := make(chan *commitment.CommitProgress, 100)

								go func() {
									logEvery := time.NewTicker(20 * time.Second)
									commitStart := time.Now()

									defer logEvery.Stop()
									var lastProgress commitment.CommitProgress

									var prevCommitedBlocks uint64
									var prevCommittedTransactions uint64
									var prevCommitedGas uint64

									logCommitted := func(commitProgress commitment.CommitProgress) {
										// this is an approximation of blcok prgress - it assumnes an
										// even distribution of keys to blocks
										if commitProgress.KeyIndex > 0 {
											progress := float64(commitProgress.KeyIndex) / float64(commitProgress.UpdateCount)
											committedGas := uint64(float64(uncommittedGas) * progress)
											committedTransactions := uint64(float64(uncommittedTransactions) * progress)
											commitedBlocks := uint64(float64(uncommittedBlocks) * progress)

											if committedTransactions-prevCommittedTransactions > 0 {
												pe.LogCommitted(commitStart,
													commitedBlocks-prevCommitedBlocks,
													committedTransactions-prevCommittedTransactions,
													committedGas-prevCommitedGas, stepsInDb, commitProgress)
											}

											lastCommitedLog = time.Now()
											prevCommitedBlocks = commitedBlocks
											prevCommittedTransactions = committedTransactions
											prevCommitedGas = committedGas
										}

										if pe.agg.HasBackgroundFilesBuild() {
											logger.Info(fmt.Sprintf("[%s] Background files build", pe.logPrefix), "progress", pe.agg.BackgroundProgress())
										}
									}

									for {
										select {
										case <-ctx.Done():
											return
										case progress, ok := <-commitProgress:
											if !ok {
												if time.Since(lastCommitedLog) > logInterval/20 {
													logCommitted(lastProgress)
												}
												return
											}
											lastProgress = *progress
										case <-logEvery.C:
											if time.Since(lastCommitedLog) > logInterval-(logInterval/90) {
												logCommitted(lastProgress)
											}
										}
									}
								}()

								if time.Since(lastExecutedLog) > logInterval/50 {
									pe.LogExecuted()
									lastExecutedLog = time.Now()
								}

								rh, err := pe.doms.ComputeCommitment(ctx, applyTx, true, applyResult.BlockNum, applyResult.lastTxNum, pe.logPrefix, commitProgress)
								close(commitProgress)
								captured := pe.doms.SetTrace(false, false)
								if err != nil {
									return err
								}
								resetCommitmentGauges(ctx)

								executor.domains().SavePastChangesetAccumulator(applyResult.BlockHash, blockNum, changeSet)
								if !inMemExec {
									if err := changeset.WriteDiffSet(applyTx, blockNum, applyResult.BlockHash, changeSet); err != nil {
										return err
									}
								}
								executor.domains().SetChangesetAccumulator(nil)

								if !bytes.Equal(rh, applyResult.StateRoot.Bytes()) {
									logger.Error(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", pe.logPrefix, applyResult.BlockNum, rh, applyResult.StateRoot.Bytes(), applyResult.BlockHash))
									if !dbg.BatchCommitments {
										for _, line := range captured {
											fmt.Println(line)
										}

										dumpTxIODebug(applyResult.BlockNum, applyResult.TxIO)
									}

									return handleIncorrectRootHashError(
										applyResult.BlockNum, applyResult.BlockHash, applyResult.ParentHash,
										applyTx, cfg, execStage, maxBlockNum, logger, u)
								}
								// fix these here - they will contain estimates after commit logging
								pe.txExecutor.lastCommittedBlockNum = lastBlockResult.BlockNum
								pe.txExecutor.lastCommittedTxNum = lastBlockResult.lastTxNum
								uncommittedBlocks = 0
								uncommittedGas = 0
								uncommittedGas = 0
							}
						}

						blockUpdateCount = 0
						blockApplyCount = 0

						if dbg.StopAfterBlock > 0 && applyResult.BlockNum == dbg.StopAfterBlock {
							return fmt.Errorf("stopping: block %d complete", applyResult.BlockNum)
						}

						if applyResult.BlockNum == maxExecBlockNum {
							switch {
							case applyResult.BlockNum == maxBlockNum:
								return nil
							case blockLimit > 0:
								return &ErrLoopExhausted{From: startBlockNum, To: applyResult.BlockNum, Reason: "block limit reached"}
							default:
								return nil
							}
						}

						if shouldGenerateChangesets && blockNum > 0 {
							changeSet = &changeset.StateChangeSet{}
							executor.domains().SetChangesetAccumulator(changeSet)
						}
					}
				case <-executorContext.Done():
					err = executor.wait(ctx)
					return fmt.Errorf("executor context failed: %w", err)
				case <-ctx.Done():
					return ctx.Err()
				case <-logEvery.C:
					if time.Since(lastExecutedLog) > logInterval-(logInterval/90) {
						lastExecutedLog = time.Now()
						pe.LogExecuted()
						if pe.agg.HasBackgroundFilesBuild() {
							logger.Info(fmt.Sprintf("[%s] Background files build", pe.logPrefix), "progress", pe.agg.BackgroundProgress())
						}
					}
				case <-flushEvery.C:
					if flushPending {
						if !initialCycle {
							return &ErrLoopExhausted{From: startBlockNum, To: blockNum, Reason: "block batch is full"}
						}

						if applyTx, err = pe.flushAndCommit(ctx, execStage, applyTx, asyncTxChan, useExternalTx); err != nil {
							return fmt.Errorf("flush failed: %w", err)
						}

						flushPending = false
					}
				}
			}
		}()

		executorCancel()

		if execErr != nil {
			if !(errors.Is(execErr, context.Canceled) || errors.Is(execErr, &ErrLoopExhausted{})) {
				return execErr
			}
		}

		if applyTx, err = pe.flushAndCommit(ctx, execStage, applyTx, asyncTxChan, useExternalTx); err != nil {
			return fmt.Errorf("flush failed: %w", err)
		}
	}

	if err := executor.wait(ctx); err != nil {
		return fmt.Errorf("executer wait failed: %w", err)
	}

	if false && !inMemExec {
		dumpPlainStateDebug(applyTx, executor.domains())
	}

	lastCommitedStep := kv.Step((executor.lastCommittedTxNum()) / doms.StepSize())
	lastFrozenStep := applyTx.StepsInFiles(kv.CommitmentDomain)

	if lastCommitedStep > 0 && lastCommitedStep <= lastFrozenStep && !dbg.DiscardCommitment() {
		logger.Warn("["+execStage.LogPrefix()+"] can't persist comittement: txn step frozen",
			"block", executor.lastCommittedBlockNum(), "txNum", executor.lastCommittedTxNum(), "step", lastCommitedStep,
			"lastFrozenStep", lastFrozenStep, "lastFrozenTxNum", ((lastFrozenStep+1)*kv.Step(doms.StepSize()))-1)
		return fmt.Errorf("can't persist comittement for blockNum %d, txNum %d: step %d is frozen",
			executor.lastCommittedBlockNum(), executor.lastCommittedTxNum(), lastCommitedStep)
	}

	if !useExternalTx && applyTx != nil {
		if err = applyTx.Commit(); err != nil {
			return err
		}
	}

	agg.BuildFilesInBackground(outputTxNum.Load())

	if !shouldReportToTxPool && cfg.notifications != nil && cfg.notifications.Accumulator != nil && !isMining && b != nil {
		// No reporting to the txn pool has been done since we are not within the "state-stream" window.
		// However, we should still at the very least report the last block number to it, so it can update its block progress.
		// Otherwise, we can get in a deadlock situation when there is a block building request in environments where
		// the Erigon process is the only block builder (e.g. some Hive tests, kurtosis testnets with one erigon block builder, etc.)
		cfg.notifications.Accumulator.StartChange(b.HeaderNoCopy(), nil, false /* unwind */)
	}

	return execErr
}

func dumpTxIODebug(blockNum uint64, txIO *state.VersionedIO) {
	maxTxIndex := len(txIO.Inputs()) - 1

	for txIndex := -1; txIndex < maxTxIndex; txIndex++ {
		txIncarnation := txIO.ReadSetIncarnation(txIndex)

		fmt.Println(
			fmt.Sprintf("%d (%d.%d) RD", blockNum, txIndex, txIncarnation), txIO.ReadSet(txIndex).Len(),
			"WRT", len(txIO.WriteSet(txIndex)))

		var reads []*state.VersionedRead
		txIO.ReadSet(txIndex).Scan(func(vr *state.VersionedRead) bool {
			reads = append(reads, vr)
			return true
		})

		slices.SortFunc(reads, func(a, b *state.VersionedRead) int { return a.Address.Cmp(b.Address) })

		for _, vr := range reads {
			fmt.Println(fmt.Sprintf("%d (%d.%d)", blockNum, txIndex, txIncarnation), "RD", vr.String())
		}

		var writes []*state.VersionedWrite

		for _, vw := range txIO.WriteSet(txIndex) {
			writes = append(writes, vw)
		}

		slices.SortFunc(writes, func(a, b *state.VersionedWrite) int { return a.Address.Cmp(b.Address) })

		for _, vw := range writes {
			fmt.Println(fmt.Sprintf("%d (%d.%d)", blockNum, txIndex, txIncarnation), "WRT", vw.String())
		}
	}
}

// nolint
func dumpPlainStateDebug(tx kv.TemporalRwTx, doms *dbstate.SharedDomains) {
	if doms != nil {
		doms.Flush(context.Background(), tx)
	}

	{
		it, err := tx.Debug().RangeLatest(kv.AccountsDomain, nil, nil, -1)
		if err != nil {
			panic(err)
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				panic(err)
			}
			a := accounts.NewAccount()
			accounts.DeserialiseV3(&a, v)
			fmt.Printf("%x, %d, %d, %d, %x\n", k, &a.Balance, a.Nonce, a.Incarnation, a.CodeHash)
		}
	}
	{
		it, err := tx.Debug().RangeLatest(kv.StorageDomain, nil, nil, -1)
		if err != nil {
			panic(1)
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				panic(err)
			}
			fmt.Printf("%x, %x\n", k, v)
		}
	}
	{
		it, err := tx.Debug().RangeLatest(kv.CommitmentDomain, nil, nil, -1)
		if err != nil {
			panic(1)
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				panic(err)
			}
			fmt.Printf("%x, %x\n", k, v)
			if bytes.Equal(k, []byte("state")) {
				fmt.Printf("state: t=%d b=%d\n", binary.BigEndian.Uint64(v[:8]), binary.BigEndian.Uint64(v[8:]))
			}
		}
	}
}

func handleIncorrectRootHashError(blockNumber uint64, blockHash common.Hash, parentHash common.Hash, applyTx kv.TemporalRwTx, cfg ExecuteBlockCfg, e *StageState, maxBlockNum uint64, logger log.Logger, u Unwinder) error {
	if cfg.badBlockHalt {
		return fmt.Errorf("%w, block=%d", ErrWrongTrieRoot, blockNumber)
	}
	if cfg.hd != nil && cfg.hd.POSSync() {
		cfg.hd.ReportBadHeaderPoS(blockHash, parentHash)
	}
	minBlockNum := e.BlockNumber
	if maxBlockNum <= minBlockNum {
		return nil
	}

	unwindToLimit, err := rawtemporaldb.CanUnwindToBlockNum(applyTx)
	if err != nil {
		return err
	}
	minBlockNum = max(minBlockNum, unwindToLimit)

	// Binary search, but not too deep
	jump := cmp.InRange(1, maxUnwindJumpAllowance, (maxBlockNum-minBlockNum)/2)
	unwindTo := maxBlockNum - jump

	// protect from too far unwind
	allowedUnwindTo, ok, err := rawtemporaldb.CanUnwindBeforeBlockNum(unwindTo, applyTx)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: requested=%d, minAllowed=%d", ErrTooDeepUnwind, unwindTo, allowedUnwindTo)
	}
	logger.Warn("Unwinding due to incorrect root hash", "to", unwindTo)
	if u != nil {
		if err := u.UnwindTo(allowedUnwindTo, BadBlock(blockHash, ErrInvalidStateRootHash), applyTx); err != nil {
			return err
		}
	}
	return nil
}

type FlushAndComputeCommitmentTimes struct {
	Flush             time.Duration
	ComputeCommitment time.Duration
}

// flushAndCheckCommitmentV3 - does write state to db and then check commitment
func flushAndCheckCommitmentV3(ctx context.Context, header *types.Header, applyTx kv.TemporalRwTx, doms *dbstate.SharedDomains, cfg ExecuteBlockCfg, e *StageState, maxBlockNum uint64, parallel bool, logger log.Logger, u Unwinder, inMemExec bool) (ok bool, times FlushAndComputeCommitmentTimes, err error) {
	start := time.Now()
	// E2 state root check was in another stage - means we did flush state even if state root will not match
	// And Unwind expecting it
	if !parallel {
		if err := e.Update(applyTx, maxBlockNum); err != nil {
			return false, times, err
		}
		if _, err := rawdb.IncrementStateVersion(applyTx); err != nil {
			return false, times, fmt.Errorf("writing plain state version: %w", err)
		}
	}

	if header == nil {
		return false, times, errors.New("header is nil")
	}

	if dbg.DiscardCommitment() {
		return true, times, nil
	}
	if doms.BlockNum() != header.Number.Uint64() {
		panic(fmt.Errorf("%d != %d", doms.BlockNum(), header.Number.Uint64()))
	}

	computedRootHash, err := doms.ComputeCommitment(ctx, applyTx, true, header.Number.Uint64(), doms.TxNum(), e.LogPrefix(), nil)

	times.ComputeCommitment = time.Since(start)
	if err != nil {
		return false, times, fmt.Errorf("compute commitment: %w", err)
	}

	if cfg.blockProduction {
		header.Root = common.BytesToHash(computedRootHash)
		return true, times, nil
	}
	if !bytes.Equal(computedRootHash, header.Root.Bytes()) {
		logger.Warn(fmt.Sprintf("[%s] Wrong trie root of block %d: %x, expected (from header): %x. Block hash: %x", e.LogPrefix(), header.Number.Uint64(), computedRootHash, header.Root.Bytes(), header.Hash()))
		err = handleIncorrectRootHashError(header.Number.Uint64(), header.Hash(), header.ParentHash,
			applyTx, cfg, e, maxBlockNum, logger, u)
		return false, times, err
	}
	if !inMemExec {
		start = time.Now()
		err := doms.Flush(ctx, applyTx)
		times.Flush = time.Since(start)
		if err != nil {
			return false, times, err
		}
	}
	return true, times, nil

}

func shouldGenerateChangeSets(cfg ExecuteBlockCfg, blockNum, maxBlockNum uint64, initialCycle bool) bool {
	if cfg.syncCfg.AlwaysGenerateChangesets {
		return true
	}
	if blockNum < cfg.blockReader.FrozenBlocks() {
		return false
	}
	if initialCycle {
		return false
	}
	// once past the initial cycle, make sure to generate changesets for the last blocks that fall in the reorg window
	return blockNum+cfg.syncCfg.MaxReorgDepth >= maxBlockNum
}
