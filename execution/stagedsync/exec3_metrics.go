package stagedsync

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/kv"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/consensus"
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

type executor interface {
	LogExecuted()
	LogCommitted(commitStart time.Time, committedBlocks uint64, committedTransactions uint64, committedGas uint64, stepsInDb float64, lastProgress commitment.CommitProgress)
	LogComplete(stepsInDb float64)
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
	mxExecBlockDuration.Set(float64(avgBlockDur))
	execVals = append(execVals, "bdur", common.Round(avgBlockDur, 0))

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
