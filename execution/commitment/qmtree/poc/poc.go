// Package poc implements a proof-of-concept that builds a qmtree from
// Ethereum state history, computing block-level and tx-level roots.
package poc

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/order"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/execution/commitment/qmtree"
)

// BlockResult holds the qmtree root and stats after processing one block.
type BlockResult struct {
	BlockNum  uint64
	Root      common.Hash
	TxCount   int
	Changes   int
	Elapsed   time.Duration
}

// Runner drives the PoC: iterates blocks, builds state entries, appends
// them to a qmtree, and records the resulting roots.
type Runner struct {
	logger      log.Logger
	db          kv.TemporalRoDB
	blockReader services.FullBlockReader
	outputDir   string
	dataDir     string // directory for tree storage files (entries + twigs)
}

func NewRunner(logger log.Logger, db kv.TemporalRoDB, br services.FullBlockReader, outputDir string) *Runner {
	return &Runner{
		logger:      logger,
		db:          db,
		blockReader: br,
		outputDir:   outputDir,
	}
}

// WithDataDir sets the directory for persisting tree data. If empty,
// a temp directory is created and removed on close.
func (r *Runner) WithDataDir(dir string) *Runner {
	r.dataDir = dir
	return r
}

// Run builds a qmtree over the block range [fromBlock, toBlock] and writes
// results to outputDir.
func (r *Runner) Run(ctx context.Context, fromBlock, toBlock uint64) ([]BlockResult, error) {
	tx, err := r.db.BeginTemporalRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	tnr := r.blockReader.TxnumReader()
	if toBlock == 0 {
		toBlock, _, err = tnr.Last(tx)
		if err != nil {
			return nil, err
		}
	}

	r.logger.Info("qmtree poc: starting", "fromBlock", fromBlock, "toBlock", toBlock)

	hasher := &qmtree.Keccak256Hasher{}

	// Create disk-backed storage if dataDir is set, otherwise use in-memory (nil).
	var entryStorage qmtree.EntryStorage
	var twigStorage qmtree.TwigStorage
	dataDir := r.dataDir
	if dataDir == "" {
		var err2 error
		dataDir, err2 = os.MkdirTemp("", "qmtree-poc-*")
		if err2 != nil {
			return nil, fmt.Errorf("create temp dir: %w", err2)
		}
		defer os.RemoveAll(dataDir)
	}

	const bufferSize = 1 << 20   // 1MB buffer
	const segmentSize = 1 << 30  // 1GB segments

	entriesDir := filepath.Join(dataDir, "entries")
	twigDir := filepath.Join(dataDir, "twigs")
	if err := os.MkdirAll(entriesDir, 0755); err != nil {
		return nil, fmt.Errorf("create entries dir: %w", err)
	}
	if err := os.MkdirAll(twigDir, 0755); err != nil {
		return nil, fmt.Errorf("create twig dir: %w", err)
	}

	ef, err := qmtree.NewEntryFile(bufferSize, segmentSize, entriesDir)
	if err != nil {
		return nil, fmt.Errorf("create entry file: %w", err)
	}
	defer ef.Close()
	entryStorage = ef

	tf, err := qmtree.NewTwigFile(bufferSize, segmentSize, twigDir, hasher)
	if err != nil {
		return nil, fmt.Errorf("create twig file: %w", err)
	}
	defer tf.Close()
	twigStorage = tf

	tree := qmtree.NewTree(hasher, 0, entryStorage, twigStorage)

	results := make([]BlockResult, 0, toBlock-fromBlock+1)

	// We need to know the starting serial number. The qmtree expects
	// contiguous serial numbers starting from 0. We use a running counter.
	var nextSN uint64

	for block := fromBlock; block <= toBlock; block++ {
		if ctx.Err() != nil {
			return results, ctx.Err()
		}

		br, err := r.processBlock(ctx, tx, tnr, tree, hasher, block, &nextSN)
		if err != nil {
			return results, fmt.Errorf("block %d: %w", block, err)
		}
		results = append(results, br)

		if block%1000 == 0 || block == toBlock {
			totalSize := ef.Size() + tf.Size()
			r.logger.Info("qmtree poc: progress",
				"block", block,
				"root", br.Root,
				"totalLeaves", nextSN,
				"elapsed", br.Elapsed,
				"totalSize", fmtSize(totalSize),
			)
		}
	}

	if r.outputDir != "" {
		if err := r.writeCSV(results); err != nil {
			return results, fmt.Errorf("write csv: %w", err)
		}
	}

	// Report final file sizes.
	entrySize := ef.Size()
	twigSize := tf.Size()
	r.logger.Info("qmtree poc: finished",
		"blocks", len(results),
		"totalLeaves", nextSN,
		"entryFileSize", fmtSize(entrySize),
		"twigFileSize", fmtSize(twigSize),
		"totalSize", fmtSize(entrySize+twigSize),
		"dataDir", dataDir,
	)
	return results, nil
}

// processBlock builds StateEntries for each txnum in the block,
// appends them to the tree, syncs, and returns the block root.
func (r *Runner) processBlock(
	ctx context.Context,
	tx kv.TemporalTx,
	tnr rawdbv3.TxNumsReader,
	tree *qmtree.Tree,
	hasher *qmtree.Keccak256Hasher,
	block uint64,
	nextSN *uint64,
) (BlockResult, error) {
	start := time.Now()

	fromTxNum, err := tnr.Min(ctx, tx, block)
	if err != nil {
		return BlockResult{}, err
	}
	maxTxNum, err := tnr.Max(ctx, tx, block)
	if err != nil {
		return BlockResult{}, err
	}

	totalChanges := 0
	txCount := 0

	// For each txnum in the block, collect state changes and append a leaf.
	for txNum := fromTxNum; txNum <= maxTxNum; txNum++ {
		changes, err := collectChanges(tx, txNum)
		if err != nil {
			return BlockResult{}, fmt.Errorf("txnum %d: %w", txNum, err)
		}

		entry := NewStateEntry(*nextSN, changes)
		if _, err := tree.AppendEntry(entry); err != nil {
			return BlockResult{}, fmt.Errorf("append sn=%d txnum=%d: %w", *nextSN, txNum, err)
		}
		*nextSN++
		totalChanges += len(changes)
		txCount++
	}

	// Sync the tree to compute the current root.
	root := syncTree(tree, hasher)

	return BlockResult{
		BlockNum: block,
		Root:     root,
		TxCount:  txCount,
		Changes:  totalChanges,
		Elapsed:  time.Since(start),
	}, nil
}

// collectChanges gathers all account and storage changes for a single txnum.
func collectChanges(tx kv.TemporalTx, txNum uint64) ([]StateChange, error) {
	var changes []StateChange
	for _, domain := range []kv.Domain{kv.AccountsDomain, kv.StorageDomain} {
		it, err := tx.HistoryRange(domain, int(txNum), int(txNum+1), order.Asc, -1)
		if err != nil {
			return nil, err
		}
		for it.HasNext() {
			k, v, err := it.Next()
			if err != nil {
				it.Close()
				return nil, err
			}
			// v from HistoryRange is the *previous* value. We need the current
			// value at this txnum. Read it via GetAsOf at txNum+1 (i.e., the
			// state after this tx committed).
			currentVal, _, err := tx.GetAsOf(domain, k, txNum+1)
			if err != nil {
				it.Close()
				return nil, fmt.Errorf("GetAsOf(%s, %x, %d): %w", domain, k, txNum+1, err)
			}
			changes = append(changes, StateChange{
				Domain: domain,
				Key:    common.Copy(k),
				Value:  common.Copy(currentVal),
			})
			_ = v // previous value not needed
		}
		it.Close()
	}
	return changes, nil
}

// syncTree runs the full three-phase sync to compute the tree root.
func syncTree(tree *qmtree.Tree, hasher *qmtree.Keccak256Hasher) common.Hash {
	return common.Hash(tree.SyncAndRoot(hasher))
}

// writeCSV writes block results to a CSV file.
func (r *Runner) writeCSV(results []BlockResult) error {
	if err := os.MkdirAll(r.outputDir, 0755); err != nil {
		return err
	}
	f, err := os.Create(path.Join(r.outputDir, "qmtree_roots.csv"))
	if err != nil {
		return err
	}
	defer f.Close()
	return writeResultsCSV(f, results)
}

func fmtSize(bytes int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
	)
	switch {
	case bytes >= gb:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(gb))
	case bytes >= mb:
		return fmt.Sprintf("%.2f MB", float64(bytes)/float64(mb))
	case bytes >= kb:
		return fmt.Sprintf("%.2f KB", float64(bytes)/float64(kb))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func writeResultsCSV(w io.Writer, results []BlockResult) error {
	cw := csv.NewWriter(w)
	defer cw.Flush()
	if err := cw.Write([]string{"block_num", "root", "tx_count", "changes", "elapsed_ns"}); err != nil {
		return err
	}
	for _, br := range results {
		if err := cw.Write([]string{
			fmt.Sprintf("%d", br.BlockNum),
			fmt.Sprintf("%x", br.Root),
			fmt.Sprintf("%d", br.TxCount),
			fmt.Sprintf("%d", br.Changes),
			fmt.Sprintf("%d", br.Elapsed.Nanoseconds()),
		}); err != nil {
			return err
		}
	}
	return nil
}
