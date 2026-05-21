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

package storage

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	dlcomp "github.com/erigontech/erigon/node/components/downloader"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/validation"
)

// AdoptionRequest is the input to a staged canonical adoption run
// (docs/plans/20260520-phase7-staged-adoption-design.md §7b-3).
type AdoptionRequest struct {
	// Verdict is the minority outcome of CheckOwnAdvertisement: the set
	// of files this node holds at a non-canonical hash.
	Verdict *snapshotsync.MinorityVerdict

	// Policy controls how far the run proceeds automatically. warn does
	// nothing; auto and stage both stage + validate (the cutover that
	// separates them is Phase 7c).
	Policy snapshotsync.AdoptionPolicy

	// CanonicalVersion names the staging directory — .staging-<version>.
	// In production it is CanonicalView.Version().
	CanonicalVersion string

	// PruneMode classifies the node's pruning so the commitment and
	// receipt validators treat an intentionally-absent body/history as
	// a skip rather than a failure.
	PruneMode prune.Mode

	// Downloader fetches the canonical files. The adoption handler does
	// not own a torrent client; FetchCanonicalBatch reuses the live one.
	Downloader *dlcomp.Provider
}

// AdoptionOutcome enumerates the result of a staged adoption run.
type AdoptionOutcome int

const (
	// AdoptionNoop — nothing to adopt, or policy=warn so the operator
	// must invoke adoption explicitly.
	AdoptionNoop AdoptionOutcome = iota
	// AdoptionSaltDivergence — the verdict includes a salt file. A salt
	// change invalidates every accessor in the datadir, a wholesale
	// reindex rather than the incremental file delta staged adoption is
	// built for. The run aborts before fetching anything; the operator
	// must intervene (memory/salt-divergence-abort-adoption.md).
	AdoptionSaltDivergence
	// AdoptionStaged — the canonical delta was fetched and passed both
	// validation stages. The staging directory is left in place for a
	// later cutover (policy=stage).
	AdoptionStaged
	// AdoptionCutOver — the staged batch was validated and atomically
	// swapped over the live files (policy=auto).
	AdoptionCutOver
)

func (o AdoptionOutcome) String() string {
	switch o {
	case AdoptionNoop:
		return "noop"
	case AdoptionSaltDivergence:
		return "salt-divergence"
	case AdoptionStaged:
		return "staged"
	case AdoptionCutOver:
		return "cut-over"
	default:
		return "unknown"
	}
}

// AdoptionResult reports the outcome of RunStagedAdoption.
type AdoptionResult struct {
	Outcome AdoptionOutcome
	// Batch is the staged, validated file set — non-nil only for
	// AdoptionStaged. Phase 7c cuts it over.
	Batch *dlcomp.StagedBatch
	// Reason is an operator-readable summary of the outcome.
	Reason string
}

// RunStagedAdoption carries a minority verdict through to a staged,
// fully-validated canonical batch — never building an index and never
// touching a live file. The pipeline is: salt guard → fetch → Stage 1
// (per-file) → Stage 2 (cross-file, against an in-code overlay of the
// staged files). On any validation failure the staging directory is
// removed; the live DB and live snapshot directory are never mutated,
// so recovery is "delete and retry" with no rollback path.
//
// The atomic cutover that promotes a staged batch to live is Phase 7c;
// this handler stops at AdoptionStaged.
func (p *Provider) RunStagedAdoption(ctx context.Context, req AdoptionRequest) (*AdoptionResult, error) {
	if req.Verdict == nil || len(req.Verdict.Adopt) == 0 {
		return &AdoptionResult{Outcome: AdoptionNoop, Reason: "no minority entries to adopt"}, nil
	}

	// Salt guard — runs before any fetch. A salt file among the verdict
	// entries means the node's state-index salt differs from canonical;
	// every accessor would need rebuilding. That is not an incremental
	// adoption — abort and escalate to the operator.
	if salt := saltFilesIn(req.Verdict.Adopt); len(salt) > 0 {
		return &AdoptionResult{
			Outcome: AdoptionSaltDivergence,
			Reason:  fmt.Sprintf("salt divergence (%s) — every accessor would need rebuilding; operator must intervene", strings.Join(salt, ", ")),
		}, nil
	}

	if req.Policy == snapshotsync.AdoptionWarn {
		return &AdoptionResult{
			Outcome: AdoptionNoop,
			Reason:  fmt.Sprintf("policy=warn — %d canonical files diverge; run 'erigon snapshots adopt'", len(req.Verdict.Adopt)),
		}, nil
	}

	files, err := canonicalFilesFromVerdict(req.Verdict)
	if err != nil {
		return nil, fmt.Errorf("adoption: %w", err)
	}
	if req.Downloader == nil {
		return nil, fmt.Errorf("adoption: nil downloader")
	}

	batch, err := req.Downloader.FetchCanonicalBatch(ctx, req.CanonicalVersion, files)
	if err != nil {
		return nil, fmt.Errorf("adoption: fetch canonical batch: %w", err)
	}

	if err := p.validateStagedBatch(ctx, batch, req.PruneMode); err != nil {
		_ = dir.RemoveAll(batch.Dir)
		return nil, fmt.Errorf("adoption: staged batch failed validation: %w", err)
	}

	// Intent marker: the batch passed Stage 1 + Stage 2 and is safe to
	// promote. A crash before the cutover finishes leaves a marked
	// staging dir that `erigon snapshots adopt` (or a re-run) completes;
	// an unmarked dir is an interrupted fetch and is never cut over.
	if err := os.WriteFile(filepath.Join(batch.Dir, snapshot.AdoptionReadyMarker), nil, 0o644); err != nil {
		_ = dir.RemoveAll(batch.Dir)
		return nil, fmt.Errorf("adoption: write ready marker: %w", err)
	}

	// policy=stage stops here — the validated batch waits on disk for an
	// operator-triggered cutover.
	if req.Policy == snapshotsync.AdoptionStage {
		if p.logger != nil {
			p.logger.Info("[storage] canonical batch staged and validated",
				"version", req.CanonicalVersion, "files", len(batch.Files), "dir", batch.Dir)
		}
		return &AdoptionResult{
			Outcome: AdoptionStaged,
			Batch:   batch,
			Reason:  fmt.Sprintf("%d canonical files staged and validated — awaiting cutover", len(batch.Files)),
		}, nil
	}

	// policy=auto — atomically swap the validated batch over the live
	// files and re-open the views.
	if err := p.cutoverStagedBatch(batch, req.Downloader); err != nil {
		return nil, fmt.Errorf("adoption: cutover: %w", err)
	}
	return &AdoptionResult{
		Outcome: AdoptionCutOver,
		Batch:   batch,
		Reason:  fmt.Sprintf("%d canonical files adopted and cut over", len(batch.Files)),
	}, nil
}

// saltFilesIn returns the names of any salt files among the mismatches.
// A salt file is salt-<scope>.txt — the same predicate the snapshot
// metadata layer uses to classify KindSalt.
func saltFilesIn(adopt []snapshotsync.AdvertisementMismatch) []string {
	var out []string
	for _, m := range adopt {
		if strings.HasPrefix(m.Name, "salt-") && strings.HasSuffix(m.Name, ".txt") {
			out = append(out, m.Name)
		}
	}
	return out
}

// canonicalFilesFromVerdict converts each minority mismatch into the
// (name, canonical info-hash) pair FetchCanonicalBatch fetches by.
func canonicalFilesFromVerdict(v *snapshotsync.MinorityVerdict) ([]dlcomp.CanonicalFile, error) {
	files := make([]dlcomp.CanonicalFile, 0, len(v.Adopt))
	for _, m := range v.Adopt {
		raw, err := hex.DecodeString(m.CanonicalHash)
		if err != nil || len(raw) != 20 {
			return nil, fmt.Errorf("bad canonical hash %q for %s", m.CanonicalHash, m.Name)
		}
		var h [20]byte
		copy(h[:], raw)
		files = append(files, dlcomp.CanonicalFile{Name: m.Name, InfoHash: h})
	}
	return files, nil
}

// validateStagedBatch runs Stage 1 (per-file) and Stage 2 (cross-file)
// against the staged files. Stage 2 reads them through an in-code
// overlay — a temporal RO tx and a BlockReader built with path
// overrides — so the cross-file validators see the staged bytes
// without a second physical datadir and without touching live files.
func (p *Provider) validateStagedBatch(ctx context.Context, batch *dlcomp.StagedBatch, pruneMode prune.Mode) error {
	entries := make([]*snapshot.FileEntry, 0, len(batch.Files))
	for _, sf := range batch.Files {
		fe := &snapshot.FileEntry{Name: sf.Name, TorrentHash: sf.InfoHash, Size: sf.Size, Local: true}
		snapshot.PopulateFromName(fe)
		entries = append(entries, fe)
	}

	// Stage 1 — per-file checks resolved against the staging directory.
	stage1 := validation.DefaultStage1ChainWithDisk(batch.Dir)
	for i, fe := range entries {
		content := validation.FileContent{Path: batch.Files[i].Path}
		if err := stage1.Validate(fe, content); err != nil {
			return fmt.Errorf("stage 1 (%s): %w", fe.Name, err)
		}
	}

	return p.validateStagedBatchStage2(ctx, batch, entries, pruneMode)
}

// validateStagedBatchStage2 builds the overlay temporal DB / BlockReader
// and runs the four cross-file StepValidators against the staged files.
func (p *Provider) validateStagedBatchStage2(ctx context.Context, batch *dlcomp.StagedBatch, entries []*snapshot.FileEntry, pruneMode prune.Mode) error {
	if p.ChainDB == nil || p.BlockReader == nil {
		return fmt.Errorf("stage 2 requires ChainDB and BlockReader")
	}

	var domainKVPaths, blockSegPaths []string
	for i, fe := range entries {
		path := batch.Files[i].Path
		switch {
		case fe.Domain != "" && strings.HasSuffix(fe.Name, ".kv"):
			domainKVPaths = append(domainKVPaths, path)
		case fe.Domain == "" && strings.HasSuffix(fe.Name, ".seg"):
			blockSegPaths = append(blockSegPaths, path)
		}
	}

	// Stage-2 DB: substitute the staged domain values files for this
	// validation only. With no domain overrides the live DB is used
	// directly — the validators read the live state, which is correct
	// when only block files are being adopted.
	var stage2DB kv.TemporalRoDB = p.ChainDB
	if len(domainKVPaths) > 0 {
		base, ok := p.ChainDB.(*temporal.DB)
		if !ok {
			return fmt.Errorf("stage 2: ChainDB is not a *temporal.DB, cannot overlay domain files")
		}
		stage2DB = &roOverlayDB{DB: base, overridePaths: domainKVPaths}
	}

	// Stage-2 BlockReader: an independent throwaway RoSnapshots with the
	// staged block segments substituted in. Closed when validation ends.
	stage2BR := p.BlockReader
	if len(blockSegPaths) > 0 {
		if p.AllSnapshots == nil {
			return fmt.Errorf("stage 2: nil AllSnapshots, cannot overlay block segments")
		}
		overlaySnaps, err := freezeblocks.OpenRoSnapshotsWithOverrides(
			p.AllSnapshots.Cfg(), p.AllSnapshots.Dir(), blockSegPaths, p.logger)
		if err != nil {
			return fmt.Errorf("stage 2: open block overlay: %w", err)
		}
		defer overlaySnaps.Close()
		stage2BR = freezeblocks.NewBlockReader(overlaySnaps, nil)
	}

	return p.runStage2Validators(ctx, stage2DB, stage2BR, entries, pruneMode)
}

// runStage2Validators runs the header-chain and tx-root validators over
// the staged block segments, and the commitment and receipt validators
// over each staged domain step. Any error — including a transient
// validation.ErrPause — aborts: a fully-staged batch validated against
// a synchronously-opened overlay has nothing legitimately pending, so a
// pause signals a real problem.
func (p *Provider) runStage2Validators(ctx context.Context, db kv.TemporalRoDB, br services.FullBlockReader, entries []*snapshot.FileEntry, pruneMode prune.Mode) error {
	hasBlock := false
	for _, fe := range entries {
		if fe.Domain == "" && (strings.HasSuffix(fe.Name, ".seg") || strings.HasSuffix(fe.Name, ".idx")) {
			hasBlock = true
			break
		}
	}

	if hasBlock {
		if err := (HeaderChainValidator{DB: db, BlockReader: br}).ValidateStep(ctx, entries); err != nil {
			return fmt.Errorf("header chain: %w", err)
		}
		if err := (TxRootValidator{DB: db, BlockReader: br}).ValidateStep(ctx, entries); err != nil {
			return fmt.Errorf("tx root: %w", err)
		}
	}

	// Commitment and receipt validators dispatch on files[0] — call them
	// once per staged domain step with that step's single values file.
	for _, fe := range entries {
		if !strings.HasSuffix(fe.Name, ".kv") {
			continue
		}
		switch fe.Domain {
		case snapshot.DomainCommitment:
			cdv := CommitmentDomainValidator{DB: db, BlockReader: br, PruneMode: pruneMode}
			if err := cdv.ValidateStep(ctx, []*snapshot.FileEntry{fe}); err != nil {
				return fmt.Errorf("commitment %s: %w", fe.Name, err)
			}
		case snapshot.DomainReceipt:
			if p.ChainConfig == nil {
				return fmt.Errorf("receipt validation needs a chain config")
			}
			rrv := ReceiptRootValidator{DB: db, BlockReader: br, ChainConfig: p.ChainConfig, PruneMode: pruneMode}
			if err := rrv.ValidateStep(ctx, []*snapshot.FileEntry{fe}); err != nil {
				return fmt.Errorf("receipt %s: %w", fe.Name, err)
			}
		}
	}
	return nil
}

// cutoverSwap is one file's move from the staging directory to its
// live location, carrying the canonical info-hash to re-stamp.
type cutoverSwap struct {
	src, dst, name string
	hash           [20]byte
}

// cutoverStagedBatch atomically swaps a validated staged batch over the
// live snapshot files on a running node and re-opens the state and
// block views. The renames and both OpenFolder rebuilds run under the
// Aggregator commit barrier (LockCollation) — the same lock the merge
// pipeline takes — so no reader observes a half-swapped file set and no
// merge runs concurrently.
//
// rename(2) is atomic and leaves an open/mmap'd old inode alive for
// existing readers, so a held view finishes on old bytes; only a new
// view opened after OpenFolder sees the canonical set. On any failure
// the error is returned loudly: by then files may be swapped but the
// live DB and snapshot dir were never validated against — crash-safe
// recovery via an intent journal is deferred
// (docs/plans/20260520-phase7-staged-adoption-design.md, open questions).
func (p *Provider) cutoverStagedBatch(batch *dlcomp.StagedBatch, dl *dlcomp.Provider) error {
	if p.Aggregator == nil || p.AllSnapshots == nil {
		return fmt.Errorf("cutover requires Aggregator and AllSnapshots")
	}
	liveSnapDir := p.AllSnapshots.Dir()

	// Pre-flight, before taking the barrier: resolve every destination
	// and create its directory, so the barrier holds only the renames
	// and the two OpenFolder rebuilds.
	swaps := make([]cutoverSwap, 0, len(batch.Files))
	for _, sf := range batch.Files {
		if _, err := os.Stat(sf.Path); err != nil {
			return fmt.Errorf("staged file %s: %w", sf.Name, err)
		}
		dst := snapshot.PathForName(liveSnapDir, sf.Name)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return fmt.Errorf("create dir for %s: %w", sf.Name, err)
		}
		swaps = append(swaps, cutoverSwap{src: sf.Path, dst: dst, name: sf.Name, hash: sf.InfoHash})
	}

	if err := p.renameAndReopen(swaps); err != nil {
		return err
	}

	// Post-barrier: re-point inventory + downloader at the canonical
	// content and re-advertise. A republish failure does not undo the
	// completed swap — log and continue.
	for _, s := range swaps {
		snapshot.SetTorrentHash(p.Inventory, s.name, s.hash)
		if dl != nil && dl.Downloader != nil {
			dl.Downloader.DropTorrentByName(s.name)
			_ = dir.RemoveFile(s.dst + ".torrent")
		}
	}
	if dl != nil && dl.Downloader != nil && p.Inventory != nil {
		if err := dl.Downloader.PublishLocalChainTomlV2(p.Inventory); err != nil && p.logger != nil {
			p.logger.Warn("[storage] adoption cutover: republish failed", "err", err)
		}
	}
	if err := dir.RemoveAll(batch.Dir); err != nil && p.logger != nil {
		p.logger.Warn("[storage] adoption cutover: staging cleanup failed", "dir", batch.Dir, "err", err)
	}
	if p.logger != nil {
		p.logger.Info("[storage] adoption cutover complete", "files", len(swaps))
	}
	return nil
}

// renameAndReopen performs the barrier-held core of the cutover: rename
// every staged file over its live counterpart, then rebuild the state
// and block visible-file views. UnlockCollation is deferred so the
// barrier is released the moment the rebuild finishes, before the
// caller's post-barrier inventory work.
func (p *Provider) renameAndReopen(swaps []cutoverSwap) error {
	p.Aggregator.LockCollation()
	defer p.Aggregator.UnlockCollation()

	for _, s := range swaps {
		if err := snapshot.MoveFileAcrossFS(s.src, s.dst); err != nil {
			return fmt.Errorf("move %s: %w", s.name, err)
		}
	}
	if err := p.Aggregator.OpenFolder(); err != nil {
		return fmt.Errorf("reopen aggregator: %w", err)
	}
	if err := p.AllSnapshots.OpenFolder(); err != nil {
		return fmt.Errorf("reopen snapshots: %w", err)
	}
	return nil
}

// roOverlayDB wraps the live temporal DB so a read transaction opened
// through it substitutes the staged domain (.kv) files in overridePaths.
// The four Stage-2 StepValidators each take a DB and open their own
// transaction; this wrapper points them at the staged files without a
// second physical datadir. Only the read entry points are overridden —
// every other method is the live DB's, inherited by embedding.
type roOverlayDB struct {
	*temporal.DB
	overridePaths []string
}

func (o *roOverlayDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	return o.DB.BeginTemporalRoWithOverrides(ctx, o.overridePaths)
}

func (o *roOverlayDB) BeginRo(ctx context.Context) (kv.Tx, error) {
	return o.BeginTemporalRo(ctx)
}

func (o *roOverlayDB) ViewTemporal(ctx context.Context, f func(kv.TemporalTx) error) error {
	tx, err := o.BeginTemporalRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(tx)
}

func (o *roOverlayDB) View(ctx context.Context, f func(kv.Tx) error) error {
	tx, err := o.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f(tx)
}
