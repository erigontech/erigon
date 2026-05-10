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

package validation

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// StepValidator runs across the grouped set of files for one batch
// (a step group, a block-range group, or any other coherent set the
// retire / merge cycle produces). It receives the file list at
// validation time — distinct from the per-file Validator (which sees
// one file at a time) and from the configure-once BatchValidator
// (which knows its inputs from its constructor).
//
// Used by the lifecycle's per-step validation hook
// (lifecycle.BuildOnBatchValidation): when a group's files are all
// at LifecycleIndexed, the chain runs once across the file list; on
// pass the whole group advances to LifecycleAdvertisable atomically;
// on fail the group stays at Indexed for the next sweep to retry.
//
// Validators that need group-key info (Domain, FromStep, ToStep,
// FromBlock, ToBlock) introspect the first file's fields — within a
// group all files share the relevant axis. Empty file lists
// short-circuit.
//
// Validators should be cheap when possible — the timing target is
// "fast enough to not delay publication". Heavy checks belong behind
// opt-in flags (--snap.validate-disk, --snap.validate-format).
type StepValidator interface {
	// Name is a stable identifier (used in error wrapping, log
	// messages, and operator alerting).
	Name() string
	// ValidateStep returns nil on accept. On reject, the wrapped
	// error should identify which file(s) failed and why.
	ValidateStep(ctx context.Context, files []*snapshot.FileEntry) error
}

// StepChain runs StepValidators in order, returning the first
// failure wrapped with the validator's Name. An empty chain accepts
// everything.
type StepChain []StepValidator

// Validate runs every validator in chain order. Stops on first
// failure.
func (c StepChain) Validate(ctx context.Context, files []*snapshot.FileEntry) error {
	for _, v := range c {
		if err := v.ValidateStep(ctx, files); err != nil {
			return fmt.Errorf("%s: %w", v.Name(), err)
		}
	}
	return nil
}

// AllFilesPresent is the default Tier 1 step validator: it stat-checks
// every file in the group to confirm the bytes are actually on disk.
// Catches the orphan-publication case where a file's metadata says
// Local=true / Indexed but the file was deleted, moved, or never
// finalised on disk.
//
// Cost: one os.Stat per file in the group. Sub-millisecond per step
// on typical filesystems. The "is it really there" question is the
// floor-level integrity check; everything below it (downloader marked
// it complete) and everything above it (size matches torrent, content
// parses) builds on the assumption that the bytes are present.
type AllFilesPresent struct {
	// SnapDir is the snapshots directory the group's files live
	// under. Empty SnapDir is rejected at validate time (programmer
	// error in the caller).
	SnapDir string
}

// Name implements StepValidator.
func (AllFilesPresent) Name() string { return "all_files_present" }

// ValidateStep implements StepValidator. Each file must be stat-able
// (i.e. exists, accessible). Files marked Local=false are skipped —
// the validator can't speak to bytes that aren't supposed to be on
// disk yet.
func (a AllFilesPresent) ValidateStep(_ context.Context, files []*snapshot.FileEntry) error {
	if a.SnapDir == "" {
		return fmt.Errorf("AllFilesPresent: empty SnapDir")
	}
	for _, f := range files {
		if !f.Local {
			continue
		}
		path := snapshot.PathForName(a.SnapDir, f.Name)
		if _, err := os.Stat(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("file %q marked Local but missing on disk", f.Name)
			}
			return fmt.Errorf("stat %q: %w", f.Name, err)
		}
	}
	return nil
}

// DefaultStepChain returns the always-on default step-validation
// chain: just the presence check. Heavier validators (content shape,
// torrent-size, format integrity) plug in via separate flag-gated
// chains and are appended explicitly by the caller.
func DefaultStepChain(snapDir string) StepChain {
	return StepChain{AllFilesPresent{SnapDir: snapDir}}
}
