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
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datastruct/btindex"
	"github.com/erigontech/erigon/db/integrity"
	"github.com/erigontech/erigon/node/components/storage/snapshot"
)

// safeProbeDomainFileAgainstBt cross-checks a .kv against its .bt
// accessor without opening either as a decompressor or mmap: reads
// bounded byte ranges of .bt to extract the EF-encoded upper offset
// bound, then compares against os.Stat(kvPath).Size().
//
// Returns nil when the accessor sits within the .kv, an
// ErrDomainFileSegInvalid-wrapped error when it addresses past the
// .kv's end (the SIGSEGV class the design-doc catches), a
// non-integrity error for missing/unreadable files.
func safeProbeDomainFileAgainstBt(kvPath, btPath string) error {
	kvSt, err := os.Stat(kvPath)
	if err != nil {
		return err
	}
	u, err := btindex.ReadKvDataSizeFromBt(btPath)
	if err != nil {
		return err
	}
	if u >= uint64(kvSt.Size()) {
		return fmt.Errorf("%w: %s: accessor u=%d >= kv size=%d", integrity.ErrDomainFileSegInvalid, kvPath, u, kvSt.Size())
	}
	return nil
}

// probeBootstrapDomainFiles walks every non-commitment state-domain
// .kv in Inventory and quarantines any whose .bt accessor advertises
// offsets past the .kv's end. Commitment files are covered by
// extractBootstrapCommitmentAnchors' safeExtractCommitmentRecord
// wrapper. Missing .bt is not an error (some domain files use .kvi
// hash-map instead); those get skipped. Best-effort: os / read
// errors on individual files are logged and skipped, not fatal.
func probeBootstrapDomainFiles(inv *snapshot.Inventory, snapDir string, logger log.Logger) {
	if inv == nil || snapDir == "" {
		return
	}
	var probed, quarantined int
	for _, domain := range snapshot.AllDomains {
		if domain == snapshot.DomainCommitment {
			continue
		}
		for _, entry := range inv.AllDomainFiles(domain) {
			if entry == nil || !strings.HasSuffix(entry.Name, ".kv") {
				continue
			}
			kvPath := snapshot.PathForName(snapDir, entry.Name)
			btPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
			if _, err := os.Stat(btPath); errors.Is(err, fs.ErrNotExist) {
				continue
			} else if err != nil {
				if logger != nil {
					logger.Debug("[storage] bootstrap probe: stat .bt failed", "file", entry.Name, "err", err)
				}
				continue
			}
			probed++
			if err := safeProbeDomainFileAgainstBt(kvPath, btPath); err != nil {
				if !errors.Is(err, integrity.ErrDomainFileSegInvalid) {
					if logger != nil {
						logger.Debug("[storage] bootstrap probe: skip on non-integrity error", "file", entry.Name, "err", err)
					}
					continue
				}
				if logger != nil {
					logger.Warn("[storage] bootstrap probe: quarantining domain file with mis-sized accessor",
						"domain", domain, "file", entry.Name, "err", err)
				}
				quarantineCorruptStateFileFamily(snapDir, entry.Name, "accessor-past-kv-end", logger)
				inv.RemoveFile(entry.Name)
				quarantined++
			}
		}
	}
	if logger != nil && probed > 0 {
		logger.Info("[storage] bootstrap domain-file accessor probe complete",
			"probed", probed, "quarantined", quarantined)
	}
}
