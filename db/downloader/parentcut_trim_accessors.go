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

package downloader

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	dirutil "github.com/erigontech/erigon/common/dir"
)

// TrimPostCutSiblings removes every accessor/history/idx file whose
// step range extends past cutStep — i.e., files that provably
// contain post-cut state history or indexes. Also drops the matching
// .torrent sidecar for each removed file so a subsequent Downloader
// SetInventory re-scan doesn't try to reseed them.
//
// Scans snapDir/{accessor,history,idx} in full AND snapDir/domain
// for sibling accessor kinds (.bt, .kvei, .kvi, .vi, .efi) that live
// alongside primary .kv files. Primary .kv itself is excluded here —
// Provider.Unwind's regenerateBoundaryStepFiles handles kv boundary
// truncation separately. Without this pass, an in-process
// debug_setFork leaves domain-side accessors on disk and the
// fork-datadir validator on the next --chain=<fork-name> restart
// classifies them as post-cut straddlers and refuses to boot.
//
// Called from Ethereum.ApplyPostSwapHooks after the chain-config
// swap — cutStep is derived from chainConfig.CutBlock via the same
// step map the validator uses. Returns the number of files removed
// (excluding .torrent sidecars) and the first error encountered
// (best-effort — the caller logs but doesn't abort on failure since
// the fork-datadir validator will surface a real problem on the
// next boot).
func TrimPostCutSiblings(snapDir string, cutStep uint64) (int, error) {
	if snapDir == "" {
		return 0, nil
	}
	removed := 0
	// Order matters: remove index files (.vi/.efi/.bt/.kvi/.kvei) before
	// their source (.v/.ef/.kv) so a mid-way crash doesn't leave a
	// live index pointing at a missing source.
	//
	// domain/ hosts primary .kv files alongside their accessor siblings
	// (.bt / .kvei / .kvi). The primary .kv is filtered by
	// isPostCutStateSibling so regenerate-boundary-step-files' truncation
	// path stays authoritative for it; the siblings get trimmed here.
	for _, sub := range []string{"accessor", "history", "idx", "domain"} {
		dir := filepath.Join(snapDir, sub)
		info, err := os.Stat(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return removed, fmt.Errorf("TrimPostCutSiblings: stat %s: %w", dir, err)
		}
		if !info.IsDir() {
			continue
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return removed, fmt.Errorf("TrimPostCutSiblings: read %s: %w", dir, err)
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			// Skip .torrent sidecars — we'll drop them alongside
			// their primary in the next branch below.
			if strings.HasSuffix(name, ".torrent") {
				continue
			}
			if !isPostCutStateSibling(name, cutStep) {
				continue
			}
			path := filepath.Join(dir, name)
			if err := dirutil.RemoveFile(path); err != nil && !os.IsNotExist(err) {
				return removed, fmt.Errorf("TrimPostCutSiblings: remove %s: %w", path, err)
			}
			removed++
			// Best-effort sidecar removal — no error if missing.
			_ = dirutil.RemoveFile(path + ".torrent")
		}
	}
	return removed, nil
}

// isPostCutStateSibling returns true iff a filename in the state
// snapshot tree has a step range that contains any post-cut txNum —
// either entirely past the cut OR straddling it. Both classes fail
// the fork-datadir validator on the next --chain=<fork-name>
// restart, so both must be trimmed.
//
// Excludes primary .kv files under domain/ — regenerate-boundary-
// step-files (Provider.Unwind) owns kv boundary truncation. Every
// other state-file extension is treated as an accessor sibling: the
// domain-side (.bt, .kvi, .kvei), the history side (.v, .vi), the
// index side (.ef, .efi).
//
// Uses the same ParseFileName the copy planner uses so we get the
// same From/To interpretation. Anything unparseable is skipped
// (returns false) — matches the copy planner's conservative
// treatment of chain-wide config files.
func isPostCutStateSibling(basename string, cutStep uint64) bool {
	// Primary .kv is owned by regen; skip.
	if strings.HasSuffix(basename, ".kv") {
		return false
	}
	parsed, isState, _ := parseSiblingFileName(basename)
	if !isState {
		return false
	}
	if parsed.To == 0 {
		return false
	}
	// The file covers steps [From, To). cutStep is the step containing
	// cutTxNum. A file with To > cutStep covers step cutStep or later,
	// meaning it includes txNums at or past the cut. Straddlers must be
	// dropped whole; the fork's next retire cycle produces fresh
	// boundary files covering only pre-cut data.
	return parsed.To > cutStep
}

// parsedStepRange holds the From/To step boundaries extracted from a
// state-file filename.
type parsedStepRange struct {
	From uint64
	To   uint64
}

// parseSiblingFileName pulls the step range out of an accessor /
// history / idx filename. Format: <version>-<domain>.<From>-<To>.<ext>
// (e.g., "v1.1-code.288-296.vi"). Returns isState=true iff the
// filename matches this shape. Unparseable / unrelated filenames
// return zero values with isState=false.
func parseSiblingFileName(basename string) (parsedStepRange, bool, error) {
	// Strip extension.
	name := basename
	if i := strings.LastIndex(name, "."); i > 0 {
		name = name[:i]
	}
	// Now name should look like "v1.1-code.288-296". Split on the
	// last "." to get the range portion "288-296".
	i := strings.LastIndex(name, ".")
	if i < 0 {
		return parsedStepRange{}, false, nil
	}
	rangePart := name[i+1:]
	found := strings.Contains(rangePart, "-")
	if !found {
		return parsedStepRange{}, false, nil
	}
	var from, to uint64
	if _, err := fmt.Sscanf(rangePart, "%d-%d", &from, &to); err != nil {
		return parsedStepRange{}, false, nil
	}
	if to == 0 || to <= from {
		return parsedStepRange{}, false, nil
	}
	return parsedStepRange{From: from, To: to}, true, nil
}
