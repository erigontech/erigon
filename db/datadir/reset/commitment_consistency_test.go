package reset

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/go-quicktest/qt"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/preverified"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/seg"
)

// Commitment values reference accounts and storage records by byte offset into the .kv of the same
// step range, so the two must come from the same build. Reset normalises preverified files to
// canonical content while leaving unknown ones alone, which can pair a local commitment with a
// canonical accounts file and invalidate every offset it holds.
func runMixedStateReset(t *testing.T, root *os.Root, preverifiedNames []string, allow bool) error {
	startEntries := []fsEntry{
		{Name: "snapshots/domain/v1.0-accounts.0-256.kv"},
		{Name: "snapshots/domain/v1.0-storage.0-256.kv"},
		{Name: "snapshots/domain/v1.0-commitment.0-256.kv"},
	}
	makeEntries(t, startEntries, root)
	r := makeTestingReset(t, startEntries, root, "", ".")
	// --keep-local-snapshots: files outside the preverified set are kept as-is.
	r.RemoveUnknown = false
	r.AllowMixedStateBuilds = allow
	for _, name := range preverifiedNames {
		r.PreverifiedSnapshots = append(r.PreverifiedSnapshots, preverified.Item{Name: name})
	}
	r.PreverifiedSnapshots.Sort()
	return r.Run()
}

func TestResetRefusesLocalCommitmentBesideCanonicalState(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		err := runMixedStateReset(t, root, []string{
			"domain/v1.0-accounts.0-256.kv",
			"domain/v1.0-storage.0-256.kv",
		}, false)
		qt.Assert(t, qt.IsNotNil(err))
		qt.Assert(t, qt.StringContains(err.Error(), "0-256"))
	})
}

// The mirror image is equally broken: a canonical commitment file addressing a local accounts build.
func TestResetRefusesCanonicalCommitmentBesideLocalState(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		err := runMixedStateReset(t, root, []string{
			"domain/v1.0-commitment.0-256.kv",
		}, false)
		qt.Assert(t, qt.IsNotNil(err))
		qt.Assert(t, qt.StringContains(err.Error(), "0-256"))
	})
}

func TestResetAllowsUniformBuilds(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		qt.Assert(t, qt.IsNil(runMixedStateReset(t, root, []string{
			"domain/v1.0-accounts.0-256.kv",
			"domain/v1.0-storage.0-256.kv",
			"domain/v1.0-commitment.0-256.kv",
		}, false)))
	})
	// Everything local is equally consistent: nothing gets normalised, so no offsets move.
	withOsRoot(t, func(root *os.Root) {
		qt.Assert(t, qt.IsNil(runMixedStateReset(t, root, nil, false)))
	})
}

// A range with no commitment file has no offsets to invalidate, whatever its accounts and storage
// builds are. The same holds for a commitment file with no state files beside it.
func TestResetIgnoresRangesWithoutBothSides(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{
			{Name: "snapshots/domain/v1.0-accounts.0-256.kv"},
			{Name: "snapshots/domain/v1.0-storage.0-256.kv"},
		}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown = false
		r.PreverifiedSnapshots = preverified.SortedItems{{Name: "domain/v1.0-accounts.0-256.kv"}}
		r.PreverifiedSnapshots.Sort()
		qt.Assert(t, qt.IsNil(r.Run()))
	})
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{{Name: "snapshots/domain/v1.0-commitment.0-256.kv"}}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown = false
		r.PreverifiedSnapshots = preverified.SortedItems{{Name: "domain/v1.0-commitment.0-256.kv"}}
		r.PreverifiedSnapshots.Sort()
		qt.Assert(t, qt.IsNil(r.Run()))
	})
}

func TestResetMixedBuildsCanBeOverridden(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		qt.Assert(t, qt.IsNil(runMixedStateReset(t, root, []string{
			"domain/v1.0-accounts.0-256.kv",
			"domain/v1.0-storage.0-256.kv",
		}, true)))
	})
}

// A refusal must leave the datadir exactly as it was, or the operator is worse off than if reset
// had never run.
func TestResetRefusalRemovesNothing(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		err := runMixedStateReset(t, root, []string{"domain/v1.0-accounts.0-256.kv"}, false)
		qt.Assert(t, qt.IsNotNil(err))
		for _, name := range []string{
			"snapshots/domain/v1.0-accounts.0-256.kv",
			"snapshots/domain/v1.0-storage.0-256.kv",
			"snapshots/domain/v1.0-commitment.0-256.kv",
		} {
			_, statErr := root.Stat(name)
			qt.Assert(t, qt.IsNil(statErr))
		}
	})
}

// Without --keep-local-snapshots reset removes everything the manifest does not describe, so nothing local
// survives to be mixed with a canonical file.
func TestResetSkipsCheckWhenUnknownFilesAreRemoved(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{
			{Name: "snapshots/domain/v1.0-accounts.0-256.kv"},
			{Name: "snapshots/domain/v1.0-commitment.0-256.kv"},
		}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown = true
		r.PreverifiedSnapshots = preverified.SortedItems{{Name: "domain/v1.0-accounts.0-256.kv"}}
		r.PreverifiedSnapshots.Sort()
		qt.Assert(t, qt.IsNil(r.Run()))
	})
}

// Reset drops the lock file and the next sync fetches whatever the manifest describes, so a file
// absent from disk today can still be canonical tomorrow.
func TestResetCountsPreverifiedFilesNotYetOnDisk(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{{Name: "snapshots/domain/v1.0-accounts.0-256.kv"}}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown = false
		// Only the commitment file is described, so it arrives canonical beside a local accounts.
		r.PreverifiedSnapshots = preverified.SortedItems{{Name: "domain/v1.0-commitment.0-256.kv"}}
		r.PreverifiedSnapshots.Sort()
		err := r.Run()
		qt.Assert(t, qt.IsNotNil(err))
		qt.Assert(t, qt.StringContains(err.Error(), "0-256"))
	})
}

// From v2.2 a commitment file stores plain keys, so its bytes do not depend on where accounts and
// storage records happen to sit.
func TestResetAllowsMixedBuildsForPlainCommitment(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{
			{Name: "snapshots/domain/v2.2-accounts.0-256.kv"},
			{Name: "snapshots/domain/v2.2-commitment.0-256.kv"},
		}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown = false
		r.PreverifiedSnapshots = preverified.SortedItems{{Name: "domain/v2.2-accounts.0-256.kv"}}
		r.PreverifiedSnapshots.Sort()
		qt.Assert(t, qt.IsNil(r.Run()))
	})
}

// Below the referencing threshold a commitment file holds plain keys whatever its version.
func TestResetAllowsMixedBuildsBelowReferencingThreshold(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{
			{Name: "snapshots/domain/v1.0-accounts.0-1.kv"},
			{Name: "snapshots/domain/v1.0-commitment.0-1.kv"},
		}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown = false
		r.PreverifiedSnapshots = preverified.SortedItems{{Name: "domain/v1.0-accounts.0-1.kv"}}
		r.PreverifiedSnapshots.Sort()
		qt.Assert(t, qt.IsNil(r.Run()))
	})
}

// Legacy files carry a bare major version, which RenameOldVersions rewrites to the dotted spelling
// — but reset does not run that first, so it has to recognise both.
func TestResetRefusesMixedBuildsWithLegacyVersionNames(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{{Name: "snapshots/domain/v1-commitment.0-256.kv"}}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown = false
		r.PreverifiedSnapshots = preverified.SortedItems{{Name: "domain/v1.0-accounts.0-256.kv"}}
		r.PreverifiedSnapshots.Sort()
		err := r.Run()
		qt.Assert(t, qt.IsNotNil(err))
		qt.Assert(t, qt.StringContains(err.Error(), "0-256"))
	})
}

// Several versions of one logical file can sit in the directory together, and only the highest is
// opened. Judging a domain by the union of its versions hides the pair that is actually loaded:
// here every domain holds both a canonical and a local file, yet the v1.1 pair that wins is mixed.
func TestResetJudgesTheHighestVersionOfEachDomain(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{
			{Name: "snapshots/domain/v1.0-accounts.0-256.kv"},   // local, superseded
			{Name: "snapshots/domain/v1.1-commitment.0-256.kv"}, // local, and the one that wins
		}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown = false
		r.PreverifiedSnapshots = preverified.SortedItems{
			{Name: "domain/v1.1-accounts.0-256.kv"},   // canonical, and the one that wins
			{Name: "domain/v1.0-commitment.0-256.kv"}, // canonical, superseded
		}
		r.PreverifiedSnapshots.Sort()
		err := r.Run()
		qt.Assert(t, qt.IsNotNil(err))
		qt.Assert(t, qt.StringContains(err.Error(), "0-256"))
	})
}

// The loader takes the highest version it finds and only then asserts the build can read it, so an
// unsupported file is the one a domain would open — and a node holding one cannot start at all.
// This check follows that selection rather than reasoning about a lower file that never loads.
func TestResetFollowsTheLoaderPastUnsupportedVersions(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{
			{Name: "snapshots/domain/v3.0-commitment.0-256.kv"}, // newer major, and still what wins
			{Name: "snapshots/domain/v2.1-commitment.0-256.kv"},
		}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown = false
		r.PreverifiedSnapshots = preverified.SortedItems{{Name: "domain/v2.0-accounts.0-256.kv"}}
		r.PreverifiedSnapshots.Sort()
		// v3.0 carries plain keys, so the range holds no offsets to invalidate.
		qt.Assert(t, qt.IsNil(r.Run()))
	})
}

// Startup renames the v1- file onto the v1.0- name the manifest describes and drops any .torrent under
// that name, so after a reset the downloader re-hashes the local bytes into the canonical file.
func TestResetTreatsALegacyFileRenamedOntoAManifestNameAsCanonical(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{
			{Name: "snapshots/domain/v1-commitment.0-256.kv"},
			{Name: "snapshots/domain/v1.0-commitment.0-256.kv.torrent"},
		}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown = false
		r.PreverifiedSnapshots = preverified.SortedItems{
			{Name: "domain/v1.0-commitment.0-256.kv"},
			{Name: "domain/v1.0-accounts.0-256.kv"},
		}
		r.PreverifiedSnapshots.Sort()
		qt.Assert(t, qt.IsNil(r.checkStateBuilds()))
		qt.Assert(t, qt.IsNil(r.Dirs.RenameOldVersions(false)))
		_, err := root.Stat("snapshots/domain/v1.0-commitment.0-256.kv.torrent")
		qt.Assert(t, qt.ErrorIs(err, fs.ErrNotExist))
	})
}

// A name whose range runs backwards describes no real file, and the span it implies underflows into
// one large enough to look like it carries references. Refusing over it would block the very
// command an operator reaches for to clean a datadir up.
func TestResetIgnoresReversedStepRanges(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{{Name: "snapshots/domain/v1.0-commitment.256-0.kv"}}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown = false
		r.PreverifiedSnapshots = preverified.SortedItems{{Name: "domain/v1.0-accounts.256-0.kv"}}
		r.PreverifiedSnapshots.Sort()
		qt.Assert(t, qt.IsNil(r.Run()))
	})
}

// The alias tie is decided by name, not by chance: the loader walks a sorted listing and keeps a
// candidate only on a strictly greater version, and '-' sorts before '.'. So when the manifest
// names the legacy spelling, the canonical file wins and the local twin never loads.
func TestResetKeepsTheWinningAliasWhenTheCanonicalOneSortsFirst(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{{Name: "snapshots/domain/v1.0-commitment.0-256.kv"}} // local twin, sorts second
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown = false
		r.PreverifiedSnapshots = preverified.SortedItems{
			{Name: "domain/v1-commitment.0-256.kv"}, // canonical, sorts first, so it is the one opened
			{Name: "domain/v1-accounts.0-256.kv"},
		}
		r.PreverifiedSnapshots.Sort()
		qt.Assert(t, qt.IsNil(r.Run()))
	})
}

func TestResetRefusesLegacyStateThatStartupRenamesIntoTheManifest(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{
			{Name: "snapshots/domain/v1-accounts.0-256.kv"},
			{Name: "snapshots/domain/v1-storage.0-256.kv"},
			{Name: "snapshots/domain/v1-commitment.0-256.kv"},
		}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown = false
		r.PreverifiedSnapshots = preverified.SortedItems{
			{Name: "domain/v1.0-accounts.0-256.kv"},
			{Name: "domain/v1.0-storage.0-256.kv"},
		}
		r.PreverifiedSnapshots.Sort()
		err := r.Run()
		qt.Assert(t, qt.IsNotNil(err))
		qt.Assert(t, qt.StringContains(err.Error(), "0-256"))
		qt.Assert(t, qt.IsNil(r.Dirs.RenameOldVersions(false)))
		qt.Assert(t, qt.IsNotNil(r.checkStateBuilds()))
	})
}

func runCoveredRangeReset(t *testing.T, root *os.Root, local, manifest []string) error {
	return runCoveredRangeResetEntries(t, root, validEntries(t, local), manifest)
}

func validEntries(t *testing.T, names []string) []fsEntry {
	var entries []fsEntry
	for _, name := range names {
		entry := fsEntry{Name: slashName(name)}
		switch filepath.Ext(name) {
		case ".kv", ".v", ".ef":
			entry.Data = validSegment(t)
		case ".kvi":
			entry.Data = validIndex(t)
		}
		entries = append(entries, entry)
	}
	return entries
}

func runCoveredRangeResetEntries(t *testing.T, root *os.Root, startEntries []fsEntry, manifest []string) error {
	makeEntries(t, startEntries, root)
	r := makeTestingReset(t, startEntries, root, "", ".")
	r.RemoveUnknown = false
	r.PreverifiedSnapshots = preverified.SortedItems{
		{Name: "domain/v1.0-accounts.0-2.kv"},
		{Name: "domain/v1.0-storage.0-2.kv"},
	}
	for _, name := range manifest {
		r.PreverifiedSnapshots = append(r.PreverifiedSnapshots, preverified.Item{Name: name})
	}
	r.PreverifiedSnapshots.Sort()
	return r.Run()
}

func validSegment(t *testing.T) string {
	fPath := filepath.Join(t.TempDir(), "v1.0-accounts.0-1.kv")
	comp, err := seg.NewCompressor(t.Context(), t.Name(), fPath, t.TempDir(), seg.DefaultCfg, log.LvlDebug, log.New())
	qt.Assert(t, qt.IsNil(err))
	defer comp.Close()
	comp.DisableFsync()
	qt.Assert(t, qt.IsNil(comp.AddWord([]byte("word"))))
	qt.Assert(t, qt.IsNil(comp.Compress()))
	data, err := os.ReadFile(fPath)
	qt.Assert(t, qt.IsNil(err))
	return string(data)
}

func validIndex(t *testing.T) string {
	fPath := filepath.Join(t.TempDir(), "v1.0-accounts.0-1.kvi")
	salt := uint32(1)
	rs, err := recsplit.NewRecSplit(recsplit.RecSplitArgs{
		KeyCount:   1,
		BucketSize: 10,
		Salt:       &salt,
		TmpDir:     t.TempDir(),
		IndexFile:  fPath,
		LeafSize:   8,
	}, log.New())
	qt.Assert(t, qt.IsNil(err))
	defer rs.Close()
	rs.DisableFsync()
	qt.Assert(t, qt.IsNil(rs.AddKey([]byte("key"), 0)))
	qt.Assert(t, qt.IsNil(rs.Build(t.Context())))
	data, err := os.ReadFile(fPath)
	qt.Assert(t, qt.IsNil(err))
	return string(data)
}

var completeLocalMerge = []string{
	"snapshots/domain/v2.1-commitment.0-2.kv",
	"snapshots/domain/v2.1-commitment.0-4.kv",
	"snapshots/domain/v2.0-commitment.0-4.kvi",
	"snapshots/domain/v1.0-accounts.0-4.kv",
	"snapshots/domain/v1.0-storage.0-4.kv",
}

func TestResetAllowsAMixedRangeHiddenByACompleteMerge(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		qt.Assert(t, qt.IsNil(runCoveredRangeReset(t, root, completeLocalMerge, nil)))
	})
	withOsRoot(t, func(root *os.Root) {
		qt.Assert(t, qt.IsNil(runCoveredRangeReset(t, root, append(slices.Clone(completeLocalMerge),
			"snapshots/history/v1.0-accounts.0-4.v",
			"snapshots/idx/v1.0-logaddrs.0-4.ef"), []string{"history/v1.0-accounts.0-2.v"})))
	})
}

func TestResetChecksAMixedRangeWhenTheMergeHasNoAccessor(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		local := slices.DeleteFunc(slices.Clone(completeLocalMerge), func(name string) bool {
			return strings.HasSuffix(name, ".kvi")
		})
		err := runCoveredRangeReset(t, root, local, nil)
		qt.Assert(t, qt.IsNotNil(err))
		qt.Assert(t, qt.StringContains(err.Error(), "0-2"))
	})
}

func TestResetChecksAMixedRangeWhenTheMergeDoesNotOpen(t *testing.T) {
	for _, broken := range []string{".kv", ".kvi"} {
		withOsRoot(t, func(root *os.Root) {
			var entries []fsEntry
			for _, name := range completeLocalMerge {
				entry := fsEntry{Name: slashName(name)}
				if !strings.Contains(name, "commitment.0-4"+broken) || !strings.HasSuffix(name, broken) {
					switch {
					case strings.HasSuffix(name, ".kv"):
						entry.Data = validSegment(t)
					case strings.HasSuffix(name, ".kvi"):
						entry.Data = validIndex(t)
					}
				}
				entries = append(entries, entry)
			}
			err := runCoveredRangeResetEntries(t, root, entries, nil)
			qt.Assert(t, qt.IsNotNil(err), qt.Commentf("%s", broken))
			qt.Assert(t, qt.StringContains(err.Error(), "0-2"))
		})
	}
}

func TestResetHidesAMixedRangeOnlyBehindAManifestMergeWithItsAccessor(t *testing.T) {
	manifestMerge := []string{
		"domain/v2.1-commitment.0-4.kv",
		"domain/v1.0-accounts.0-4.kv",
		"domain/v1.0-storage.0-4.kv",
	}
	withOsRoot(t, func(root *os.Root) {
		qt.Assert(t, qt.IsNil(runCoveredRangeReset(t, root, []string{"snapshots/domain/v2.1-commitment.0-2.kv"},
			append(slices.Clone(manifestMerge), "domain/v2.0-commitment.0-4.kvi"))))
	})
	withOsRoot(t, func(root *os.Root) {
		err := runCoveredRangeReset(t, root, []string{"snapshots/domain/v2.1-commitment.0-2.kv"}, manifestMerge)
		qt.Assert(t, qt.IsNotNil(err))
		qt.Assert(t, qt.StringContains(err.Error(), "0-2"))
	})
}

func TestResetChecksAMixedRangeWhenTheMergeIsIncomplete(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		err := runCoveredRangeReset(t, root, []string{
			"snapshots/domain/v2.1-commitment.0-2.kv",
			"snapshots/domain/v2.1-commitment.0-4.kv",
			"snapshots/domain/v1.0-accounts.0-4.kv",
		}, nil)
		qt.Assert(t, qt.IsNotNil(err))
		qt.Assert(t, qt.StringContains(err.Error(), "0-2"))
	})
}

func TestResetChecksAMixedRangeWhenAnotherEntityHoldsTheMergeBack(t *testing.T) {
	for _, lagging := range []string{
		"history/v1.0-accounts.0-2.v",
		"idx/v1.0-logaddrs.0-2.ef",
		"domain/v1.0-code.0-2.kv",
	} {
		for _, fromManifest := range []bool{false, true} {
			local, manifest := slices.Clone(completeLocalMerge), []string(nil)
			if fromManifest {
				manifest = append(manifest, lagging)
			} else {
				local = append(local, "snapshots/"+lagging)
			}
			withOsRoot(t, func(root *os.Root) {
				err := runCoveredRangeReset(t, root, local, manifest)
				qt.Assert(t, qt.IsNotNil(err), qt.Commentf("%s, from manifest: %v", lagging, fromManifest))
				qt.Assert(t, qt.StringContains(err.Error(), "0-2"))
			})
		}
	}
}

func TestResetChecksAdjacentRangesThatDoNotCoverEachOther(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		err := runCoveredRangeReset(t, root, []string{
			"snapshots/domain/v2.1-commitment.0-2.kv",
			"snapshots/domain/v2.1-commitment.2-4.kv",
		}, []string{
			"domain/v1.0-accounts.2-4.kv",
			"domain/v1.0-storage.2-4.kv",
		})
		qt.Assert(t, qt.IsNotNil(err))
		qt.Assert(t, qt.StringContains(err.Error(), "0-2, 2-4"))
	})
}

func TestResetAllowsALegacyFileThatStartupRenamesOverItsCanonicalTwin(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{
			{Name: "snapshots/domain/v1.0-accounts.0-256.kv"},
			{Name: "snapshots/domain/v1.0-accounts.0-256.kv.torrent"},
			{Name: "snapshots/domain/v1-accounts.0-256.kv"},
			{Name: "snapshots/domain/v1.0-storage.0-256.kv"},
			{Name: "snapshots/domain/v1.0-commitment.0-256.kv"},
		}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown = false
		r.PreverifiedSnapshots = preverified.SortedItems{
			{Name: "domain/v1.0-accounts.0-256.kv"},
			{Name: "domain/v1.0-storage.0-256.kv"},
			{Name: "domain/v1.0-commitment.0-256.kv"},
		}
		r.PreverifiedSnapshots.Sort()
		qt.Assert(t, qt.IsNil(r.checkStateBuilds()))
		qt.Assert(t, qt.IsNil(r.Dirs.RenameOldVersions(false)))
		_, err := root.Stat("snapshots/domain/v1.0-accounts.0-256.kv.torrent")
		qt.Assert(t, qt.ErrorIs(err, fs.ErrNotExist))
	})
}

func TestResetIgnoresMalformedNamesWhenFindingTheVisibleEnd(t *testing.T) {
	for _, malformed := range []string{
		"snapshots/idx/v1.0-logaddrs.8-4.ef",
		"snapshots/idx/v1.0-logaddrs.99999999999999999999-4.ef",
	} {
		withOsRoot(t, func(root *os.Root) {
			err := runCoveredRangeReset(t, root, append(slices.Clone(completeLocalMerge),
				"snapshots/idx/v1.0-logaddrs.0-2.ef", malformed), nil)
			qt.Assert(t, qt.IsNotNil(err), qt.Commentf("%s", malformed))
			qt.Assert(t, qt.StringContains(err.Error(), "0-2"))
		})
	}
}

func TestResetSkipsLegacyCommitmentHistoryThatStartupRemoves(t *testing.T) {
	for _, legacy := range []string{
		"snapshots/history/v1-commitment.0-2.v",
		"snapshots/idx/v1-commitment.0-2.ef",
	} {
		withOsRoot(t, func(root *os.Root) {
			err := runCoveredRangeReset(t, root, append(slices.Clone(completeLocalMerge), legacy), nil)
			qt.Assert(t, qt.IsNil(err), qt.Commentf("%s", legacy))
		})
	}
	for _, pair := range [][2]string{
		{"snapshots/history/v1.0-commitment.0-2.v", "snapshots/history/v1-commitment.0-4.v"},
		{"snapshots/idx/v1.0-commitment.0-2.ef", "snapshots/idx/v1-commitment.0-4.ef"},
	} {
		withOsRoot(t, func(root *os.Root) {
			err := runCoveredRangeReset(t, root, append(slices.Clone(completeLocalMerge), pair[0], pair[1]), nil)
			qt.Assert(t, qt.IsNotNil(err), qt.Commentf("%s", pair[1]))
			qt.Assert(t, qt.StringContains(err.Error(), "0-2"))
		})
	}
}

func TestResetCountsLegacyCommitmentHistoryBehindASymlinkedDir(t *testing.T) {
	for sub, name := range map[string]string{"history": "v1-commitment.0-2.v", "idx": "v1-commitment.0-2.ef"} {
		withOsRoot(t, func(root *os.Root) {
			external := t.TempDir()
			qt.Assert(t, qt.IsNil(os.MkdirAll(filepath.Join(root.Name(), "snapshots"), 0o755)))
			qt.Assert(t, qt.IsNil(os.Symlink(external, filepath.Join(root.Name(), "snapshots", sub))))
			qt.Assert(t, qt.IsNil(os.WriteFile(filepath.Join(external, name), []byte(validSegment(t)), 0o644)))
			err := runCoveredRangeReset(t, root, completeLocalMerge, nil)
			qt.Assert(t, qt.IsNotNil(err), qt.Commentf("%s", sub))
			qt.Assert(t, qt.StringContains(err.Error(), "0-2"))
		})
	}
}

func TestResetChecksAMixedRangeWhenTheAccessorStartupOpensIsCorrupt(t *testing.T) {
	for _, pair := range [][2]string{
		{"snapshots/domain/v2.10-commitment.0-4.kvi", "snapshots/domain/v2.9-commitment.0-4.kvi"},
		{"snapshots/domain/v1-commitment.0-4.kvi", "snapshots/domain/v1.0-commitment.0-4.kvi"},
	} {
		withOsRoot(t, func(root *os.Root) {
			local := slices.DeleteFunc(slices.Clone(completeLocalMerge), func(name string) bool {
				return strings.HasSuffix(name, ".kvi")
			})
			entries := append(validEntries(t, local),
				fsEntry{Name: slashName(pair[0]), Data: "x"},
				fsEntry{Name: slashName(pair[1]), Data: validIndex(t)})
			err := runCoveredRangeResetEntries(t, root, entries, nil)
			qt.Assert(t, qt.IsNotNil(err), qt.Commentf("%s", pair[0]))
			qt.Assert(t, qt.StringContains(err.Error(), "0-2"))
		})
	}
}

func TestResetChecksAMixedRangeWhenACorruptFileExtendsAnEntity(t *testing.T) {
	for _, files := range [][]string{
		{"snapshots/domain/v1.0-code.0-2.kv", "snapshots/domain/v1.0-code.0-4.kv"},
		{"snapshots/history/v1.0-accounts.0-2.v", "snapshots/history/v1.0-accounts.0-4.v"},
		{"snapshots/idx/v1.0-logaddrs.0-2.ef", "snapshots/idx/v1.0-logaddrs.0-4.ef"},
		{"snapshots/domain/v1.0-code.0-2.kv", "snapshots/domain/v1.0-code.0-4.kv", "snapshots/domain/v1.1-code.0-4.kv"},
	} {
		withOsRoot(t, func(root *os.Root) {
			valid, corrupt := files[:len(files)-1], files[len(files)-1]
			truncated := validSegment(t)
			entries := append(validEntries(t, append(slices.Clone(completeLocalMerge), valid...)),
				fsEntry{Name: slashName(corrupt), Data: truncated[:len(truncated)/2]})
			err := runCoveredRangeResetEntries(t, root, entries, nil)
			qt.Assert(t, qt.IsNotNil(err), qt.Commentf("%s", corrupt))
			qt.Assert(t, qt.StringContains(err.Error(), "0-2"))
		})
	}
}
