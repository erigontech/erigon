package reset

import (
	"os"
	"testing"

	"github.com/go-quicktest/qt"

	"github.com/erigontech/erigon/db/preverified"
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
	// --local=false: files outside the preverified set are kept as-is.
	r.RemoveUnknown = false
	r.RemoveLocal = false
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
		r.RemoveUnknown, r.RemoveLocal = false, false
		r.PreverifiedSnapshots = preverified.SortedItems{{Name: "domain/v1.0-accounts.0-256.kv"}}
		r.PreverifiedSnapshots.Sort()
		qt.Assert(t, qt.IsNil(r.Run()))
	})
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{{Name: "snapshots/domain/v1.0-commitment.0-256.kv"}}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown, r.RemoveLocal = false, false
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

// The default --local=true removes everything the manifest does not describe, so nothing local
// survives to be mixed with a canonical file.
func TestResetSkipsCheckWhenUnknownFilesAreRemoved(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{
			{Name: "snapshots/domain/v1.0-accounts.0-256.kv"},
			{Name: "snapshots/domain/v1.0-commitment.0-256.kv"},
		}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		r.RemoveUnknown, r.RemoveLocal = true, true
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
		r.RemoveUnknown, r.RemoveLocal = false, false
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
		r.RemoveUnknown, r.RemoveLocal = false, false
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
		r.RemoveUnknown, r.RemoveLocal = false, false
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
		r.RemoveUnknown, r.RemoveLocal = false, false
		r.PreverifiedSnapshots = preverified.SortedItems{{Name: "domain/v1.0-accounts.0-256.kv"}}
		r.PreverifiedSnapshots.Sort()
		err := r.Run()
		qt.Assert(t, qt.IsNotNil(err))
		qt.Assert(t, qt.StringContains(err.Error(), "0-256"))
	})
}
