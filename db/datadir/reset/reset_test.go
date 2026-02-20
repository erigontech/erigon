//go:build go1.25

package reset

import (
	"fmt"
	"io/fs"
	"iter"
	"maps"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"syscall"
	"testing"

	g "github.com/anacrolix/generics"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/preverified"
	"github.com/go-quicktest/qt"
)

func haveDir(name string) fsChecker {
	return haveEntry(fsEntry{
		Name: slashName(name),
		Mode: fs.ModeDir,
	})
}

func TestSnapshots(t *testing.T) {
	withOsRoot(t, func(root *os.Root) {
		startEntries := []fsEntry{
			{Name: "snapshots/keep/me"},
			{Name: "snapshots/dont/keep/me"},
		}
		makeEntries(t, startEntries, root)
		r := makeTestingReset(t, startEntries, root, "", ".")
		printFs(t, root.FS())
		r.PreverifiedSnapshots = preverified.SortedItems{
			{Name: "keep/me"},
		}
		r.PreverifiedSnapshots.Sort()
		qt.Assert(t, qt.IsNil(r.Run()))
		checkFs(t, root.FS(), haveEntries(startEntries[0])...)
	})
}

func TestResetChaindata(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		testResetDatadirRoot(t, []fsEntry{
			{Name: "chaindata/mdbx.dat"},
			{Name: "chaindata/mdbx.lck"},
		}, []fsChecker{haveDir(".")}, qt.IsNil)
	})
	t.Run("DanglingSymlink", func(t *testing.T) {
		startEntries := []fsEntry{
			{Name: "chaindata", Mode: fs.ModeSymlink, Data: "symlink"},
		}
		testResetDatadirRoot(t, startEntries, haveEntries(slices.Clone(startEntries)...), qt.IsNotNil)
	})
	t.Run("Symlinked", func(t *testing.T) {
		startEntries := []fsEntry{
			{Name: "chaindata", Mode: fs.ModeSymlink, Data: "fast/chaindata"},
			{Name: "fast/chaindata", Mode: fs.ModeDir},
			{Name: "fast/chaindata/mdbx.dat"},
			{Name: "fast/chaindata/mdbx.lck"},
		}
		testResetDatadirRoot(t, startEntries, haveEntries(slices.Clone(startEntries[:2])...), qt.IsNil)
	})
	t.Run("LinkLoop", func(t *testing.T) {
		startEntries := []fsEntry{
			{Name: "chaindata/a", Mode: fs.ModeSymlink, Data: "b"},
			{Name: "chaindata/b", Mode: fs.ModeSymlink, Data: "a"},
		}
		testResetDatadirRoot(t, startEntries, haveEntries(startEntries...), qt.IsNotNil)
	})
}

func fakeMount(name string) fsEntry {
	return fsEntry{Name: slashName(name), Mode: fs.ModeDir, Remove: func() error {
		// I hope this works on Windows...
		return syscall.EBUSY
	}}
}

func TestResetCheapDiskExample(t *testing.T) {
	withOsRoot(t, func(testRoot *os.Root) {
		absFastDiskLink, err := filepath.Abs(filepath.Join(testRoot.Name(), "fastdisk"))
		qt.Assert(t, qt.IsNil(err))
		t.Log("absolute fastdisk name:", absFastDiskLink)
		startEntries := []fsEntry{
			{Name: "bystander"},
			{Name: "datadir/chaindata", Mode: fs.ModeSymlink, Data: "../fastdisk"},
			{Name: "fastdisk", Mode: fs.ModeDir},
			{Name: "mount", Mode: fs.ModeDir},
			{Name: "datadir/snapshots", Mode: fs.ModeDir},
			{
				Name: "datadir/snapshots/domain",
				Mode: fs.ModeSymlink,
				// Symlinks are converted to and from slash-names internally, so we need to "escape"
				// this path.
				Data: filepath.ToSlash(absFastDiskLink),
			},
			fakeMount("datadir/heimdall/mystuff"),
			{Name: "datadir/snapshots/history", Mode: fs.ModeDir},
		}
		endEntries := append(
			slices.Clone(startEntries[:7]),
			fsEntry{Name: ".", Mode: fs.ModeDir})
		makeEntries(t, startEntries, testRoot)
		rootFS := testRoot.FS()
		printFs(t, rootFS)
		r := makeTestingReset(t, startEntries, testRoot, "", "datadir")
		qt.Assert(t, qt.IsNil(r.Run()))
		checkFs(t, rootFS, haveEntries(endEntries...)...)
	})
}

func TestResetSymlinkToExternalFile(t *testing.T) {
	startEntries := []fsEntry{
		{Name: "escape"},
		{Name: "jail/snapshots/badlink", Mode: fs.ModeSymlink, Data: "../../escape"},
	}
	withOsRoot(t, func(osRoot *os.Root) {
		makeEntries(t, startEntries, osRoot)
		rootFS := osRoot.FS()
		printFs(t, rootFS)
		r := makeTestingReset(t, startEntries, osRoot, "jail", ".")
		err := r.Run()
		qt.Check(t, qt.IsNil(err))
		endEntries := haveEntries(
			startEntries[0],
			fsEntry{Name: "jail/snapshots", Mode: fs.ModeDir},
		)
		checkFs(t, rootFS, endEntries...)
	})
}

func TestResetSymlinkToExternalDirWithContents(t *testing.T) {
	startEntries := []fsEntry{
		{Name: "jail/snapshots/link", Mode: fs.ModeSymlink, Data: "../../escape"},
		{Name: "escape/halp"},
	}
	endEntries := slices.Clone(startEntries)
	withOsRoot(t, func(osRoot *os.Root) {
		makeEntries(t, startEntries, osRoot)
		rootFS := osRoot.FS()
		printFs(t, rootFS)
		r := makeTestingReset(t, startEntries, osRoot, "jail", ".")
		err := r.Run()
		qt.Check(t, qt.ErrorMatches(err, pathEscapesFromParent))
		checkFs(t, rootFS, haveEntries(endEntries...)...)
	})
}

var pathEscapesFromParent = regexp.MustCompile("path escapes from parent$")

type fsEntry struct {
	Name   slashName
	Data   string
	Mode   fs.FileMode
	Stat   func() (fs.FileInfo, error)
	Remove func() error
}

func (me fsEntry) isSymlink() bool {
	return me.Mode&fs.ModeSymlink != 0
}

func (me fsEntry) readData(fsys fs.FS) (data string, err error) {
	switch mt := me.Mode.Type(); mt {
	case fs.ModeSymlink:
		data, err = fs.ReadLink(fsys, string(me.Name))
		if err != nil {
			return
		}
		data = filepath.ToSlash(data)
		return
	case 0:
		b, err := fs.ReadFile(fsys, string(me.Name))
		return string(b), err
	case fs.ModeDir:
		return "", nil
	default:
		return "", fmt.Errorf("unhandled file type %v", mt)
	}
}

func withOsRoot(t *testing.T, with func(root *os.Root)) {
	dir := t.TempDir()
	osRoot, err := os.OpenRoot(dir)
	qt.Assert(t, qt.IsNil(err))
	t.Log("rootfs is at", osRoot.Name())
	defer func() {
		if t.Failed() {
			t.Log("fs state at failure")
			printFs(t, os.DirFS(osRoot.Name()))
		}
	}()
	with(osRoot)
}

func testResetDatadirRoot(t *testing.T, startEntries []fsEntry, checkers []fsChecker, runChecker func(error) qt.Checker) {
	withOsRoot(t, func(osRoot *os.Root) {
		makeEntries(t, startEntries, osRoot)
		rootFS := osRoot.FS()
		printFs(t, rootFS)
		r := makeTestingReset(t, startEntries, osRoot, "", ".")
		qt.Assert(t, runChecker(r.Run()))
		checkFs(t, rootFS, checkers...)
	})
}

func parentNames(name slashName) iter.Seq[slashName] {
	return func(yield func(name2 slashName) bool) {
		for {
			name = slashName(path.Dir(string(name)))
			if name == "" {
				return
			}
			if !yield(name) {
				return
			}
			if name == "." {
				return
			}
		}
	}
}

func haveEntries(entries ...fsEntry) (ret []fsChecker) {
	byName := make(map[slashName]fsChecker)
	for _, e := range entries {
		g.MapMustAssignNew(byName, e.Name, haveEntry(e))
	}
	for _, e := range entries {
		for name := range parentNames(e.Name) {
			if g.MapContains(byName, name) {
				continue
			}
			g.MapMustAssignNew(byName, name, haveEntry(fsEntry{Name: name, Mode: fs.ModeDir}))
		}
	}
	return slices.Collect(maps.Values(byName))
}

func haveEntry(entry fsEntry) fsChecker {
	name := entry.Name
	checkErr := fs.ErrNotExist
	return fsCheckerFuncs{
		onWalkDir: func(input fsCheckerWalkInput) (stop bool, err error) {
			if input.name != name {
				return
			}
			stop = true
			data, err := entry.readData(input.fs)
			if err != nil {
				return
			}
			if data == entry.Data {
				checkErr = nil
			} else {
				checkErr = fmt.Errorf("wrong data, expected %q, got %q", entry.Data, data)
			}
			return
		},
		check: func(t *testing.T) {
			if checkErr != nil {
				t.Errorf("entry %q failed check: %v", name, checkErr)
			}
		},
	}
}

func printFs(t *testing.T, rootFS fs.FS) {
	qt.Assert(t, qt.IsNil(fs.WalkDir(rootFS, ".", func(path string, d fs.DirEntry, err error) error {
		t.Logf("name: %q, mode: %v, err: %v\n", path, d.Type(), err)
		return nil
	})))
}

type fsCheckerFuncs struct {
	onWalkDir fsCheckerWalkFunc
	check     func(t *testing.T)
}

func (f fsCheckerFuncs) OnWalkDir(input fsCheckerWalkInput) (bool, error) {
	return f.onWalkDir(input)
}

func (f fsCheckerFuncs) Check(t *testing.T) {
	f.check(t)
}

type fsCheckerWalkFunc = func(input fsCheckerWalkInput) (bool, error)

type fsChecker interface {
	// Return early with error if return false
	OnWalkDir(input fsCheckerWalkInput) (bool, error)
	// Tell us if you pass
	Check(t *testing.T)
}

type fsCheckerWalkInput struct {
	name slashName
	d    fs.DirEntry
	err  error
	fs   fs.FS
}

func checkFs(t *testing.T, fsRoot fs.FS, checkers ...fsChecker) {
	qt.Assert(t, qt.IsNil(fs.WalkDir(
		fsRoot,
		".",
		func(path string, d fs.DirEntry, err error) error {
			println("checkFs", path, d, err)
			matched := false
			for _, c := range checkers {
				stop, err := c.OnWalkDir(fsCheckerWalkInput{
					name: slashName(path),
					d:    d,
					err:  err,
					fs:   fsRoot,
				})
				if err != nil {
					return err
				}
				if stop {
					matched = true
				}
			}
			if !matched {
				t.Errorf("unmatched name %q", path)
			}
			return nil
		},
	)))
	for _, checker := range checkers {
		checker.Check(t)
	}
}

func makeEntries(t *testing.T, entries []fsEntry, root *os.Root) {
	assertNoErr := func(err error) {
		qt.Assert(t, qt.IsNil(err))
	}
	// Are you serious? Windows has 2 types of symlinks. Go will guess what kind to use if the
	// target already exists. Move symlinks to the back of the slice.
	entries = slices.Clone(entries)
	slices.SortFunc(entries, func(l, r fsEntry) int {
		if l.isSymlink() == r.isSymlink() {
			return 0
		}
		if l.isSymlink() {
			return 1
		} else {
			return -1
		}
	})
	for _, entry := range entries {
		localName, err := filepath.Localize(string(entry.Name))
		qt.Assert(t, qt.IsNil(err), qt.Commentf("localizing entry name %q", entry.Name))
		root.MkdirAll(filepath.Dir(localName), dir.DirPerm)
		if entry.Mode&fs.ModeSymlink != 0 {
			// Windows doesn't seem to create SYMLINKD types when going through os.Root. So we do
			// appropriate path conversions going into Symlink. That's on top of having to make sure
			// the target exists if we want a SYMLINKD and not just a SYMLINK.
			targetFilePath := filepath.FromSlash(entry.Data)
			newName := filepath.Join(root.Name(), localName)
			err := os.Symlink(targetFilePath, newName)
			if err != nil {
				panic(fmt.Sprintf("creating symlink %q -> %q: %v", localName, targetFilePath, err))
			}
		} else if entry.Mode&fs.ModeDir != 0 {
			assertNoErr(root.Mkdir(localName, dir.DirPerm))
		} else {
			f, err := root.Create(localName)
			assertNoErr(err)
			f.Close()
		}
	}
}

func makeTestingReset(
	t *testing.T,
	entries []fsEntry,
	osRoot *os.Root,
	jailPath string, // Optional jail subdirectory. Empty string means no jail.
	datadirSubDir slashName,
) Reset {
	logger := log.New("test", t.Name())
	logger.SetHandler(log.StderrHandler)

	osRootPath := OsFilePath(osRoot.Name())
	removeRoot := osRoot

	// If jailPath is specified, open a sub-root for path escape detection
	if jailPath != "" {
		var err error
		removeRoot, err = osRoot.OpenRoot(jailPath)
		qt.Assert(t, qt.IsNil(err))
		osRootPath = osRootPath.Join(OsFilePath(jailPath))
	}

	datadirOs := osRootPath.Join(datadirSubDir.FromSlash())
	dirs := datadir.Open(datadirOs.ToString())
	return Reset{
		Dirs:                 &dirs,
		Logger:               logger,
		PreverifiedSnapshots: nil,
		RemoveUnknown:        true,
		RemoveLocal:          true,
		RemoveFunc: func(osFilePath OsFilePath) error {
			slashName := osFilePath.mustLocalRelSlash(osRootPath)
			for _, entry := range entries {
				if entry.Name == slashName && entry.Remove != nil {
					return entry.Remove()
				}
			}
			return removeRoot.Remove(string(osFilePath.MustRel(osRootPath)))
		},
	}
}
