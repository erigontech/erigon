package app

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
	"github.com/go-quicktest/qt"
)

func haveDir(name string) fsChecker {
	return haveEntry(fsEntry{
		Name: slashName(name),
		Mode: fs.ModeDir,
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
			{Name: "chaindata", Mode: fs.ModeSymlink, Data: "symlink"},
			{Name: "symlink", Mode: fs.ModeDir},
			{Name: "symlink/mdbx.dat"},
			{Name: "symlink/mdbx.lck"},
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
			{Name: "datadir/snapshots/domain", Mode: fs.ModeSymlink, Data: absFastDiskLink},
			fakeMount("datadir/heimdall/mystuff"),
			{Name: "datadir/snapshots/history", Mode: fs.ModeDir},
		}
		endEntries := append(
			slices.Clone(startEntries[:7]),
			fsEntry{Name: ".", Mode: fs.ModeDir})
		makeEntries(t, startEntries, testRoot)
		rootFS := testRoot.FS()
		printFs(t, rootFS)
		r := makeTestingReset(t, startEntries, testRoot, "datadir")
		qt.Assert(t, qt.IsNil(r.run()))
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
		resetJail, err := osRoot.OpenRoot("jail")
		qt.Assert(t, qt.IsNil(err))
		rootFS := osRoot.FS()
		printFs(t, rootFS)
		r := makeTestingReset(t, startEntries, resetJail, ".")
		err = r.run()
		qt.Check(t, qt.IsNil(err))
		endEntries := haveEntries(startEntries[0])
		endEntries = append(endEntries, haveDir("jail"))
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
		resetJail, err := osRoot.OpenRoot("jail")
		qt.Assert(t, qt.IsNil(err))
		rootFS := osRoot.FS()
		printFs(t, rootFS)
		r := makeTestingReset(t, startEntries, resetJail, ".")
		err = r.run()
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

func (me fsEntry) readData(fsys fs.FS) (string, error) {
	switch mt := me.Mode.Type(); mt {
	case fs.ModeSymlink:
		return fs.ReadLink(fsys, string(me.Name))
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
	osRoot, err := os.OpenRoot(t.TempDir())
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
		r := makeTestingReset(t, startEntries, osRoot, ".")
		qt.Assert(t, runChecker(r.run()))
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
	for _, entry := range entries {
		localName, err := filepath.Localize(string(entry.Name))
		qt.Assert(t, qt.IsNil(err), qt.Commentf("localizing entry name %q", entry.Name))
		root.MkdirAll(filepath.Dir(localName), dir.DirPerm)
		if entry.Mode&fs.ModeSymlink != 0 {
			assertNoErr(root.Symlink(entry.Data, localName))
		} else if entry.Mode&fs.ModeDir != 0 {
			assertNoErr(root.Mkdir(localName, dir.DirPerm))
		} else {
			f, err := root.Create(localName)
			assertNoErr(err)
			f.Close()
		}
	}
	return
}

func makeTestingReset(
	t *testing.T,
	entries []fsEntry,
	osRoot *os.Root,
	datadir slashName,
) reset {
	logger := log.New("test", t.Name())
	logger.SetHandler(log.StderrHandler)
	osRootPath := osFilePath(osRoot.Name())
	datadirOs := osFilePath(osRoot.Name()).Join(datadir.FromSlash())
	return reset{
		datadir:              datadirOs,
		logger:               logger,
		preverifiedSnapshots: nil,
		removeUnknown:        true,
		removeLocal:          true,
		removeFunc: func(osFilePath osFilePath) error {
			slashName := osFilePath.MustLocalRelSlash(osRootPath)
			for _, entry := range entries {
				if entry.Name == slashName && entry.Remove != nil {
					return entry.Remove()
				}
			}
			return osRoot.Remove(string(osFilePath.MustRel(osRootPath)))
		},
	}
}

//func TestAbsSymlinkInsideRoot(t *testing.T) {
//	d := t.TempDir()
//	t.Logf("temp dir: %q", d)
//	root, err := os.OpenRoot(d)
//	qt.Assert(t, qt.IsNil(err))
//	err = root.Mkdir("a", dir.DirPerm)
//	qt.Assert(t, qt.IsNil(err))
//	err = root.Mkdir("b", dir.DirPerm)
//	qt.Assert(t, qt.IsNil(err))
//	absSymlinkName := filepath.Join(d, "a", "c")
//	err = root.Symlink(filepath.Join(d, "b"), "a/c")
//	qt.Assert(t, qt.IsNil(err))
//	fsys := os.DirFS(d)
//	_, err = fs.ReadDir(fsys, "a/c")
//	qt.Check(t, qt.IsNil(err))
//	_, err = root.Open(absSymlinkName)
//	qt.Assert(t, qt.IsNil(err))
//}
