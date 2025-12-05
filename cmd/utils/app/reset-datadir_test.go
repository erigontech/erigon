package app

import (
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"testing"

	g "github.com/anacrolix/generics"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/go-quicktest/qt"
)

func TestResetChaindata(t *testing.T) {
	t.Run("Normal", func(t *testing.T) {
		testResetLocalFs(t, []fsEntry{
			{Name: "chaindata/mdbx.dat"},
			{Name: "chaindata/mdbx.lck"},
		}, nil, qt.IsNil)
	})
	t.Run("DanglingSymlink", func(t *testing.T) {
		startEntries := []fsEntry{
			{Name: "chaindata", Mode: fs.ModeSymlink, Data: "symlink"},
		}
		testResetLocalFs(t, startEntries, haveEntries(slices.Clone(startEntries)...), qt.IsNil)
	})
	t.Run("Symlinked", func(t *testing.T) {
		startEntries := []fsEntry{
			{Name: "chaindata", Mode: fs.ModeSymlink, Data: "symlink"},
			{Name: "symlink", Mode: fs.ModeDir},
			{Name: "symlink/mdbx.dat"},
			{Name: "symlink/mdbx.lck"},
		}
		testResetLocalFs(t, startEntries, haveEntries(slices.Clone(startEntries[:2])...), qt.IsNil)
	})
	t.Run("LinkLoop", func(t *testing.T) {
		startEntries := []fsEntry{
			{Name: "chaindata/a", Mode: fs.ModeSymlink, Data: "b"},
			{Name: "chaindata/b", Mode: fs.ModeSymlink, Data: "a"},
		}
		testResetLocalFs(t, startEntries, haveEntries(startEntries...), qt.IsNotNil)
	})
	t.Run("CheapDiskExample", func(t *testing.T) {
		startEntries := []fsEntry{
			{Name: "chaindata", Mode: fs.ModeSymlink, Data: "fastdisk"},
			{Name: "snapshots", Mode: fs.ModeDir},
			{Name: "symlink/mdbx.dat"},
			{Name: "symlink/mdbx.lck"},
		}
		testResetLocalFs(t, startEntries, haveEntries(slices.Clone(startEntries[:2])...), qt.IsNil)
	})
}

func TestReset(t *testing.T) {
	const fastDiskName = "../fastdisk"
	startEntries := []fsEntry{
		{Name: "chaindata", Mode: fs.ModeSymlink, Data: fastDiskName},
		{Name: "snapshots", Mode: fs.ModeDir},
		{Name: "snapshots/domain", Mode: fs.ModeSymlink, Data: fastDiskName},
		{Name: "snapshots/history", Mode: fs.ModeDir},
		{Name: fastDiskName, Mode: fs.ModeDir},
	}
	testResetLocalFs(t, startEntries, haveEntries(slices.Clone(startEntries[:2])...), qt.IsNil)

}

type fsEntry struct {
	Name string
	Data string
	Mode fs.FileMode
}

func (me fsEntry) readData(fsys fs.FS) (string, error) {
	switch mt := me.Mode.Type(); mt {
	case fs.ModeSymlink:
		return fs.ReadLink(fsys, me.Name)
	case 0:
		b, err := os.ReadFile(me.Name)
		return string(b), err
	case fs.ModeDir:
		return "", nil
	default:
		return "", fmt.Errorf("unhandled file type %v", mt)
	}
}

func testResetLocalFs(t *testing.T, startEntries []fsEntry, checkers []fsChecker, runChecker func(error) qt.Checker) {
	osRoot, err := os.OpenRoot(t.TempDir())
	qt.Assert(t, qt.IsNil(err))
	t.Log("datadir test root is", osRoot.Name())
	rootFS := os.DirFS(osRoot.Name())
	defer func() {
		if t.Failed() {
			t.Log("fs state at failure")
			printFs(t, rootFS)
		}
	}()
	makeEntries(t, startEntries, osRoot)
	printFs(t, rootFS)
	r := makeTestingReset(t)
	r.fs = rootFS
	r.removeFunc = osRoot.Remove
	qt.Assert(t, runChecker(r.run()))
	checkFs(t, rootFS, append(
		checkers,
		// We're running on a temp dir that always exists. Reset doesn't remove the top-level
		// datadir.
		haveEntry(fsEntry{Name: ".", Mode: fs.ModeDir}))...)
}

func haveEntries(entries ...fsEntry) (ret []fsChecker) {
	byName := make(map[string]fsChecker)
	for _, e := range entries {
		g.MapMustAssignNew(byName, e.Name, haveEntry(e))
		dir := path.Dir(e.Name)
		if dir == "." {
			continue
		}
		if g.MapContains(byName, dir) {
			continue
		}
		println(e.Name, dir)
		g.MapMustAssignNew(byName, dir, haveEntry(fsEntry{Name: dir, Mode: fs.ModeDir}))
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
		fmt.Printf("path: %q, mode: %v, err: %v\n", path, d.Type(), err)
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
	name string
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
			for _, c := range checkers {
				stop, err := c.OnWalkDir(fsCheckerWalkInput{
					name: path,
					d:    d,
					err:  err,
					fs:   fsRoot,
				})
				// Maybe this is t.Errorf is stop is false?
				if err != nil {
					return err
				}
				if stop {
					return nil
				}
			}
			t.Errorf("unexpected path %q", path)
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
		localName, err := filepath.Localize(entry.Name)
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

func makeTestingReset(t *testing.T) reset {
	logger := log.New( /*"test", t.Name()*/ )
	logger.SetHandler(log.StdoutHandler)
	return reset{
		logger:               logger,
		preverifiedSnapshots: nil,
		removeUnknown:        true,
		removeLocal:          true,
		linkLimit:            3,
	}
}
