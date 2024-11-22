// Copyright 2024 The Erigon Authors
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

package beacontest

import (
	"io/fs"
	"os"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/afero"
)

var (
	_ afero.Lstater  = (*BasePathFs)(nil)
	_ fs.ReadDirFile = (*BasePathFile)(nil)
)

// This is a version of the afero basepathfs that uses path instead of filepath.
// this is needed to work with things like zipfs and embedfs on windows
type BasePathFs struct {
	source afero.Fs
	path   string
}

type BasePathFile struct {
	afero.File
	path string
}

func (f *BasePathFile) Name() string {
	sourcename := f.File.Name()
	return strings.TrimPrefix(sourcename, path.Clean(f.path))
}

func (f *BasePathFile) ReadDir(n int) ([]fs.DirEntry, error) {
	if rdf, ok := f.File.(fs.ReadDirFile); ok {
		return rdf.ReadDir(n)
	}
	return readDirFile{f.File}.ReadDir(n)
}

func NewBasePathFs(source afero.Fs, path string) afero.Fs {
	return &BasePathFs{source: source, path: path}
}

// on a file outside the base path it returns the given file name and an error,
// else the given file with the base path prepended
func (b *BasePathFs) RealPath(name string) (p string, err error) {
	if err := validateBasePathName(name); err != nil {
		return name, err
	}

	bpath := path.Clean(b.path)
	p = path.Clean(path.Join(bpath, name))
	if !strings.HasPrefix(p, bpath) {
		return name, os.ErrNotExist
	}

	return p, nil
}

func validateBasePathName(name string) error {
	if runtime.GOOS != "windows" {
		// Not much to do here;
		// the virtual file paths all look absolute on *nix.
		return nil
	}

	// On Windows a common mistake would be to provide an absolute OS path
	// We could strip out the base part, but that would not be very portable.
	if path.IsAbs(name) {
		return os.ErrNotExist
	}

	return nil
}

func (b *BasePathFs) Chtimes(name string, atime, mtime time.Time) (err error) {
	if name, err = b.RealPath(name); err != nil {
		return &os.PathError{Op: "chtimes", Path: name, Err: err}
	}
	return b.source.Chtimes(name, atime, mtime)
}

func (b *BasePathFs) Chmod(name string, mode os.FileMode) (err error) {
	if name, err = b.RealPath(name); err != nil {
		return &os.PathError{Op: "chmod", Path: name, Err: err}
	}
	return b.source.Chmod(name, mode)
}

func (b *BasePathFs) Chown(name string, uid, gid int) (err error) {
	if name, err = b.RealPath(name); err != nil {
		return &os.PathError{Op: "chown", Path: name, Err: err}
	}
	return b.source.Chown(name, uid, gid)
}

func (b *BasePathFs) Name() string {
	return "BasePathFs"
}

func (b *BasePathFs) Stat(name string) (fi os.FileInfo, err error) {
	if name, err = b.RealPath(name); err != nil {
		return nil, &os.PathError{Op: "stat", Path: name, Err: err}
	}
	return b.source.Stat(name)
}

func (b *BasePathFs) Rename(oldname, newname string) (err error) {
	if oldname, err = b.RealPath(oldname); err != nil {
		return &os.PathError{Op: "rename", Path: oldname, Err: err}
	}
	if newname, err = b.RealPath(newname); err != nil {
		return &os.PathError{Op: "rename", Path: newname, Err: err}
	}
	return b.source.Rename(oldname, newname)
}

func (b *BasePathFs) RemoveAll(name string) (err error) {
	if name, err = b.RealPath(name); err != nil {
		return &os.PathError{Op: "remove_all", Path: name, Err: err}
	}
	return b.source.RemoveAll(name)
}

func (b *BasePathFs) Remove(name string) (err error) {
	if name, err = b.RealPath(name); err != nil {
		return &os.PathError{Op: "remove", Path: name, Err: err}
	}
	return b.source.Remove(name)
}

func (b *BasePathFs) OpenFile(name string, flag int, mode os.FileMode) (f afero.File, err error) {
	if name, err = b.RealPath(name); err != nil {
		return nil, &os.PathError{Op: "openfile", Path: name, Err: err}
	}
	sourcef, err := b.source.OpenFile(name, flag, mode)
	if err != nil {
		return nil, err
	}
	return &BasePathFile{sourcef, b.path}, nil
}

func (b *BasePathFs) Open(name string) (f afero.File, err error) {
	if name, err = b.RealPath(name); err != nil {
		return nil, &os.PathError{Op: "open", Path: name, Err: err}
	}
	sourcef, err := b.source.Open(name)
	if err != nil {
		return nil, err
	}
	return &BasePathFile{File: sourcef, path: b.path}, nil
}

func (b *BasePathFs) Mkdir(name string, mode os.FileMode) (err error) {
	if name, err = b.RealPath(name); err != nil {
		return &os.PathError{Op: "mkdir", Path: name, Err: err}
	}
	return b.source.Mkdir(name, mode)
}

func (b *BasePathFs) MkdirAll(name string, mode os.FileMode) (err error) {
	if name, err = b.RealPath(name); err != nil {
		return &os.PathError{Op: "mkdir", Path: name, Err: err}
	}
	return b.source.MkdirAll(name, mode)
}

func (b *BasePathFs) Create(name string) (f afero.File, err error) {
	if name, err = b.RealPath(name); err != nil {
		return nil, &os.PathError{Op: "create", Path: name, Err: err}
	}
	sourcef, err := b.source.Create(name)
	if err != nil {
		return nil, err
	}
	return &BasePathFile{File: sourcef, path: b.path}, nil
}

func (b *BasePathFs) LstatIfPossible(name string) (os.FileInfo, bool, error) {
	name, err := b.RealPath(name)
	if err != nil {
		return nil, false, &os.PathError{Op: "lstat", Path: name, Err: err}
	}
	if lstater, ok := b.source.(afero.Lstater); ok {
		return lstater.LstatIfPossible(name)
	}
	fi, err := b.source.Stat(name)
	return fi, false, err
}

func (b *BasePathFs) SymlinkIfPossible(oldname, newname string) error {
	oldname, err := b.RealPath(oldname)
	if err != nil {
		return &os.LinkError{Op: "symlink", Old: oldname, New: newname, Err: err}
	}
	newname, err = b.RealPath(newname)
	if err != nil {
		return &os.LinkError{Op: "symlink", Old: oldname, New: newname, Err: err}
	}
	if linker, ok := b.source.(afero.Linker); ok {
		return linker.SymlinkIfPossible(oldname, newname)
	}
	return &os.LinkError{Op: "symlink", Old: oldname, New: newname, Err: afero.ErrNoSymlink}
}

func (b *BasePathFs) ReadlinkIfPossible(name string) (string, error) {
	name, err := b.RealPath(name)
	if err != nil {
		return "", &os.PathError{Op: "readlink", Path: name, Err: err}
	}
	if reader, ok := b.source.(afero.LinkReader); ok {
		return reader.ReadlinkIfPossible(name)
	}
	return "", &os.PathError{Op: "readlink", Path: name, Err: afero.ErrNoReadlink}
}

// the readDirFile helper is required

// readDirFile provides adapter from afero.File to fs.ReadDirFile needed for correct Open
type readDirFile struct {
	afero.File
}

var _ fs.ReadDirFile = readDirFile{}

func (r readDirFile) ReadDir(n int) ([]fs.DirEntry, error) {
	items, err := r.File.Readdir(n)
	if err != nil {
		return nil, err
	}

	ret := make([]fs.DirEntry, len(items))
	for i := range items {
		ret[i] = fileInfoDirEntry{FileInfo: items[i]}
	}

	return ret, nil
}

// FileInfoDirEntry provides an adapter from os.FileInfo to fs.DirEntry
type fileInfoDirEntry struct {
	fs.FileInfo
}

var _ fs.DirEntry = fileInfoDirEntry{}

func (d fileInfoDirEntry) Type() fs.FileMode { return d.FileInfo.Mode().Type() }

func (d fileInfoDirEntry) Info() (fs.FileInfo, error) { return d.FileInfo, nil }
