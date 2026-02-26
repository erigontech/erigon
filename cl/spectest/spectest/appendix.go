package spectest

import (
	"io/fs"
)

type Appendix map[string]*Format

func (a Appendix) Add(name string) *Format {
	o := NewFormat()
	a[name] = o
	return o
}

type RunDirectoryOptions struct {
	FS fs.FS
}

type formatRoot struct {
	rawpath     string
	handlername string
	format      *Format
	root        fs.DirEntry
}
