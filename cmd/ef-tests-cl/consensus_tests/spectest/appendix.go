package spectest

import (
	"io/fs"
	"path/filepath"
	"testing"
)

type Appendix map[string]*Format

func (a Appendix) Add(name string) *Format {
	o := NewFormat()
	a[name] = o
	return o
}

type RunDirectoryOptions struct {
	FS fs.ReadDirFS
}

type formatRoot struct {
	rawpath     string
	handlername string
	format      *Format
	root        fs.DirEntry
}

func (a Appendix) RunDirectory(t *testing.T, opts *RunDirectoryOptions) error {
	formatRoots := make([]formatRoot, 0, 16)
	if err := fs.WalkDir(opts.FS, "", func(path string, d fs.DirEntry, err error) error {
		dirname := filepath.Dir(path)
		if val, ok := a[dirname]; ok {
			formatRoots = append(formatRoots, formatRoot{
				rawpath:     path,
				handlername: dirname,
				format:      val,
			})
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

type TestCase struct {
	ConfigName    string
	ForkPhaseName string
	RunnerName    string
	HandlerName   string
	SuiteName     string
	CaseName      string
}

type TestCases []TestCase

func ReadTestCases(root fs.ReadDirFS) (out TestCases, err error) {
	if err := fs.WalkDir(root, "", func(path string, d fs.DirEntry, err error) error {
		pathList := filepath.SplitList(path)
		//TODO: probably we can do more sanitation here
		if len(pathList) != 6 {
			return nil
		}
		c := TestCase{
			ConfigName:    pathList[0],
			ForkPhaseName: pathList[1],
			RunnerName:    pathList[2],
			HandlerName:   pathList[3],
			SuiteName:     pathList[4],
			CaseName:      pathList[5],
		}
		out = append(out, c)
		return nil
	}); err != nil {
		return nil, err
	}
	return
}
