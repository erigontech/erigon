package spectest

import (
	"io/fs"
	"os"
	"strings"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/transition/machine"

	"gfx.cafe/util/go/generic"
)

type TestCase struct {
	ConfigName    string
	ForkPhaseName string
	RunnerName    string
	HandlerName   string
	SuiteName     string
	CaseName      string

	Machine machine.Interface
}

func (t *TestCase) Version() clparams.StateVersion {
	v, _ := clparams.StringToClVersion(t.ForkPhaseName)
	return v
}

type TestCases struct {
	tc   []TestCase
	tree generic.Map6[string, string, string, string, string, string, TestCase]
}

func (tx *TestCases) add(t TestCase) {
	tx.tc = append(tx.tc, t)
	tx.tree.Store(
		t.ConfigName,
		t.ForkPhaseName,
		t.RunnerName,
		t.HandlerName,
		t.SuiteName,
		t.CaseName,
		t,
	)
}

func (t *TestCases) Slice() []TestCase {
	return t.tc
}
func (t *TestCases) Filter(fn func(t TestCase) bool) *TestCases {
	o := &TestCases{}
	for _, v := range t.tc {
		if fn(v) {
			o.add(v)
		}
	}
	return o
}

func ReadTestCases(root fs.FS) (out *TestCases, err error) {
	out = &TestCases{}
	if err := fs.WalkDir(root, ".", func(path string, d fs.DirEntry, err error) error {
		pathList := strings.Split(path, string(os.PathSeparator))
		// Skip hidden folders (those starting with '.')
		for _, part := range pathList {
			if strings.HasPrefix(part, ".") {
				return nil
			}
		}

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
		out.add(c)
		return nil
	}); err != nil {
		return out, err
	}
	return out, nil
}
