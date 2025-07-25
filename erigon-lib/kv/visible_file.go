package kv

type VisibleFile interface {
	Fullpath() string
	StartRootNum() uint64
	EndRootNum() uint64
}

type VisibleFiles []VisibleFile

func (v VisibleFiles) Fullpaths() []string {
	names := make([]string, 0, len(v))
	for _, f := range v {
		names = append(names, f.Fullpath())
	}
	return names
}
func (v VisibleFiles) EndRootNum() uint64 {
	if len(v) == 0 {
		return 0
	}
	return v[len(v)-1].EndRootNum()
}
