package dirs

import (
	"path/filepath"
)

type Dirs struct {
	Data      string
	Chaindata string
	Tmp       string
	Snap      string
}

func MakeDirs(datadir string) Dirs {
	return Dirs{
		Data:      datadir,
		Chaindata: filepath.Join(datadir, "chaindata"),
		Tmp:       filepath.Join(datadir, "etl-temp"),
		Snap:      filepath.Join(datadir, "snapshots"),
	}
}
