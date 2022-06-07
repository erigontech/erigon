package datadir

import (
	"path/filepath"
)

type Dirs struct {
	DataDir   string
	Chaindata string
	Tmp       string
	Snap      string
	TxPool    string
}

func New(datadir string) Dirs {
	return Dirs{
		DataDir:   datadir,
		Chaindata: filepath.Join(datadir, "chaindata"),
		Tmp:       filepath.Join(datadir, "etl-temp"),
		Snap:      filepath.Join(datadir, "snapshots"),
		TxPool:    filepath.Join(datadir, "txpool"),
	}
}
