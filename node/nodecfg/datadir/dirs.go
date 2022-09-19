package datadir

import (
	"path/filepath"
)

// Dirs is the file system folder the node should use for any data storage
// requirements. The configured data directory will not be directly shared with
// registered services, instead those can use utility methods to create/access
// databases or flat files
type Dirs struct {
	DataDir         string
	RelativeDataDir string // like dataDir, but without filepath.Abs() resolution
	Chaindata       string
	Tmp             string
	Snap            string
	SnapHistory     string
	TxPool          string
	Nodes           string
}

func New(datadir string) Dirs {
	relativeDataDir := datadir
	if datadir != "" {
		var err error
		absdatadir, err := filepath.Abs(datadir)
		if err != nil {
			panic(err)
		}
		datadir = absdatadir
	}

	return Dirs{
		RelativeDataDir: relativeDataDir,
		DataDir:         datadir,
		Chaindata:       filepath.Join(datadir, "chaindata"),
		Tmp:             filepath.Join(datadir, "etl-temp"),
		Snap:            filepath.Join(datadir, "snapshots"),
		SnapHistory:     filepath.Join(datadir, "snapshots", "history"),
		TxPool:          filepath.Join(datadir, "txpool"),
		Nodes:           filepath.Join(datadir, "nodes"),
	}
}
