/*
   Copyright 2021 Erigon contributors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

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
		Tmp:             filepath.Join(datadir, "temp"),
		Snap:            filepath.Join(datadir, "snapshots"),
		SnapHistory:     filepath.Join(datadir, "snapshots", "history"),
		TxPool:          filepath.Join(datadir, "txpool"),
		Nodes:           filepath.Join(datadir, "nodes"),
	}
}
