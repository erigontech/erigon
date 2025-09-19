package state

import (
	"errors"
	"path/filepath"

	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon/db/datadir"
)

func CheckSaltFilesExist(dirs datadir.Dirs) (bool, error) {
	exists, err := dir.FileExist(filepath.Join(dirs.Snap, "salt-blocks.txt"))
	if err != nil {
		return false, err
	}
	exists2, err := dir.FileExist(filepath.Join(dirs.Snap, "salt-state.txt"))
	if err != nil {
		return false, err
	}

	return exists && exists2, nil
}

var ErrCannotStartWithoutSaltFiles = errors.New("cannot start RPC daemon: salt files missing in datadir. REQUIRED STEPS: (1) Start Erigon and wait for OtterSync to complete. (2) Once salt files exist (snapshot/salt-blocks.txt, snapshot/salt-state.txt), start RPC daemon independently")
