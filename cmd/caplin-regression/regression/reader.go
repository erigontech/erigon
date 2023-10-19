package regression

import (
	clparams2 "github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/phase1/core/state"
	"github.com/ledgerwatch/erigon/cl/utils"
	"io/fs"
	"io/ioutil"
	"path"
	"path/filepath"
	"sort"
)

func (r *RegressionTester) readStartingState() (*state.CachingBeaconState, error) {
	stateFile, err := ioutil.ReadFile(path.Join(r.testDirectory, regressionPath, startingStatePath))
	if err != nil {
		return nil, err
	}
	s := state.New(&clparams2.MainnetBeaconConfig)
	if err := utils.DecodeSSZSnappy(s, stateFile, int(clparams2.CapellaVersion)); err != nil {
		return nil, err
	}
	return s, nil
}

func (r *RegressionTester) initBlocks() error {
	r.blockList = nil
	if err := filepath.Walk(filepath.Join(r.testDirectory, regressionPath, signedBeaconBlockPath), func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info == nil || info.IsDir() || info.Name() != "data.bin" {
			return nil
		}
		f, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		b := new(cltypes.SignedBeaconBlock)
		if err := utils.DecodeSSZSnappy(b, f, int(clparams2.CapellaVersion)); err != nil {
			return err
		}
		r.blockList = append(r.blockList, b)
		return nil
	}); err != nil {
		return err
	}
	sort.Slice(r.blockList, func(i, j int) bool {
		return r.blockList[i].Block.Slot < r.blockList[j].Block.Slot
	})
	return nil
}
