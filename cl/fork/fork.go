/*
   Copyright 2022 Erigon-Lightclient contributors
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

package fork

import (
	"errors"
	"sort"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/types/ssz"

	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
)

var NO_GENESIS_TIME_ERR error = errors.New("genesis time is not set")
var NO_VALIDATOR_ROOT_HASH error = errors.New("genesis validators root is not set")

type fork struct {
	epoch   uint64
	version [4]byte
}

func forkList(schedule map[libcommon.Bytes4]uint64) (f []fork) {
	for version, epoch := range schedule {
		f = append(f, fork{epoch: epoch, version: version})
	}
	sort.Slice(f, func(i, j int) bool {
		return f[i].epoch < f[j].epoch
	})
	return
}

func ComputeDomain(
	domainType []byte,
	currentVersion [4]byte,
	genesisValidatorsRoot [32]byte,
) ([]byte, error) {
	var currentVersion32 libcommon.Hash
	copy(currentVersion32[:], currentVersion[:])
	forkDataRoot := utils.Sha256(currentVersion32[:], genesisValidatorsRoot[:])
	return append(domainType, forkDataRoot[:28]...), nil
}

func ComputeSigningRoot(
	obj ssz.HashableSSZ,
	domain []byte,
) ([32]byte, error) {
	objRoot, err := obj.HashSSZ()
	if err != nil {
		return [32]byte{}, err
	}
	return utils.Sha256(objRoot[:], domain), nil
}

func Domain(fork *cltypes.Fork, epoch uint64, domainType [4]byte, genesisRoot libcommon.Hash) ([]byte, error) {
	if fork == nil {
		return []byte{}, errors.New("nil fork or domain type")
	}
	var forkVersion []byte
	if epoch < fork.Epoch {
		forkVersion = fork.PreviousVersion[:]
	} else {
		forkVersion = fork.CurrentVersion[:]
	}
	if len(forkVersion) != 4 {
		return []byte{}, errors.New("fork version length is not 4 byte")
	}
	var forkVersionArray [4]byte
	copy(forkVersionArray[:], forkVersion[:4])
	return ComputeDomain(domainType[:], forkVersionArray, genesisRoot)
}
