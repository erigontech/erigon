// Copyright 2022 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package fork

import (
	"errors"
	"sort"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/types/ssz"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/utils"
)

var NO_GENESIS_TIME_ERR error = errors.New("genesis time is not set")
var NO_VALIDATOR_ROOT_HASH error = errors.New("genesis validators root is not set")

type fork struct {
	epoch   uint64
	version [4]byte
}

func forkList(schedule map[common.Bytes4]uint64) (f []fork) {
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
	var currentVersion32 common.Hash
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

func Domain(fork *cltypes.Fork, epoch uint64, domainType [4]byte, genesisRoot common.Hash) ([]byte, error) {
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
