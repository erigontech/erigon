// Copyright 2026 The Erigon Authors
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

package l1

import (
	"context"
	"errors"
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/protocol"
)

// ErrNoManager reports a rollup the registry records no manager for yet.
var ErrNoManager = errors.New("rollup has no manager")

// Key is a proof system's verification key in a manager, and the address
// its attestations recover to.
type Key struct {
	VKey   common.Hash
	Signer common.Address
}

// Registration is what a rollup's manager registers at an L1 block: the
// number of proofs a batch needs, and the proof systems it accepts.
type Registration struct {
	Threshold uint64
	Keys      map[common.Address]Key
}

// ReadRegistration reads, after blockHash, the registration of proofSystems
// by the manager of rollupID: threshold(), each proof system's
// verificationKey and, for one the manager keys, its signer(). A zero key
// leaves a proof system out.
func ReadRegistration(ctx context.Context, sr StateReader, registry common.Address, rollupID uint64, proofSystems []common.Address, blockHash common.Hash) (Registration, error) {
	out, err := sr.CallAt(ctx, Call{To: registry, Data: protocol.EncodeRollups(rollupID)}, blockHash)
	if err != nil {
		return Registration{}, fmt.Errorf("rollups(%d): %w", rollupID, err)
	}
	record, err := protocol.DecodeRollupsReturn(out)
	if err != nil {
		return Registration{}, fmt.Errorf("%w: %x answered rollups(%d): %w", ErrNotRollups, registry, rollupID, err)
	}
	manager := record.RollupContract
	if manager == (common.Address{}) {
		return Registration{}, fmt.Errorf("%w: rollup %d", ErrNoManager, rollupID)
	}
	read := func(to common.Address, data []byte, what string) ([]byte, error) {
		out, err := sr.CallAt(ctx, Call{To: to, Data: data}, blockHash)
		if err != nil {
			return nil, fmt.Errorf("%s of %x: %w", what, to, err)
		}
		return out, nil
	}
	out, err = read(manager, protocol.EncodeThreshold(), "threshold")
	if err != nil {
		return Registration{}, err
	}
	reg := Registration{Keys: map[common.Address]Key{}}
	if reg.Threshold, err = protocol.DecodeThreshold(out); err != nil {
		return Registration{}, err
	}
	for _, ps := range proofSystems {
		out, err := read(manager, protocol.EncodeVerificationKey(ps), "verificationKey")
		if err != nil {
			return Registration{}, err
		}
		vkey, err := protocol.DecodeVerificationKey(out)
		if err != nil {
			return Registration{}, err
		}
		if vkey == (common.Hash{}) {
			continue
		}
		if out, err = read(ps, protocol.EncodeSigner(), "signer"); err != nil {
			return Registration{}, err
		}
		signer, err := protocol.DecodeSigner(out)
		if err != nil {
			return Registration{}, err
		}
		reg.Keys[ps] = Key{VKey: vkey, Signer: signer}
	}
	return reg, nil
}
