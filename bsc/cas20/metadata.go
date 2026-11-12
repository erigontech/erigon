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

package cas20

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/vm"
)

// Metadata writes are not pause-gated (BEP-702 3.6).

var (
	selContractURI       = selector("contractURI()")
	selUpdateName        = selector("updateName(string)")
	selUpdateSymbol      = selector("updateSymbol(string)")
	selUpdateContractURI = selector("updateContractURI(string)")
	selPausedFeatures    = selector("pausedFeatures()")
	selSupplyCap         = selector("supplyCap()")
	selEIP712Domain      = selector("eip712Domain()")

	cas20TopicNameUpdated         = eventTopic("NameUpdated(address,string)")
	cas20TopicSymbolUpdated       = eventTopic("SymbolUpdated(address,string)")
	cas20TopicContractURIUpdated  = eventTopic("ContractURIUpdated()")
	cas20TopicEIP712DomainChanged = eventTopic("EIP712DomainChanged()")
)

func (t cas20Token) dispatchMetadata(sel [4]byte, args []byte) (ret []byte, err error, ok bool) {
	switch sel {
	case selContractURI:
		v, ok := t.s.contractURI()
		if !ok {
			return nil, vm.ErrOutOfGas, true
		}
		return encString(v), nil, true
	case selSupplyCap:
		return encU256(t.s.supplyCap()), nil, true
	case selPausedFeatures:
		return encodeTuple(abiWordArray(t.pausedFeatures())), nil, true
	case selEIP712Domain:
		d, ok := t.eip712Domain()
		if !ok {
			return nil, vm.ErrOutOfGas, true
		}
		return d, nil, true

	case selUpdateName:
		v, err := readStringArg(args, 0)
		if err != nil {
			return nil, err, true
		}
		return nil, t.updateName(v), true
	case selUpdateSymbol:
		v, err := readStringArg(args, 0)
		if err != nil {
			return nil, err, true
		}
		return nil, t.updateSymbol(v), true
	case selUpdateContractURI:
		v, err := readStringArg(args, 0)
		if err != nil {
			return nil, err, true
		}
		return nil, t.updateContractURI(v), true
	}
	return nil, nil, false
}

func (t cas20Token) ensureMetadataWrite() error {
	if t.ctx.ReadOnly {
		return vm.ErrWriteProtection
	}
	return t.ensureRole(roleMetadata)
}

func (t cas20Token) updateName(v string) error {
	if err := t.ensureMetadataWrite(); err != nil {
		return err
	}
	if !t.s.setName(v) {
		return vm.ErrOutOfGas
	}
	if !t.ctx.AddLog([]common.Hash{cas20TopicNameUpdated, addrKey(t.ctx.Caller)}, encString(v)) {
		return vm.ErrOutOfGas
	}
	if !t.ctx.AddLog([]common.Hash{cas20TopicEIP712DomainChanged}, nil) {
		return vm.ErrOutOfGas
	}
	return nil
}

func (t cas20Token) updateSymbol(v string) error {
	if err := t.ensureMetadataWrite(); err != nil {
		return err
	}
	if !t.s.setSymbol(v) {
		return vm.ErrOutOfGas
	}
	if !t.ctx.AddLog([]common.Hash{cas20TopicSymbolUpdated, addrKey(t.ctx.Caller)}, encString(v)) {
		return vm.ErrOutOfGas
	}
	return nil
}

func (t cas20Token) updateContractURI(v string) error {
	if err := t.ensureMetadataWrite(); err != nil {
		return err
	}
	if !t.s.setContractURI(v) {
		return vm.ErrOutOfGas
	}
	if !t.ctx.AddLog([]common.Hash{cas20TopicContractURIUpdated}, nil) {
		return vm.ErrOutOfGas
	}
	return nil
}

func (t cas20Token) pausedFeatures() []common.Hash {
	p := t.s.paused()
	out := make([]common.Hash, 0, cas20PauseSeize+1)
	for f := uint(0); f <= cas20PauseSeize; f++ {
		if new(uint256.Int).Rsh(p, f).Uint64()&1 == 1 {
			out = append(out, wU8(byte(f)))
		}
	}
	return out
}

// ERC-5267. fields 0x0f names the four members domainSeparator hashes.
func (t cas20Token) eip712Domain() ([]byte, bool) {
	name, ok := t.s.name()
	if !ok {
		return nil, false
	}
	var fields common.Hash
	fields[0] = 0x0f
	return encodeTuple(
		abiWord(fields),
		abiString(name),
		abiString(cas20EIP712Version),
		abiWord(wU256(t.ctx.ChainID())),
		abiWord(addrKey(t.ctx.Self)),
		abiWord(common.Hash{}),
		abiWordArray(nil),
	), true
}
