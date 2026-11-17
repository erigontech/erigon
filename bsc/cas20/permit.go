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
	"github.com/erigontech/erigon/execution/vm"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/protocol/params"
)

// The EIP-712 domain is derived from the live token name, so updateName
// invalidates outstanding permits; there is no cached separator to roll.

const cas20EIP712Version = "1"

var (
	selDomainSeparator      = selector("DOMAIN_SEPARATOR()")
	selNonces               = selector("nonces(address)")
	selPermit               = selector("permit(address,address,uint256,uint256,uint8,bytes32,bytes32)")
	selTransferWithMemo     = selector("transferWithMemo(address,uint256,bytes32)")
	selTransferFromWithMemo = selector("transferFromWithMemo(address,address,uint256,bytes32)")
	selMintWithMemo         = selector("mintWithMemo(address,uint256,bytes32)")
	selBurnWithMemo         = selector("burnWithMemo(uint256,bytes32)")

	cas20DomainTypehash = keccakHash([]byte("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"))
	cas20PermitTypehash = keccakHash([]byte("Permit(address owner,address spender,uint256 value,uint256 nonce,uint256 deadline)"))
	cas20TopicMemo      = eventTopic("Memo(address,bytes32)")
)

func (t cas20Token) dispatchPermitMemo(sel [4]byte, args []byte) (ret []byte, err error, ok bool) {
	switch sel {
	case selDomainSeparator:
		d, ok := t.domainSeparator()
		if !ok {
			return nil, vm.ErrOutOfGas, true
		}
		return d[:], nil, true
	case selNonces:
		owner, err := readAddress(args, 0)
		if err != nil {
			return nil, err, true
		}
		return encU256(t.s.nonce(owner)), nil, true
	case selPermit:
		ret, err := t.decodePermit(args)
		return ret, err, true

	case selTransferWithMemo:
		to, amount, memo, err := readToAmountMemo(args)
		if err != nil {
			return nil, err, true
		}
		ret, err := t.transfer(t.ctx.Caller, to, amount)
		if err == nil {
			if !t.emitMemo(memo) {
				return nil, vm.ErrOutOfGas, true
			}
		}
		return ret, err, true
	case selTransferFromWithMemo:
		from, err := readAddress(args, 0)
		if err != nil {
			return nil, err, true
		}
		to, err := readAddress(args, 1)
		if err != nil {
			return nil, err, true
		}
		amount, err := readU256(args, 2)
		if err != nil {
			return nil, err, true
		}
		memo, err := readWord(args, 3)
		if err != nil {
			return nil, err, true
		}
		ret, err := t.transferFrom(t.ctx.Caller, from, to, amount)
		if err == nil {
			if !t.emitMemo(memo) {
				return nil, vm.ErrOutOfGas, true
			}
		}
		return ret, err, true
	case selMintWithMemo:
		to, amount, memo, err := readToAmountMemo(args)
		if err != nil {
			return nil, err, true
		}
		if err := t.mint(to, amount); err != nil {
			return nil, err, true
		}
		if !t.emitMemo(memo) {
			return nil, vm.ErrOutOfGas, true
		}
		return nil, nil, true
	case selBurnWithMemo:
		amount, err := readU256(args, 0)
		if err != nil {
			return nil, err, true
		}
		memo, err := readWord(args, 1)
		if err != nil {
			return nil, err, true
		}
		if err := t.burn(t.ctx.Caller, amount); err != nil {
			return nil, err, true
		}
		if !t.emitMemo(memo) {
			return nil, vm.ErrOutOfGas, true
		}
		return nil, nil, true
	}
	return nil, nil, false
}

func (t cas20Token) emitMemo(memo common.Hash) bool {
	return t.ctx.AddLog([]common.Hash{cas20TopicMemo, addrKey(t.ctx.Caller), memo}, nil)
}

func readToAmountMemo(args []byte) (common.Address, *uint256.Int, common.Hash, error) {
	to, err := readAddress(args, 0)
	if err != nil {
		return common.Address{}, nil, common.Hash{}, err
	}
	amount, err := readU256(args, 1)
	if err != nil {
		return common.Address{}, nil, common.Hash{}, err
	}
	memo, err := readWord(args, 2)
	if err != nil {
		return common.Address{}, nil, common.Hash{}, err
	}
	return to, amount, memo, nil
}

// --- EIP-2612 permit --------------------------------------------------------

func (t cas20Token) domainSeparator() (common.Hash, bool) {
	name, ok := t.s.name()
	if !ok {
		return common.Hash{}, false
	}
	if !t.ctx.chargeKeccak(len(name)) ||
		!t.ctx.chargeKeccak(len(cas20EIP712Version)) ||
		!t.ctx.chargeKeccak(160) {
		return common.Hash{}, false
	}
	return cas20DomainSeparator(name, t.ctx.ChainID(), t.ctx.Self), true
}

func cas20DomainSeparator(name string, chainID *uint256.Int, self common.Address) common.Hash {
	nameHash := keccakHash([]byte(name))
	versionHash := keccakHash([]byte(cas20EIP712Version))
	id := chainID.Bytes32()

	enc := make([]byte, 0, 160)
	enc = append(enc, cas20DomainTypehash[:]...)
	enc = append(enc, nameHash[:]...)
	enc = append(enc, versionHash[:]...)
	enc = append(enc, id[:]...)
	enc = append(enc, hashBytes(addrKey(self))...)
	return keccakHash(enc)
}

func (t cas20Token) decodePermit(args []byte) ([]byte, error) {
	owner, err := readAddress(args, 0)
	if err != nil {
		return nil, err
	}
	spender, err := readAddress(args, 1)
	if err != nil {
		return nil, err
	}
	value, err := readU256(args, 2)
	if err != nil {
		return nil, err
	}
	deadline, err := readU256(args, 3)
	if err != nil {
		return nil, err
	}
	v, err := readStrictUint8(args, 4)
	if err != nil {
		return nil, err
	}
	r, err := readWord(args, 5)
	if err != nil {
		return nil, err
	}
	s, err := readWord(args, 6)
	if err != nil {
		return nil, err
	}
	return t.permit(owner, spender, value, deadline, v, r, s)
}

func (t cas20Token) permit(owner, spender common.Address, value, deadline *uint256.Int, v byte, r, s common.Hash) ([]byte, error) {
	if t.ctx.ReadOnly {
		return nil, vm.ErrWriteProtection
	}
	if owner == (common.Address{}) {
		return nil, revCAS20("InvalidApprover(address)", errSelInvalidApprover, addrKey(owner))
	}
	if deadline.LtUint64(t.ctx.BlockTime()) {
		return nil, revCAS20("ExpiredSignature(uint256)", errSelExpiredSignature, wU256(deadline))
	}
	nonce := t.s.nonce(owner)

	structHash := make([]byte, 0, 192)
	structHash = append(structHash, cas20PermitTypehash[:]...)
	structHash = append(structHash, hashBytes(addrKey(owner))...)
	structHash = append(structHash, hashBytes(addrKey(spender))...)
	vb := value.Bytes32()
	structHash = append(structHash, vb[:]...)
	nb := nonce.Bytes32()
	structHash = append(structHash, nb[:]...)
	db := deadline.Bytes32()
	structHash = append(structHash, db[:]...)

	dom, paid := t.domainSeparator()
	if !paid {
		return nil, vm.ErrOutOfGas
	}
	// Charged whether or not the signature turns out to be valid, as ECRECOVER would be.
	if !t.ctx.chargeKeccak(len(structHash)) ||
		!t.ctx.chargeKeccak(66) ||
		!t.ctx.chargeGas(params.EcrecoverGas) {
		return nil, vm.ErrOutOfGas
	}
	digest := crypto.Keccak256([]byte{0x19, 0x01}, dom[:], crypto.Keccak256(structHash))

	signer, ok := ecrecoverAddress(digest, v, r, s)
	if !ok || signer != owner {
		return nil, revCAS20("InvalidSigner(address,address)", errSelInvalidSigner,
			addrKey(signer), addrKey(owner))
	}

	// After the signature, so a bad signature naming the zero spender is InvalidSigner.
	if spender == (common.Address{}) {
		return nil, revCAS20("InvalidSpender(address)", errSelInvalidSpender, addrKey(spender))
	}
	t.s.setNonce(owner, new(uint256.Int).AddUint64(nonce, 1))
	t.s.setAllowance(owner, spender, value)
	if !t.emit(cas20TopicApproval, owner, spender, value) {
		return nil, vm.ErrOutOfGas
	}
	return nil, nil
}

// EIP-2 low-s and v ∈ {27,28}; ERC-1271 contract signatures are not supported.
func ecrecoverAddress(hash []byte, v byte, r, s common.Hash) (common.Address, bool) {
	if v != 27 && v != 28 {
		return common.Address{}, false
	}
	rU, sU := new(uint256.Int).SetBytes(r[:]), new(uint256.Int).SetBytes(s[:])
	if !crypto.TransactionSignatureIsValid(v-27, rU, sU, false) {
		return common.Address{}, false
	}
	sig := make([]byte, 65)
	copy(sig[0:32], r[:])
	copy(sig[32:64], s[:])
	sig[64] = v - 27
	pub, err := crypto.Ecrecover(hash, sig)
	if err != nil || len(pub) == 0 {
		return common.Address{}, false
	}
	var addr common.Address
	copy(addr[:], crypto.Keccak256(pub[1:])[12:])
	return addr, true
}
