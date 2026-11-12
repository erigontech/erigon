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
	"errors"
	"fmt"

	"github.com/erigontech/erigon/execution/vm"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
)

// Business-rule failures are ABI custom errors; malformed calldata and unknown
// selectors revert with empty returndata (BEP-702 3.2).

// cas20ErrSigs lets a test read what the code actually raises rather than a list of it.
var cas20ErrSigs = map[string][4]byte{}

func eventTopic(sig string) common.Hash {
	return keccakHash([]byte(sig))
}

func cas20ErrorSel(sig string) [4]byte {
	var s [4]byte
	copy(s[:], crypto.Keccak256([]byte(sig)))
	cas20ErrSigs[sig] = s
	return s
}

// cas20RevertError never reaches the EVM directly; finishCAS20 converts it.
type cas20RevertError struct {
	sig  string
	data []byte
}

func (e *cas20RevertError) Error() string { return fmt.Sprintf("cas20 revert: %s", e.sig) }

func (e *cas20RevertError) Is(target error) bool { return target == vm.ErrExecutionReverted }

func finishCAS20(ret []byte, err error) ([]byte, error) {
	// A refused call form is an ABI error, not an exceptional halt (BEP-702 3.2).
	switch {
	case errors.Is(err, ErrCAS20DelegateCall):
		err = revCAS20("DelegateCallNotAllowed()", errSelDelegateCallDenied)
	case errors.Is(err, vm.ErrWriteProtection):
		err = revCAS20("StaticCallNotAllowed()", errSelStaticCallDenied)
	}
	if rev, ok := errors.AsType[*cas20RevertError](err); ok {
		return rev.data, vm.ErrExecutionReverted
	}
	return ret, err
}

// Write protection, then an exhausted budget, outrank whatever the logic returned.
func finishCAS20Metered(ctx *PrecompileContext, ret []byte, err error) ([]byte, error) {
	if ctx.writeProtectionViolated() {
		return finishCAS20(nil, vm.ErrWriteProtection)
	}
	if ctx.OutOfGas() {
		return nil, vm.ErrOutOfGas
	}
	return finishCAS20(ret, err)
}

func revCAS20(sig string, sel [4]byte, words ...common.Hash) error {
	data := make([]byte, 4+32*len(words))
	copy(data, sel[:])
	for i := range words {
		copy(data[4+i*32:], words[i][:])
	}
	return &cas20RevertError{sig: sig, data: data}
}

func revCAS20Bytes(sig string, sel [4]byte, payload []byte) error {
	return &cas20RevertError{sig: sig, data: append(sel[:], encodeTuple(abiBytes(payload))...)}
}

// The shape BSC's system contracts use to report a rejected parameter change.
func revCAS20StringBytes(sig string, sel [4]byte, key string, value []byte) error {
	return &cas20RevertError{sig: sig, data: append(sel[:], encodeTuple(abiString(key), abiBytes(value))...)}
}

func wU256(v *uint256.Int) common.Hash { return v.Bytes32() }
func wU64(v uint64) common.Hash        { return uint256.NewInt(v).Bytes32() }
func wU8(v byte) common.Hash           { var h common.Hash; h[31] = v; return h }

var (
	errSelNonPayable          = cas20ErrorSel("NonPayable()")
	errSelInvalidReceiver     = cas20ErrorSel("InvalidReceiver(address)")
	errSelInvalidSender       = cas20ErrorSel("InvalidSender(address)")
	errSelInvalidSpender      = cas20ErrorSel("InvalidSpender(address)")
	errSelInvalidApprover     = cas20ErrorSel("InvalidApprover(address)")
	errSelInsufficientBalance = cas20ErrorSel("InsufficientBalance(address,uint256,uint256)")
	errSelInsufficientAllow   = cas20ErrorSel("InsufficientAllowance(address,uint256,uint256)")
	errSelSupplyCapExceeded   = cas20ErrorSel("SupplyCapExceeded(uint256,uint256)")
	errSelInvalidSupplyCap    = cas20ErrorSel("InvalidSupplyCap(uint256,uint256)")
	errSelContractPaused      = cas20ErrorSel("ContractPaused(uint8)")
	errSelExpiredSignature    = cas20ErrorSel("ExpiredSignature(uint256)")
	errSelInvalidSigner       = cas20ErrorSel("InvalidSigner(address,address)")
	errSelACUnauthorized      = cas20ErrorSel("AccessControlUnauthorizedAccount(address,bytes32)")
	errSelACBadConfirmation   = cas20ErrorSel("AccessControlBadConfirmation()")
	errSelLastAdminRenounce   = cas20ErrorSel("LastAdminCannotRenounce()")
	errSelNotSoleAdmin        = cas20ErrorSel("NotSoleAdmin()")
	errSelPolicyForbids       = cas20ErrorSel("PolicyForbids(bytes32,uint64)")
	// IPolicyRegistry answers about the id the caller named; ICAS20 reports which one it could not find.
	errSelPolicyNotFound     = cas20ErrorSel("PolicyNotFound()")
	errSelPolicyNotFoundID   = cas20ErrorSel("PolicyNotFound(uint64)")
	errSelUnsupportedScope   = cas20ErrorSel("UnsupportedPolicyType(bytes32)")
	errSelIncompatibleType   = cas20ErrorSel("IncompatiblePolicyType()")
	errSelInvalidChildPolicy = cas20ErrorSel("InvalidChildPolicy(uint64)")
	errSelChildrenOutOfRange = cas20ErrorSel("ChildPoliciesOutsideOfRange()")
	errSelBatchTooLarge      = cas20ErrorSel("BatchSizeTooLarge(uint256)")
	errSelEmptyBatch         = cas20ErrorSel("EmptyBatch()")
	errSelLengthMismatch     = cas20ErrorSel("LengthMismatch(uint256,uint256)")
	errSelEmptyFeatureSet    = cas20ErrorSel("EmptyFeatureSet()")
	errSelInvalidMultiplier  = cas20ErrorSel("InvalidMultiplier()")
	errSelEffectiveAtInPast  = cas20ErrorSel("EffectiveAtInPast(uint256)")
	errSelEffectiveAtTooFar  = cas20ErrorSel("EffectiveAtTooFar(uint256)")
	errSelUIMulExists        = cas20ErrorSel("UIMultiplierUpdateExists(uint256)")
	errSelUIMulMissing       = cas20ErrorSel("UIMultiplierUpdateDoesNotExist()")
	errSelInvalidMetadataKey = cas20ErrorSel("InvalidMetadataKey()")
	errSelAnnounceInProgress = cas20ErrorSel("AnnouncementInProgress()")
	errSelAnnounceIdUsed     = cas20ErrorSel("AnnouncementIdAlreadyUsed(string)")
	errSelInternalMalformed  = cas20ErrorSel("InternalCallMalformed(bytes)")
	errSelInternalFailed     = cas20ErrorSel("InternalCallFailed(bytes)")
	errSelInvalidVariant     = cas20ErrorSel("InvalidVariant()")
	errSelTokenExists        = cas20ErrorSel("TokenAlreadyExists(address)")
	errSelInitCallFailed     = cas20ErrorSel("InitCallFailed(uint256)")
	errSelUnauthorized       = cas20ErrorSel("Unauthorized()")
	errSelNoPendingAdmin     = cas20ErrorSel("NoPendingAdmin()")
	errSelZeroAddress        = cas20ErrorSel("ZeroAddress()")
	errSelAccountNotSeizable = cas20ErrorSel("AccountNotSeizable(address)")
	errSelPanic              = cas20ErrorSel("Panic(uint256)")
	errSelFeatureNotActive   = cas20ErrorSel("FeatureNotActivated(bytes32)")
	errSelAlreadyActivated   = cas20ErrorSel("AlreadyActivated(bytes32)")
	errSelUnauthorizedAddr   = cas20ErrorSel("Unauthorized(address)")
	errSelInvalidValue       = cas20ErrorSel("InvalidValue(string,bytes)")
	errSelUnknownParam       = cas20ErrorSel("UnknownParam(string,bytes)")
	errSelDelegateCallDenied = cas20ErrorSel("DelegateCallNotAllowed()")
	errSelStaticCallDenied   = cas20ErrorSel("StaticCallNotAllowed()")
	errSelUnsupportedVersion = cas20ErrorSel("UnsupportedVersion(uint8,uint8)")
	errSelInvalidDecimals    = cas20ErrorSel("InvalidDecimals(uint8)")
	errSelMissingField       = cas20ErrorSel("MissingRequiredField(string)")
	errSelInvalidCurrency    = cas20ErrorSel("InvalidCurrency(string)")
	errSelInvalidFormatId    = cas20ErrorSel("InvalidFormatId()")
)

// Only 0x11 arises here: a malformed argument is a decode failure and reverts empty.
func revPanic(code byte) error {
	return revCAS20("Panic(uint256)", errSelPanic, wU8(code))
}
