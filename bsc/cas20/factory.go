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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/vm"
)

const cas20ParamsVersion = 1

// BEP-702 4.10.
const (
	cas20MinDecimals = 6
	cas20MaxDecimals = 18
)

var (
	cas20TopicCAS20Created = eventTopic("CAS20Created(address,uint8,string,string,uint8,bytes)")

	selCreateCAS20        = selector("createCAS20(uint8,bytes32,bytes,bytes[])")
	selGetCAS20Address    = selector("getCAS20Address(uint8,address,bytes32)")
	selIsCAS20            = selector("isCAS20(address)")
	selVariantOf          = selector("variantOf(address)")
	selIsCAS20Initialized = selector("isCAS20Initialized(address)")
)

func cas20VariantRecognized(variant byte) bool {
	_, ok := cas20Variants[variant]
	return ok
}

var cas20NoSupplyCap = new(uint256.Int).Sub(new(uint256.Int).Lsh(uint256.NewInt(1), 128), uint256.NewInt(1))

// 0xCA52 ++ 8×0x00 ++ variant ++ keccak256(abi.encode(creator, salt))[:9].
func cas20DeriveAddress(variant byte, creator common.Address, salt common.Hash) common.Address {
	h := crypto.Keccak256(common.LeftPadBytes(creator[:], 32), salt[:])
	var a common.Address
	a[0], a[1] = cas20MarkerPrefix[0], cas20MarkerPrefix[1]
	a[10] = variant
	copy(a[11:20], h[:9])
	return a
}

func runCAS20Factory(ctx *PrecompileContext, input []byte) ([]byte, error) {
	if len(input) < 4 {
		return nil, vm.ErrExecutionReverted
	}
	var sel [4]byte
	copy(sel[:], input[:4])
	args := input[4:]

	switch sel {
	case selGetCAS20Address:
		variant, err := readWord(args, 0)
		if err != nil {
			return nil, err
		}
		sender, err := readAddress(args, 1)
		if err != nil {
			return nil, err
		}
		salt, err := readWord(args, 2)
		if err != nil {
			return nil, err
		}
		// Decoded as createCAS20 decodes it, so prediction and creation agree.
		if !isEnumWord(variant, cas20VariantMax) {
			return nil, vm.ErrExecutionReverted
		}
		if !ctx.chargeKeccak(64) {
			return nil, vm.ErrOutOfGas
		}
		addr := cas20DeriveAddress(variant[31], sender, salt)
		return hashBytes(addrKey(addr)), nil
	case selIsCAS20:
		a, err := readAddress(args, 0)
		if err != nil {
			return nil, err
		}
		return encBool(IsCAS20Address(a)), nil
	case selVariantOf:
		a, err := readAddress(args, 0)
		if err != nil {
			return nil, err
		}
		// The return type is an enum, so an unrecognized variant reverts rather than
		// handing the caller a value its decoder rejects.
		if !IsCAS20Address(a) || !cas20VariantRecognized(a[10]) {
			return nil, revCAS20("InvalidVariant()", errSelInvalidVariant)
		}
		return hashBytes(wU8(a[10])), nil
	case selIsCAS20Initialized:
		a, err := readAddress(args, 0)
		if err != nil {
			return nil, err
		}
		return encBool(IsCAS20Address(a) && cas20InitializedMetered(ctx, a)), nil
	case selCreateCAS20:
		return createCAS20(ctx, args)
	}
	return nil, vm.ErrExecutionReverted
}

func createCAS20(ctx *PrecompileContext, args []byte) ([]byte, error) {
	if ctx.ReadOnly {
		return nil, vm.ErrWriteProtection
	}
	variantWord, err := readWord(args, 0)
	if err != nil {
		return nil, err
	}
	salt, err := readWord(args, 1)
	if err != nil {
		return nil, err
	}
	params, err := readBytesArg(args, 2)
	if err != nil {
		return nil, err
	}
	initCalls, err := readBytesArray(args, 3)
	if err != nil {
		return nil, err
	}

	// Variant and feature gate before the params blob (BEP-702 3.4): a closed
	// feature is reported as such whatever the payload.
	if !isEnumWord(variantWord, cas20VariantMax) {
		return nil, vm.ErrExecutionReverted
	}
	variant := variantWord[31]
	feature, ok := variantFeature(variant)
	if !ok {
		return nil, revCAS20("InvalidVariant()", errSelInvalidVariant)
	}
	if err := ensureFeatureActivated(ctx, feature); err != nil {
		return nil, err
	}
	create, err := decodeCreateParams(variant, params)
	if err != nil {
		return nil, err
	}
	if variant == cas20VariantStablecoin {
		if err := validateCurrency(create.currency); err != nil {
			return nil, err
		}
	}
	creator := ctx.Caller
	if !ctx.chargeKeccak(64) {
		return nil, vm.ErrOutOfGas
	}
	addr := cas20DeriveAddress(variant, creator, salt)

	if cas20AddressOccupied(ctx, addr) {
		return nil, revCAS20("TokenAlreadyExists(address)", errSelTokenExists, addrKey(addr))
	}
	if !ctx.chargeCodeWrite(addr, CAS20MarkerCode) {
		return nil, vm.ErrOutOfGas
	}
	ctx.StateDB.SetCode(addr, CAS20MarkerCode, tracing.CodeChangeContractCreation)

	decimals := create.decimals
	tokenCtx := ctx.spawnBootstrap(addr, CAS20FactoryAddress)
	tok := newCAS20TokenBootstrap(tokenCtx, decimals)

	if !tok.s.setName(create.name) || !tok.s.setSymbol(create.symbol) {
		return nil, vm.ErrOutOfGas
	}
	tok.s.setSupplyCap(cas20NoSupplyCap)
	if variant == cas20VariantAsset {
		initAssetExtension(tokenCtx, create.decimals)
	} else if !newStablecoinExt(tokenCtx).setCurrency(create.currency) {
		return nil, vm.ErrOutOfGas
	}
	if !ctx.AddLog(
		[]common.Hash{cas20TopicCAS20Created, addrKey(addr), wU8(variant)},
		encodeCAS20CreatedData(create),
	) {
		return nil, vm.ErrOutOfGas
	}
	initialAdmin := create.initialAdmin
	if initialAdmin != (common.Address{}) {
		tok.s.setRole(roleDefaultAdmin, initialAdmin, true)
		tok.s.setAdminCount(uint256.NewInt(1))
		if !tokenCtx.AddLog([]common.Hash{cas20TopicRoleGranted, roleDefaultAdmin, addrKey(initialAdmin), addrKey(CAS20FactoryAddress)}, nil) {
			return nil, vm.ErrOutOfGas
		}
	}

	// The variant's full dispatcher, not the shared half: an Asset token has to be
	// able to set its multiplier at creation.
	dispatch := func(call []byte) ([]byte, error) { return stablecoinDispatch(tok, newStablecoinExt(tokenCtx), call) }
	if variant == cas20VariantAsset {
		dispatch = func(call []byte) ([]byte, error) { return assetDispatch(tok, newAssetExt(tokenCtx), call) }
	}

	for i, call := range initCalls {
		if !ctx.chargeInternalDispatch(call) {
			return nil, vm.ErrOutOfGas
		}
		if len(call) < 4 {
			return nil, revCAS20Bytes("InternalCallMalformed(bytes)", errSelInternalMalformed, call)
		}
		if _, err := dispatch(call); err != nil {
			if !errors.Is(err, vm.ErrExecutionReverted) {
				return nil, err
			}
			return nil, revCAS20("InitCallFailed(uint256)", errSelInitCallFailed, wU64(uint64(i)))
		}
	}
	if ctx.OutOfGas() {
		return nil, vm.ErrOutOfGas
	}
	return hashBytes(addrKey(addr)), nil
}

type cas20CreateParams struct {
	variant      byte
	name         string
	symbol       string
	initialAdmin common.Address
	decimals     byte
	currency     string // Stablecoin only
}

func decodeCreateParams(variant byte, params []byte) (cas20CreateParams, error) {
	out := cas20CreateParams{variant: variant}

	// The single-struct offset word; see abiEncodeStruct.
	off, ok := wordU64(params, 0)
	if !ok || off > uint64(len(params)) {
		return out, vm.ErrExecutionReverted
	}
	body := params[off:]

	version, err := readStrictUint8(body, 0)
	if err != nil {
		return out, err
	}
	if version != cas20ParamsVersion {
		return out, revCAS20("UnsupportedVersion(uint8,uint8)", errSelUnsupportedVersion,
			wU8(version), wU8(variant))
	}
	if out.name, err = readStringArg(body, 1); err != nil {
		return out, err
	}
	if out.symbol, err = readStringArg(body, 2); err != nil {
		return out, err
	}
	if out.initialAdmin, err = readAddress(body, 3); err != nil {
		return out, err
	}

	if variant == cas20VariantAsset {
		if out.decimals, err = readStrictUint8(body, 4); err != nil {
			return out, err
		}
		if out.decimals < cas20MinDecimals || out.decimals > cas20MaxDecimals {
			return out, revCAS20("InvalidDecimals(uint8)", errSelInvalidDecimals, wU8(out.decimals))
		}
		return out, nil
	}

	// Stablecoin decimals are fixed and not carried on the wire.
	out.decimals = 6
	if out.currency, err = readStringArg(body, 4); err != nil {
		return out, err
	}
	return out, nil
}

func validateCurrency(code string) error {
	if code == "" {
		return revCAS20Bytes("MissingRequiredField(string)", errSelMissingField, []byte("currency"))
	}
	for i := 0; i < len(code); i++ {
		if code[i] < 'A' || code[i] > 'Z' {
			return revCAS20Bytes("InvalidCurrency(string)", errSelInvalidCurrency, []byte(code))
		}
	}
	return nil
}

func readStrictUint8(args []byte, i int) (byte, error) {
	w, err := readWord(args, i)
	if err != nil {
		return 0, err
	}
	if !isEnumWord(w, 0xff) {
		return 0, vm.ErrExecutionReverted
	}
	return w[31], nil
}

func encodeCAS20CreatedData(c cas20CreateParams) []byte {
	var variantParams []byte
	if c.variant == cas20VariantStablecoin {
		variantParams = abiEncodeStruct(
			abiWord(wU8(cas20ParamsVersion)),
			abiString(c.currency),
		)
	}
	return encodeTuple(
		abiString(c.name),
		abiString(c.symbol),
		abiWord(wU8(c.decimals)),
		abiBytes(variantParams),
	)
}
