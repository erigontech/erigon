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
	"fmt"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

const (
	cas20VariantAsset      = 0x00
	cas20VariantStablecoin = 0x01
	cas20VariantMax        = cas20VariantStablecoin
)

type cas20Precompile interface {
	vm.PrecompiledContract
	RunStateful(ctx *PrecompileContext, input []byte) ([]byte, error)
}

// Resolve routes the CAS20 addresses to their precompiles; Enabled gates it.
func Resolve(addr accounts.Address) (vm.StatefulPrecompile, bool) {
	p, ok := resolveCAS20(addr.Value())
	if !ok {
		return nil, false
	}
	return vmPrecompiles[p], true
}

// Enabled gates CAS20 on Jenner, and on BSC, so that a non-BSC config with a
// Jenner time does not route the reserved address space.
func Enabled(rules *chain.Rules) bool { return rules.IsParlia && rules.IsJenner }

var vmPrecompiles = map[cas20Precompile]vm.StatefulPrecompile{
	cas20Factory:    vmPrecompile{cas20Factory},
	cas20Policy:     vmPrecompile{cas20Policy},
	cas20Activation: vmPrecompile{cas20Activation},
	cas20Asset:      vmPrecompile{cas20Asset},
	cas20Stablecoin: vmPrecompile{cas20Stablecoin},
}

type vmPrecompile struct{ p cas20Precompile }

func (w vmPrecompile) Name() string                     { return w.p.Name() }
func (w vmPrecompile) RequiredGas(input []byte) uint64  { return w.p.RequiredGas(input) }
func (w vmPrecompile) Run(input []byte) ([]byte, error) { return w.p.Run(input) }

func (w vmPrecompile) RunStateful(env *vm.PrecompileEnv, input []byte) ([]byte, error) {
	ctx := newPrecompileContext(env)
	ret, err := w.p.RunStateful(ctx, input)
	if stateErr := ctx.StateDB.Err(); stateErr != nil {
		return nil, fmt.Errorf("%w: %w", vm.ErrIntraBlockStateFailed, stateErr)
	}
	return ret, err
}

func resolveCAS20Token(addr common.Address) (cas20Precompile, bool) {
	v, ok := cas20Variants[addr[10]]
	return v.precompile, ok
}

// One entry per ordinal, so routing and feature gating cannot disagree.
var cas20Variants = map[byte]struct {
	precompile cas20Precompile
	feature    common.Hash
}{
	cas20VariantAsset:      {cas20Asset, featureCAS20Asset},
	cas20VariantStablecoin: {cas20Stablecoin, featureCAS20Stablecoin},
}

// resolveCAS20 leaves fork gating to the caller.
func resolveCAS20(addr common.Address) (cas20Precompile, bool) {
	switch addr {
	case CAS20FactoryAddress:
		return cas20Factory, true
	case CAS20PolicyRegistryAddress:
		return cas20Policy, true
	case CAS20ActivationRegistryAddress:
		return cas20Activation, true
	}
	if IsCAS20Address(addr) {
		return resolveCAS20Token(addr)
	}
	return nil, false
}

var (
	cas20Factory    = &cas20FactoryPrecompile{}
	cas20Policy     = &cas20PolicyPrecompile{}
	cas20Activation = &cas20ActivationPrecompile{}
	cas20Asset      = &cas20AssetPrecompile{}
	cas20Stablecoin = &cas20StablecoinPrecompile{}
)

type cas20StatefulBase struct{}

func (cas20StatefulBase) Run([]byte) ([]byte, error) { return nil, ErrCAS20StatelessDispatch }

// RequiredGas : so all metering happens inside RunStateful.
func (cas20StatefulBase) RequiredGas([]byte) uint64 { return 0 }

type cas20FactoryPrecompile struct{ cas20StatefulBase }

func (p *cas20FactoryPrecompile) Name() string { return "CAS20Factory" }

func (p *cas20FactoryPrecompile) RunStateful(ctx *PrecompileContext, input []byte) ([]byte, error) {
	if err := cas20EnterCall(ctx, input); err != nil {
		return finishCAS20(nil, err)
	}
	ret, err := runCAS20Factory(ctx, input)
	return finishCAS20Metered(ctx, ret, err)
}

// Stateless: the token is ctx.Self, so one value serves every Asset address.
type cas20AssetPrecompile struct{ cas20StatefulBase }

func (p *cas20AssetPrecompile) Name() string { return "CAS20Asset" }

func (p *cas20AssetPrecompile) RunStateful(ctx *PrecompileContext, input []byte) ([]byte, error) {
	return runCAS20Token(ctx, input, bindAsset)
}

func bindAsset(ctx *PrecompileContext, input []byte) ([]byte, error) {
	return assetDispatch(newCAS20Token(ctx, 0), newAssetExt(ctx), input)
}

type cas20StablecoinPrecompile struct{ cas20StatefulBase }

func (p *cas20StablecoinPrecompile) Name() string { return "CAS20Stablecoin" }

func (p *cas20StablecoinPrecompile) RunStateful(ctx *PrecompileContext, input []byte) ([]byte, error) {
	return runCAS20Token(ctx, input, bindStablecoin)
}

func bindStablecoin(ctx *PrecompileContext, input []byte) ([]byte, error) {
	return stablecoinDispatch(newCAS20Token(ctx, 6), newStablecoinExt(ctx), input)
}

func runCAS20Token(ctx *PrecompileContext, input []byte,
	bind func(*PrecompileContext, []byte) ([]byte, error),
) ([]byte, error) {
	if err := cas20EnterCall(ctx, input); err != nil {
		return finishCAS20(nil, err)
	}
	if !cas20InitializedMetered(ctx, ctx.Self) {
		return finishCAS20Metered(ctx, nil, vm.ErrExecutionReverted)
	}
	ret, err := bind(ctx, input)
	return finishCAS20Metered(ctx, ret, err)
}
