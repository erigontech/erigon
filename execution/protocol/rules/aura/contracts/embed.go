// Copyright 2024 The Erigon Authors
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

package contracts

import (
	"bytes"
	_ "embed"

	"github.com/erigontech/erigon/execution/abi"
)

//go:embed block_reward.json
var BlockReward []byte
var BlockRewardABI = mustABI(BlockReward)

//go:embed certifier.json
var Certifier []byte
var CertifierABI = mustABI(Certifier)

//go:embed registrar.json
var Registrar []byte
var RegistrarABI = mustABI(Registrar)

//go:embed withdrawal.json
var Withdrawal []byte
var WithdrawalABI = mustABI(Withdrawal)

//go:embed block_gas_limit.json
var BlockGasLimit []byte
var BlockGasLimitABI = mustABI(BlockGasLimit)

func mustABI(in []byte) abi.ABI {
	a, err := abi.JSON(bytes.NewReader(in))
	if err != nil {
		panic(err)
	}
	return a
}
