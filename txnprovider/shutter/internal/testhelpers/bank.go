// Copyright 2025 The Erigon Authors
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

package testhelpers

import (
	"crypto/ecdsa"
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/types"
)

type Bank struct {
	privKey        *ecdsa.PrivateKey
	initialBalance *big.Int
}

func NewBank(initialBalance *big.Int) Bank {
	privKey, err := crypto.GenerateKey()
	if err != nil {
		panic(err)
	}

	return Bank{
		privKey:        privKey,
		initialBalance: initialBalance,
	}
}

func (b Bank) Address() common.Address {
	return crypto.PubkeyToAddress(b.privKey.PublicKey)
}

func (b Bank) PrivKey() *ecdsa.PrivateKey {
	return b.privKey
}

func (b Bank) RegisterGenesisAlloc(genesis *types.Genesis) {
	genesis.Alloc[b.Address()] = types.GenesisAccount{Balance: b.initialBalance}
}
