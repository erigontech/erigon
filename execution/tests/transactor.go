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

package executiontests

import (
	"crypto/ecdsa"
	"fmt"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/requests"
)

type Transactor struct {
	rpcApiClient requests.RequestGenerator
	chainId      *big.Int
}

func NewTransactor(rpcApiClient requests.RequestGenerator, chainId *big.Int) Transactor {
	return Transactor{
		rpcApiClient: rpcApiClient,
		chainId:      chainId,
	}
}

func (t Transactor) SubmitSimpleTransfer(from *ecdsa.PrivateKey, to common.Address, amount *big.Int) (types.Transaction, error) {
	signedTxn, err := t.CreateSimpleTransfer(from, to, amount)
	if err != nil {
		return nil, fmt.Errorf("failed to create a simple transfer: %w", err)
	}

	_, err = t.rpcApiClient.SendTransaction(signedTxn)
	if err != nil {
		return nil, fmt.Errorf("failed to send a transaction: %w", err)
	}

	return signedTxn, nil
}

func (t Transactor) CreateSimpleTransfer(
	from *ecdsa.PrivateKey,
	to common.Address,
	amount *big.Int,
) (types.Transaction, error) {
	amountU256, _ := uint256.FromBig(amount)
	fromAddr := crypto.PubkeyToAddress(from.PublicKey)
	txnCount, err := t.rpcApiClient.GetTransactionCount(fromAddr, rpc.PendingBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction count: %w", err)
	}

	gasPrice, err := t.rpcApiClient.GasPrice()
	if err != nil {
		return nil, fmt.Errorf("failed to get gas price: %w", err)
	}

	gasPriceU256, _ := uint256.FromBig(gasPrice)
	nonce := txnCount.Uint64()
	txn := &types.LegacyTx{
		CommonTx: types.CommonTx{
			Nonce:    nonce,
			GasLimit: 21_000,
			To:       &to,
			Value:    amountU256,
		},
		GasPrice: gasPriceU256,
	}

	signer := types.LatestSignerForChainID(t.chainId)
	signedTxn, err := types.SignTx(txn, *signer, from)
	if err != nil {
		return nil, fmt.Errorf("failed to sign a transaction: %w", err)
	}

	return signedTxn, nil
}

func (t Transactor) RpcClient() requests.RequestGenerator {
	return t.rpcApiClient
}

func (t Transactor) ChainId() *big.Int {
	return new(big.Int).Set(t.chainId)
}
