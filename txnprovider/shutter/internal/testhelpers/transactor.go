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
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"math/big"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/requests"
	"github.com/erigontech/erigon/txnprovider/shutter"
	shuttercontracts "github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
	shuttercrypto "github.com/erigontech/erigon/txnprovider/shutter/internal/crypto"
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
	signedTxn, err := t.createSimpleTransfer(from, to, amount)
	if err != nil {
		return nil, err
	}

	_, err = t.rpcApiClient.SendTransaction(signedTxn)
	if err != nil {
		return nil, err
	}

	return signedTxn, nil
}

func (t Transactor) createSimpleTransfer(
	from *ecdsa.PrivateKey,
	to common.Address,
	amount *big.Int,
) (types.Transaction, error) {
	amountU256, _ := uint256.FromBig(amount)
	fromAddr := crypto.PubkeyToAddress(from.PublicKey)
	txnCount, err := t.rpcApiClient.GetTransactionCount(fromAddr, rpc.PendingBlock)
	if err != nil {
		return nil, err
	}

	gasPrice, err := t.rpcApiClient.GasPrice()
	if err != nil {
		return nil, err
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
	return types.SignTx(txn, *signer, from)
}

type EncryptedTransactor struct {
	Transactor
	encryptorPrivKey *ecdsa.PrivateKey
	sequencer        *shuttercontracts.Sequencer
}

func NewEncryptedTransactor(
	base Transactor,
	encryptorPrivKey *ecdsa.PrivateKey,
	sequencerAddr string,
	cb bind.ContractBackend,
) EncryptedTransactor {
	sequencer, err := shuttercontracts.NewSequencer(common.HexToAddress(sequencerAddr), cb)
	if err != nil {
		panic(err)
	}

	return EncryptedTransactor{
		Transactor:       base,
		encryptorPrivKey: encryptorPrivKey,
		sequencer:        sequencer,
	}
}

func (et EncryptedTransactor) SubmitEncryptedTransfer(
	ctx context.Context,
	from *ecdsa.PrivateKey,
	to common.Address,
	amount *big.Int,
	eon shutter.Eon,
) (EncryptedSubmission, error) {
	signedTxn, err := et.createSimpleTransfer(from, to, amount)
	if err != nil {
		return EncryptedSubmission{}, err
	}

	var signedTxnBuf bytes.Buffer
	err = signedTxn.MarshalBinary(&signedTxnBuf)
	if err != nil {
		return EncryptedSubmission{}, err
	}

	eonPublicKey, err := eon.PublicKey()
	if err != nil {
		return EncryptedSubmission{}, err
	}

	block, err := et.rpcApiClient.GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)
	if err != nil {
		return EncryptedSubmission{}, err
	}

	gasLimit := new(big.Int).SetUint64(signedTxn.GetGasLimit())
	opts, err := bind.NewKeyedTransactorWithChainID(et.encryptorPrivKey, et.chainId)
	if err != nil {
		return EncryptedSubmission{}, err
	}

	opts.Value = new(big.Int).Mul(block.BaseFee, gasLimit)
	sigma, err := shuttercrypto.RandomSigma(rand.Reader)
	if err != nil {
		return EncryptedSubmission{}, err
	}

	identityPrefix, err := shuttercrypto.RandomSigma(rand.Reader)
	if err != nil {
		return EncryptedSubmission{}, err
	}

	ip := shutter.IdentityPreimageFromSenderPrefix(identityPrefix, opts.From)
	epochId := shuttercrypto.ComputeEpochID(ip[:])
	encryptedTxn := shuttercrypto.Encrypt(signedTxnBuf.Bytes(), eonPublicKey, epochId, sigma)
	eonIndex := uint64(eon.Index)
	submissionTxn, err := et.sequencer.SubmitEncryptedTransaction(
		opts,
		eonIndex,
		identityPrefix,
		encryptedTxn.Marshal(),
		gasLimit,
	)
	if err != nil {
		return EncryptedSubmission{}, err
	}

	sub := EncryptedSubmission{
		OriginalTxn:      signedTxn,
		SubmissionTxn:    submissionTxn,
		EonIndex:         eon.Index,
		IdentityPreimage: ip,
		GasLimit:         gasLimit,
	}

	return sub, nil
}

type EncryptedSubmission struct {
	OriginalTxn      types.Transaction
	SubmissionTxn    types.Transaction
	EonIndex         shutter.EonIndex
	IdentityPreimage *shutter.IdentityPreimage
	GasLimit         *big.Int
}
