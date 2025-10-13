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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/abi/bind"
	executiontests "github.com/erigontech/erigon/execution/tests"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/txnprovider/shutter"
	shuttercontracts "github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
	shuttercrypto "github.com/erigontech/erigon/txnprovider/shutter/internal/crypto"
)

type EncryptedTransactor struct {
	base             executiontests.Transactor
	encryptorPrivKey *ecdsa.PrivateKey
	sequencer        *shuttercontracts.Sequencer
}

func NewEncryptedTransactor(
	base executiontests.Transactor,
	encryptorPrivKey *ecdsa.PrivateKey,
	sequencerAddr string,
	cb bind.ContractBackend,
) EncryptedTransactor {
	sequencer, err := shuttercontracts.NewSequencer(common.HexToAddress(sequencerAddr), cb)
	if err != nil {
		panic(err)
	}

	return EncryptedTransactor{
		base:             base,
		encryptorPrivKey: encryptorPrivKey,
		sequencer:        sequencer,
	}
}

func (et EncryptedTransactor) SubmitSimpleTransfer(from *ecdsa.PrivateKey, to common.Address, amount *big.Int) (types.Transaction, error) {
	return et.base.SubmitSimpleTransfer(from, to, amount)
}

func (et EncryptedTransactor) SubmitEncryptedTransfer(
	ctx context.Context,
	from *ecdsa.PrivateKey,
	to common.Address,
	amount *big.Int,
	eon shutter.Eon,
) (EncryptedSubmission, error) {
	signedTxn, err := et.base.CreateSimpleTransfer(from, to, amount)
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

	block, err := et.base.RpcClient().GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)
	if err != nil {
		return EncryptedSubmission{}, err
	}

	gasLimit := new(big.Int).SetUint64(signedTxn.GetGasLimit())
	opts, err := bind.NewKeyedTransactorWithChainID(et.encryptorPrivKey, et.base.ChainId())
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
		EncryptedTxn:     encryptedTxn,
		EonIndex:         eon.Index,
		IdentityPreimage: ip,
		GasLimit:         gasLimit,
	}

	return sub, nil
}

type EncryptedSubmission struct {
	OriginalTxn      types.Transaction
	SubmissionTxn    types.Transaction
	EncryptedTxn     *shuttercrypto.EncryptedMessage
	EonIndex         shutter.EonIndex
	IdentityPreimage *shutter.IdentityPreimage
	GasLimit         *big.Int
}
