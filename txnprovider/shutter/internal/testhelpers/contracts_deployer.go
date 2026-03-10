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
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/engineapi/engineapitester"
	shuttercontracts "github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
)

type ContractsDeployer struct {
	key                  *ecdsa.PrivateKey
	address              common.Address
	contractBackend      bind.ContractBackend
	cl                   *MockCl
	chainId              *big.Int
	txnInclusionVerifier engineapitester.TxnInclusionVerifier
}

func NewContractsDeployer(
	key *ecdsa.PrivateKey,
	cb bind.ContractBackend,
	cl *MockCl,
	chainId *big.Int,
	txnInclusionVerifier engineapitester.TxnInclusionVerifier,
) ContractsDeployer {
	return ContractsDeployer{
		key:                  key,
		address:              crypto.PubkeyToAddress(key.PublicKey),
		contractBackend:      cb,
		cl:                   cl,
		chainId:              chainId,
		txnInclusionVerifier: txnInclusionVerifier,
	}
}

// waitForPendingNonce polls PendingNonceAt until it reaches expectedNonce. This is
// necessary because ForkChoiceUpdated persistence is asynchronous: after a block is
// built and receipts become available, the txpool may not yet have updated its view
// of the account nonce. Submitting the next transaction before the nonce is committed
// can result in a stale nonce being used, causing the transaction to revert.
func (d ContractsDeployer) waitForPendingNonce(ctx context.Context, expectedNonce uint64) error {
	waitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return backoff.Retry(func() error {
		nonce, err := d.contractBackend.PendingNonceAt(waitCtx, d.address)
		if err != nil {
			return backoff.Permanent(err)
		}
		if nonce < expectedNonce {
			return fmt.Errorf("pending nonce %d not yet at expected %d, retrying", nonce, expectedNonce)
		}
		return nil
	}, backoff.WithContext(backoff.NewConstantBackOff(50*time.Millisecond), waitCtx))
}

func (d ContractsDeployer) DeployCore(ctx context.Context) (_ ContractsDeployment, err error) {
	defer func() {
		if err != nil {
			fmt.Println("DEPLOY ERR", err, dbg.Stack())
		}
	}()

	startNonce, err := d.contractBackend.PendingNonceAt(ctx, d.address)
	if err != nil {
		return ContractsDeployment{}, err
	}

	transactOpts, err := bind.NewKeyedTransactorWithChainID(d.key, d.chainId)
	if err != nil {
		return ContractsDeployment{}, err
	}

	sequencerAddr, sequencerDeployTxn, _, err := shuttercontracts.DeploySequencer(
		transactOpts,
		d.contractBackend,
	)
	if err != nil {
		return ContractsDeployment{}, err
	}

	ksmAddr, ksmDeployTxn, ksm, err := shuttercontracts.DeployKeyperSetManager(
		transactOpts,
		d.contractBackend,
		d.address,
	)
	if err != nil {
		return ContractsDeployment{}, err
	}

	keyBroadcastAddr, keyBroadcastDeployTxn, _, err := shuttercontracts.DeployKeyBroadcastContract(
		transactOpts,
		d.contractBackend,
		ksmAddr,
	)
	if err != nil {
		return ContractsDeployment{}, err
	}

	block, err := d.cl.BuildBlock(ctx)
	if err != nil {
		return ContractsDeployment{}, err
	}

	err = d.txnInclusionVerifier.VerifyTxnsInclusion(ctx, block, sequencerDeployTxn.Hash(), ksmDeployTxn.Hash(), keyBroadcastDeployTxn.Hash())
	if err != nil {
		return ContractsDeployment{}, err
	}

	// Wait for the 3 deploy txns to be reflected in the pending nonce before
	// submitting the next transaction. The txpool may briefly return a stale
	// nonce after ForkChoiceUpdated persistence, causing the Initialize call
	// to use nonce 0 instead of 3, which would revert.
	if err = d.waitForPendingNonce(ctx, startNonce+3); err != nil {
		return ContractsDeployment{}, err
	}

	ksmInitTxn, err := ksm.Initialize(transactOpts, d.address, d.address)
	if err != nil {
		return ContractsDeployment{}, err
	}

	block, err = d.cl.BuildBlock(ctx)
	if err != nil {
		return ContractsDeployment{}, err
	}

	err = d.txnInclusionVerifier.VerifyTxnsInclusion(ctx, block, ksmInitTxn.Hash())
	if err != nil {
		return ContractsDeployment{}, err
	}

	res := ContractsDeployment{
		SequencerAddr:    sequencerAddr,
		KsmAddr:          ksmAddr,
		KeyBroadcastAddr: keyBroadcastAddr,
	}

	return res, nil
}

func (d ContractsDeployer) DeployKeyperSet(
	ctx context.Context,
	dep ContractsDeployment,
	ekg EonKeyGeneration,
) (common.Address, *shuttercontracts.KeyperSet, error) {
	startNonce, err := d.contractBackend.PendingNonceAt(ctx, d.address)
	if err != nil {
		return common.Address{}, nil, err
	}

	transactOpts, err := bind.NewKeyedTransactorWithChainID(d.key, d.chainId)
	if err != nil {
		return common.Address{}, nil, err
	}

	keyperSetAddr, keyperSetDeployTxn, keyperSet, err := shuttercontracts.DeployKeyperSet(transactOpts, d.contractBackend)
	if err != nil {
		return common.Address{}, nil, err
	}

	block, err := d.cl.BuildBlock(ctx)
	if err != nil {
		return common.Address{}, nil, err
	}

	err = d.txnInclusionVerifier.VerifyTxnsInclusion(ctx, block, keyperSetDeployTxn.Hash())
	if err != nil {
		return common.Address{}, nil, err
	}

	if err = d.waitForPendingNonce(ctx, startNonce+1); err != nil {
		return common.Address{}, nil, err
	}

	setPublisherTxn, err := keyperSet.SetPublisher(transactOpts, d.address)
	if err != nil {
		return common.Address{}, nil, err
	}

	setThresholdTxn, err := keyperSet.SetThreshold(transactOpts, ekg.Threshold)
	if err != nil {
		return common.Address{}, nil, err
	}

	addMembersTxn, err := keyperSet.AddMembers(transactOpts, ekg.Members())
	if err != nil {
		return common.Address{}, nil, err
	}

	setFinalizedTxn, err := keyperSet.SetFinalized(transactOpts)
	if err != nil {
		return common.Address{}, nil, err
	}

	block, err = d.cl.BuildBlock(ctx)
	if err != nil {
		return common.Address{}, nil, err
	}

	err = d.txnInclusionVerifier.VerifyTxnsInclusion(ctx, block, setPublisherTxn.Hash(), setThresholdTxn.Hash(), addMembersTxn.Hash(), setFinalizedTxn.Hash())
	if err != nil {
		return common.Address{}, nil, err
	}

	if err = d.waitForPendingNonce(ctx, startNonce+5); err != nil {
		return common.Address{}, nil, err
	}

	ksm, err := shuttercontracts.NewKeyperSetManager(dep.KsmAddr, d.contractBackend)
	if err != nil {
		return common.Address{}, nil, err
	}

	addKeyperSetTxn, err := ksm.AddKeyperSet(transactOpts, ekg.ActivationBlock, keyperSetAddr)
	if err != nil {
		return common.Address{}, nil, err
	}

	block, err = d.cl.BuildBlock(ctx)
	if err != nil {
		return common.Address{}, nil, err
	}

	err = d.txnInclusionVerifier.VerifyTxnsInclusion(ctx, block, addKeyperSetTxn.Hash())
	if err != nil {
		return common.Address{}, nil, err
	}

	if err = d.waitForPendingNonce(ctx, startNonce+6); err != nil {
		return common.Address{}, nil, err
	}

	keyBroadcast, err := shuttercontracts.NewKeyBroadcastContract(dep.KeyBroadcastAddr, d.contractBackend)
	if err != nil {
		return common.Address{}, nil, err
	}

	broadcastKeyTxn, err := keyBroadcast.BroadcastEonKey(transactOpts, uint64(ekg.EonIndex), ekg.EonPublicKey.Marshal())
	if err != nil {
		return common.Address{}, nil, err
	}

	block, err = d.cl.BuildBlock(ctx)
	if err != nil {
		return common.Address{}, nil, err
	}

	err = d.txnInclusionVerifier.VerifyTxnsInclusion(ctx, block, broadcastKeyTxn.Hash())
	if err != nil {
		return common.Address{}, nil, err
	}

	return keyperSetAddr, keyperSet, nil
}

type ContractsDeployment struct {
	SequencerAddr    common.Address
	KsmAddr          common.Address
	KeyBroadcastAddr common.Address
}
