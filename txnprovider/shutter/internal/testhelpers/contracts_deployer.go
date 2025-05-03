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
	"math/big"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/execution/abi/bind"
	shuttercontracts "github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
)

type ContractsDeployer struct {
	key                  *ecdsa.PrivateKey
	address              common.Address
	contractBackend      bind.ContractBackend
	cl                   *MockCl
	chainId              *big.Int
	txnInclusionVerifier TxnInclusionVerifier
}

func NewContractsDeployer(
	key *ecdsa.PrivateKey,
	cb bind.ContractBackend,
	cl *MockCl,
	chainId *big.Int,
	txnInclusionVerifier TxnInclusionVerifier,
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

func (d ContractsDeployer) DeployCore(ctx context.Context) (ContractsDeployment, error) {
	transactOpts, err := bind.NewKeyedTransactorWithChainID(d.key, d.chainId)
	if err != nil {
		return ContractsDeployment{}, err
	}

	sequencerAddr, sequencerDeployTxn, sequencer, err := shuttercontracts.DeploySequencer(
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

	keyBroadcastAddr, keyBroadcastDeployTxn, keyBroadcast, err := shuttercontracts.DeployKeyBroadcastContract(
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
		Sequencer:        sequencer,
		SequencerAddr:    sequencerAddr,
		Ksm:              ksm,
		KsmAddr:          ksmAddr,
		KeyBroadcast:     keyBroadcast,
		KeyBroadcastAddr: keyBroadcastAddr,
	}

	return res, nil
}

func (d ContractsDeployer) DeployKeyperSet(
	ctx context.Context,
	dep ContractsDeployment,
	ekg EonKeyGeneration,
) (common.Address, *shuttercontracts.KeyperSet, error) {
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

	addKeyperSetTxn, err := dep.Ksm.AddKeyperSet(transactOpts, ekg.ActivationBlock, keyperSetAddr)
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

	broadcastKeyTxn, err := dep.KeyBroadcast.BroadcastEonKey(transactOpts, uint64(ekg.EonIndex), ekg.EonPublicKey.Marshal())
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
	Sequencer        *shuttercontracts.Sequencer
	SequencerAddr    common.Address
	Ksm              *shuttercontracts.KeyperSetManager
	KsmAddr          common.Address
	KeyBroadcast     *shuttercontracts.KeyBroadcastContract
	KeyBroadcastAddr common.Address
}
