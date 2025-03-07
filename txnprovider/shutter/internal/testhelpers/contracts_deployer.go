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

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/accounts/abi/bind"
	"github.com/erigontech/erigon/core/types"
	shuttercontracts "github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
)

type ContractsDeployer struct {
	key             *ecdsa.PrivateKey
	address         libcommon.Address
	contractBackend bind.ContractBackend
	cl              *MockCl
	chainId         *big.Int
}

func NewContractsDeployer(key *ecdsa.PrivateKey, cb bind.ContractBackend, cl *MockCl, chainId *big.Int) ContractsDeployer {
	return ContractsDeployer{
		key:             key,
		address:         crypto.PubkeyToAddress(key.PublicKey),
		contractBackend: cb,
		cl:              cl,
		chainId:         chainId,
	}
}

func (d ContractsDeployer) DeployCore(ctx context.Context) (ContractsDeployment, error) {
	transactOpts := d.transactOpts()
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

	err = VerifyTxnsInclusion(block, sequencerDeployTxn.Hash(), ksmDeployTxn.Hash(), keyBroadcastDeployTxn.Hash())
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

	err = VerifyTxnsInclusion(block, ksmInitTxn.Hash())
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
	ksm *shuttercontracts.KeyperSetManager,
	ekg EonKeyGeneration,
) (libcommon.Address, *shuttercontracts.KeyperSet, error) {
	transactOpts := d.transactOpts()
	keyperSetAddr, keyperSetDeployTxn, keyperSet, err := shuttercontracts.DeployKeyperSet(transactOpts, d.contractBackend)
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	block, err := d.cl.BuildBlock(ctx)
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	err = VerifyTxnsInclusion(block, keyperSetDeployTxn.Hash())
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	setPublisherTxn, err := keyperSet.SetPublisher(transactOpts, d.address)
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	setThresholdTxn, err := keyperSet.SetThreshold(transactOpts, ekg.Threshold)
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	addMembersTxn, err := keyperSet.AddMembers(transactOpts, ekg.Members())
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	setFinalizedTxn, err := keyperSet.SetFinalized(transactOpts)
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	block, err = d.cl.BuildBlock(ctx)
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	err = VerifyTxnsInclusion(block, setPublisherTxn.Hash(), setThresholdTxn.Hash(), addMembersTxn.Hash(), setFinalizedTxn.Hash())
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	addKeyperSetTxn, err := ksm.AddKeyperSet(transactOpts, ekg.ActivationBlock, keyperSetAddr)
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	block, err = d.cl.BuildBlock(ctx)
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	err = VerifyTxnsInclusion(block, addKeyperSetTxn.Hash())
	if err != nil {
		return libcommon.Address{}, nil, err
	}

	return keyperSetAddr, keyperSet, nil
}

func (d ContractsDeployer) transactOpts() *bind.TransactOpts {
	return &bind.TransactOpts{
		From: d.address,
		Signer: func(address libcommon.Address, txn types.Transaction) (types.Transaction, error) {
			return types.SignTx(txn, *types.LatestSignerForChainID(d.chainId), d.key)
		},
	}
}

type ContractsDeployment struct {
	Sequencer        *shuttercontracts.Sequencer
	SequencerAddr    libcommon.Address
	Ksm              *shuttercontracts.KeyperSetManager
	KsmAddr          libcommon.Address
	KeyBroadcast     *shuttercontracts.KeyBroadcastContract
	KeyBroadcastAddr libcommon.Address
}
