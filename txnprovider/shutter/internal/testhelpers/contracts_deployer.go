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

// nonceAtProvider allows reading committed (not pending) account nonce.
type nonceAtProvider interface {
	NonceAt(ctx context.Context, account common.Address, blockNumber *big.Int) (uint64, error)
}

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

// waitForNonceAt waits for the committed nonce (NonceAt with latest block) to reach
// at least expectedNonce. This ensures the state trie is fully committed after
// VerifyTxnsInclusion, since fcu persistence is asynchronous.
func (d ContractsDeployer) waitForNonceAt(ctx context.Context, expectedNonce uint64) error {
	provider, ok := d.contractBackend.(nonceAtProvider)
	if !ok {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	bo := backoff.WithContext(backoff.NewConstantBackOff(50*time.Millisecond), ctx)
	return backoff.Retry(func() error {
		committed, err := provider.NonceAt(ctx, d.address, nil)
		if err != nil {
			return backoff.Permanent(err)
		}
		if committed < expectedNonce {
			return fmt.Errorf("committed nonce %d < expected %d", committed, expectedNonce)
		}
		return nil
	}, bo)
}

func (d ContractsDeployer) DeployCore(ctx context.Context) (_ ContractsDeployment, err error) {
	defer func() {
		if err != nil {
			fmt.Println("DEPLOY ERR", err, dbg.Stack())
		}
	}()
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

	// Submit the Initialize txn to the txpool before building the block so it
	// gets included in the same block as the deploys. This avoids a cross-block
	// state dependency that is susceptible to the async ForkChoiceUpdated state
	// flush race: the block builder may open its write transaction before the
	// previous block's state is fully committed.
	// Set GasLimit to bypass PendingCodeAt/EstimateGas checks — the contract
	// code is not yet available at the pending state level (deploy is still
	// in the txpool), but will exist once the block builder executes the
	// deploy transaction earlier in the same block.
	transactOpts.GasLimit = 1_000_000
	ksmInitTxn, err := ksm.Initialize(transactOpts, d.address, d.address)
	transactOpts.GasLimit = 0 // reset for subsequent calls
	if err != nil {
		return ContractsDeployment{}, err
	}

	block, err := d.cl.BuildBlock(ctx)
	if err != nil {
		return ContractsDeployment{}, err
	}

	err = d.txnInclusionVerifier.VerifyTxnsInclusion(ctx, block, sequencerDeployTxn.Hash(), ksmDeployTxn.Hash(), keyBroadcastDeployTxn.Hash(), ksmInitTxn.Hash())
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
	transactOpts, err := bind.NewKeyedTransactorWithChainID(d.key, d.chainId)
	if err != nil {
		return common.Address{}, nil, err
	}

	// DeployCore ended with VerifyTxnsInclusion — wait for committed state (4 txns: 3 deploys + 1 init)
	if err = d.waitForNonceAt(ctx, 4); err != nil {
		return common.Address{}, nil, err
	}

	keyperSetAddr, keyperSetDeployTxn, keyperSet, err := shuttercontracts.DeployKeyperSet(transactOpts, d.contractBackend)
	if err != nil {
		return common.Address{}, nil, err
	}

	// Submit all KeyperSet configuration txns before building the block so
	// they get included in the same block as the deploy. This avoids cross-
	// block state dependencies susceptible to the async state flush race.
	// GasLimit is set to bypass PendingCodeAt/EstimateGas — the KeyperSet
	// contract is in the txpool but not yet committed.
	transactOpts.GasLimit = 1_000_000

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

	// AddKeyperSet and BroadcastEonKey also go in the same block — they
	// depend on SetFinalized having run and the KSM/KeyBroadcast contracts
	// from DeployCore being accessible.
	ksm, err := shuttercontracts.NewKeyperSetManager(dep.KsmAddr, d.contractBackend)
	if err != nil {
		return common.Address{}, nil, err
	}

	addKeyperSetTxn, err := ksm.AddKeyperSet(transactOpts, ekg.ActivationBlock, keyperSetAddr)
	if err != nil {
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

	transactOpts.GasLimit = 0 // reset for any future calls

	block, err := d.cl.BuildBlock(ctx)
	if err != nil {
		return common.Address{}, nil, err
	}

	err = d.txnInclusionVerifier.VerifyTxnsInclusion(ctx, block,
		keyperSetDeployTxn.Hash(),
		setPublisherTxn.Hash(),
		setThresholdTxn.Hash(),
		addMembersTxn.Hash(),
		setFinalizedTxn.Hash(),
		addKeyperSetTxn.Hash(),
		broadcastKeyTxn.Hash(),
	)
	if err != nil {
		return common.Address{}, nil, err
	}

	// Build 3 empty blocks to maintain the expected block count (4 total).
	// Callers compute keyper set activation blocks based on this count.
	for range 3 {
		if _, err = d.cl.BuildBlock(ctx); err != nil {
			return common.Address{}, nil, err
		}
	}

	return keyperSetAddr, keyperSet, nil
}

type ContractsDeployment struct {
	SequencerAddr    common.Address
	KsmAddr          common.Address
	KeyBroadcastAddr common.Address
}
