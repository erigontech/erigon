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

package shutter

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/holiman/uint256"
	blst "github.com/supranational/blst/bindings/go"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/rpc/contracts"
	shuttercontracts "github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
)

func NewValidatorRegistryChecker(
	logger log.Logger,
	cb contracts.Backend,
	registryAddr common.Address,
	chainId *uint256.Int,
) ValidatorRegistryChecker {
	registry, err := shuttercontracts.NewValidatorRegistry(registryAddr, cb)
	if err != nil {
		panic(fmt.Errorf("could not create validator registry: %w", err))
	}
	return ValidatorRegistryChecker{
		logger:       logger,
		chainId:      chainId.Uint64(),
		registry:     registry,
		registryAddr: registryAddr,
	}
}

type ValidatorRegistryChecker struct {
	logger       log.Logger
	chainId      uint64
	registry     *shuttercontracts.ValidatorRegistry
	registryAddr common.Address
}

func (c ValidatorRegistryChecker) FilterRegistered(ctx context.Context, validators ValidatorInfo) (ValidatorInfo, error) {
	callOpts := bind.CallOpts{Context: ctx}
	numUpdates, err := c.registry.GetNumUpdates(&callOpts)
	if err != nil {
		return nil, err
	}

	registered := make(ValidatorInfo, len(validators))
	nonces := make(map[int64]uint32, len(validators))
	numUpdatesU64 := numUpdates.Uint64()
	bigI := new(big.Int)
	for i := uint64(0); i < numUpdatesU64; i++ {
		update, err := c.registry.GetUpdate(&callOpts, bigI.SetUint64(i))
		if err != nil {
			return nil, err
		}

		var msg AggregateRegistrationMessage
		err = msg.Unmarshal(update.Message)
		if err != nil {
			c.logger.Warn("ignoring registration message due to unmarshalling issue", "updateIndex", i, "err", err)
			continue
		}

		err = checkStaticRegistrationMsgFields(&msg, c.chainId, c.registryAddr)
		if err != nil {
			c.logger.Warn("ignoring registration message due to static fields check issue", "updateIndex", i, "err", err)
			continue
		}

		validatorPubKey, ok := validators[ValidatorIndex(msg.ValidatorIndex)]
		if !ok {
			c.logger.Warn(
				"ignoring registration message since it is not for a validator of interest",
				"updateIndex", i,
				"validatorIndex", msg.ValidatorIndex,
				"err", err,
			)
			continue
		}

		err = checkNonces(&msg, nonces)
		if err != nil {
			c.logger.Warn("ignoring registration message due to nonce check issue", "updateIndex", i, "err", err)
			continue
		}

		err = verifyRegistrationSignature(&msg, update.Signature, validators)
		if err != nil {
			c.logger.Warn("ignoring registration message due to signature verification issue", "updateIndex", i, "err", err)
			continue
		}

		if msg.IsRegistration {
			for _, validatorIndex := range msg.ValidatorIndices() {
				registered[ValidatorIndex(validatorIndex)] = validatorPubKey
				nonces[validatorIndex] = msg.Nonce
			}
		} else {
			for _, validatorIndex := range msg.ValidatorIndices() {
				delete(registered, ValidatorIndex(validatorIndex))
				nonces[validatorIndex] = msg.Nonce
			}
		}
	}

	return registered, nil
}

func checkStaticRegistrationMsgFields(msg *AggregateRegistrationMessage, chainId uint64, registry common.Address) error {
	if msg.Version != AggregateValidatorRegistrationMessageVersion &&
		msg.Version != LegacyValidatorRegistrationMessageVersion {
		return fmt.Errorf("invalid version %d", msg.Version)
	}

	if msg.ChainId != chainId {
		return fmt.Errorf("invalid chain id %d", msg.ChainId)
	}

	if msg.ValidatorRegistryAddress != registry {
		return fmt.Errorf("invalid validator registry address %s", msg.ValidatorRegistryAddress)
	}

	if msg.ValidatorIndex > math.MaxInt64 {
		return fmt.Errorf("invalid validator index %d", msg.ValidatorIndex)
	}

	return nil
}

func checkNonces(msg *AggregateRegistrationMessage, nonces map[int64]uint32) error {
	for _, validatorIdx := range msg.ValidatorIndices() {
		latestNonce := nonces[validatorIdx]
		if msg.Nonce <= latestNonce {
			return fmt.Errorf("nonce %d is lte latest nonce %d for validator %d", msg.Nonce, latestNonce, validatorIdx)
		}
	}
	return nil
}

var dst = []byte("BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_")

func verifyRegistrationSignature(msg *AggregateRegistrationMessage, sig []byte, validators ValidatorInfo) error {
	signature := new(blst.P2Affine).Uncompress(sig)
	if signature == nil {
		return errors.New("could not uncompress signature")
	}

	var pubKeys []*blst.P1Affine
	for _, validatorIdx := range msg.ValidatorIndices() {
		pubKey, ok := validators[ValidatorIndex(validatorIdx)]
		if !ok {
			return fmt.Errorf("could not find validator public key for index %d", validatorIdx)
		}

		pubKeyBytes, err := hex.DecodeString(string(pubKey))
		if err != nil {
			return fmt.Errorf("could not hex decode validator public key: %w", err)
		}

		pk := new(blst.P1Affine).Uncompress(pubKeyBytes)
		if pk == nil {
			return fmt.Errorf("could not uncompress validator public key")
		}

		pubKeys = append(pubKeys, pk)
	}

	var valid bool
	if msg.Version == AggregateValidatorRegistrationMessageVersion {
		valid = verifyAggregateRegistrationSignature(signature, pubKeys, msg)
	} else {
		valid = verifyLegacyRegistrationSignature(signature, pubKeys[0], &LegacyRegistrationMessage{
			Version:                  msg.Version,
			ChainId:                  msg.ChainId,
			ValidatorRegistryAddress: msg.ValidatorRegistryAddress,
			ValidatorIndex:           msg.ValidatorIndex,
			Nonce:                    uint64(msg.Nonce),
			IsRegistration:           msg.IsRegistration,
		})
	}
	if !valid {
		return fmt.Errorf("signature verification failed")
	}
	return nil
}

func verifyAggregateRegistrationSignature(sig *blst.P2Affine, pks []*blst.P1Affine, msg *AggregateRegistrationMessage) bool {
	if msg.Version < AggregateValidatorRegistrationMessageVersion {
		return false
	}
	if len(pks) != int(msg.Count) {
		return false
	}
	msgHash := crypto.Keccak256(msg.Marshal())
	msgs := make([][]byte, len(pks))
	for i := range pks {
		msgs[i] = msgHash
	}
	return sig.AggregateVerify(true, pks, true, msgs, dst)
}

func verifyLegacyRegistrationSignature(sig *blst.P2Affine, pubkey *blst.P1Affine, msg *LegacyRegistrationMessage) bool {
	msgHash := crypto.Keccak256(msg.Marshal())
	return sig.Verify(true, pubkey, true, msgHash, dst)
}

type (
	ValidatorIndex  int64
	ValidatorPubKey string
	ValidatorInfo   map[ValidatorIndex]ValidatorPubKey
)
