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

//go:build !abigen

package shutter

import (
	"context"
	"fmt"
	"math/big"

	lru "github.com/hashicorp/golang-lru/v2"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/accounts/abi/bind"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
)

type EncryptedTxnsPool struct {
	config            Config
	sequencerContract *contracts.Sequencer
	submissions       *lru.Cache[TxnIndex, EncryptedTxnSubmission]
}

func NewEncryptedTxnsPool(config Config, contractBackend bind.ContractBackend) EncryptedTxnsPool {
	sequencerContractAddress := libcommon.HexToAddress(config.SequencerContractAddress)
	sequencerContract, err := contracts.NewSequencer(sequencerContractAddress, contractBackend)
	if err != nil {
		panic(fmt.Errorf("failed to create shutter sequencer contract: %w", err))
	}

	submissions, err := lru.New[TxnIndex, EncryptedTxnSubmission](int(config.MaxPooledEncryptedTxns))
	if err != nil {
		panic(fmt.Errorf("failed to create shutter submissions LRU cache: %w", err))
	}

	return EncryptedTxnsPool{
		config:            config,
		sequencerContract: sequencerContract,
		submissions:       submissions,
	}
}

func (utp EncryptedTxnsPool) Run(ctx context.Context) error {
	submissionEventC := make(chan *contracts.SequencerTransactionSubmitted)
	submissionEventSub, err := utp.sequencerContract.WatchTransactionSubmitted(&bind.WatchOpts{}, submissionEventC)
	if err != nil {
		return fmt.Errorf("failed to subscribe to sequencer TransactionSubmitted event: %w", err)
	}

	defer submissionEventSub.Unsubscribe()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event := <-submissionEventC:
			encryptedTxnSubmission := EncryptedTxnSubmission{
				Eon:                  event.Eon,
				TxnIndex:             TxnIndex(event.TxIndex),
				IdentityPrefix:       event.IdentityPrefix,
				Sender:               event.Sender,
				EncryptedTransaction: event.EncryptedTransaction,
				GasLimit:             event.GasLimit,
			}

			utp.submissions.Add(encryptedTxnSubmission.TxnIndex, encryptedTxnSubmission)
		}
	}
}

type TxnIndex uint64

// EncryptedTxnSubmission mimics contracts.SequencerTransactionSubmitted but without the "Raw" attribute to save memory.
type EncryptedTxnSubmission struct {
	Eon                  uint64
	TxnIndex             TxnIndex
	IdentityPrefix       [32]byte
	Sender               libcommon.Address
	EncryptedTransaction []byte
	GasLimit             *big.Int
}
