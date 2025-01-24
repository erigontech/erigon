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
