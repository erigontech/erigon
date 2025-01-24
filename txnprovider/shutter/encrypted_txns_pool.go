package shutter

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	lru "github.com/hashicorp/golang-lru/v2"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/accounts/abi"
	"github.com/erigontech/erigon/accounts/abi/bind"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
)

type EncryptedTxnsPool struct {
	config              Config
	contractBacked      bind.ContractBackend
	sequencerAbi        abi.ABI
	submissionEventId   libcommon.Hash
	submissionEventName string
	submissions         *lru.Cache[TxnIndex, EncryptedTxnSubmission]
}

func NewEncryptedTxnsPool(config Config, contractBackend bind.ContractBackend) EncryptedTxnsPool {
	sequencerAbi, err := abi.JSON(strings.NewReader(contracts.SequencerABI))
	if err != nil {
		panic(fmt.Errorf("failed to parse shutter Sequencer ABI: %w", err))
	}

	submissions, err := lru.New[TxnIndex, EncryptedTxnSubmission](int(config.MaxPooledEncryptedTxns))
	if err != nil {
		panic(fmt.Errorf("failed to create shutter submissions LRU cache: %w", err))
	}

	const submissionEventName = "TransactionSubmitted"
	return EncryptedTxnsPool{
		config:              config,
		contractBacked:      contractBackend,
		sequencerAbi:        sequencerAbi,
		submissionEventId:   sequencerAbi.Events[submissionEventName].ID,
		submissionEventName: submissionEventName,
		submissions:         submissions,
	}
}

func (utp EncryptedTxnsPool) Run(ctx context.Context) error {
	sequencerContractAddress := libcommon.HexToAddress(utp.config.SequencerContractAddress)
	sequencer, err := contracts.NewSequencer(sequencerContractAddress, utp.contractBacked)
	if err != nil {
		return fmt.Errorf("failed to create shutter sequencer contract: %w", err)
	}

	submissionEventC := make(chan *contracts.SequencerTransactionSubmitted)
	submissionEventSub, err := sequencer.WatchTransactionSubmitted(&bind.WatchOpts{}, submissionEventC)
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
