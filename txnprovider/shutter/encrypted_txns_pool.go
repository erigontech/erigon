package shutter

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	lru "github.com/hashicorp/golang-lru/v2"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	"github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/accounts/abi"
	"github.com/erigontech/erigon/turbo/shards"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/contracts"
)

type EncryptedTxnsPool struct {
	config              Config
	chainEvents         *shards.Events
	sequencerAbi        abi.ABI
	submissionEventId   libcommon.Hash
	submissionEventName string
	submissions         *lru.Cache[TxnIndex, EncryptedTxnSubmission]
}

func NewEncryptedTxnsPool(config Config, chainEvents *shards.Events) EncryptedTxnsPool {
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
		chainEvents:         chainEvents,
		sequencerAbi:        sequencerAbi,
		submissionEventId:   sequencerAbi.Events[submissionEventName].ID,
		submissionEventName: submissionEventName,
		submissions:         submissions,
	}
}

func (utp EncryptedTxnsPool) Run(ctx context.Context) error {
	logsSub, closeLogsSub := utp.chainEvents.AddLogsSubscription()
	defer closeLogsSub()

	//
	// TODO - switch to using contracts.Backend for logs
	//

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case logs := <-logsSub:
			err := utp.processLogs(logs)
			if err != nil {
				return err
			}
		}
	}
}

func (utp EncryptedTxnsPool) processLogs(logs []*remoteproto.SubscribeLogsReply) error {
	for _, l := range logs {
		address := libcommon.Address(gointerfaces.ConvertH160toAddress(l.Address))
		if address.String() != utp.config.SequencerContractAddress {
			continue
		}

		eventId := libcommon.Hash(gointerfaces.ConvertH256ToHash(l.Topics[0]))
		if eventId != utp.submissionEventId {
			continue
		}

		event := contracts.SequencerTransactionSubmitted{}
		err := utp.sequencerAbi.UnpackIntoInterface(&event, "TransactionSubmitted", l.Data)
		if err != nil {
			return fmt.Errorf("failed to unpack log data: %w", err)
		}

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

	return nil
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
