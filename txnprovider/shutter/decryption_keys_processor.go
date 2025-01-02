package shutter

import (
	"context"

	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"github.com/erigontech/erigon-lib/log/v3"
)

type DecryptionKeysProcessor struct {
	logger log.Logger
	queue  chan *pubsub.Message
}

func NewDecryptionKeysProcessor(logger log.Logger) DecryptionKeysProcessor {
	return DecryptionKeysProcessor{
		logger: logger,
		queue:  make(chan *pubsub.Message),
	}
}

func (dkp DecryptionKeysProcessor) Enqueue(msg *pubsub.Message) {
	dkp.queue <- msg
}

func (dkp DecryptionKeysProcessor) Run(ctx context.Context) error {
	dkp.logger.Info("running decryption keys processor")

	for {
		select {
		case _ = <-dkp.queue:
			dkp.logger.Debug("received decryption keys message")
		//
		// TODO process msg
		//
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
