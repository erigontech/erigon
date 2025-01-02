package shutter

import (
	"context"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"github.com/erigontech/erigon-lib/event"
	"github.com/erigontech/erigon-lib/log/v3"
)

const DecryptionKeysTopic = "decryptionKeys"

type DecryptionKeysListener struct {
	logger    log.Logger
	observers *event.Observers[*pubsub.Message]
}

func NewDecryptionKeysListener(logger log.Logger) DecryptionKeysListener {
	return DecryptionKeysListener{
		logger:    logger,
		observers: event.NewObservers[*pubsub.Message](),
	}
}

func (dkl DecryptionKeysListener) Register(observer event.Observer[*pubsub.Message]) event.UnregisterFunc {
	return dkl.observers.Register(observer)
}

func (dkl DecryptionKeysListener) Run(ctx context.Context) error {
	dkl.logger.Info("running decryption keys listener")

	//
	// TODO set options
	//
	host, err := libp2p.New()
	if err != nil {
		return err
	}

	pubSub, err := pubsub.NewGossipSub(ctx, host)
	if err != nil {
		return err
	}

	topic, err := pubSub.Join(DecryptionKeysTopic)
	if err != nil {
		return err
	}
	defer func() {
		if err := topic.Close(); err != nil {
			dkl.logger.Error("failed to close decryption keys topic", "err", err)
		}
	}()

	sub, err := topic.Subscribe()
	if err != nil {
		return err
	}
	defer sub.Cancel()

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return err
		}

		dkl.observers.Notify(msg)
	}
}
