package shutter

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"

	"github.com/erigontech/erigon-lib/event"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/params"
)

const DecryptionKeysTopic = "decryptionKeys"

type DecryptionKeysListener struct {
	logger    log.Logger
	config    Config
	observers *event.Observers[*pubsub.Message]
}

func NewDecryptionKeysListener(logger log.Logger, config Config) DecryptionKeysListener {
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

	host, err := libp2p.New(
		//
		// TODO construct multiaddr from port number
		//
		libp2p.ListenAddrs(dkl.config.ListenAddrs...),
		libp2p.UserAgent(fmt.Sprintf("erigon/shutter/%s", params.VersionWithCommit(params.GitCommit))),
		libp2p.ProtocolVersion("/shutter/0.1.0"),
	)
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
