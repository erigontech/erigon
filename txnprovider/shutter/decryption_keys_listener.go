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
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/event"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
	"github.com/erigontech/erigon/txnprovider/shutter/shuttercfg"
)

type DecryptionKeysListener struct {
	logger    log.Logger
	config    shuttercfg.Config
	source    DecryptionKeysSource
	observers *event.Observers[*proto.DecryptionKeys]
}

func NewDecryptionKeysListener(logger log.Logger, config shuttercfg.Config, source DecryptionKeysSource) *DecryptionKeysListener {
	return &DecryptionKeysListener{
		logger:    logger,
		config:    config,
		source:    source,
		observers: event.NewObservers[*proto.DecryptionKeys](),
	}
}

func (dkl DecryptionKeysListener) RegisterObserver(observer event.Observer[*proto.DecryptionKeys]) event.UnregisterFunc {
	return dkl.observers.Register(observer)
}

func (dkl DecryptionKeysListener) Run(ctx context.Context) error {
	defer dkl.logger.Info("decryption keys listener stopped")
	dkl.logger.Info("running decryption keys listener")

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		err := dkl.source.Run(ctx)
		if err != nil {
			return fmt.Errorf("decryption keys source run failure: %w", err)
		}
		return nil
	})

	eg.Go(func() error {
		err := dkl.listenLoop(ctx)
		if err != nil {
			return fmt.Errorf("decryption keys listen loop failure: %w", err)
		}
		return nil
	})

	return eg.Wait()
}

func (dkl DecryptionKeysListener) listenLoop(ctx context.Context) error {
	sub, err := dkl.source.Subscribe(ctx)
	if err != nil {
		return err
	}

	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return err
		}

		decryptionKeys, err := proto.UnmarshallDecryptionKeys(msg.Data)
		if err != nil {
			dkl.logger.Debug("failed to unmarshal decryption keys, skipping message", "err", err)
			continue
		}

		dkl.observers.Notify(decryptionKeys)
	}
}
