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
