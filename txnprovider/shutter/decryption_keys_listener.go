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

	"github.com/erigontech/erigon-lib/event"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/txnprovider/shutter/proto"
)

type DecryptionKeysListener struct {
	logger    log.Logger
	streamer  DecryptionKeysStreamer
	observers *event.Observers[*proto.DecryptionKeys]
}

func NewDecryptionKeysListener(logger log.Logger, streamer DecryptionKeysStreamer) DecryptionKeysListener {
	return DecryptionKeysListener{
		logger:    logger,
		streamer:  streamer,
		observers: event.NewObservers[*proto.DecryptionKeys](),
	}
}

func (dkl DecryptionKeysListener) RegisterObserver(observer event.Observer[*proto.DecryptionKeys]) event.UnregisterFunc {
	return dkl.observers.Register(observer)
}

func (dkl DecryptionKeysListener) Run(ctx context.Context) error {
	dkl.logger.Info("running decryption keys streamer")

	for {
		// note streamer is context aware, will return error when ctx is cancelled
		keys, err := dkl.streamer.Next(ctx)
		if err != nil {
			return nil
		}

		dkl.observers.Notify(keys)
	}
}
