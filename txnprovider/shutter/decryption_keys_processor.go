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

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/txnprovider/shutter/internal/proto"
)

type DecryptionKeysProcessor struct {
	logger log.Logger
	queue  chan *proto.DecryptionKeys
}

func NewDecryptionKeysProcessor(logger log.Logger) DecryptionKeysProcessor {
	return DecryptionKeysProcessor{
		logger: logger,
		queue:  make(chan *proto.DecryptionKeys),
	}
}

func (dkp DecryptionKeysProcessor) Enqueue(msg *proto.DecryptionKeys) {
	dkp.queue <- msg
}

func (dkp DecryptionKeysProcessor) Run(ctx context.Context) error {
	dkp.logger.Info("running decryption keys processor")

	for {
		select {
		case msg := <-dkp.queue:
			dkp.logger.Debug(
				"processing decryption keys message",
				"instanceId", msg.InstanceId,
				"eon", msg.Eon,
				"slot", msg.GetGnosis().Slot,
				"txPointer", msg.GetGnosis().TxPointer,
			)
		//
		// TODO process msg
		//
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
