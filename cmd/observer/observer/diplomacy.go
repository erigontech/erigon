// Copyright 2024 The Erigon Authors
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

package observer

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/log/v3"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/v3/cmd/observer/database"
	"github.com/erigontech/erigon/v3/cmd/observer/observer/node_utils"
	"github.com/erigontech/erigon/v3/cmd/observer/utils"
)

type Diplomacy struct {
	db        database.DBRetrier
	saveQueue *utils.TaskQueue

	privateKey        *ecdsa.PrivateKey
	concurrencyLimit  uint
	refreshTimeout    time.Duration
	retryDelay        time.Duration
	maxHandshakeTries uint

	statusLogPeriod time.Duration
	log             log.Logger
}

func NewDiplomacy(
	db database.DBRetrier,
	saveQueue *utils.TaskQueue,
	privateKey *ecdsa.PrivateKey,
	concurrencyLimit uint,
	refreshTimeout time.Duration,
	retryDelay time.Duration,
	maxHandshakeTries uint,
	statusLogPeriod time.Duration,
	logger log.Logger,
) *Diplomacy {
	instance := Diplomacy{
		db,
		saveQueue,
		privateKey,
		concurrencyLimit,
		refreshTimeout,
		retryDelay,
		maxHandshakeTries,
		statusLogPeriod,
		logger,
	}
	return &instance
}

func (diplomacy *Diplomacy) startSelectCandidates(ctx context.Context) <-chan database.NodeID {
	candidatesChan := make(chan database.NodeID)
	go func() {
		err := diplomacy.selectCandidates(ctx, candidatesChan)
		if (err != nil) && !errors.Is(err, context.Canceled) {
			diplomacy.log.Error("Failed to select handshake candidates", "err", err)
		}
		close(candidatesChan)
	}()
	return candidatesChan
}

func (diplomacy *Diplomacy) selectCandidates(ctx context.Context, candidatesChan chan<- database.NodeID) error {
	for ctx.Err() == nil {
		candidates, err := diplomacy.db.TakeHandshakeCandidates(
			ctx,
			diplomacy.concurrencyLimit)
		if err != nil {
			if diplomacy.db.IsConflictError(err) {
				diplomacy.log.Warn("Failed to take handshake candidates", "err", err)
			} else {
				return err
			}
		}

		if len(candidates) == 0 {
			if err := libcommon.Sleep(ctx, 1*time.Second); err != nil {
				return err
			}
		}

		for _, id := range candidates {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case candidatesChan <- id:
			}
		}
	}

	return ctx.Err()
}

func (diplomacy *Diplomacy) Run(ctx context.Context) error {
	candidatesChan := diplomacy.startSelectCandidates(ctx)
	sem := semaphore.NewWeighted(int64(diplomacy.concurrencyLimit))

	doneCount := 0
	statusLogDate := time.Now()
	clientIDCountPtr := new(uint64)

	for id := range candidatesChan {
		if err := sem.Acquire(ctx, 1); err != nil {
			if !errors.Is(err, context.Canceled) {
				return fmt.Errorf("failed to acquire semaphore: %w", err)
			} else {
				break
			}
		}

		doneCount++
		if time.Since(statusLogDate) > diplomacy.statusLogPeriod {
			clientIDCount := atomic.LoadUint64(clientIDCountPtr)

			remainingCount, err := diplomacy.db.CountHandshakeCandidates(ctx)
			if err != nil {
				if diplomacy.db.IsConflictError(err) {
					diplomacy.log.Warn("Failed to count handshake candidates", "err", err)
					sem.Release(1)
					continue
				}
				return fmt.Errorf("failed to count handshake candidates: %w", err)
			}

			diplomacy.log.Info(
				"Handshaking",
				"done", doneCount,
				"remaining", remainingCount,
				"clientIDs", clientIDCount,
			)
			statusLogDate = time.Now()
		}

		nodeAddr, err := diplomacy.db.FindNodeAddr(ctx, id)
		if err != nil {
			if diplomacy.db.IsConflictError(err) {
				diplomacy.log.Warn("Failed to get the node address", "err", err)
				sem.Release(1)
				continue
			}
			return fmt.Errorf("failed to get the node address: %w", err)
		}

		node, err := node_utils.MakeNodeFromAddr(id, *nodeAddr)
		if err != nil {
			return fmt.Errorf("failed to make node from node address: %w", err)
		}

		nodeDesc := node.URLv4()
		logger := diplomacy.log.New("node", nodeDesc)

		handshakeLastErrors, err := diplomacy.db.FindHandshakeLastErrors(ctx, id, diplomacy.maxHandshakeTries)
		if err != nil {
			if diplomacy.db.IsConflictError(err) {
				diplomacy.log.Warn("Failed to get handshake last errors", "err", err)
				sem.Release(1)
				continue
			}
			return fmt.Errorf("failed to get handshake last errors: %w", err)
		}

		diplomat := NewDiplomat(
			node,
			diplomacy.privateKey,
			handshakeLastErrors,
			diplomacy.refreshTimeout,
			diplomacy.retryDelay,
			diplomacy.maxHandshakeTries,
			logger)

		go func(id database.NodeID) {
			defer sem.Release(1)

			result := diplomat.Run(ctx)
			clientID := result.ClientID

			if clientID != nil {
				atomic.AddUint64(clientIDCountPtr, 1)
			}

			var isCompatFork *bool
			if (clientID != nil) && IsClientIDBlacklisted(*clientID) {
				isCompatFork = new(bool)
				*isCompatFork = false
			}

			nextRetryTime := diplomat.NextRetryTime(result.HandshakeErr)

			diplomacy.saveQueue.EnqueueTask(ctx, func(ctx context.Context) error {
				return diplomacy.saveDiplomatResult(ctx, id, result, isCompatFork, nextRetryTime)
			})
		}(id)
	}
	return nil
}

func (diplomacy *Diplomacy) saveDiplomatResult(
	ctx context.Context,
	id database.NodeID,
	result DiplomatResult,
	isCompatFork *bool,
	nextRetryTime time.Time,
) error {
	if result.ClientID != nil {
		dbErr := diplomacy.db.UpdateClientID(ctx, id, *result.ClientID)
		if dbErr != nil {
			return dbErr
		}

		dbErr = diplomacy.db.DeleteHandshakeErrors(ctx, id)
		if dbErr != nil {
			return dbErr
		}
	}

	if result.NetworkID != nil {
		dbErr := diplomacy.db.UpdateNetworkID(ctx, id, uint(*result.NetworkID))
		if dbErr != nil {
			return dbErr
		}
	}

	if result.EthVersion != nil {
		dbErr := diplomacy.db.UpdateEthVersion(ctx, id, uint(*result.EthVersion))
		if dbErr != nil {
			return dbErr
		}
	}

	if result.HandshakeErr != nil {
		dbErr := diplomacy.db.InsertHandshakeError(ctx, id, result.HandshakeErr.StringCode())
		if dbErr != nil {
			return dbErr
		}
	}

	dbErr := diplomacy.db.UpdateHandshakeTransientError(ctx, id, result.HasTransientErr)
	if dbErr != nil {
		return dbErr
	}

	if isCompatFork != nil {
		dbErr := diplomacy.db.UpdateForkCompatibility(ctx, id, *isCompatFork)
		if dbErr != nil {
			return dbErr
		}
	}

	return diplomacy.db.UpdateHandshakeRetryTime(ctx, id, nextRetryTime)
}
