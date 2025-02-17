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

package database

import (
	"context"
	"math/rand"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cmd/observer/utils"
)

type DBRetrier struct {
	db  DB
	log log.Logger
}

func NewDBRetrier(db DB, logger log.Logger) DBRetrier {
	return DBRetrier{db, logger}
}

func retryBackoffTime(attempt int) time.Duration {
	if attempt <= 0 {
		return 0
	}
	jitter := rand.Int63n(30 * time.Millisecond.Nanoseconds() * int64(attempt)) // nolint: gosec
	var ns int64
	if attempt <= 6 {
		ns = ((50 * time.Millisecond.Nanoseconds()) << (attempt - 1)) + jitter
	} else {
		ns = 1600*time.Millisecond.Nanoseconds() + jitter
	}
	return time.Duration(ns)
}

func (db DBRetrier) retry(ctx context.Context, opName string, op func(context.Context) (interface{}, error)) (interface{}, error) {
	const retryCount = 40
	return utils.Retry(ctx, retryCount, retryBackoffTime, db.db.IsConflictError, db.log, opName, op)
}

func (db DBRetrier) UpsertNodeAddr(ctx context.Context, id NodeID, addr NodeAddr) error {
	_, err := db.retry(ctx, "UpsertNodeAddr", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpsertNodeAddr(ctx, id, addr)
	})
	return err
}

func (db DBRetrier) FindNodeAddr(ctx context.Context, id NodeID) (*NodeAddr, error) {
	resultAny, err := db.retry(ctx, "FindNodeAddr", func(ctx context.Context) (interface{}, error) {
		return db.db.FindNodeAddr(ctx, id)
	})

	if resultAny == nil {
		return nil, err
	}
	result := resultAny.(*NodeAddr)
	return result, err
}

func (db DBRetrier) ResetPingError(ctx context.Context, id NodeID) error {
	_, err := db.retry(ctx, "ResetPingError", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.ResetPingError(ctx, id)
	})
	return err
}

func (db DBRetrier) UpdatePingError(ctx context.Context, id NodeID) error {
	_, err := db.retry(ctx, "UpdatePingError", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpdatePingError(ctx, id)
	})
	return err
}

func (db DBRetrier) CountPingErrors(ctx context.Context, id NodeID) (*uint, error) {
	resultAny, err := db.retry(ctx, "CountPingErrors", func(ctx context.Context) (interface{}, error) {
		return db.db.CountPingErrors(ctx, id)
	})

	if resultAny == nil {
		return nil, err
	}
	result := resultAny.(*uint)
	return result, err
}

func (db DBRetrier) UpdateClientID(ctx context.Context, id NodeID, clientID string) error {
	_, err := db.retry(ctx, "UpdateClientID", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpdateClientID(ctx, id, clientID)
	})
	return err
}

func (db DBRetrier) FindClientID(ctx context.Context, id NodeID) (*string, error) {
	resultAny, err := db.retry(ctx, "FindClientID", func(ctx context.Context) (interface{}, error) {
		return db.db.FindClientID(ctx, id)
	})

	if resultAny == nil {
		return nil, err
	}
	result := resultAny.(*string)
	return result, err
}

func (db DBRetrier) UpdateNetworkID(ctx context.Context, id NodeID, networkID uint) error {
	_, err := db.retry(ctx, "UpdateNetworkID", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpdateNetworkID(ctx, id, networkID)
	})
	return err
}

func (db DBRetrier) UpdateEthVersion(ctx context.Context, id NodeID, ethVersion uint) error {
	_, err := db.retry(ctx, "UpdateEthVersion", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpdateEthVersion(ctx, id, ethVersion)
	})
	return err
}

func (db DBRetrier) UpdateHandshakeTransientError(ctx context.Context, id NodeID, hasTransientErr bool) error {
	_, err := db.retry(ctx, "UpdateHandshakeTransientError", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpdateHandshakeTransientError(ctx, id, hasTransientErr)
	})
	return err
}

func (db DBRetrier) InsertHandshakeError(ctx context.Context, id NodeID, handshakeErr string) error {
	_, err := db.retry(ctx, "InsertHandshakeError", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.InsertHandshakeError(ctx, id, handshakeErr)
	})
	return err
}

func (db DBRetrier) DeleteHandshakeErrors(ctx context.Context, id NodeID) error {
	_, err := db.retry(ctx, "DeleteHandshakeErrors", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.DeleteHandshakeErrors(ctx, id)
	})
	return err
}

func (db DBRetrier) FindHandshakeLastErrors(ctx context.Context, id NodeID, limit uint) ([]HandshakeError, error) {
	resultAny, err := db.retry(ctx, "FindHandshakeLastErrors", func(ctx context.Context) (interface{}, error) {
		return db.db.FindHandshakeLastErrors(ctx, id, limit)
	})

	if resultAny == nil {
		return nil, err
	}
	result := resultAny.([]HandshakeError)
	return result, err
}

func (db DBRetrier) UpdateHandshakeRetryTime(ctx context.Context, id NodeID, retryTime time.Time) error {
	_, err := db.retry(ctx, "UpdateHandshakeRetryTime", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpdateHandshakeRetryTime(ctx, id, retryTime)
	})
	return err
}

func (db DBRetrier) FindHandshakeRetryTime(ctx context.Context, id NodeID) (*time.Time, error) {
	resultAny, err := db.retry(ctx, "FindHandshakeRetryTime", func(ctx context.Context) (interface{}, error) {
		return db.db.FindHandshakeRetryTime(ctx, id)
	})

	if resultAny == nil {
		return nil, err
	}
	result := resultAny.(*time.Time)
	return result, err
}

func (db DBRetrier) CountHandshakeCandidates(ctx context.Context) (uint, error) {
	resultAny, err := db.retry(ctx, "CountHandshakeCandidates", func(ctx context.Context) (interface{}, error) {
		return db.db.CountHandshakeCandidates(ctx)
	})

	if resultAny == nil {
		return 0, err
	}
	result := resultAny.(uint)
	return result, err
}

func (db DBRetrier) TakeHandshakeCandidates(ctx context.Context, limit uint) ([]NodeID, error) {
	resultAny, err := db.retry(ctx, "TakeHandshakeCandidates", func(ctx context.Context) (interface{}, error) {
		return db.db.TakeHandshakeCandidates(ctx, limit)
	})

	if resultAny == nil {
		return nil, err
	}
	result := resultAny.([]NodeID)
	return result, err
}

func (db DBRetrier) UpdateForkCompatibility(ctx context.Context, id NodeID, isCompatFork bool) error {
	_, err := db.retry(ctx, "UpdateForkCompatibility", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpdateForkCompatibility(ctx, id, isCompatFork)
	})
	return err
}

func (db DBRetrier) UpdateNeighborBucketKeys(ctx context.Context, id NodeID, keys []string) error {
	_, err := db.retry(ctx, "UpdateNeighborBucketKeys", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpdateNeighborBucketKeys(ctx, id, keys)
	})
	return err
}

func (db DBRetrier) FindNeighborBucketKeys(ctx context.Context, id NodeID) ([]string, error) {
	resultAny, err := db.retry(ctx, "FindNeighborBucketKeys", func(ctx context.Context) (interface{}, error) {
		return db.db.FindNeighborBucketKeys(ctx, id)
	})

	if resultAny == nil {
		return nil, err
	}
	result := resultAny.([]string)
	return result, err
}

func (db DBRetrier) UpdateSentryCandidatesLastEventTime(ctx context.Context, value time.Time) error {
	_, err := db.retry(ctx, "UpdateSentryCandidatesLastEventTime", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpdateSentryCandidatesLastEventTime(ctx, value)
	})
	return err
}

func (db DBRetrier) FindSentryCandidatesLastEventTime(ctx context.Context) (*time.Time, error) {
	resultAny, err := db.retry(ctx, "FindSentryCandidatesLastEventTime", func(ctx context.Context) (interface{}, error) {
		return db.db.FindSentryCandidatesLastEventTime(ctx)
	})

	if resultAny == nil {
		return nil, err
	}
	result := resultAny.(*time.Time)
	return result, err
}

func (db DBRetrier) UpdateCrawlRetryTime(ctx context.Context, id NodeID, retryTime time.Time) error {
	_, err := db.retry(ctx, "UpdateCrawlRetryTime", func(ctx context.Context) (interface{}, error) {
		return nil, db.db.UpdateCrawlRetryTime(ctx, id, retryTime)
	})
	return err
}

func (db DBRetrier) CountCandidates(ctx context.Context) (uint, error) {
	resultAny, err := db.retry(ctx, "CountCandidates", func(ctx context.Context) (interface{}, error) {
		return db.db.CountCandidates(ctx)
	})

	if resultAny == nil {
		return 0, err
	}
	result := resultAny.(uint)
	return result, err
}

func (db DBRetrier) TakeCandidates(ctx context.Context, limit uint) ([]NodeID, error) {
	resultAny, err := db.retry(ctx, "TakeCandidates", func(ctx context.Context) (interface{}, error) {
		return db.db.TakeCandidates(ctx, limit)
	})

	if resultAny == nil {
		return nil, err
	}
	result := resultAny.([]NodeID)
	return result, err
}

func (db DBRetrier) IsConflictError(err error) bool {
	return db.db.IsConflictError(err)
}
