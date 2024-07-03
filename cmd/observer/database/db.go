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
	"io"
	"net"
	"time"
)

type NodeID string

type NodeAddr1 struct {
	IP       net.IP
	PortDisc uint16
	PortRLPx uint16
}

type NodeAddr struct {
	NodeAddr1
	IPv6 NodeAddr1
}

type HandshakeError struct {
	StringCode string
	Time       time.Time
}

type DB interface {
	io.Closer

	UpsertNodeAddr(ctx context.Context, id NodeID, addr NodeAddr) error
	FindNodeAddr(ctx context.Context, id NodeID) (*NodeAddr, error)

	ResetPingError(ctx context.Context, id NodeID) error
	UpdatePingError(ctx context.Context, id NodeID) error
	CountPingErrors(ctx context.Context, id NodeID) (*uint, error)

	UpdateClientID(ctx context.Context, id NodeID, clientID string) error
	FindClientID(ctx context.Context, id NodeID) (*string, error)
	UpdateNetworkID(ctx context.Context, id NodeID, networkID uint) error
	UpdateEthVersion(ctx context.Context, id NodeID, ethVersion uint) error
	UpdateHandshakeTransientError(ctx context.Context, id NodeID, hasTransientErr bool) error
	InsertHandshakeError(ctx context.Context, id NodeID, handshakeErr string) error
	DeleteHandshakeErrors(ctx context.Context, id NodeID) error
	FindHandshakeLastErrors(ctx context.Context, id NodeID, limit uint) ([]HandshakeError, error)
	UpdateHandshakeRetryTime(ctx context.Context, id NodeID, retryTime time.Time) error
	FindHandshakeRetryTime(ctx context.Context, id NodeID) (*time.Time, error)
	CountHandshakeCandidates(ctx context.Context) (uint, error)
	FindHandshakeCandidates(ctx context.Context, limit uint) ([]NodeID, error)
	MarkTakenHandshakeCandidates(ctx context.Context, nodes []NodeID) error
	// TakeHandshakeCandidates runs FindHandshakeCandidates + MarkTakenHandshakeCandidates in a transaction.
	TakeHandshakeCandidates(ctx context.Context, limit uint) ([]NodeID, error)

	UpdateForkCompatibility(ctx context.Context, id NodeID, isCompatFork bool) error

	UpdateNeighborBucketKeys(ctx context.Context, id NodeID, keys []string) error
	FindNeighborBucketKeys(ctx context.Context, id NodeID) ([]string, error)

	UpdateSentryCandidatesLastEventTime(ctx context.Context, value time.Time) error
	FindSentryCandidatesLastEventTime(ctx context.Context) (*time.Time, error)

	UpdateCrawlRetryTime(ctx context.Context, id NodeID, retryTime time.Time) error
	CountCandidates(ctx context.Context) (uint, error)
	FindCandidates(ctx context.Context, limit uint) ([]NodeID, error)
	MarkTakenNodes(ctx context.Context, nodes []NodeID) error
	// TakeCandidates runs FindCandidates + MarkTakenNodes in a transaction.
	TakeCandidates(ctx context.Context, limit uint) ([]NodeID, error)

	IsConflictError(err error) bool

	CountNodes(ctx context.Context, maxPingTries uint, networkID uint) (uint, error)
	CountIPs(ctx context.Context, maxPingTries uint, networkID uint) (uint, error)
	CountClients(ctx context.Context, clientIDPrefix string, maxPingTries uint, networkID uint) (uint, error)
	CountClientsWithNetworkID(ctx context.Context, clientIDPrefix string, maxPingTries uint) (uint, error)
	CountClientsWithHandshakeTransientError(ctx context.Context, clientIDPrefix string, maxPingTries uint) (uint, error)
	EnumerateClientIDs(ctx context.Context, maxPingTries uint, networkID uint, enumFunc func(clientID *string)) error
}
