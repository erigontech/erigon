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
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/v3/cmd/observer/database"
	"github.com/erigontech/erigon/v3/p2p"
	"github.com/erigontech/erigon/v3/p2p/enode"
)

type Diplomat struct {
	node       *enode.Node
	privateKey *ecdsa.PrivateKey

	handshakeLastErrors     []database.HandshakeError
	handshakeRefreshTimeout time.Duration
	handshakeRetryDelay     time.Duration
	handshakeMaxTries       uint

	log log.Logger
}

type DiplomatResult struct {
	ClientID        *string
	NetworkID       *uint64
	EthVersion      *uint32
	HandshakeErr    *HandshakeError
	HasTransientErr bool
}

func NewDiplomat(
	node *enode.Node,
	privateKey *ecdsa.PrivateKey,
	handshakeLastErrors []database.HandshakeError,
	handshakeRefreshTimeout time.Duration,
	handshakeRetryDelay time.Duration,
	handshakeMaxTries uint,
	logger log.Logger,
) *Diplomat {
	instance := Diplomat{
		node,
		privateKey,
		handshakeLastErrors,
		handshakeRefreshTimeout,
		handshakeRetryDelay,
		handshakeMaxTries,
		logger,
	}
	return &instance
}

func (diplomat *Diplomat) handshake(ctx context.Context) (*HelloMessage, *StatusMessage, *HandshakeError) {
	node := diplomat.node
	return Handshake(ctx, node.IP(), node.TCP(), node.Pubkey(), diplomat.privateKey)
}

func (diplomat *Diplomat) Run(ctx context.Context) DiplomatResult {
	diplomat.log.Debug("Handshaking with a node")
	hello, status, handshakeErr := diplomat.handshake(ctx)

	var result DiplomatResult

	if (handshakeErr != nil) && !errors.Is(handshakeErr, context.Canceled) {
		result.HandshakeErr = handshakeErr
		diplomat.log.Debug("Failed to handshake", "err", handshakeErr)
	}
	result.HasTransientErr = diplomat.hasRecentTransientError(handshakeErr)

	if hello != nil {
		result.ClientID = &hello.ClientID
		diplomat.log.Debug("Got client ID", "clientID", *result.ClientID)
	}

	if status != nil {
		result.NetworkID = &status.NetworkID
		diplomat.log.Debug("Got network ID", "networkID", *result.NetworkID)
	}
	if status != nil {
		result.EthVersion = &status.ProtocolVersion
		diplomat.log.Debug("Got eth version", "ethVersion", *result.EthVersion)
	}

	return result
}

func (diplomat *Diplomat) NextRetryTime(handshakeErr *HandshakeError) time.Time {
	return time.Now().Add(diplomat.NextRetryDelay(handshakeErr))
}

func (diplomat *Diplomat) NextRetryDelay(handshakeErr *HandshakeError) time.Duration {
	if handshakeErr == nil {
		return diplomat.handshakeRefreshTimeout
	}

	dbHandshakeErr := database.HandshakeError{
		StringCode: handshakeErr.StringCode(),
		Time:       time.Now(),
	}

	lastErrors := append([]database.HandshakeError{dbHandshakeErr}, diplomat.handshakeLastErrors...)

	if uint(len(lastErrors)) < diplomat.handshakeMaxTries {
		return diplomat.handshakeRetryDelay
	}

	if containsHandshakeError(diplomat.transientError(), lastErrors) {
		return diplomat.handshakeRetryDelay
	}

	if len(lastErrors) < 2 {
		return 1000000 * time.Hour // never
	}

	backOffDelay := 2 * lastErrors[0].Time.Sub(lastErrors[1].Time)
	if backOffDelay < diplomat.handshakeRetryDelay {
		return diplomat.handshakeRetryDelay
	}

	return backOffDelay
}

func (diplomat *Diplomat) transientError() *HandshakeError {
	return NewHandshakeError(HandshakeErrorIDDisconnect, p2p.DiscTooManyPeers, uint64(p2p.DiscTooManyPeers))
}

func (diplomat *Diplomat) hasRecentTransientError(handshakeErr *HandshakeError) bool {
	if handshakeErr == nil {
		return false
	}

	dbHandshakeErr := database.HandshakeError{
		StringCode: handshakeErr.StringCode(),
		Time:       time.Now(),
	}

	lastErrors := append([]database.HandshakeError{dbHandshakeErr}, diplomat.handshakeLastErrors...)
	return containsHandshakeError(diplomat.transientError(), lastErrors)
}

func containsHandshakeError(target *HandshakeError, list []database.HandshakeError) bool {
	for _, err := range list {
		if err.StringCode == target.StringCode() {
			return true
		}
	}
	return false
}
