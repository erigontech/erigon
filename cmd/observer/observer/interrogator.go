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
	"strings"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon-lib/log/v3"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cmd/observer/utils"
	"github.com/erigontech/erigon/core/forkid"
	"github.com/erigontech/erigon/eth/protocols/eth"
	"github.com/erigontech/erigon/p2p/enode"
)

type DiscV4Transport interface {
	RequestENR(*enode.Node) (*enode.Node, error)
	Ping(*enode.Node) error
	FindNode(toNode *enode.Node, targetKey *ecdsa.PublicKey) ([]*enode.Node, error)
}

type Interrogator struct {
	node       *enode.Node
	transport  DiscV4Transport
	forkFilter forkid.Filter

	diplomat           *Diplomat
	handshakeRetryTime *time.Time

	keygenTimeout     time.Duration
	keygenConcurrency uint
	keygenSemaphore   *semaphore.Weighted
	keygenCachedKeys  []*ecdsa.PublicKey

	log log.Logger
}

type InterrogationResult struct {
	Node               *enode.Node
	IsCompatFork       *bool
	HandshakeResult    *DiplomatResult
	HandshakeRetryTime *time.Time
	KeygenKeys         []*ecdsa.PublicKey
	Peers              []*enode.Node
}

func NewInterrogator(
	node *enode.Node,
	transport DiscV4Transport,
	forkFilter forkid.Filter,
	diplomat *Diplomat,
	handshakeRetryTime *time.Time,
	keygenTimeout time.Duration,
	keygenConcurrency uint,
	keygenSemaphore *semaphore.Weighted,
	keygenCachedKeys []*ecdsa.PublicKey,
	logger log.Logger,
) (*Interrogator, error) {
	instance := Interrogator{
		node,
		transport,
		forkFilter,
		diplomat,
		handshakeRetryTime,
		keygenTimeout,
		keygenConcurrency,
		keygenSemaphore,
		keygenCachedKeys,
		logger,
	}
	return &instance, nil
}

func (interrogator *Interrogator) Run(ctx context.Context) (*InterrogationResult, *InterrogationError) {
	interrogator.log.Debug("Interrogating a node")

	err := interrogator.transport.Ping(interrogator.node)
	if err != nil {
		return nil, NewInterrogationError(InterrogationErrorPing, err)
	}

	// The outgoing Ping above triggers an incoming Ping.
	// We need to wait until Server sends a Pong reply to that.
	// The remote side is waiting for this Pong no longer than v4_udp.respTimeout.
	// If we don't wait, the ENRRequest/FindNode might fail due to errUnknownNode.
	if err := libcommon.Sleep(ctx, 500*time.Millisecond); err != nil {
		return nil, NewInterrogationError(InterrogationErrorCtxCancelled, err)
	}

	// request client ID
	var handshakeResult *DiplomatResult
	var handshakeRetryTime *time.Time
	if (interrogator.handshakeRetryTime == nil) || interrogator.handshakeRetryTime.Before(time.Now()) {
		result := interrogator.diplomat.Run(ctx)
		clientID := result.ClientID
		if (clientID != nil) && IsClientIDBlacklisted(*clientID) {
			return nil, NewInterrogationError(InterrogationErrorBlacklistedClientID, errors.New(*clientID))
		}
		handshakeResult = &result
		handshakeRetryTime = new(time.Time)
		*handshakeRetryTime = interrogator.diplomat.NextRetryTime(result.HandshakeErr)
	}

	// request ENR
	var forkID *forkid.ID
	var enr *enode.Node
	if (handshakeResult == nil) || (handshakeResult.ClientID == nil) || isENRRequestSupportedByClientID(*handshakeResult.ClientID) {
		enr, err = interrogator.transport.RequestENR(interrogator.node)
	}
	if err != nil {
		interrogator.log.Debug("ENR request failed", "err", err)
	} else if enr != nil {
		interrogator.log.Debug("Got ENR", "enr", enr)
		forkID, err = eth.LoadENRForkID(enr.Record())
		if err != nil {
			return nil, NewInterrogationError(InterrogationErrorENRDecode, err)
		}
		if forkID == nil {
			interrogator.log.Debug("Got ENR, but it doesn't contain a ForkID")
		}
	}

	// filter by fork ID
	var isCompatFork *bool
	if forkID != nil {
		err := interrogator.forkFilter(*forkID)
		isCompatFork = new(bool)
		*isCompatFork = (err == nil) || !errors.Is(err, forkid.ErrLocalIncompatibleOrStale)
		if !*isCompatFork {
			return nil, NewInterrogationError(InterrogationErrorIncompatibleForkID, err)
		}
	}

	// keygen
	keys, err := interrogator.keygen(ctx)
	if err != nil {
		return nil, NewInterrogationError(InterrogationErrorKeygen, err)
	}

	// FindNode
	peersByID := make(map[enode.ID]*enode.Node)
	for _, key := range keys {
		neighbors, err := interrogator.findNode(ctx, key)
		if err != nil {
			if isFindNodeTimeoutError(err) {
				return nil, NewInterrogationError(InterrogationErrorFindNodeTimeout, err)
			}
			return nil, NewInterrogationError(InterrogationErrorFindNode, err)
		}

		for _, node := range neighbors {
			if node.Incomplete() {
				continue
			}
			peersByID[node.ID()] = node
		}

		if err := libcommon.Sleep(ctx, 1*time.Second); err != nil {
			return nil, NewInterrogationError(InterrogationErrorCtxCancelled, err)
		}
	}

	peers := valuesOfIDToNodeMap(peersByID)

	result := InterrogationResult{
		interrogator.node,
		isCompatFork,
		handshakeResult,
		handshakeRetryTime,
		keys,
		peers,
	}
	return &result, nil
}

func (interrogator *Interrogator) keygen(ctx context.Context) ([]*ecdsa.PublicKey, error) {
	if interrogator.keygenCachedKeys != nil {
		return interrogator.keygenCachedKeys, nil
	}

	if err := interrogator.keygenSemaphore.Acquire(ctx, 1); err != nil {
		return nil, err
	}
	defer interrogator.keygenSemaphore.Release(1)

	keys := keygen(
		ctx,
		interrogator.node.Pubkey(),
		interrogator.keygenTimeout,
		interrogator.keygenConcurrency,
		interrogator.log)

	interrogator.log.Trace(fmt.Sprintf("Generated %d keys", len(keys)))
	if (len(keys) < 13) && (ctx.Err() == nil) {
		msg := "Generated just %d keys within a given timeout and concurrency (expected 16-17). " +
			"If this happens too often, try to increase keygen-timeout/keygen-concurrency parameters."
		interrogator.log.Warn(fmt.Sprintf(msg, len(keys)))
	}
	return keys, ctx.Err()
}

func (interrogator *Interrogator) findNode(ctx context.Context, targetKey *ecdsa.PublicKey) ([]*enode.Node, error) {
	delayForAttempt := func(attempt int) time.Duration { return 2 * time.Second }
	resultAny, err := utils.Retry(ctx, 2, delayForAttempt, isFindNodeTimeoutError, interrogator.log, "FindNode", func(ctx context.Context) (interface{}, error) {
		return interrogator.transport.FindNode(interrogator.node, targetKey)
	})

	if resultAny == nil {
		return nil, err
	}
	result := resultAny.([]*enode.Node)
	return result, err
}

func isFindNodeTimeoutError(err error) bool {
	return (err != nil) && (err.Error() == "RPC timeout")
}

func isENRRequestSupportedByClientID(clientID string) bool {
	isUnsupported := strings.HasPrefix(clientID, "Parity-Ethereum") ||
		strings.HasPrefix(clientID, "OpenEthereum") ||
		strings.HasPrefix(clientID, "Nethermind")
	return !isUnsupported
}

func valuesOfIDToNodeMap(m map[enode.ID]*enode.Node) []*enode.Node {
	values := make([]*enode.Node, 0, len(m))
	for _, value := range m {
		values = append(values, value)
	}
	return values
}
