// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package p2p

import (
	"fmt"
)

type PeerErrorCode uint8

const (
	PeerErrorInvalidMessageCode PeerErrorCode = iota
	PeerErrorInvalidMessage
	PeerErrorPingFailure
	PeerErrorPongFailure
	PeerErrorDiscReason
	PeerErrorDiscReasonRemote
	PeerErrorMessageReceive
	PeerErrorMessageSizeLimit
	PeerErrorMessageObsolete
	PeerErrorMessageSend
	PeerErrorLocalStatusNeeded
	PeerErrorStatusSend
	PeerErrorStatusReceive
	PeerErrorStatusDecode
	PeerErrorStatusIncompatible
	PeerErrorStatusHandshakeTimeout
	PeerErrorStatusUnexpected
	PeerErrorFirstMessageSend
	PeerErrorTest
)

var peerErrorCodeToString = map[PeerErrorCode]string{
	PeerErrorInvalidMessageCode:     "invalid message code",
	PeerErrorInvalidMessage:         "invalid message",
	PeerErrorPingFailure:            "ping failure",
	PeerErrorPongFailure:            "pong failure",
	PeerErrorDiscReason:             "disconnect reason",
	PeerErrorDiscReasonRemote:       "remote disconnect reason",
	PeerErrorMessageReceive:         "failed to receive a message",
	PeerErrorMessageSizeLimit:       "too big message",
	PeerErrorMessageObsolete:        "obsolete message",
	PeerErrorMessageSend:            "failed to send a message",
	PeerErrorLocalStatusNeeded:      "need a local status message",
	PeerErrorStatusSend:             "failed to send the local status",
	PeerErrorStatusReceive:          "failed to receive the remote status",
	PeerErrorStatusDecode:           "failed to decode the remote status",
	PeerErrorStatusIncompatible:     "incompatible remote status",
	PeerErrorStatusHandshakeTimeout: "handshake timeout",
	PeerErrorStatusUnexpected:       "unexpected remote status",
	PeerErrorFirstMessageSend:       "failed to send the first message",
	PeerErrorTest:                   "test error",
}

func (c PeerErrorCode) String() string {
	if len(peerErrorCodeToString) <= int(c) {
		return fmt.Sprintf("unknown code %d", c)
	}
	return peerErrorCodeToString[c]
}

func (c PeerErrorCode) Error() string {
	return c.String()
}

type PeerError struct {
	Code    PeerErrorCode
	Reason  DiscReason
	Err     error
	Message string
}

func NewPeerError(code PeerErrorCode, reason DiscReason, err error, message string) *PeerError {
	return &PeerError{
		code,
		reason,
		err,
		message,
	}
}

func (pe *PeerError) String() string {
	return fmt.Sprintf("PeerError(code=%s, reason=%s, err=%v, message=%s)",
		pe.Code, pe.Reason, pe.Err, pe.Message)
}

func (pe *PeerError) Error() string {
	return pe.String()
}

type DiscReason uint8

const (
	DiscRequested DiscReason = iota
	DiscNetworkError
	DiscProtocolError
	DiscUselessPeer
	DiscTooManyPeers
	DiscAlreadyConnected
	DiscIncompatibleVersion
	DiscInvalidIdentity
	DiscQuitting
	DiscUnexpectedIdentity
	DiscSelf
	DiscReadTimeout
	DiscSubprotocolError = 0x10
)

var discReasonToString = [...]string{
	DiscRequested:           "disconnect requested",
	DiscNetworkError:        "network error",
	DiscProtocolError:       "breach of protocol",
	DiscUselessPeer:         "useless peer",
	DiscTooManyPeers:        "too many peers",
	DiscAlreadyConnected:    "already connected",
	DiscIncompatibleVersion: "incompatible p2p protocol version",
	DiscInvalidIdentity:     "invalid node identity",
	DiscQuitting:            "client quitting",
	DiscUnexpectedIdentity:  "unexpected identity",
	DiscSelf:                "connected to self",
	DiscReadTimeout:         "read timeout",
	DiscSubprotocolError:    "subprotocol error",
}

func (d DiscReason) String() string {
	if len(discReasonToString) <= int(d) {
		return fmt.Sprintf("unknown disconnect reason %d", d)
	}
	return discReasonToString[d]
}

func (d DiscReason) Error() string {
	return d.String()
}
