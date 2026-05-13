// Copyright 2026 The Erigon Authors
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

package libsentry

// MessagesQueueSize bounds the per-subscriber channels that fan out inbound
// P2P messages and peer events from a sentry to its stream consumers. Kept
// small so that peer-driven traffic cannot push the process toward OOM via
// the multi-MB ethp2p message size limit; paired with EvictOldestIfHalfFull
// at fan-out writes so fresh messages are preferred when consumers fall
// behind.
const MessagesQueueSize = 1024

// EvictOldestIfHalfFull drops up to cap(ch)/4 oldest items from ch when it
// is more than half full. Non-blocking: returns as soon as the drain target
// is reached or ch becomes empty.
//
// Intended to be called right after a successful send on fan-out channels,
// mirroring the eviction policy used by the per-subscriber Messages
// channels in p2p/sentry.
func EvictOldestIfHalfFull[T any](ch chan T) {
	if len(ch) <= cap(ch)/2 {
		return
	}
	drain := cap(ch) / 4
	for range drain {
		select {
		case <-ch:
		default:
			return
		}
	}
}
