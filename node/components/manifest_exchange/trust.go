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

package manifest_exchange

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/erigontech/erigon/node/components/snapshotauth"
)

// UCANFetcher fetches the two UCAN sidecars the consumer's trust gate
// needs: the Authority UCAN (FetchPeerUCAN, by the V2 manifest's
// AuthorityUCANHash) and the per-generation Content UCAN
// (FetchPeerContentUCAN, by the peer ENR's ContentUCANHash). Production
// wires downloader.Provider, which satisfies both; tests substitute a
// stub.
type UCANFetcher interface {
	FetchPeerUCAN(ctx context.Context, peerID string, infoHash [20]byte, peerIP net.IP, peerPort uint16) ([]byte, error)
	FetchPeerContentUCAN(ctx context.Context, peerID string, infoHash [20]byte, peerIP net.IP, peerPort uint16) ([]byte, error)
}

// DefaultBlacklistDuration is the time a peer stays on the blacklist
// after a failed UCAN verification when TrustConfig.BlacklistDuration
// is zero.
const DefaultBlacklistDuration = time.Hour

// TrustConfig pulls together the trust roots, the verifier, the UCAN
// fetcher, and the policy knobs that govern manifest_exchange's UCAN
// gate. A nil *TrustConfig (the zero-value of Provider.trust) means
// "trust everyone" — preserves the pre-UCAN behaviour.
type TrustConfig struct {
	// Verifier evaluates a peer's delegation chain against the
	// configured roots. Required when TrustConfig is non-nil.
	Verifier *snapshotauth.Verifier

	// Fetcher pulls the UCAN sidecar bytes for a peer. Required when
	// TrustConfig is non-nil. Production: downloader.Provider.
	Fetcher UCANFetcher

	// RequiredCapabilities is the set of capabilities a peer's leaf
	// delegation must grant for the verifier to accept. For the
	// snapshot consumer path this is typically [snapshot:advertise,
	// snapshot:serve] — the advertiser-side authorisation. Empty
	// rejects every peer; callers always populate this.
	RequiredCapabilities []string

	// ReverifyOnReconnect, when true, evicts a peer's cached trust on
	// every disconnect so the next reconnect re-verifies the UCAN.
	// Off by default (cheaper) but operators in adversarial
	// environments can flip this to defend against node-takeover —
	// a key rotation between original issuance and a hostile takeover
	// of the node would slip past the expiry-honour path otherwise.
	ReverifyOnReconnect bool

	// BlacklistDuration is the period a peer ID stays untrusted after
	// a failed UCAN verification. Zero defers to
	// DefaultBlacklistDuration (1h). The blacklist is in-memory only;
	// process restart clears it.
	BlacklistDuration time.Duration
}

func (c *TrustConfig) blacklistDuration() time.Duration {
	if c == nil || c.BlacklistDuration <= 0 {
		return DefaultBlacklistDuration
	}
	return c.BlacklistDuration
}

// trustState is the in-memory cache of per-peer trust verdicts. Two
// kinds of entries:
//
//   - verifiedUntil[peerID] = wall-clock time the peer's UCAN expires.
//     A peer with a valid entry is trusted without re-verification on
//     subsequent reconnects (unless ReverifyOnReconnect is set, which
//     evicts on disconnect — see Provider.onPeerDisconnected).
//
//   - blacklistedUntil[peerID] = wall-clock time after which a failed
//     verification is forgotten and the peer can re-attempt.
//
// Both maps are guarded by mu. Entries are not auto-expired in the
// background; expired entries are observed lazily on the next access.
// This keeps trustState dependency-free (no goroutine, no timer
// fan-out) at the cost of a stale entry consuming memory until the
// next access — bounded in practice by sentry's churn shape.
type trustState struct {
	mu               sync.Mutex
	verifiedUntil    map[string]time.Time
	blacklistedUntil map[string]time.Time
}

func newTrustState() *trustState {
	return &trustState{
		verifiedUntil:    map[string]time.Time{},
		blacklistedUntil: map[string]time.Time{},
	}
}

// trusted reports whether peerID has a cached verified-until entry
// that has not yet expired. Returns false if there is no entry, or
// the entry expired.
func (s *trustState) trusted(peerID string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	until, ok := s.verifiedUntil[peerID]
	if !ok {
		return false
	}
	if !until.IsZero() && now.After(until) {
		delete(s.verifiedUntil, peerID)
		return false
	}
	return true
}

// markVerified caches the peerID as trusted until expiresAt. A zero
// expiresAt is treated as "indefinite" — never expires (matches the
// snapshotauth indefinite-expiry sentinel).
func (s *trustState) markVerified(peerID string, expiresAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.verifiedUntil[peerID] = expiresAt
	delete(s.blacklistedUntil, peerID) // a fresh accept clears any prior blacklist
}

// blacklisted reports whether peerID is currently denylisted. The
// entry is auto-evicted on first observation past its expiry.
func (s *trustState) blacklisted(peerID string, now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	until, ok := s.blacklistedUntil[peerID]
	if !ok {
		return false
	}
	if now.After(until) {
		delete(s.blacklistedUntil, peerID)
		return false
	}
	return true
}

// markBlacklisted records peerID as denied until `until`.
func (s *trustState) markBlacklisted(peerID string, until time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blacklistedUntil[peerID] = until
	delete(s.verifiedUntil, peerID) // can't be both
}

// forgetVerified evicts peerID's verified-until entry. Used by
// onPeerDisconnected when ReverifyOnReconnect is set.
func (s *trustState) forgetVerified(peerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.verifiedUntil, peerID)
}

// Trusted satisfies the flow.TrustFilter interface so the orchestrator
// can gate DownloadRequested on the manifest-exchange trust state. A
// peer is trusted if (a) it has a verified-until entry that has not
// expired, AND (b) it is not currently blacklisted. Returns false if
// no trust state is configured (caller should treat that as
// trust-everyone via a nil filter rather than a non-nil filter that
// rejects all peers).
func (p *Provider) Trusted(peerID string) bool {
	p.mu.Lock()
	state := p.trustState
	now := p.nowFn
	p.mu.Unlock()
	if state == nil {
		return false
	}
	t := time.Now()
	if now != nil {
		t = now()
	}
	if state.blacklisted(peerID, t) {
		return false
	}
	return state.trusted(peerID, t)
}
