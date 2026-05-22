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

package snapshotsync

import (
	"fmt"
	"strings"
)

// RevalidationPolicy governs how a publisher reacts when its startup
// pre-flight re-validation finds a local snapshot file with an incorrect
// info-hash or that fails the local validator set
// (docs/plans/20260522-publisher-startup-preflight.md, Guarantee 3).
type RevalidationPolicy int

const (
	// RevalidationRedownload demotes the bad file and re-queues it for
	// download — the self-healing minority-client default.
	RevalidationRedownload RevalidationPolicy = iota
	// RevalidationStop halts on the first re-validation failure so a
	// corrupt local archive is surfaced loudly rather than silently healed.
	RevalidationStop
	// RevalidationWarn logs the failure and continues without
	// re-downloading; the suspect file is still excluded from the manifest.
	RevalidationWarn
)

func (p RevalidationPolicy) String() string {
	switch p {
	case RevalidationRedownload:
		return "redownload"
	case RevalidationStop:
		return "stop"
	case RevalidationWarn:
		return "warn"
	default:
		return "unknown"
	}
}

// ParseRevalidationPolicy maps a --snapshot.revalidation-policy flag value
// to a RevalidationPolicy. An empty string defaults to RevalidationRedownload.
func ParseRevalidationPolicy(s string) (RevalidationPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "redownload":
		return RevalidationRedownload, nil
	case "stop":
		return RevalidationStop, nil
	case "warn":
		return RevalidationWarn, nil
	default:
		return RevalidationRedownload, fmt.Errorf("unknown revalidation policy %q (want redownload, stop or warn)", s)
	}
}
