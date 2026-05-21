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

// AdoptionPolicy governs how far a node carries staged canonical
// adoption automatically when CheckOwnAdvertisement returns a
// *MinorityVerdict (docs/plans/20260520-phase7-staged-adoption-design.md).
// The cutover reopens segments and republishes — disruptive — so an
// operator who favours continuous operation controls when it happens.
type AdoptionPolicy int

const (
	// AdoptionAuto stages, validates and cuts over automatically — the
	// self-healing default.
	AdoptionAuto AdoptionPolicy = iota
	// AdoptionStage stages and validates automatically but leaves the
	// cutover to the operator (`erigon snapshots adopt`).
	AdoptionStage
	// AdoptionWarn does nothing automatic beyond warning; the operator
	// runs `erigon snapshots adopt` for the whole staged adoption.
	AdoptionWarn
)

func (p AdoptionPolicy) String() string {
	switch p {
	case AdoptionAuto:
		return "auto"
	case AdoptionStage:
		return "stage"
	case AdoptionWarn:
		return "warn"
	default:
		return "unknown"
	}
}

// ParseAdoptionPolicy maps a --snapshot.adoption-policy flag value to
// an AdoptionPolicy. An empty string defaults to AdoptionAuto.
func ParseAdoptionPolicy(s string) (AdoptionPolicy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "auto":
		return AdoptionAuto, nil
	case "stage":
		return AdoptionStage, nil
	case "warn":
		return AdoptionWarn, nil
	default:
		return AdoptionAuto, fmt.Errorf("unknown adoption policy %q (want auto, stage or warn)", s)
	}
}
