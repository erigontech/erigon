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

package handler

import (
	_ "embed"
	"testing"

	"github.com/erigontech/erigon/cl/beacon/beacontest"
	"github.com/erigontech/erigon/cl/clparams"
)

func TestHarnessPhase0(t *testing.T) {
	beacontest.Execute(
		append(
			defaultHarnessOpts(harnessConfig{t: t, v: clparams.Phase0Version}),
			beacontest.WithTestFromFs(Harnesses, "blocks"),
			beacontest.WithTestFromFs(Harnesses, "config"),
			beacontest.WithTestFromFs(Harnesses, "headers"),
			beacontest.WithTestFromFs(Harnesses, "committees"),
			beacontest.WithTestFromFs(Harnesses, "duties_attester"),
			beacontest.WithTestFromFs(Harnesses, "duties_proposer"),
		)...,
	)
}

func TestHarnessPhase0Finalized(t *testing.T) {
	beacontest.Execute(
		append(
			defaultHarnessOpts(harnessConfig{t: t, v: clparams.Phase0Version, finalized: true}),
			beacontest.WithTestFromFs(Harnesses, "liveness"),
			beacontest.WithTestFromFs(Harnesses, "duties_attester_f"),
			beacontest.WithTestFromFs(Harnesses, "committees_f"),
		)...,
	)
}

func TestHarnessBellatrix(t *testing.T) {
	beacontest.Execute(
		append(
			defaultHarnessOpts(harnessConfig{t: t, v: clparams.BellatrixVersion, finalized: true}),
			beacontest.WithTestFromFs(Harnesses, "attestation_rewards_bellatrix"),
			beacontest.WithTestFromFs(Harnesses, "duties_sync_bellatrix"),
			beacontest.WithTestFromFs(Harnesses, "lightclient"),
			beacontest.WithTestFromFs(Harnesses, "validators"),
			beacontest.WithTestFromFs(Harnesses, "lighthouse"),
			beacontest.WithTestFromFs(Harnesses, "blob_sidecars"),
		)...,
	)
}

func TestHarnessCapella(t *testing.T) {
	beacontest.Execute(
		append(
			defaultHarnessOpts(harnessConfig{t: t, v: clparams.CapellaVersion, finalized: true}),
			beacontest.WithTestFromFs(Harnesses, "expected_withdrawals"),
		)...,
	)
}

func TestHarnessForkChoice(t *testing.T) {
	beacontest.Execute(
		append(
			defaultHarnessOpts(harnessConfig{t: t, v: clparams.BellatrixVersion, forkmode: 1}),
			beacontest.WithTestFromFs(Harnesses, "fork_choice"),
		)...,
	)
}
