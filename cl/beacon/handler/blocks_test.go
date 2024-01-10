package handler_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/beacon/beacontest"

	_ "embed"
)

func TestHarnessBlindedBlock(t *testing.T) {
	beacontest.Execute(
		append(
			defaultHarnessOpts(t),
			beacontest.WithTestFromFs(Harnesses, "./blocks.yml"),
		)...,
	)
}
