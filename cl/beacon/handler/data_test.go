package handler_test

import (
	"embed"
	"os"
	"strings"
	"testing"

	"github.com/ledgerwatch/erigon/cl/beacon/beacontest"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

//go:embed test_data/*
var testData embed.FS

var TestDatae = afero.NewBasePathFs(afero.FromIOFS{FS: testData}, "test_data")

//go:embed harness/*
var testHarness embed.FS

var Harnesses = afero.NewBasePathFs(afero.FromIOFS{FS: testHarness}, "harness")

func defaultHarnessOpts(t *testing.T) []beacontest.HarnessOption {
	logger := log.New()
	for _, v := range os.Args {
		if !strings.Contains(v, "test.v") || strings.Contains(v, "test.v=false") {
			logger.SetHandler(log.DiscardHandler())
		}
	}

	_, blocks, _, _, _, handler, _, _, fcu := setupTestingHandler(t, clparams.Phase0Version, logger)

	var err error
	fcu.HeadVal, err = blocks[len(blocks)-1].Block.HashSSZ()
	require.NoError(t, err)

	fcu.HeadSlotVal = blocks[len(blocks)-1].Block.Slot
	return []beacontest.HarnessOption{
		beacontest.WithTesting(t),
		beacontest.WithFilesystem("td", TestDatae),
		beacontest.WithHandler("i", handler),
	}
}
