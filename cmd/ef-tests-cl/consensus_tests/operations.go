package consensustests

import (
	"fmt"
	"os"

	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cmd/erigon-cl/core/transition"
)

const (
	attestationFileName = "attestation.ssz_snappy"
)

func operationAttestationHandler(context testContext) error {
	preState, err := decodeStateFromFile(context, "pre.ssz_snappy")
	if err != nil {
		return err
	}
	postState, err := decodeStateFromFile(context, "post.ssz_snappy")
	expectedError := os.IsNotExist(err)
	if err != nil && !expectedError {
		return err
	}
	att := &cltypes.Attestation{}
	if err := decodeSSZObjectFromFile(att, context.version, attestationFileName); err != nil {
		return err
	}
	trans := transition.New(preState, &clparams.MainnetBeaconConfig, nil, false)
	if err := trans.ProcessAttestations([]*cltypes.Attestation{att}); err != nil {
		if expectedError {
			return nil
		}
		return err
	}
	if expectedError {
		return fmt.Errorf("expected error")
	}
	root, err := preState.HashSSZ()
	if err != nil {
		return err
	}
	expectedRoot, err := postState.HashSSZ()
	if err != nil {
		return err
	}
	if root != expectedRoot {
		return fmt.Errorf("mismatching state roots")
	}
	return nil
}
