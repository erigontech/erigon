package devvalidator

import (
	"strconv"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
)

// attestationSubmission describes the endpoint and body for submitting an
// attestation, which differs between the pre-Electra and Electra Beacon APIs.
type attestationSubmission struct {
	path    string
	body    interface{}
	version string // Eth-Consensus-Version header; empty for the pre-Electra V1 route
}

// buildAttestationSubmission constructs the correct attestation submission for
// the given fork version. The pre-Electra V1 pool endpoint takes an aggregate
// with aggregation bits; from Electra onwards a single validator's vote is a
// SingleAttestation submitted to the V2 endpoint with an Eth-Consensus-Version
// header.
func buildAttestationSubmission(
	version clparams.StateVersion,
	committeeIndex, validatorIndex uint64,
	attData *solid.AttestationData,
	sig common.Bytes96,
	committeeLength, validatorPosition uint64,
) attestationSubmission {
	if version >= clparams.ElectraVersion {
		single := map[string]interface{}{
			"committee_index": strconv.FormatUint(committeeIndex, 10),
			"attester_index":  strconv.FormatUint(validatorIndex, 10),
			"data":            attData,
			"signature":       hexutil.Encode(sig[:]),
		}
		return attestationSubmission{
			path:    "/eth/v2/beacon/pool/attestations",
			body:    []interface{}{single},
			version: version.String(),
		}
	}

	aggBits := make([]byte, (committeeLength+7)/8)
	aggBits[validatorPosition/8] |= 1 << (validatorPosition % 8)
	legacy := map[string]interface{}{
		"aggregation_bits": hexutil.Encode(aggBits),
		"data":             attData,
		"signature":        hexutil.Encode(sig[:]),
	}
	return attestationSubmission{
		path: "/eth/v1/beacon/pool/attestations",
		body: []interface{}{legacy},
	}
}
