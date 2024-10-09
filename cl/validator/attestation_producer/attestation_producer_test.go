package attestation_producer_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/antiquary/tests"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/validator/attestation_producer"
	"github.com/stretchr/testify/require"
)

func TestAttestationProducer(t *testing.T) {
	attProducer := attestation_producer.New(&clparams.MainnetBeaconConfig)

	_, _, headState := tests.GetPhase0Random()

	att, err := attProducer.ProduceAndCacheAttestationData(headState, headState.Slot(), 0)
	require.NoError(t, err)

	attJson, err := att.MarshalJSON()
	require.NoError(t, err)

	// check if the json match with the expected value
	require.Equal(t, string(attJson), `{"slot":"8322","index":"0","beacon_block_root":"0xeffdd8ef40c3c901f0724d48e04ce257967cf1da31929f3b6db614f89ef8d660","source":{"epoch":"258","root":"0x31885d5a2405876b7203f9cc1a7e115b9977412107c51c81ab4fd49bde93905e"},"target":{"epoch":"260","root":"0x86979f6f6dc7626064ef0d38d4dffb89e91d1d4c18492e3fb7d7ee93cedca3ed"}}`)
}
