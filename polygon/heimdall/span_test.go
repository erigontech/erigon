package heimdall

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/polygon/bor/valset"
	"github.com/ledgerwatch/erigon/polygon/heimdall/heimdalltest"
)

func TestSpanJsonMarshall(t *testing.T) {
	validators := []*valset.Validator{
		valset.NewValidator(libcommon.HexToAddress("deadbeef"), 1),
		valset.NewValidator(libcommon.HexToAddress("cafebabe"), 2),
	}

	validatorSet := valset.ValidatorSet{
		Validators: validators,
		Proposer:   validators[0],
	}

	span := Span{
		Id:                1,
		StartBlock:        100,
		EndBlock:          200,
		ValidatorSet:      validatorSet,
		SelectedProducers: []valset.Validator{*validators[0]},
		ChainID:           "bor",
	}

	heimdalltest.AssertJsonMarshalUnmarshal(t, &span)
}
