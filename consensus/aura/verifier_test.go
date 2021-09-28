package aura_test

import (
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/aura"
	"github.com/ledgerwatch/erigon/consensus/aura/test"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestVerifyHeader(t *testing.T) {
	auraDB := memdb.NewTestDB(t)
	require := require.New(t)

	engine, err := aura.NewAuRa(nil, auraDB, common.Address{}, test.AuthorityRoundBlockRewardContract)
	//engine.FakeDiff = true

	engine.VerifyHeader()
}
