package aura_test

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/aura"
	"github.com/ledgerwatch/erigon/consensus/aura/test"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/require"
)

func TestVerifyHeader(t *testing.T) {

	auraDB, require := memdb.NewTestDB(t), require.New(t)
	engine, err := aura.NewAuRa(nil, auraDB, common.Address{}, test.AuthorityRoundBlockRewardContract)

	require.NoError(err)

	m := stages.MockWithGenesisEngine(t, core.DefaultSokolGenesisBlock(), engine)
	m.EnableLogs()

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 23, func(i int, gen *core.BlockGen) {
		switch i {
		case 21:
			header := gen.GetHeader()
			currentStep := engine.GetStep()

			newStep, err := rlp.EncodeToBytes(currentStep + 1)

			if err != nil {
				panic(err)
			}
			// assinging future step to the 21st block
			header.Seal[0] = newStep
		case 22:

			header := gen.GetHeader()
			currentStep := engine.GetStep()

			newStep, err := rlp.EncodeToBytes(currentStep - 1)

			if err != nil {
				panic(err)
			}
			// assinging previous step to the 22nd block
			// validator tries multiple blocks at the same
			header.Seal[0] = newStep

		case 23:
			// adding faulty previous block to uncle
			previousBlock := gen.PrevBlock(1).Header()

			gen.AddUncle(previousBlock)

		}

	}, false)

	require.NoError(err)

	err = m.InsertChain(chain)
	require.NoError(err)

	for i, block := range chain.Blocks {
		switch i {
		case 21:
			err := engine.VerifyHeader(auraDB, block.Header(), false)

			if err == nil {
				t.Fatal("did not identify that the step was a future step", err, block.Header())
			}

		case 22:
			err := engine.VerifyHeader(auraDB, block.Header(), false)

			if err == nil {
				t.Fatal("did not identify that the validator added multiple blocks on the same step", err, block.Header())
			}

		case 23:
			err := engine.VerifyHeader(auraDB, block.Header(), false)

			if err == nil {
				t.Fatal("did not identify that the uncle block is faulty", err, block.Header())
			}
		}
	}

}
