package aura_test

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/aura"
	"github.com/ledgerwatch/erigon/consensus/aura/test"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/require"
)

func TestVerifyHeader(t *testing.T) {
	var (
		auraDB    = memdb.NewTestDB(t)
		engine, _ = aura.NewAuRa(nil, auraDB, common.Address{}, test.AuthorityRoundBlockRewardContract)
		require   = require.New(t)
		signer1   = []byte("0x7d577a597b2742b498cb5cf0c26cdcd726d39e6e")
		//signer2 = []byte("0x82a978b3f5962a5b0957d9ee9eef472ee55b42f1")
	)

	// genspec := &core.Genesis{
	// 	ExtraData: make([]byte, aura.ExtraVanity+aura.ExtraSeal),
	// 	Mixhash:    common.Hash{},
	// 	Config:     params.TestChainAuraConfig,
	// }
	types.SetHeaderSealFlag(true)
	genspec := core.DefaultSokolGenesisBlock()

	m := stages.MockWithGenesisEngine(t, genspec, engine)
	m.EnableLogs()

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 25, func(i int, block *core.BlockGen) {
		block.SetCoinbase(common.BytesToAddress(signer1))
	}, false)

	require.NoError(err)

	for i, block := range chain.Blocks {
		header := block.Header()
		step := engine.GetStep()

		if i > 0 {

			header.ParentHash = chain.Blocks[i-1].Hash()

			// calculate the difficulty of the current header using parent and child step with 0 empty steps since they are meanigless right now
			parentStep, err := aura.HeaderStep(chain.Blocks[i-1].Header())

			if err != nil {
				t.Fatal("Couldnt get parentStep", err, parentStep)
			}

			diff := aura.CalculateScore(parentStep, step, 0).ToBig()
			header.Difficulty = diff
		}

		validatorSet := engine.GetValidatorSet()

		validator, err := aura.GetFromValidatorSet(validatorSet, header.ParentHash, uint(step), nil)

		if err != nil {
			t.Fatal("Couldnt extract validator from the validator set", err, validator)
		}

		encodedValidator, err := rlp.EncodeToBytes(validator)

		if err != nil {
			t.Fatal("Couldnt encode validator", err, encodedValidator)
		}

		encodedStep, err := rlp.EncodeToBytes(step)

		if err != nil {
			t.Fatal("Couldnt encode step", err, encodedStep)
		}

		header.Extra = make([]byte, aura.ExtraVanity+aura.ExtraSeal)

		copy(header.Extra[len(header.Extra)-aura.ExtraSeal:], append(encodedStep, encodedValidator...))

		header.Seal = make([]rlp.RawValue, 2)

		// adds seal param into the header index 0 is for the step and index 1 for the validator address
		header.Seal[0] = encodedStep
		header.Seal[1] = encodedValidator

		chain.Headers[i] = header
		chain.Blocks[i] = block.WithSeal(header)
	}

	// insert the first 20 blocks to make sure they are valid
	err = m.InsertChain(chain.Slice(1, 21))
	require.NoError(err)

	// inserting block 21 with future step err
	if err := m.InsertChain(chain.Slice(21, 22)); err == nil {
		t.Fatal("Couldnt recognize future step", err, 21)
	}

	// if err := m.InsertChain(chain.Slice(22, 23)); err == nil {
	// 	t.Fatal("Couldnt recognize multiple blocks in one step", err, 22)
	// }

	// if err := m.InsertChain(chain.Slice(23, 24)); err == nil {
	// 	t.Fatal("Couldnt recognize faulty uncle", err, 23)
	// }

}
