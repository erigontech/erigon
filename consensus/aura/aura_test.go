package aura_test

import (
	"context"
	"fmt"
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/consensus/aura"
	"github.com/ledgerwatch/erigon/consensus/aura/consensusconfig"
	"github.com/ledgerwatch/erigon/consensus/aura/test"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

/*
#[test]

	fn block_reward_contract() {
	    let spec = Spec::new_test_round_block_reward_contract();
	    let tap = Arc::new(AccountProvider::transient_provider());

	    let addr1 = tap.insert_account(keccak("1").into(), &"1".into()).unwrap();

	    let engine = &*spec.engine;
	    let genesis_header = spec.genesis_header();
	    let db1 = spec
	        .ensure_db_good(get_temp_state_db(), &Default::default())
	        .unwrap();
	    let db2 = spec
	        .ensure_db_good(get_temp_state_db(), &Default::default())
	        .unwrap();

	    let last_hashes = Arc::new(vec![genesis_header.hash()]);

	    let client = generate_dummy_client_with_spec(Spec::new_test_round_block_reward_contract);
	    engine.register_client(Arc::downgrade(&client) as _);

	    // step 2
	    let b1 = OpenBlock::new(
	        engine,
	        Default::default(),
	        false,
	        db1,
	        &genesis_header,
	        last_hashes.clone(),
	        addr1,
	        (3141562.into(), 31415620.into()),
	        vec![],
	        false,
	        None,
	    )
	    .unwrap();
	    let b1 = b1.close_and_lock().unwrap();

	    // since the block is empty it isn't sealed and we generate empty steps
	    engine.set_signer(Some(Box::new((tap.clone(), addr1, "1".into()))));
	    assert_eq!(engine.generate_seal(&b1, &genesis_header), Seal::None);
	    engine.step();

	    // step 3
	    // the signer of the accumulated empty step message should be rewarded
	    let b2 = OpenBlock::new(
	        engine,
	        Default::default(),
	        false,
	        db2,
	        &genesis_header,
	        last_hashes.clone(),
	        addr1,
	        (3141562.into(), 31415620.into()),
	        vec![],
	        false,
	        None,
	    )
	    .unwrap();
	    let addr1_balance = b2.state.balance(&addr1).unwrap();

	    // after closing the block `addr1` should be reward twice, one for the included empty step
	    // message and another for block creation
	    let b2 = b2.close_and_lock().unwrap();

	    // the contract rewards (1000 + kind) for each benefactor/reward kind
	    assert_eq!(
	        b2.state.balance(&addr1).unwrap(),
	        addr1_balance + (1000 + 0) + (1000 + 2),
	    )
	}
*/
func TestRewardContract(t *testing.T) {
	t.Skip("not ready yet")
	auraDB, require := memdb.NewTestDB(t), require.New(t)
	engine, err := aura.NewAuRa(nil, auraDB, libcommon.Address{}, test.AuthorityRoundBlockRewardContract)
	require.NoError(err)
	m := stages.MockWithGenesisEngine(t, core.ChiadoGenesisBlock(), engine, false)
	m.EnableLogs()

	var accBefore *accounts.Account
	err = auraDB.View(context.Background(), func(tx kv.Tx) (err error) { _, err = rawdb.ReadAccount(tx, m.Address, accBefore); return err })
	require.NoError(err)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, gen *core.BlockGen) {
		gen.SetCoinbase(m.Address)
	}, false /* intermediateHashes */)
	require.NoError(err)

	err = m.InsertChain(chain)
	require.NoError(err)

	var accAfter *accounts.Account
	err = auraDB.View(context.Background(), func(tx kv.Tx) (err error) { _, err = rawdb.ReadAccount(tx, m.Address, accAfter); return err })
	require.NoError(err)

	fmt.Printf("balance: %d\n", accAfter.Balance.Uint64())
	/*

	   	let spec = Spec::new_test_round_block_reward_contract();
	              let tap = Arc::new(AccountProvider::transient_provider());

	              let addr1 = tap.insert_account(keccak("1").into(), &"1".into()).unwrap();

	              let engine = &*spec.engine;
	              let genesis_header = spec.genesis_header();
	              let db1 = spec
	                  .ensure_db_good(get_temp_state_db(), &Default::default())
	                  .unwrap();
	              let db2 = spec
	                  .ensure_db_good(get_temp_state_db(), &Default::default())
	                  .unwrap();

	              let last_hashes = Arc::new(vec![genesis_header.hash()]);

	              let client = generate_dummy_client_with_spec(Spec::new_test_round_block_reward_contract);
	              engine.register_client(Arc::downgrade(&client) as _);

	              // step 2
	              let b1 = OpenBlock::new(
	                  engine,
	                  Default::default(),
	                  false,
	                  db1,
	                  &genesis_header,
	                  last_hashes.clone(),
	                  addr1,
	                  (3141562.into(), 31415620.into()),
	                  vec![],
	                  false,
	                  None,
	              )
	              .unwrap();
	              let b1 = b1.close_and_lock().unwrap();

	              // since the block is empty it isn't sealed and we generate empty steps
	              engine.set_signer(Some(Box::new((tap.clone(), addr1, "1".into()))));
	              assert_eq!(engine.generate_seal(&b1, &genesis_header), Seal::None);
	              engine.step();

	              // step 3
	              // the signer of the accumulated empty step message should be rewarded
	              let b2 = OpenBlock::new(
	                  engine,
	                  Default::default(),
	                  false,
	                  db2,
	                  &genesis_header,
	                  last_hashes.clone(),
	                  addr1,
	                  (3141562.into(), 31415620.into()),
	                  vec![],
	                  false,
	                  None,
	              )
	              .unwrap();
	              let addr1_balance = b2.state.balance(&addr1).unwrap();

	              // after closing the block `addr1` should be reward twice, one for the included empty step
	              // message and another for block creation
	              let b2 = b2.close_and_lock().unwrap();

	              // the contract rewards (1000 + kind) for each benefactor/reward kind
	              assert_eq!(
	                  b2.state.balance(&addr1).unwrap(),
	                  addr1_balance + (1000 + 0) + (1000 + 2),
	              )
	*/
}

// Check that the first block of Gnosis Chain, which doesn't have any transactions,
// does not change the state root.
func TestEmptyBlock(t *testing.T) {
	if ethconfig.EnableHistoryV3InTest {
		t.Skip("")
	}

	require := require.New(t)
	genesis := core.GnosisGenesisBlock()
	genesisBlock, _, err := core.GenesisToBlock(genesis, "")
	require.NoError(err)

	genesis.Config.TerminalTotalDifficultyPassed = false

	chainConfig := genesis.Config
	auraDB := memdb.NewTestDB(t)
	engine, err := aura.NewAuRa(chainConfig.Aura, auraDB, chainConfig.Aura.Etherbase, consensusconfig.GetConfigByChain(chainConfig.ChainName))
	require.NoError(err)
	m := stages.MockWithGenesisEngine(t, genesis, engine, false)

	time := uint64(1539016985)
	header := core.MakeEmptyHeader(genesisBlock.Header(), chainConfig, time, nil)
	header.UncleHash = types.EmptyUncleHash
	header.TxHash = trie.EmptyRoot
	header.ReceiptHash = trie.EmptyRoot
	header.Coinbase = libcommon.HexToAddress("0xcace5b3c29211740e595850e80478416ee77ca21")
	header.Difficulty = engine.CalcDifficulty(nil, time,
		0,
		genesisBlock.Difficulty(),
		genesisBlock.NumberU64(),
		genesisBlock.Hash(),
		genesisBlock.UncleHash(),
		genesisBlock.Header().AuRaStep,
	)

	block := types.NewBlockWithHeader(header)

	headers, blocks, receipts := make([]*types.Header, 1), make(types.Blocks, 1), make([]types.Receipts, 1)
	headers[0] = header
	blocks[0] = block

	chain := &core.ChainPack{Headers: headers, Blocks: blocks, Receipts: receipts, TopBlock: block}
	err = m.InsertChain(chain)
	require.NoError(err)
}
