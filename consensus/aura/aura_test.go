package aura_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/aura"
	"github.com/ledgerwatch/erigon/consensus/aura/test"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/require"
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
	engine, err := aura.NewAuRa(nil, auraDB, common.Address{}, test.AuthorityRoundBlockRewardContract)
	require.NoError(err)
	m := stages.MockWithGenesisEngine(t, core.DefaultSokolGenesisBlock(), engine)
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
