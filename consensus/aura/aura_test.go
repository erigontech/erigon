package aura_test

import (
	"testing"
)

/*
#[test]
    fn reward_empty_steps() {
        let (spec, tap, accounts) = setup_empty_steps();

        let addr1 = accounts[0];

        let engine = &*spec.engine;
        let genesis_header = spec.genesis_header();
        let db1 = spec
            .ensure_db_good(get_temp_state_db(), &Default::default())
            .unwrap();
        let db2 = spec
            .ensure_db_good(get_temp_state_db(), &Default::default())
            .unwrap();

        let last_hashes = Arc::new(vec![genesis_header.hash()]);

        let client = generate_dummy_client_with_spec(Spec::new_test_round_empty_steps);
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

        // after closing the block `addr1` should be reward twice, one for the included empty step message and another for block creation
        let b2 = b2.close_and_lock().unwrap();

        // the spec sets the block reward to 10
        assert_eq!(b2.state.balance(&addr1).unwrap(), addr1_balance + (10 * 2))
    }
*/
func TestRewardEmptyStep(t *testing.T) {

}
