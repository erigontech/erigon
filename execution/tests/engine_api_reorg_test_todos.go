package executiontests

//
// TODO test for NewPayload with 2 block reorg back
//      -> it caches the shared doms
//      -> then do fcu for block at T-1
//      -> then for T (where new payload is)
//      -> flush extending fork should not happen at T because we've already done changes at T-1
//      (NotifyCurrentHeight and ClearWithUnwind should handle this i think but better write a test to be sure)
//
// TODO since we are going to need to create side chains by using a 2nd engine api tester then we won't need the
//      functionality for WithParentElBlock to create side chains
//      -> remove it and simplify the MockCl
//      -> write the tests with side chains
//      (Fcu with old canonical head should not start block building --- should add a test and fix for this in a follow up)
//
// TODO write a test with contract creation re-orgs with points3d (in a follow up)
//      to try and find the wrong trie root problem from prysm example
//      -> if points3d doesnt catch it then
//      -> explore if included withdrawals change the state root
//      -> explore if include blob txns change the state root
//

// NOTE: this one works but doesn't capture the issue with unwinds and then either bad root or bad nonce
//       might need some more sophisticated state writes like point3dfactory, withdrawals, transfers, deposits, etc.
//func TestEngineApiInvalidPayloadThenValidShouldSucceed(t *testing.T) {
//	DefaultEngineApiTester(t).Run(t, func(ctx context.Context, t *testing.T, eat EngineApiTester) {
//		// deploy changer at b2
//		transactOpts, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainId())
//		require.NoError(t, err)
//		_, txn, changer, err := contracts.DeployChanger(transactOpts, eat.ContractBackend)
//		require.NoError(t, err)
//		b2Canon, err := eat.MockCl.BuildCanonicalBlock(ctx)
//		require.NoError(t, err)
//		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b2Canon.ExecutionPayload, txn.Hash())
//		require.NoError(t, err)
//		// change changer at b3
//		txn, err = changer.Change(transactOpts)
//		require.NoError(t, err)
//		b3Canon, err := eat.MockCl.BuildCanonicalBlock(ctx)
//		require.NoError(t, err)
//		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b3Canon.ExecutionPayload, txn.Hash())
//		require.NoError(t, err)
//		// create a payload at b4 that changes the changer again but first try to insert a fault block at b4
//		txn, err = changer.Change(transactOpts)
//		require.NoError(t, err)
//		b4Canon, err := eat.MockCl.BuildNewPayload(ctx)
//		require.NoError(t, err)
//		b4Faulty := executiontests.TamperMockClPayloadStateRoot(b4Canon, common.HexToHash("0xb4f"))
//		status, err := eat.MockCl.InsertNewPayload(ctx, b4Faulty.ExecutionPayload, b4Faulty.ParentBeaconBlockRoot)
//		require.NoError(t, err)
//		require.Equal(t, enginetypes.InvalidStatus, status.Status)
//		require.True(t, strings.Contains(status.ValidationError.Error().Error(), "wrong trie root"))
//		status, err = eat.MockCl.InsertNewPayload(ctx, b4Canon.ExecutionPayload, b4Canon.ParentBeaconBlockRoot)
//		require.NoError(t, err)
//		require.Equal(t, enginetypes.ValidStatus, status.Status)
//		err = eat.MockCl.UpdateForkChoice(ctx, b4Canon.ExecutionPayload.BlockHash, b4Canon.ExecutionPayload.Timestamp.Uint64())
//		require.NoError(t, err)
//		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b4Canon.ExecutionPayload, txn.Hash())
//		require.NoError(t, err)
//	})
//}

//NOTE: this doesnt work because we cant do FCU block building at an older canonical ancestor as per https://github.com/ethereum/execution-apis/blob/main/src/engine/paris.md#engine_forkchoiceupdatedv1
//      for this we need to create 2 engine api testers with same genesis and create 2 diff chains
//func TestEngineApiInvalidPayloadSeveralBlocksBackThenValidShouldSucceed(t *testing.T) {
//	DefaultEngineApiTester(t).Run(t, func(ctx context.Context, t *testing.T, eat EngineApiTester) {
//		// deploy changer at b2
//		transactOpts, err := bind.NewKeyedTransactorWithChainID(eat.CoinbaseKey, eat.ChainId())
//		require.NoError(t, err)
//		_, txn, changer, err := contracts.DeployChanger(transactOpts, eat.ContractBackend)
//		require.NoError(t, err)
//		b2Canon, err := eat.MockCl.BuildCanonicalBlock(ctx)
//		require.NoError(t, err)
//		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b2Canon.ExecutionPayload, txn.Hash())
//		require.NoError(t, err)
//		// change changer at b3
//		txn, err = changer.Change(transactOpts)
//		require.NoError(t, err)
//		b3Canon, err := eat.MockCl.BuildCanonicalBlock(ctx)
//		require.NoError(t, err)
//		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b3Canon.ExecutionPayload, txn.Hash())
//		require.NoError(t, err)
//		// change changer at b4
//		txn, err = changer.Change(transactOpts)
//		require.NoError(t, err)
//		b4Canon, err := eat.MockCl.BuildCanonicalBlock(ctx)
//		require.NoError(t, err)
//		err = eat.TxnInclusionVerifier.VerifyTxnsInclusion(ctx, b4Canon.ExecutionPayload, txn.Hash())
//		require.NoError(t, err)
//		// create a side chain 2 blocks back and try to insert a faulty block and then the correct one
//		b3Side, err := eat.MockCl.BuildNewPayload(ctx, executiontests.WithParentElBlock(b2Canon.ExecutionPayload.BlockHash))
//		require.NoError(t, err)
//		// building a new payload at b3Side unwinds the fork choice to b2Canon, so we set it back to b4Canon
//		err = eat.MockCl.UpdateForkChoice(ctx, b4Canon.ExecutionPayload.BlockHash, b4Canon.ExecutionPayload.Timestamp.Uint64())
//		require.NoError(t, err)
//		b3SideFaulty := executiontests.TamperMockClPayloadStateRoot(b3Side, common.HexToHash("0xb3f"))
//		status, err := eat.MockCl.InsertNewPayload(ctx, b3SideFaulty.ExecutionPayload, b3Side.ParentBeaconBlockRoot)
//		require.Error(t, err)
//		require.Equal(t, enginetypes.InvalidStatus, status.Status)
//		require.True(t, strings.Contains(status.ValidationError.Error().Error(), "wrong trie root"))
//		status, err = eat.MockCl.InsertNewPayload(ctx, b3Side.ExecutionPayload, b3Side.ParentBeaconBlockRoot)
//		require.NoError(t, err)
//		require.Equal(t, enginetypes.ValidStatus, status.Status)
//	})
//}
