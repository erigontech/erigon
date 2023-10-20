package snapshot_format_test

// func TestAttestationsEncoding(t *testing.T) {
// 	attVec := solid.NewDynamicListSSZ[*solid.Attestation](256)
// 	for i := 0; i < 256; i++ {
// 		attVec.Append(solid.NewAttestionFromParameters(
// 			[]byte{byte(i)},
// 			solid.NewAttestionDataFromParameters(
// 				uint64(i*i*i),
// 				uint64(i*i*i),
// 				[32]byte{},
// 				solid.NewCheckpointFromParameters([32]byte{45, 67}, 219),
// 				solid.NewCheckpointFromParameters([32]byte{67, 98}, 219),
// 			), libcommon.Bytes96{byte(i)}))
// 	}
// 	plain, err := attVec.EncodeSSZ(nil)
// 	require.NoError(t, err)

// 	compacted := format.EncodeAttestationsForStorage(attVec, nil)
// 	require.Less(t, len(compacted), len(plain))

// 	// Now-decode it back.
// 	resAttVec := solid.NewDynamicListSSZ[*solid.Attestation](256)
// 	require.NoError(t, format.DecodeAttestationsForStorage(compacted, resAttVec))

// 	require.Equal(t, attVec.Len(), resAttVec.Len())

// 	for i := 0; i < 256; i++ {
// 		require.Equal(t, attVec.Get(i).Signature(), resAttVec.Get(i).Signature())
// 		require.Equal(t, attVec.Get(i).AggregationBits(), resAttVec.Get(i).AggregationBits())

// 		require.Equal(t, attVec.Get(i).AttestantionData().Slot(), resAttVec.Get(i).AttestantionData().Slot())
// 		require.Equal(t, attVec.Get(i).AttestantionData().ValidatorIndex(), resAttVec.Get(i).AttestantionData().ValidatorIndex())
// 		require.Equal(t, attVec.Get(i).AttestantionData().BeaconBlockRoot(), resAttVec.Get(i).AttestantionData().BeaconBlockRoot())
// 		require.Equal(t, attVec.Get(i).AttestantionData().Source(), resAttVec.Get(i).AttestantionData().Source())
// 		require.Equal(t, attVec.Get(i).AttestantionData().Target(), resAttVec.Get(i).AttestantionData().Target())

// 		require.Equal(t, attVec.Get(i), resAttVec.Get(i))
// 	}
// }
