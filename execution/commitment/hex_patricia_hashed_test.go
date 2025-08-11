// Copyright 2022 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commitment

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/empty"
	"github.com/erigontech/erigon-lib/common/length"
)

func Test_HexPatriciaHashed_ResetThenSingularUpdates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(1, ms)
	hph.SetTrace(false)
	plainKeys, updates := NewUpdateBuilder().
		Balance("00", 4).
		Balance("01", 5).
		Balance("02", 6).
		Balance("03", 7).
		Balance("04", 8).
		Storage("04", "01", "0401").
		Storage("03", "56", "050505").
		Storage("03", "57", "060606").
		Balance("05", 9).
		Storage("05", "02", "8989").
		Storage("05", "04", "9898").
		Build()

	upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()

	fmt.Printf("1. Generated %d updates\n", len(updates))
	//renderUpdates(branchNodeUpdates)

	err := ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	firstRootHash, err := hph.Process(ctx, upds, "")
	require.NoError(t, err)

	t.Logf("rootHash %x\n", firstRootHash)

	hph.Reset()
	//hph.SetTrace(true)
	plainKeys, updates = NewUpdateBuilder().
		Storage("03", "58", "050506").
		Build()
	fmt.Printf("2. Generated single update %s\n", updates[0].String())

	WrapKeyUpdatesInto(t, upds, plainKeys, updates)

	err = ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	secondRootHash, err := hph.Process(ctx, upds, "")
	require.NoError(t, err)
	require.NotEqual(t, firstRootHash, secondRootHash)
	t.Logf("rootHash %x\n", secondRootHash)

	hph.Reset()
	plainKeys, updates = NewUpdateBuilder().
		Storage("03", "58", "020807").
		Build()

	fmt.Printf("3. Generated single update %s\n", updates[0].String())
	err = ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	WrapKeyUpdatesInto(t, upds, plainKeys, updates)

	thirdRootHash, err := hph.Process(ctx, upds, "")
	t.Logf("rootHash %x\n", thirdRootHash)
	require.NoError(t, err)
	require.NotEqual(t, secondRootHash, thirdRootHash)
}

func Test_HexPatriciaHashed_EmptyUpdate(t *testing.T) {
	t.Parallel()

	ms := NewMockState(t)
	ctx := context.Background()
	hph := NewHexPatriciaHashed(1, ms)
	hph.SetTrace(false)
	plainKeys, updates := NewUpdateBuilder().
		Balance("00", 4).
		Nonce("00", 246462653).
		Balance("01", 5).
		CodeHash("03", "aaaaaaaaaaf7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a870").
		Delete("00").
		Storage("04", "01", "0401").
		Storage("03", "56", "050505").
		Build()

	err := ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()

	hashBeforeEmptyUpdate, err := hph.Process(ctx, upds, "")
	require.NoError(t, err)
	require.NotEmpty(t, hashBeforeEmptyUpdate)

	fmt.Printf("1. Applied %d updates\n", len(updates))
	//renderUpdates(branchNodeUpdates)

	// generate empty updates and do NOT reset tree
	plainKeys, updates = NewUpdateBuilder().Build()

	err = ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	WrapKeyUpdatesInto(t, upds, plainKeys, updates)

	hashAfterEmptyUpdate, err := hph.Process(ctx, upds, "")
	require.NoError(t, err)

	fmt.Println("2. Empty updates applied without state reset")
	require.Equal(t, hashBeforeEmptyUpdate, hashAfterEmptyUpdate)
}

func Test_HexPatriciaHashed_UniqueRepresentation2(t *testing.T) {
	t.Parallel()

	msOne := NewMockState(t)
	msTwo := NewMockState(t)
	ctx := context.Background()

	plainKeys, updates := NewUpdateBuilder().
		Balance("71562b71999873db5b286df957af199ec94617f7", 999860099).
		Nonce("71562b71999873db5b286df957af199ec94617f7", 3).
		Balance("3a220f351252089d385b29beca14e27f204c296a", 900234).
		Balance("0000000000000000000000000000000000000000", 2000000000000138901).
		Balance("1337beef00000000000000000000000000000000", 4000000000000138901).
		Build()

	trieOne := NewHexPatriciaHashed(length.Addr, msOne)
	trieTwo := NewHexPatriciaHashed(length.Addr, msTwo)

	//trieOne.SetTrace(true)
	//trieTwo.SetTrace(true)

	var rSeq, rBatch []byte
	{
		fmt.Printf("1. Trie sequential update (%d updates)\n", len(updates))
		for i := 0; i < len(updates); i++ {
			err := msOne.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1])
			require.NoError(t, err)

			updsOne := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[i:i+1], updates[i:i+1])

			sequentialRoot, err := trieOne.Process(ctx, updsOne, "")
			require.NoError(t, err)

			t.Logf("sequential root @%d hash %x\n", i, sequentialRoot)
			rSeq = common.Copy(sequentialRoot)

			updsOne.Close()
		}
	}
	{
		err := msTwo.applyPlainUpdates(plainKeys, updates)
		require.NoError(t, err)

		updsTwo := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)

		fmt.Printf("\n2. Trie batch update (%d updates)\n", len(updates))
		rh, err := trieTwo.Process(ctx, updsTwo, "")
		require.NoError(t, err)
		t.Logf("batch of %d root hash %x\n", len(updates), rh)

		updsTwo.Close()

		rBatch = common.Copy(rh)
	}
	require.Equal(t, rSeq, rBatch, "sequential and batch root should match")

	plainKeys, updates = NewUpdateBuilder().
		Balance("71562b71999873db5b286df957af199ec94617f7", 2345234560099).
		Nonce("71562b71999873db5b286df957af199ec94617f7", 4).
		Balance("3a220f351252089d385b29beca14e27f204c296a", 820234).
		Balance("0000000000000000000000000000000000000000", 3000000000000138901).
		Build()

	{
		fmt.Printf("\n3. Trie follow-up update (%d updates)\n", len(updates))
		for i := 0; i < len(updates); i++ {
			err := msOne.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1])
			require.NoError(t, err)

			updsOne := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[i:i+1], updates[i:i+1])

			sequentialRoot, err := trieOne.Process(ctx, updsOne, "")
			require.NoError(t, err)

			t.Logf("sequential root @%d hash %x\n", i, sequentialRoot)
			rSeq = common.Copy(sequentialRoot)

			updsOne.Close()
		}
	}
	{
		fmt.Printf("\n2. Trie batch update (%d updates)\n", len(updates))
		err := msTwo.applyPlainUpdates(plainKeys, updates)
		require.NoError(t, err)

		updsTwo := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)

		rh, err := trieTwo.Process(ctx, updsTwo, "")
		require.NoError(t, err)
		t.Logf("batch of %d root hash %x\n", len(updates), rh)

		rBatch = common.Copy(rh)
		updsTwo.Close()
	}
	require.Equal(t, rBatch, rSeq, "sequential and batch root should match")
}

func Test_Trie_CorrectSwitchForConcurrentAndSequential(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ms := NewMockState(t)
	hph := NewHexPatriciaHashed(length.Addr, ms)
	hph.SetTrace(false)

	// generate list of updates diverging from first nibble (good case for parallelization))
	plainKeysList, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 0, 150)
	builder := NewUpdateBuilder()
	for i := 0; i < len(plainKeysList); i++ {
		builder.Balance(common.Bytes2Hex(plainKeysList[i]), uint64(i))
	}

	plainKeys, updates := builder.Build()
	err := ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	toProcess := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer toProcess.Close()

	_, err = hph.Process(ctx, toProcess, "")
	require.NoError(t, err)

	ms.SetConcurrentCommitment(true)
	paratrie := NewConcurrentPatriciaHashed(hph, ms)
	canParallel, err := paratrie.CanDoConcurrentNext()
	require.NoError(t, err)
	require.True(t, canParallel, "should be able to parallelize next run")

	builder = NewUpdateBuilder()
	for i := 2; i < len(plainKeys); i++ {
		builder.Delete(common.Bytes2Hex(plainKeys[i]))
	}
	plainKeys, updates = builder.Build()
	err = ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	toProcess2 := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer toProcess2.Close()

	_, err = paratrie.Process(ctx, toProcess2, "")
	require.NoError(t, err)

	canParallel, err = paratrie.CanDoConcurrentNext()
	require.NoError(t, err)
	require.False(t, canParallel, "should be NOT able to parallelize next run")
}

func Test_HexPatriciaHashed_BrokenUniqueReprParallel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	uniqTest := func(t *testing.T, sortHashedKeys bool, trace bool) {
		t.Helper()

		stateSeq := NewMockState(t)
		stateBatch := NewMockState(t)

		plainKeys, updates := NewUpdateBuilder().
			Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
			Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
			Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1233).
			Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
			Balance("27456647f49ba65e220e86cba9abfc4fc1587b81", 065606).
			Balance("b13363d527cdc18173c54ac5d4a54af05dbec22e", 4*1e17).
			Balance("d995768ab23a0a333eb9584df006da740e66f0aa", 5).
			Balance("eabf041afbb6c6059fbd25eab0d3202db84e842d", 6).
			Balance("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", 7).
			Balance("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", 100000).
			Storage("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "0fa41642c48ecf8f2059c275353ce4fee173b3a8ce5480f040c4d2901603d14e", "050505").
			Balance("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", 9*1e16).
			Storage("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", "de3fea338c95ca16954e80eb603cd81a261ed6e2b10a03d0c86cf953fe8769a4", "060606").
			Balance("14c4d3bba7f5009599257d3701785d34c7f2aa27", 6*1e18).
			Nonce("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 169356).
			Storage("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", "9f49fdd48601f00df18ebc29b1264e27d09cf7cbd514fe8af173e534db038033", "8989").
			Storage("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", "d1664244ae1a8a05f8f1d41e45548fbb7aa54609b985d6439ee5fd9bb0da619f", "9898").
			Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
			Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
			Build()

		keyLen := 20
		trieSequential := NewHexPatriciaHashed(keyLen, stateSeq)

		stateBatch.SetConcurrentCommitment(true)
		trieBatchR := NewHexPatriciaHashed(keyLen, stateBatch)
		trieBatch := NewConcurrentPatriciaHashed(trieBatchR, stateBatch)

		if sortHashedKeys {
			plainKeys, updates = sortUpdatesByHashIncrease(t, trieSequential, plainKeys, updates)
		}

		//trieSequential.SetTrace(trace)
		//trieBatch.SetParticularTrace(trace, 9)

		var rSeq, rBatch []byte
		{
			fmt.Printf("1. Trie sequential update (%d updates)\n", len(updates))
			for i := 0; i < len(updates); i++ {
				err := stateSeq.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1])
				require.NoError(t, err)

				updsOne := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[i:i+1], updates[i:i+1])

				sequentialRoot, err := trieSequential.Process(ctx, updsOne, "")
				require.NoError(t, err)

				t.Logf("sequential root @%d hash %x\n", i, sequentialRoot)
				rSeq = common.Copy(sequentialRoot)

				updsOne.Close()
			}
		}
		{
			// exec few lines first so root is not empty
			err := stateBatch.applyPlainUpdates(plainKeys[:3], updates[:3])
			require.NoError(t, err)

			updsOne := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[:3], updates[:3])

			startRoot, err := trieBatchR.Process(ctx, updsOne, "")
			require.NoError(t, err)

			fmt.Printf("\nBatch will start with %x\n", startRoot)

			fmt.Printf("\n2. Trie batch update (%d updates)\n", len(updates))

			err = stateBatch.applyPlainUpdates(plainKeys[3:], updates[3:])
			require.NoError(t, err)

			updsTwo := WrapKeyUpdatesParallel(t, ModeDirect, KeyToHexNibbleHash, plainKeys[3:], updates[3:])

			rh, err := trieBatch.Process(ctx, updsTwo, "")
			require.NoError(t, err)
			t.Logf("batch of %d root hash %x\n", len(updates), rh)

			rBatch = common.Copy(rh)
			updsTwo.Close()
		}
		require.Equal(t, rBatch, rSeq, "sequential and batch root should match")

		plainKeys, updates = NewUpdateBuilder().
			Delete("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9").
			Delete("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2").
			Build()

		if sortHashedKeys {
			plainKeys, updates = sortUpdatesByHashIncrease(t, trieSequential, plainKeys, updates)
		}

		trieSequential.SetTrace(false)
		trieBatch.SetTrace(false)

		{
			fmt.Printf("3. Trie sequential update (%d updates)\n", len(updates))
			for i := 0; i < len(updates); i++ {
				err := stateSeq.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1])
				require.NoError(t, err)

				updsOne := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[i:i+1], updates[i:i+1])

				sequentialRoot, err := trieSequential.Process(ctx, updsOne, "")
				require.NoError(t, err)

				t.Logf("3) sequential root @%d hash %x\n", i, sequentialRoot)
				rSeq = common.Copy(sequentialRoot)

				updsOne.Close()
			}
		}
		{
			fmt.Printf("\n4. Trie batch update (%d updates)\n", len(updates))
			fmt.Printf("active rows %d touchmap %16b aftermap %16b\n", trieBatchR.activeRows, trieBatchR.touchMap[0], trieBatchR.afterMap[0])

			err := stateBatch.applyPlainUpdates(plainKeys[:], updates[:])
			require.NoError(t, err)

			updsTwo := WrapKeyUpdatesParallel(t, ModeDirect, KeyToHexNibbleHash, plainKeys[:], updates[:])

			rh, err := trieBatch.Process(ctx, updsTwo, "")
			require.NoError(t, err)
			t.Logf("batch of %d root hash %x\n", len(updates), rh)

			rBatch = common.Copy(rh)
			updsTwo.Close()
		}
		require.Equal(t, rBatch, rSeq, "sequential and batch root should match")

		plainKeys, updates = NewUpdateBuilder().
			Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 130).
			Storage("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "0fa41642c48ecf8f2059c275353ce4fee173b3a8ce5480f040c4d2901603d14e", "050545").
			Storage("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "fff41642c48ecf8f2059c275353ce4fee173b3a8ce5480f040c4d2901603d14e", "050545").
			Storage("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "eff41642c48ecf8f2059c275353ce4fee173b3a8ce5480f040c4d2901603d14e", "050545").
			DeleteStorage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed").
			Balance("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", 1300000000000).
			Build()

		if sortHashedKeys {
			plainKeys, updates = sortUpdatesByHashIncrease(t, trieSequential, plainKeys, updates)
		}

		trieSequential.SetTrace(false)
		trieBatch.SetTrace(false)

		{
			fmt.Printf("5. Trie sequential update (%d updates)\n", len(updates))
			for i := 0; i < len(updates); i++ {
				err := stateSeq.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1])
				require.NoError(t, err)

				updsOne := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[i:i+1], updates[i:i+1])

				sequentialRoot, err := trieSequential.Process(ctx, updsOne, "")
				require.NoError(t, err)

				t.Logf("3) sequential root @%d hash %x\n", i, sequentialRoot)
				rSeq = common.Copy(sequentialRoot)

				updsOne.Close()
			}
		}
		{
			fmt.Printf("\n6. Trie batch update (%d updates)\n", len(updates))
			fmt.Printf("active rows %d touchmap %16b aftermap %16b\n", trieBatchR.activeRows, trieBatchR.touchMap[0], trieBatchR.afterMap[0])

			err := stateBatch.applyPlainUpdates(plainKeys[:], updates[:])
			require.NoError(t, err)

			updsTwo := WrapKeyUpdatesParallel(t, ModeDirect, KeyToHexNibbleHash, plainKeys[:], updates[:])

			rh, err := trieBatch.Process(ctx, updsTwo, "")
			require.NoError(t, err)
			t.Logf("batch of %d root hash %x\n", len(updates), rh)

			rBatch = common.Copy(rh)
			updsTwo.Close()
		}
		require.Equal(t, rBatch, rSeq, "sequential and batch root should match")
	}

	// Same PLAIN prefix is not necessary while HASHED CPL>0 is required
	t.Run("InsertStorageWhenCPL==0", func(t *testing.T) {
		// ordering of keys differs
		uniqTest(t, true, true)
	})
	t.Run("InsertStorageWhenCPL>0", func(t *testing.T) {
		// ordering of keys differs
		uniqTest(t, false, false)
	})
}

func Test_ParallelHexPatriciaHashed_EdgeCases(t *testing.T) {
	t.Parallel()

	// generate subtrie with 4 keys with the same prefix
	plainKeysList, hashedKeysList := generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 4, 4)

	builder := NewUpdateBuilder()

	for i := 0; i < len(plainKeysList); i++ {
		fmt.Printf("added %x -> %x\n", plainKeysList[i], hashedKeysList[i])
		builder.Balance(common.Bytes2Hex(plainKeysList[i]), 1000*uint64(i))
	}

	// generate another 4 keys with the same prefix
	plainKeysList, hashedKeysList = generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 4, 4)

	for i := 0; i < len(plainKeysList); i++ {
		fmt.Printf("added %x -> %x\n", plainKeysList[i], hashedKeysList[i])
		builder.Balance(common.Bytes2Hex(plainKeysList[i]), 1000*uint64(i))
	}

	plainKeys, updates := builder.Build()
	_, _ = plainKeys, updates

	stateSeq := NewMockState(t)
	stateBatch := NewMockState(t)

	// plainKeys, updates := NewUpdateBuilder().
	// 	Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
	// 	Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
	// 	Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1233).
	// 	Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
	// 	Balance("27456647f49ba65e220e86cba9abfc4fc1587b81", 065606).
	// 	Balance("b13363d527cdc18173c54ac5d4a54af05dbec22e", 4*1e17).
	// 	Balance("d995768ab23a0a333eb9584df006da740e66f0aa", 5).
	// 	Balance("eabf041afbb6c6059fbd25eab0d3202db84e842d", 6).
	// 	Balance("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", 7).
	// 	Balance("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", 100000).
	// 	Storage("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "0fa41642c48ecf8f2059c275353ce4fee173b3a8ce5480f040c4d2901603d14e", "050505").
	// 	Balance("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", 9*1e16).
	// 	Storage("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", "de3fea338c95ca16954e80eb603cd81a261ed6e2b10a03d0c86cf953fe8769a4", "060606").
	// 	Balance("14c4d3bba7f5009599257d3701785d34c7f2aa27", 6*1e18).
	// 	Nonce("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 169356).
	// 	Storage("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", "9f49fdd48601f00df18ebc29b1264e27d09cf7cbd514fe8af173e534db038033", "8989").
	// 	Storage("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", "d1664244ae1a8a05f8f1d41e45548fbb7aa54609b985d6439ee5fd9bb0da619f", "9898").
	// 	Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
	// 	Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
	// 	Build()

	trieSequential := NewHexPatriciaHashed(length.Addr, stateSeq)

	stateBatch.SetConcurrentCommitment(true)
	trieBatchR := NewHexPatriciaHashed(length.Addr, stateBatch)
	trieBatch := NewConcurrentPatriciaHashed(trieBatchR, stateBatch)

	plainKeys, updates = sortUpdatesByHashIncrease(t, trieSequential, plainKeys, updates)
	ctx := context.Background()

	trieSequential.SetTrace(false)
	trieBatch.SetTrace(false)

	var rSeq, rBatch []byte
	{
		fmt.Printf("1. Trie sequential update (%d updates)\n", len(updates))
		// for i := 0; i < len(updates); i++ {
		// err := stateSeq.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1])
		err := stateSeq.applyPlainUpdates(plainKeys[:], updates[:])
		require.NoError(t, err)

		updsOne := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[:], updates[:])

		sequentialRoot, err := trieSequential.Process(ctx, updsOne, "")
		require.NoError(t, err)

		t.Logf("sequential root @%d hash %x\n", len(plainKeys), sequentialRoot)
		rSeq = common.Copy(sequentialRoot)

		updsOne.Close()
		// }
	}
	{
		// // exec few lines first so root is not empty
		// err := stateBatch.applyPlainUpdates(plainKeys[:3], updates[:3])
		// require.NoError(t, err)

		// updsOne := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[:3], updates[:3])

		// startRoot, err := trieBatchR.Process(ctx, updsOne, "")
		// require.NoError(t, err)

		// fmt.Printf("\nBatch will start with %x\n", startRoot)

		fmt.Printf("\n2. Trie batch update (%d updates)\n", len(updates))

		err := stateBatch.applyPlainUpdates(plainKeys[:], updates[:])
		require.NoError(t, err)

		updsTwo := WrapKeyUpdatesParallel(t, ModeDirect, KeyToHexNibbleHash, plainKeys[:], updates[:])

		rh, err := trieBatch.Process(ctx, updsTwo, "")
		require.NoError(t, err)
		t.Logf("batch of %d root hash %x\n", len(updates), rh)

		rBatch = common.Copy(rh)
		updsTwo.Close()
	}
	require.Equal(t, rBatch, rSeq, "sequential and batch root should match")
}

func Test_HexPatriciaHashed_BrokenUniqueRepr(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	uniqTest := func(t *testing.T, sortHashedKeys bool, trace bool) {
		t.Helper()

		stateSeq := NewMockState(t)
		stateBatch := NewMockState(t)

		plainKeys, updates := NewUpdateBuilder().
			Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
			Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
			Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1233).
			Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
			Balance("27456647f49ba65e220e86cba9abfc4fc1587b81", 065606).
			Balance("b13363d527cdc18173c54ac5d4a54af05dbec22e", 4*1e17).
			Balance("d995768ab23a0a333eb9584df006da740e66f0aa", 5).
			Balance("eabf041afbb6c6059fbd25eab0d3202db84e842d", 6).
			Balance("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", 7).
			Balance("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", 100000).
			Storage("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "0fa41642c48ecf8f2059c275353ce4fee173b3a8ce5480f040c4d2901603d14e", "050505").
			Balance("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", 9*1e16).
			Storage("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", "de3fea338c95ca16954e80eb603cd81a261ed6e2b10a03d0c86cf953fe8769a4", "060606").
			Balance("14c4d3bba7f5009599257d3701785d34c7f2aa27", 6*1e18).
			Nonce("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 169356).
			Storage("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", "9f49fdd48601f00df18ebc29b1264e27d09cf7cbd514fe8af173e534db038033", "8989").
			Storage("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", "d1664244ae1a8a05f8f1d41e45548fbb7aa54609b985d6439ee5fd9bb0da619f", "9898").
			Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
			Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
			Build()

		keyLen := 20
		trieSequential := NewHexPatriciaHashed(keyLen, stateSeq)
		trieBatch := NewHexPatriciaHashed(keyLen, stateBatch)

		if sortHashedKeys {
			plainKeys, updates = sortUpdatesByHashIncrease(t, trieSequential, plainKeys, updates)
		}

		trieSequential.SetTrace(trace)
		trieBatch.SetTrace(trace)

		var rSeq, rBatch []byte
		{
			fmt.Printf("1. Trie sequential update (%d updates)\n", len(updates))
			for i := 0; i < len(updates); i++ {
				err := stateSeq.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1])
				require.NoError(t, err)

				updsOne := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[i:i+1], updates[i:i+1])

				sequentialRoot, err := trieSequential.Process(ctx, updsOne, "")
				require.NoError(t, err)

				t.Logf("sequential root @%d hash %x\n", i, sequentialRoot)
				rSeq = common.Copy(sequentialRoot)

				updsOne.Close()
			}
		}
		{
			fmt.Printf("\n2. Trie batch update (%d updates)\n", len(updates))
			err := stateBatch.applyPlainUpdates(plainKeys, updates)
			require.NoError(t, err)

			updsTwo := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)

			rh, err := trieBatch.Process(ctx, updsTwo, "")
			require.NoError(t, err)
			t.Logf("batch of %d root hash %x\n", len(updates), rh)

			rBatch = common.Copy(rh)
			updsTwo.Close()
		}
		require.Equal(t, rBatch, rSeq, "sequential and batch root should match")
	}

	// Same PLAIN prefix is not necessary while HASHED CPL>0 is required
	t.Run("InsertStorageWhenCPL==0", func(t *testing.T) {
		// ordering of keys differs
		uniqTest(t, true, false)
	})
	t.Run("InsertStorageWhenCPL>0", func(t *testing.T) {
		// ordering of keys differs
		uniqTest(t, false, false)
	})
}

func Test_HexPatriciaHashed_UniqueRepresentation(t *testing.T) {
	ctx := context.Background()
	stateSeq := NewMockState(t)
	stateBatch := NewMockState(t)

	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
		Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
		Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1233).
		Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
		Balance("27456647f49ba65e220e86cba9abfc4fc1587b81", 065606).
		Balance("b13363d527cdc18173c54ac5d4a54af05dbec22e", 4*1e17).
		Balance("d995768ab23a0a333eb9584df006da740e66f0aa", 5).
		Balance("eabf041afbb6c6059fbd25eab0d3202db84e842d", 6).
		Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1237).
		Balance("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", 7).
		Balance("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", 5*1e17).
		Storage("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "0fa41642c48ecf8f2059c275353ce4fee173b3a8ce5480f040c4d2901603d14e", "050505").
		CodeHash("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed").
		Balance("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", 9*1e16).
		Storage("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", "de3fea338c95ca16954e80eb603cd81a261ed6e2b10a03d0c86cf953fe8769a4", "060606").
		Balance("14c4d3bba7f5009599257d3701785d34c7f2aa27", 6*1e18).
		Nonce("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 169356).
		Storage("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", "9f49fdd48601f00df18ebc29b1264e27d09cf7cbd514fe8af173e534db038033", "8989").
		Storage("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", "d1664244ae1a8a05f8f1d41e45548fbb7aa54609b985d6439ee5fd9bb0da619f", "9898").
		Build()

	trieSequential := NewHexPatriciaHashed(length.Addr, stateSeq)
	trieSequential.trace = true
	trieBatch := NewHexPatriciaHashed(length.Addr, stateBatch)
	trieBatch.trace = true

	plainKeys, updates = sortUpdatesByHashIncrease(t, trieSequential, plainKeys, updates)

	// trieSequential.SetTrace(true)
	// trieBatch.SetTrace(true)

	var rSeq, rBatch []byte
	{
		fmt.Printf("1. Trie sequential update (%d updates)\n", len(updates))
		for i := 0; i < len(updates); i++ {
			err := stateSeq.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1])
			require.NoError(t, err)

			updsOne := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[i:i+1], updates[i:i+1])

			sequentialRoot, err := trieSequential.Process(ctx, updsOne, "")
			require.NoError(t, err)

			t.Logf("sequential root @%d hash %x\n", i, sequentialRoot)
			rSeq = common.Copy(sequentialRoot)

			updsOne.Close()
		}
	}
	{
		fmt.Printf("\n2. Trie batch update (%d updates)\n", len(updates))
		err := stateBatch.applyPlainUpdates(plainKeys, updates)
		require.NoError(t, err)

		updsTwo := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)

		rh, err := trieBatch.Process(ctx, updsTwo, "")
		require.NoError(t, err)
		t.Logf("batch of %d root hash %x\n", len(updates), rh)

		rBatch = common.Copy(rh)
		updsTwo.Close()
	}
	require.Equal(t, rBatch, rSeq, "sequential and batch root should match")
}

func Test_HexPatriciaHashed_Sepolia(t *testing.T) {
	t.Parallel()

	state := NewMockState(t)
	ctx := context.Background()

	type TestData struct {
		balances     map[string][]byte
		expectedRoot string
	}

	tests := []TestData{
		{
			expectedRoot: "5eb6e371a698b8d68f665192350ffcecbbbf322916f4b51bd79bb6887da3f494",
			balances: map[string][]byte{
				"a2a6d93439144ffe4d27c9e088dcd8b783946263": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"bc11295936aa79d594139de1b2e12629414f3bdb": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"7cf5b79bfe291a67ab02b393e456ccc4c266f753": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"aaec86394441f915bce3e6ab399977e9906f3b69": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"f47cae1cf79ca6758bfc787dbd21e6bdbe7112b8": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"d7eddb78ed295b3c9629240e8924fb8d8874ddd8": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"8b7f0977bb4f0fbe7076fa22bc24aca043583f5e": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"e2e2659028143784d557bcec6ff3a0721048880a": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"d9a5179f091d85051d3c982785efd1455cec8699": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"beef32ca5b9a198d27b4e02f4c70439fe60356cf": {0xd3, 0xc2, 0x1b, 0xce, 0xcc, 0xed, 0xa1, 0x00, 0x00, 0x00},
				"0000006916a87b82333f4245046623b23794c65c": {0x08, 0x45, 0x95, 0x16, 0x14, 0x01, 0x48, 0x4a, 0x00, 0x00, 0x00},
				"b21c33de1fab3fa15499c62b59fe0cc3250020d1": {0x52, 0xb7, 0xd2, 0xdc, 0xc8, 0x0c, 0xd2, 0xe4, 0x00, 0x00, 0x00},
				"10f5d45854e038071485ac9e402308cf80d2d2fe": {0x52, 0xb7, 0xd2, 0xdc, 0xc8, 0x0c, 0xd2, 0xe4, 0x00, 0x00, 0x00},
				"d7d76c58b3a519e9fa6cc4d22dc017259bc49f1e": {0x52, 0xb7, 0xd2, 0xdc, 0xc8, 0x0c, 0xd2, 0xe4, 0x00, 0x00, 0x00},
				"799d329e5f583419167cd722962485926e338f4a": {0x0d, 0xe0, 0xb6, 0xb3, 0xa7, 0x64, 0x00, 0x00},
			},
		},
		{
			expectedRoot: "c91d4ecd59dce3067d340b3aadfc0542974b4fb4db98af39f980a91ea00db9dc",
			balances: map[string][]byte{
				"2f14582947e292a2ecd20c430b46f2d27cfe213c": {0x1B, 0xC1, 0x6D, 0x67, 0x4E, 0xC8, 0x00, 0x00},
			},
		},
		{
			expectedRoot: "c91d4ecd59dce3067d340b3aadfc0542974b4fb4db98af39f980a91ea00db9dc",
			balances:     map[string][]byte{},
		},
	}

	hph := NewHexPatriciaHashed(length.Addr, state)
	//hph.SetTrace(true)

	for _, testData := range tests {
		builder := NewUpdateBuilder()

		for address, balance := range testData.balances {
			builder.IncrementBalance(address, balance)
		}
		plainKeys, updates := builder.Build()

		err := state.applyPlainUpdates(plainKeys, updates)
		require.NoError(t, err)

		upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
		rootHash, err := hph.Process(ctx, upds, "")
		require.NoError(t, err)
		require.Equal(t, testData.expectedRoot, fmt.Sprintf("%x", rootHash))
		upds.Close()
	}
}

func Test_Cell_EncodeDecode(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(time.Now().UnixMilli()))
	first := &cell{
		hashLen:         length.Hash,
		accountAddrLen:  length.Addr,
		storageAddrLen:  length.Addr + length.Hash,
		hashedExtLen:    rnd.Intn(129),
		extLen:          rnd.Intn(65),
		hashedExtension: [128]byte{},
		extension:       [64]byte{},
		storageAddr:     [52]byte{},
		hash:            [32]byte{},
		accountAddr:     [20]byte{},
	}
	b := uint256.NewInt(rnd.Uint64())
	first.Balance = *b

	rnd.Read(first.hashedExtension[:first.hashedExtLen])
	rnd.Read(first.extension[:first.extLen])
	rnd.Read(first.storageAddr[:])
	rnd.Read(first.accountAddr[:])
	rnd.Read(first.hash[:])

	second := new(cell)
	err := second.Decode(first.Encode())
	require.NoError(t, err)

	cellMustEqual(t, first, second)
}

func Test_HexPatriciaHashed_StateEncode(t *testing.T) {
	t.Parallel()

	//trie := NewHexPatriciaHashed(length.Hash, nil, nil, nil)
	var s state
	s.Root = make([]byte, 128)
	rnd := rand.New(rand.NewSource(42))

	n, err := rnd.Read(s.Root[:])
	require.NoError(t, err)
	require.Equal(t, len(s.Root), n)
	s.RootPresent = true
	s.RootTouched = true
	s.RootChecked = true

	for i := 0; i < len(s.Depths); i++ {
		s.Depths[i] = rnd.Intn(256)
	}
	for i := 0; i < len(s.TouchMap); i++ {
		s.TouchMap[i] = uint16(rnd.Intn(1<<16 - 1))
	}
	for i := 0; i < len(s.AfterMap); i++ {
		s.AfterMap[i] = uint16(rnd.Intn(1<<16 - 1))
	}
	for i := 0; i < len(s.BranchBefore); i++ {
		if rnd.Intn(100) > 49 {
			s.BranchBefore[i] = true
		}
	}

	enc, err := s.Encode(nil)
	require.NoError(t, err)
	require.NotEmpty(t, enc)

	var s1 state
	err = s1.Decode(enc)
	require.NoError(t, err)

	require.Equal(t, s.Root[:], s1.Root[:])
	require.Equal(t, s.Depths[:], s1.Depths[:])
	require.Equal(t, s.AfterMap[:], s1.AfterMap[:])
	require.Equal(t, s.TouchMap[:], s1.TouchMap[:])
	require.Equal(t, s.BranchBefore[:], s1.BranchBefore[:])
	require.Equal(t, s.RootTouched, s1.RootTouched)
	require.Equal(t, s.RootPresent, s1.RootPresent)
	require.Equal(t, s.RootChecked, s1.RootChecked)
}

func Test_HexPatriciaHashed_StateEncodeDecodeSetup(t *testing.T) {
	t.Parallel()

	ms := NewMockState(t)
	ctx := context.Background()

	plainKeys, updates := NewUpdateBuilder().
		Balance("f5", 4).
		Balance("ff", 900234).
		Balance("03", 7).
		Storage("03", "56", "050505").
		Balance("05", 9).
		Storage("03", "87", "060606").
		Balance("b9", 6).
		Nonce("ff", 169356).
		Storage("05", "02", "8989").
		Storage("f5", "04", "9898").
		Build()

	before := NewHexPatriciaHashed(1, ms)
	after := NewHexPatriciaHashed(1, ms)

	err := ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()

	// process updates
	rhBefore, err := before.Process(ctx, upds, "")
	require.NoError(t, err)

	state, err := before.EncodeCurrentState(nil)
	require.NoError(t, err)

	// save and transfer state into 'after' trie
	err = after.SetState(state)
	require.NoError(t, err)

	rhAfter, err := after.RootHash()
	require.NoError(t, err)
	require.Equal(t, rhBefore, rhAfter)

	// create new update and apply it to both tries
	nextPK, nextUpdates := NewUpdateBuilder().
		Nonce("ff", 4).
		Balance("b9", 6000000000).
		Balance("ad", 8000000000).
		Build()

	err = ms.applyPlainUpdates(nextPK, nextUpdates)
	require.NoError(t, err)

	WrapKeyUpdatesInto(t, upds, nextPK, nextUpdates)

	rh2Before, err := before.Process(ctx, upds, "")
	require.NoError(t, err)

	WrapKeyUpdatesInto(t, upds, nextPK, nextUpdates) // they're resetted after Process

	rh2After, err := after.Process(ctx, upds, "")
	require.NoError(t, err)
	require.Equal(t, rh2Before, rh2After)
}

func Test_HexPatriciaHashed_StateRestoreAndContinue(t *testing.T) {
	t.Skip("TODO: concurrent map write crush fix needed")

	t.Parallel()

	msOne := NewMockState(t)
	msTwo := NewMockState(t)
	ctx := context.Background()
	plainKeys, updates := NewUpdateBuilder().
		Balance("f5", 4).
		Balance("01", 5).
		Balance("02", 6).
		Balance("03", 7).
		Balance("ff", 900234).
		Build()

	trieOne := NewHexPatriciaHashed(1, msOne)
	err := msOne.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	err = msTwo.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	updOne := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer updOne.Close()

	withoutRestore, err := trieOne.Process(ctx, updOne, "")
	require.NoError(t, err)
	t.Logf("root before restore %x\n", withoutRestore)

	// Has to copy commitment state from msOne to msTwo.
	// Previously we did not apply updates in this test - trieTwo simply read same commitment data from msOne.
	// Now when branch data is written during ProcessKeys, need to use separated state for this exact case.
	for ck, cv := range msOne.cm {
		err = msTwo.PutBranch([]byte(ck), cv, nil, 0)
		require.NoError(t, err)
	}

	buf, err := trieOne.EncodeCurrentState(nil)
	require.NoError(t, err)
	require.NotEmpty(t, buf)

	trieTwo := NewHexPatriciaHashed(1, msTwo)
	err = trieTwo.SetState(buf)
	require.NoError(t, err)

	hashAfterRestore, err := trieTwo.RootHash()
	require.NoError(t, err)
	t.Logf("restored state to another trie, root %x\n", hashAfterRestore)
	require.Equal(t, withoutRestore, hashAfterRestore)

	plainKeys, updates = NewUpdateBuilder().
		Balance("ff", 900234).
		Balance("04", 1233).
		Storage("04", "01", "0401").
		Balance("ba", 065606).
		Balance("00", 4).
		Balance("01", 5).
		Balance("02", 6).
		Balance("03", 7).
		Storage("03", "56", "050505").
		Balance("05", 9).
		Storage("03", "87", "060606").
		Balance("b9", 6).
		Nonce("ff", 169356).
		Storage("05", "02", "8989").
		Storage("f5", "04", "9898").
		Build()

	err = msOne.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)
	err = msTwo.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	WrapKeyUpdatesInto(t, updOne, plainKeys, updates)

	withoutRestore, err = trieOne.Process(ctx, updOne, "")
	require.NoError(t, err)

	t.Logf("batch without restore (%d) root %x\n", len(updates), withoutRestore)

	updTwo := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer updTwo.Close()

	afterRestore, err := trieTwo.Process(ctx, updTwo, "")
	require.NoError(t, err)
	t.Logf("batch after restore (%d) root %x\n", len(updates), afterRestore)

	require.Equal(t, withoutRestore, afterRestore)
}

func Test_HexPatriciaHashed_RestoreAndContinue(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	ms := NewMockState(t)

	plainKeys, updates := NewUpdateBuilder().
		Balance("f5", 4).
		Balance("ff", 900234).
		Balance("04", 1233).
		Storage("04", "01", "0401").
		Balance("ba", 065606).
		Balance("00", 4).
		Balance("01", 5).
		Balance("02", 6).
		Balance("03", 7).
		Storage("03", "56", "050505").
		Balance("05", 9).
		Storage("03", "87", "060606").
		Balance("b9", 6).
		Nonce("ff", 169356).
		Storage("05", "02", "8989").
		Storage("f5", "04", "9898").
		Build()

	trieOne := NewHexPatriciaHashed(1, ms)
	trieTwo := NewHexPatriciaHashed(1, ms)

	err := ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	updTwo := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer updTwo.Close()

	beforeRestore, err := trieTwo.Process(ctx, updTwo, "")
	require.NoError(t, err)

	buf, err := trieTwo.EncodeCurrentState(nil)
	require.NoError(t, err)
	require.NotEmpty(t, buf)

	err = trieOne.SetState(buf)
	require.NoError(t, err)
	require.Equal(t, beforeRestore[:], trieOne.root.hash[:])

	hashAfterRestore, err := trieOne.RootHash()
	require.NoError(t, err)
	require.Equal(t, beforeRestore, hashAfterRestore)

	t.Logf("restored state to another trie, root %x\n", hashAfterRestore)

	plainKeys, updates = NewUpdateBuilder().
		Delete("f5").
		Delete("ff").
		Delete("04").
		DeleteStorage("04", "01").
		Delete("ba").
		Delete("00").
		Delete("01").
		Build()

	err = ms.applyPlainUpdates(plainKeys, updates)
	require.NoError(t, err)

	WrapKeyUpdatesInto(t, updTwo, plainKeys, updates)

	// process updates
	AfterRestore, err := trieOne.Process(ctx, updTwo, "")
	require.NoError(t, err)

	WrapKeyUpdatesInto(t, updTwo, plainKeys, updates)
	// process updates again but keep in mind that two tries sharing same ms, so result should be equal (second time we just go over same data)
	withoutRestore, err := trieTwo.Process(ctx, updTwo, "")
	require.NoError(t, err)

	require.Equal(t, withoutRestore, AfterRestore)
}

func Test_HexPatriciaHashed_ProcessUpdates_UniqueRepresentation_AfterStateRestore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	stateSeq := NewMockState(t)
	stateBatch := NewMockState(t)

	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
		Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
		Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1233).
		Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
		Balance("27456647f49ba65e220e86cba9abfc4fc1587b81", 065606).
		Balance("b13363d527cdc18173c54ac5d4a54af05dbec22e", 4*1e17).
		Balance("d995768ab23a0a333eb9584df006da740e66f0aa", 5).
		Balance("eabf041afbb6c6059fbd25eab0d3202db84e842d", 6).
		Balance("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", 7).
		Balance("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", 5*1e17).
		Storage("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "0fa41642c48ecf8f2059c275353ce4fee173b3a8ce5480f040c4d2901603d14e", "050505").
		Balance("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", 9*1e16).
		Storage("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", "de3fea338c95ca16954e80eb603cd81a261ed6e2b10a03d0c86cf953fe8769a4", "060606").
		Balance("14c4d3bba7f5009599257d3701785d34c7f2aa27", 6*1e18).
		Nonce("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 169356).
		Storage("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", "9f49fdd48601f00df18ebc29b1264e27d09cf7cbd514fe8af173e534db038033", "8989").
		Storage("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", "d1664244ae1a8a05f8f1d41e45548fbb7aa54609b985d6439ee5fd9bb0da619f", "9898").
		Build()

	trieSequential := NewHexPatriciaHashed(length.Addr, stateSeq)
	trieBatch := NewHexPatriciaHashed(length.Addr, stateBatch)

	plainKeys, updates = sortUpdatesByHashIncrease(t, trieSequential, plainKeys, updates)

	var rSeq, rBatch []byte
	{
		fmt.Printf("1. Trie sequential update (%d updates)\n", len(updates))
		for i := 0; i < len(updates); i++ {
			err := stateSeq.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1])
			require.NoError(t, err)

			updsOne := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[i:i+1], updates[i:i+1])

			sequentialRoot, err := trieSequential.Process(ctx, updsOne, "")
			require.NoError(t, err)

			t.Logf("trieSequential root @%d hash %x\n", i, sequentialRoot)
			rSeq = common.Copy(sequentialRoot)

			updsOne.Close()

			if i == (len(updates) / 2) {
				prevState, err := trieSequential.EncodeCurrentState(nil)
				require.NoError(t, err)

				trieSequential.Reset()
				trieSequential = NewHexPatriciaHashed(length.Addr, stateSeq)

				err = trieSequential.SetState(prevState)
				require.NoError(t, err)
			}
		}
	}
	{
		fmt.Printf("\n2. Trie batch update (%d updates)\n", len(updates))
		err := stateBatch.applyPlainUpdates(plainKeys, updates)
		require.NoError(t, err)

		updsTwo := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)

		rh, err := trieBatch.Process(ctx, updsTwo, "")
		require.NoError(t, err)
		t.Logf("trieBatch of %d root hash %x\n", len(updates), rh)

		rBatch = common.Copy(rh)
		updsTwo.Close()
	}
	require.Equal(t, rBatch, rSeq, "sequential and trieBatch root should match")
}

func Test_HexPatriciaHashed_ProcessUpdates_UniqueRepresentationInTheMiddle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	stateSeq := NewMockState(t)
	stateBatch := NewMockState(t)

	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
		Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
		Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1233).
		Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
		Balance("27456647f49ba65e220e86cba9abfc4fc1587b81", 065606).
		Balance("b13363d527cdc18173c54ac5d4a54af05dbec22e", 4*1e17).
		Balance("d995768ab23a0a333eb9584df006da740e66f0aa", 5).
		Balance("eabf041afbb6c6059fbd25eab0d3202db84e842d", 6).
		Balance("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", 7).
		Balance("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", 5*1e17).
		Storage("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "0fa41642c48ecf8f2059c275353ce4fee173b3a8ce5480f040c4d2901603d14e", "050505").
		Balance("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", 9*1e16).
		Storage("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", "de3fea338c95ca16954e80eb603cd81a261ed6e2b10a03d0c86cf953fe8769a4", "060606").
		Balance("14c4d3bba7f5009599257d3701785d34c7f2aa27", 6*1e18).
		Nonce("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 169356).
		Storage("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", "0000fdd48601f00df18ebc29b1264e27d09cf7cbd514fe8af173e534db038033", "8989").
		Storage("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", "d1664244ae1a8a05f8f1d41e45548fbb7aa54609b985d6439ee5fd9bb0da619f", "9898").
		Balance("27456647f49ba65e220e86cba9abfc4fc1587b81", 065606).
		Nonce("27456647f49ba65e220e86cba9abfc4fc1587b81", 1).
		Balance("b13363d527cdc18173c54ac5d4a54af05dbec22e", 3*1e17).
		Nonce("b13363d527cdc18173c54ac5d4a54af05dbec22e", 1).
		Balance("d995768ab23a0a333eb9584df006da740e66f0aa", 5).
		Storage("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", "de3fea338c95ca16954e80eb603cd81a261ed6e2b10a03d0c86cf953fe8769a4", "909090").
		Balance("14c4d3bba7f5009599257d3701785d34c7f2aa27", 5*1e18).
		Nonce("14c4d3bba7f5009599257d3701785d34c7f2aa27", 1).
		Nonce("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 169356).
		Storage("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", "d1664244ae1a444448f1d41e45548fbb7aa54609b985d6439ee5fd9bb0da619f", "9898").
		//Storage("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", "0000000000000000018ebc29b1264e27d09cf7cbd514fe8af173e534db038033", "8989").
		//Storage("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", "9f49fdd48601f00df18ebc29b1264e27d09cf7cbd514fe8af173e77777778033", "8989").
		Storage("88e76c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", "d22222222e1a8a05f8f1d41e45548fbb7aa54609b985d6439ee5fd9bb0da619f", "9898").
		Balance("eabf041afbb6c6059fbd25eab0d3202db84e842d", 6000000).
		Nonce("eabf041afbb6c6059fbd25eab0d3202db84e842d", 1).
		Balance("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", 7).
		Balance("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", 5*1e17).
		Build()

	sequential := NewHexPatriciaHashed(length.Addr, stateSeq)
	batch := NewHexPatriciaHashed(length.Addr, stateBatch)

	plainKeys, updates = sortUpdatesByHashIncrease(t, sequential, plainKeys, updates)

	//sequential.SetTrace(true)
	//batch.SetTrace(true)
	somewhere := 6
	somewhereRoot := make([]byte, 0)

	var rSeq, rBatch []byte
	{
		fmt.Printf("1. Trie sequential update (%d updates)\n", len(updates))
		for i := 0; i < len(updates); i++ {
			err := stateSeq.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1])
			require.NoError(t, err)

			updsOne := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[i:i+1], updates[i:i+1])

			sequentialRoot, err := sequential.Process(ctx, updsOne, "")
			require.NoError(t, err)

			t.Logf("sequential root @%d hash %x\n", i, sequentialRoot)
			rSeq = common.Copy(sequentialRoot)

			updsOne.Close()

			if i == somewhere {
				prevState, err := sequential.EncodeCurrentState(nil)
				require.NoError(t, err)

				sequential.Reset()
				sequential = NewHexPatriciaHashed(length.Addr, stateSeq)

				err = sequential.SetState(prevState)
				require.NoError(t, err)
				somewhereRoot = common.Copy(sequentialRoot)
			}
		}
	}
	{
		fmt.Printf("\n2. Trie batch update (%d updates)\n", len(updates))
		err := stateBatch.applyPlainUpdates(plainKeys, updates)
		require.NoError(t, err)

		updsTwo := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[:somewhere+1], updates[:somewhere+1])

		rh, err := batch.Process(ctx, updsTwo, "")
		require.NoError(t, err)
		t.Logf("(first half) batch of %d root hash %x\n", somewhere, rh)
		require.Equal(t, rh, somewhereRoot)

		WrapKeyUpdatesInto(t, updsTwo, plainKeys[somewhere+1:], updates[somewhere+1:])

		rh, err = batch.Process(ctx, updsTwo, "")
		require.NoError(t, err)
		t.Logf("(second half) batch of %d root hash %x\n", len(updates)-somewhere, rh)

		rBatch = common.Copy(rh)
		updsTwo.Close()
	}
	require.Equal(t, rBatch, rSeq, "sequential and batch root should match")
}

func Test_ParallelHexPatriciaHashed_ProcessUpdates_UniqueRepresentationInTheMiddle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	stateSeq := NewMockState(t)
	stateBatch := NewMockState(t)

	plainKeys, updates := NewUpdateBuilder().
		Balance("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", 4).
		Balance("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 900234).
		Balance("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", 1233).
		Storage("8e5476fc5990638a4fb0b5fd3f61bb4b5c5f395e", "24f3a02dc65eda502dbf75919e795458413d3c45b38bb35b51235432707900ed", "0401").
		Balance("27456647f49ba65e220e86cba9abfc4fc1587b81", 065606).
		Balance("b13363d527cdc18173c54ac5d4a54af05dbec22e", 4*1e17).
		Balance("d995768ab23a0a333eb9584df006da740e66f0aa", 5).
		Balance("eabf041afbb6c6059fbd25eab0d3202db84e842d", 6).
		Balance("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", 7).
		Balance("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", 5*1e17).
		Storage("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", "0fa41642c48ecf8f2059c275353ce4fee173b3a8ce5480f040c4d2901603d14e", "050505").
		Balance("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", 9*1e16).
		Storage("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", "de3fea338c95ca16954e80eb603cd81a261ed6e2b10a03d0c86cf953fe8769a4", "060606").
		Balance("14c4d3bba7f5009599257d3701785d34c7f2aa27", 6*1e18).
		Nonce("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 169356).
		Storage("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", "0000fdd48601f00df18ebc29b1264e27d09cf7cbd514fe8af173e534db038033", "8989").
		Storage("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", "d1664244ae1a8a05f8f1d41e45548fbb7aa54609b985d6439ee5fd9bb0da619f", "9898").
		Balance("27456647f49ba65e220e86cba9abfc4fc1587b81", 065606).
		Nonce("27456647f49ba65e220e86cba9abfc4fc1587b81", 1).
		Balance("b13363d527cdc18173c54ac5d4a54af05dbec22e", 3*1e17).
		Nonce("b13363d527cdc18173c54ac5d4a54af05dbec22e", 1).
		Balance("d995768ab23a0a333eb9584df006da740e66f0aa", 5).
		Storage("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", "de3fea338c95ca16954e80eb603cd81a261ed6e2b10a03d0c86cf953fe8769a4", "909090").
		Balance("14c4d3bba7f5009599257d3701785d34c7f2aa27", 5*1e18).
		Nonce("14c4d3bba7f5009599257d3701785d34c7f2aa27", 1).
		Nonce("18f4dcf2d94402019d5b00f71d5f9d02e4f70e40", 169356).
		Storage("68ee6c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", "d1664244ae1a444448f1d41e45548fbb7aa54609b985d6439ee5fd9bb0da619f", "9898").
		Storage("88e76c0e9cdc73b2b2d52dbd79f19d24fe25e2f9", "d22222222e1a8a05f8f1d41e45548fbb7aa54609b985d6439ee5fd9bb0da619f", "9898").
		Storage("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", "0000000000000000018ebc29b1264e27d09cf7cbd514fe8af173e534db038033", "8989").
		Storage("a8f8d73af90eee32dc9729ce8d5bb762f30d21a4", "9f49fdd48601f00df18ebc29b1264e27d09cf7cbd514fe8af173e77777778033", "8989").
		Balance("eabf041afbb6c6059fbd25eab0d3202db84e842d", 6000000).
		Nonce("eabf041afbb6c6059fbd25eab0d3202db84e842d", 1).
		Balance("93fe03620e4d70ea39ab6e8c0e04dd0d83e041f2", 7).
		Balance("ba7a3b7b095d3370c022ca655c790f0c0ead66f5", 5*1e17).
		Balance("00000000000000000000000000000000000000f5", 4).
		Balance("00000000000000000000000000000000000000ff", 900234).
		Balance("0000000000000000000000000000000000000004", 1233).
		Storage("0000000000000000000000000000000000000004", "01", "0401").
		Balance("00000000000000000000000000000000000000ba", 065606).
		Balance("0000000000000000000000000000000000000000", 4).
		Balance("0000000000000000000000000000000000000001", 5).
		Balance("0000000000000000000000000000000000000002", 6).
		Balance("0000000000000000000000000000000000000003", 7).
		Storage("0000000000000000000000000000000000000003", "56", "050505").
		Balance("0000000000000000000000000000000000000005", 9).
		Storage("0000000000000000000000000000000000000003", "87", "060606").
		Balance("00000000000000000000000000000000000000b9", 6).
		Nonce("00000000000000000000000000000000000000ff", 169356).
		Storage("0000000000000000000000000000000000000005", "02", "8989").
		Storage("00000000000000000000000000000000000000f5", "04", "9898").
		Storage("00000000000000000000000000000000000000f5", "05", "1234").
		Storage("00000000000000000000000000000000000000f5", "06", "5678").
		Storage("00000000000000000000000000000000000000f5", "07", "9abc").
		Storage("00000000000000000000000000000000000000f5", "08", "def0").
		Storage("00000000000000000000000000000000000000f5", "09", "1111").
		Storage("00000000000000000000000000000000000000f5", "0a", "2222").
		Storage("00000000000000000000000000000000000000f5", "0b", "3333").
		Storage("00000000000000000000000000000000000000f5", "0c", "4444").
		Storage("00000000000000000000000000000000000000f5", "0d", "5555").
		Storage("00000000000000000000000000000000000000f5", "0e", "6666").
		Storage("00000000000000000000000000000000000000f5", "0f", "7777").
		Storage("00000000000000000000000000000000000000f5", "10", "8888").
		Storage("00000000000000000000000000000000000000f5", "11", "9999").
		Storage("00000000000000000000000000000000000000f5", "d680a8cdb8eeb05a00b8824165b597d7a2c2f608057537dd2cee058569114be0", "aaaa").
		Storage("00000000000000000000000000000000000000f5", "e9018287c0d9d38524c16f7450cf3ed7ca7b2a466a4746910462343626cb7e9b", "bbbb").
		Storage("00000000000000000000000000000000000000f5", "e5635458dccace734b0f3fe6bae307a6d23282dae083218bd0db7ecf8b784b41", "cccc").
		Storage("00000000000000000000000000000000000000f5", "0a1c82a16bce90d07e4aed8d44cb584b25f39d8d8dd61dea068f144e985326a2", "dddd").
		Storage("00000000000000000000000000000000000000f5", "778e0ba7ae9d62a62b883cfb447343673f37854d335595b4934b2c20ff936a5f", "eeee").
		Storage("00000000000000000000000000000000000000f5", "787ec6ab994586c0f3116e311c61479d4a171287ef1b4a97afcce56044d698dc", "ffff").
		Storage("00000000000000000000000000000000000000f5", "1bf6be2031cd9a8e204ffae1fea4dcfef0c85fb20d189a0a7b0880ef9b7bb3c7", "0000").
		Storage("00000000000000000000000000000000000000f5", "ab4756ebb7abc2631dddf5f362155e571c947465add47812794d8641ff04c283", "1111").
		Storage("00000000000000000000000000000000000000f5", "f094bf04ad37fc7aa047784f3346e12ed72b799fc7dc70c9d8eac296829c592e", "2222").
		Storage("00000000000000000000000000000000000000f5", "c88ebea9f05008643aa43f6f610eec0f81c3d736c3a85b12a09034359d744021", "4444").
		Storage("00000000000000000000000000000000000000f5", "58a60d4461d743243c8d77a05708351bde842bf3702dfb3276a6a948603dca7d", "ffff").
		Storage("00000000000000000000000000000000000000f5", "377c067adec6f257f25dff4bc98fd74800df84974189199801ed8b560c805a95", "aaaa").
		Storage("00000000000000000000000000000000000000f5", "c8a1d3e638914407d095a9a0f785d5dac4ad580bca47c924d6864e1431b74a23", "eeee").
		Storage("00000000000000000000000000000000000000f5", "1f00000000000000000000000000000000000000f5", "00000000000000000000000000000000000000f5").
		Build()

	sequential := NewHexPatriciaHashed(length.Addr, stateSeq)
	batch := NewHexPatriciaHashed(length.Addr, stateBatch)

	plainKeys, updates = sortUpdatesByHashIncrease(t, sequential, plainKeys, updates)

	//sequential.SetTrace(true)
	//batch.SetTrace(true)
	somewhere := 16
	var rSeq, rBatch, somewhereRoot []byte
	_ = rSeq
	{
		fmt.Printf("1. Trie sequential update (%d updates)\n", len(updates))
		err := stateSeq.applyPlainUpdates(plainKeys[:somewhere+1], updates[:somewhere+1])
		require.NoError(t, err)

		updsOne := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[:somewhere+1], updates[:somewhere+1])

		sequential.SetTrace(false)
		sequentialRoot, err := sequential.Process(ctx, updsOne, "")
		require.NoError(t, err)
		//sequential.SetTrace(false)

		t.Logf("sequential root @%d hash %x\n", somewhere, sequentialRoot)
		somewhereRoot = common.Copy(sequentialRoot)

		updsOne.Close()

		WrapKeyUpdatesInto(t, updsOne, plainKeys[somewhere+1:], updates[somewhere+1:])
		err = stateSeq.applyPlainUpdates(plainKeys[somewhere+1:], updates[somewhere+1:])
		require.NoError(t, err)

		sequentialRoot, err = sequential.Process(ctx, updsOne, "")
		require.NoError(t, err)

		t.Logf("sequential root @%d hash %x\n", len(plainKeys), sequentialRoot)
		rSeq = common.Copy(sequentialRoot)
	}
	{
		err := stateBatch.applyPlainUpdates(plainKeys, updates)
		require.NoError(t, err)

		//updsTwo := WrapKeyUpdates(t, ModeDirect, batch.hashAndNibblizeKey, plainKeys[:somewhere+1], updates[:somewhere+1])
		//
		//batch.trace = true
		//rh, err := batch.Process(ctx, updsTwo, "")
		//require.NoError(t, err)
		//t.Logf("(first half) batch of %d root hash %x\n", somewhere, rh)
		//require.EqualValues(t, rh, somewhereRoot)

		//updsTwo.Close()
		fmt.Printf("\n2. Trie parallel update (%d updates)\n", len(updates))
		stateBatch.SetConcurrentCommitment(true)
		trieBatch := NewConcurrentPatriciaHashed(batch, stateBatch)
		updsTwo := WrapKeyUpdatesParallel(t, ModeDirect, KeyToHexNibbleHash, plainKeys[:somewhere+1], updates[:somewhere+1])

		trieBatch.SetTrace(false)
		rh, err := trieBatch.Process(ctx, updsTwo, "")
		require.NoError(t, err)
		t.Logf("(first half) batch of %d root hash %x\n", somewhere, rh)
		require.Equal(t, somewhereRoot, rh)

		// trieBatch.SetParticularTrace(true, 0x9)
		WrapKeyUpdatesInto(t, updsTwo, plainKeys[somewhere+1:], updates[somewhere+1:])

		rh, err = trieBatch.Process(ctx, updsTwo, "")
		require.NoError(t, err)

		t.Logf("(second half) batch of %d root hash %x\n", len(updates)-somewhere, rh)

		rBatch = common.Copy(rh)
		updsTwo.Close()
		require.Equal(t, rSeq, rBatch, "sequential and batch root should match")
		t.Logf("sequential and parallel root matches")
	}
	//require.EqualValues(t, rBatch, rSeq, "sequential and batch root should match")
}

func TestUpdate_EncodeDecode(t *testing.T) {
	t.Parallel()

	updates := []Update{
		{Flags: BalanceUpdate, Balance: *uint256.NewInt(123), CodeHash: empty.CodeHash},
		{Flags: BalanceUpdate | NonceUpdate, Balance: *uint256.NewInt(45639015), Nonce: 123, CodeHash: empty.CodeHash},
		{Flags: BalanceUpdate | NonceUpdate | CodeUpdate, Balance: *uint256.NewInt(45639015), Nonce: 123,
			CodeHash: [length.Hash]byte{
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
				0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
				0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
				0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20}},
		{Flags: StorageUpdate, Storage: [length.Hash]byte{0x21, 0x22, 0x23, 0x24}, StorageLen: 4, CodeHash: empty.CodeHash},
		{Flags: DeleteUpdate, CodeHash: empty.CodeHash},
	}

	var numBuf [10]byte
	for i, update := range updates {
		encoded := update.Encode(nil, numBuf[:])

		decoded := Update{}
		n, err := decoded.Decode(encoded, 0)
		require.NoError(t, err, i)
		require.Equal(t, len(encoded), n, i)

		require.Equal(t, update.Flags, decoded.Flags, i)
		require.Equal(t, update.Balance, decoded.Balance, i)
		require.Equal(t, update.Nonce, decoded.Nonce, i)
		require.Equal(t, update.CodeHash, decoded.CodeHash, i)
		require.Equal(t, update.Storage, decoded.Storage, i)
		require.Equal(t, update.StorageLen, decoded.StorageLen, i)
	}
}

func TestUpdate_Merge(t *testing.T) {
	type tcase struct {
		a, b, e Update
	}

	updates := []tcase{
		{
			a: Update{Flags: BalanceUpdate, Balance: *uint256.NewInt(123), CodeHash: empty.CodeHash},
			b: Update{Flags: BalanceUpdate | NonceUpdate, Balance: *uint256.NewInt(45639015), Nonce: 123, CodeHash: empty.CodeHash},
			e: Update{Flags: BalanceUpdate | NonceUpdate, Balance: *uint256.NewInt(45639015), Nonce: 123, CodeHash: empty.CodeHash},
		},
		{
			a: Update{Flags: BalanceUpdate | NonceUpdate, Balance: *uint256.NewInt(45639015), Nonce: 123, CodeHash: empty.CodeHash},
			b: Update{Flags: BalanceUpdate | NonceUpdate | CodeUpdate, Balance: *uint256.NewInt(1000000), Nonce: 547, CodeHash: empty.CodeHash},
			e: Update{Flags: BalanceUpdate | NonceUpdate | CodeUpdate, Balance: *uint256.NewInt(1000000), Nonce: 547, CodeHash: empty.CodeHash},
		},
		{
			a: Update{Flags: BalanceUpdate | NonceUpdate | CodeUpdate, Balance: *uint256.NewInt(4568314), Nonce: 123, CodeHash: empty.CodeHash},
			b: Update{Flags: BalanceUpdate | NonceUpdate | CodeUpdate, Balance: *uint256.NewInt(45639015), Nonce: 124,
				CodeHash: [length.Hash]byte{
					0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
					0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
					0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
					0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20}},
			e: Update{Flags: BalanceUpdate | NonceUpdate | CodeUpdate, Balance: *uint256.NewInt(45639015), Nonce: 124, CodeHash: [length.Hash]byte{
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
				0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
				0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
				0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20}},
		},
		{
			a: Update{Flags: StorageUpdate, Storage: [length.Hash]byte{0x21, 0x22, 0x23, 0x24}, StorageLen: 4, CodeHash: empty.CodeHash},
			b: Update{Flags: DeleteUpdate, CodeHash: empty.CodeHash},
			e: Update{Flags: DeleteUpdate, CodeHash: empty.CodeHash},
		},
	}

	var numBuf [10]byte
	for i, tc := range updates {
		tc.a.Merge(&tc.b)
		encA := tc.a.Encode(nil, numBuf[:])
		encE := tc.e.Encode(nil, numBuf[:])
		require.Equal(t, encE, encA, i)
	}
}

func TestCell_setFromUpdate(t *testing.T) {
	t.Parallel()

	rnd := rand.New(rand.NewSource(42))

	b := uint256.NewInt(rnd.Uint64())
	update := Update{}
	update.Reset()

	update.Balance = *b
	update.Nonce = rand.Uint64()
	rnd.Read(update.CodeHash[:])
	update.Flags = BalanceUpdate | NonceUpdate | CodeUpdate

	target := new(cell)
	target.setFromUpdate(&update)
	require.True(t, update.Balance.Eq(&target.Balance))
	require.Equal(t, update.Nonce, target.Nonce)
	require.Equal(t, update.CodeHash, target.CodeHash)
	require.Equal(t, 0, target.StorageLen)

	update.Reset()

	update.Balance.SetUint64(0)
	update.Nonce = rand.Uint64()
	rnd.Read(update.CodeHash[:])
	update.Flags = NonceUpdate | CodeUpdate

	target.reset()
	target.setFromUpdate(&update)

	require.True(t, update.Balance.Eq(&target.Balance))
	require.Equal(t, update.Nonce, target.Nonce)
	require.Equal(t, update.CodeHash, target.CodeHash)
	require.Equal(t, 0, target.StorageLen)

	update.Reset()

	update.Balance.SetUint64(rnd.Uint64() + rnd.Uint64())
	update.Nonce = rand.Uint64()
	rnd.Read(update.Storage[:])
	update.StorageLen = len(update.Storage)
	update.Flags = NonceUpdate | BalanceUpdate | StorageUpdate

	target.reset()
	target.setFromUpdate(&update)

	require.True(t, update.Balance.Eq(&target.Balance))
	require.Equal(t, update.Nonce, target.Nonce)
	require.Equal(t, update.CodeHash, target.CodeHash)
	require.Equal(t, update.StorageLen, target.StorageLen)
	require.Equal(t, update.Storage[:update.StorageLen], target.Storage[:target.StorageLen])

	update.Reset()

	update.Balance.SetUint64(rnd.Uint64() + rnd.Uint64())
	update.Nonce = rand.Uint64()
	rnd.Read(update.Storage[:rnd.Intn(len(update.Storage))])
	update.StorageLen = len(update.Storage)
	update.Flags = NonceUpdate | BalanceUpdate | StorageUpdate

	target.reset()
	target.setFromUpdate(&update)

	require.True(t, update.Balance.Eq(&target.Balance))
	require.Equal(t, update.Nonce, target.Nonce)
	require.Equal(t, update.CodeHash, target.CodeHash)
	require.EqualValues(t, empty.CodeHash[:], target.CodeHash)
	require.Equal(t, update.StorageLen, target.StorageLen)
	require.Equal(t, update.Storage[:update.StorageLen], target.Storage[:target.StorageLen])

	update.Reset()
	update.Flags = DeleteUpdate
	target.reset()
	target.setFromUpdate(&update)

	require.True(t, update.Balance.Eq(&target.Balance))
	require.Equal(t, update.Nonce, target.Nonce)
	require.EqualValues(t, empty.CodeHash[:], target.CodeHash)
	require.Equal(t, update.StorageLen, target.StorageLen)
	require.Equal(t, update.Storage[:update.StorageLen], target.Storage[:target.StorageLen])
}

func TestCell_fillFromFields(t *testing.T) {
	row, bm := generateCellRow(t, 16)
	rnd := rand.New(rand.NewSource(0))

	cg := func(nibble int, skip bool) (*cell, error) {
		c := row[nibble]
		if c.storageAddrLen > 0 || c.accountAddrLen > 0 {
			rnd.Read(c.stateHash[:])
			c.stateHashLen = 32
		}
		fmt.Printf("enc cell %x %v\n", nibble, c.FullString())

		return c, nil
	}

	be := NewBranchEncoder(1024)
	enc, _, err := be.EncodeBranch(bm, bm, bm, cg)
	require.NoError(t, err)

	//original := common.Copy(enc)
	fmt.Printf("%s\n", enc.String())

	tm, am, decRow, err := enc.decodeCells()
	require.NoError(t, err)
	require.Equal(t, bm, am)
	require.Equal(t, bm, tm)

	for i := 0; i < len(decRow); i++ {
		t.Logf("cell %d\n", i)
		first, second := row[i], decRow[i]
		// after decoding extension == hashedExtension, dhk will be derived from extension
		require.Equal(t, second.extLen, second.hashedExtLen)
		require.Equal(t, first.extLen, second.hashedExtLen)
		require.Equal(t, second.extension[:second.extLen], second.hashedExtension[:second.hashedExtLen])

		require.Equal(t, first.hashLen, second.hashLen)
		require.Equal(t, first.hash[:first.hashLen], second.hash[:second.hashLen])
		require.Equal(t, first.accountAddrLen, second.accountAddrLen)
		require.Equal(t, first.storageAddrLen, second.storageAddrLen)
		require.Equal(t, first.accountAddr[:], second.accountAddr[:])
		require.Equal(t, first.storageAddr[:], second.storageAddr[:])
		require.Equal(t, first.extension[:first.extLen], second.extension[:second.extLen])
		require.Equal(t, first.stateHash[:first.stateHashLen], second.stateHash[:second.stateHashLen])
	}
}

func cellMustEqual(tb testing.TB, first, second *cell) {
	tb.Helper()
	require.Equal(tb, first.hashedExtLen, second.hashedExtLen)
	require.Equal(tb, first.hashedExtension[:first.hashedExtLen], second.hashedExtension[:second.hashedExtLen])
	require.Equal(tb, first.hashLen, second.hashLen)
	require.Equal(tb, first.hash[:first.hashLen], second.hash[:second.hashLen])
	require.Equal(tb, first.accountAddrLen, second.accountAddrLen)
	require.Equal(tb, first.storageAddrLen, second.storageAddrLen)
	require.Equal(tb, first.accountAddr[:], second.accountAddr[:])
	require.Equal(tb, first.storageAddr[:], second.storageAddr[:])
	require.Equal(tb, first.extension[:first.extLen], second.extension[:second.extLen])
	require.Equal(tb, first.stateHash[:first.stateHashLen], second.stateHash[:second.stateHashLen])

	// encode doesn't code Nonce, Balance, CodeHash and Storage, Delete fields
}

func Test_HexPatriciaHashed_ProcessWithDozensOfStorageKeys(t *testing.T) {
	ctx := context.Background()
	msOne := NewMockState(t)
	msTwo := NewMockState(t)

	plainKeys, updates := NewUpdateBuilder().
		Balance("00000000000000000000000000000000000000f5", 4).
		Balance("00000000000000000000000000000000000000ff", 900234).
		Balance("0000000000000000000000000000000000000004", 1233).
		Storage("0000000000000000000000000000000000000004", "01", "0401").
		Balance("00000000000000000000000000000000000000ba", 065606).
		Balance("0000000000000000000000000000000000000000", 4).
		Balance("0000000000000000000000000000000000000001", 5).
		Balance("0000000000000000000000000000000000000002", 6).
		Balance("0000000000000000000000000000000000000003", 7).
		Storage("0000000000000000000000000000000000000003", "56", "050505").
		Balance("0000000000000000000000000000000000000005", 9).
		Storage("0000000000000000000000000000000000000003", "87", "060606").
		Balance("00000000000000000000000000000000000000b9", 6).
		Nonce("00000000000000000000000000000000000000ff", 169356).
		Storage("0000000000000000000000000000000000000005", "02", "8989").
		Storage("00000000000000000000000000000000000000f5", "04", "9898").
		Storage("00000000000000000000000000000000000000f5", "05", "1234").
		Storage("00000000000000000000000000000000000000f5", "06", "5678").
		Storage("00000000000000000000000000000000000000f5", "07", "9abc").
		Storage("00000000000000000000000000000000000000f5", "08", "def0").
		Storage("00000000000000000000000000000000000000f5", "09", "1111").
		Storage("00000000000000000000000000000000000000f5", "0a", "2222").
		Storage("00000000000000000000000000000000000000f5", "0b", "3333").
		Storage("00000000000000000000000000000000000000f5", "0c", "4444").
		Storage("00000000000000000000000000000000000000f5", "0d", "5555").
		Storage("00000000000000000000000000000000000000f5", "0e", "6666").
		Storage("00000000000000000000000000000000000000f5", "0f", "7777").
		Storage("00000000000000000000000000000000000000f5", "10", "8888").
		Storage("00000000000000000000000000000000000000f5", "11", "9999").
		Storage("00000000000000000000000000000000000000f5", "d680a8cdb8eeb05a00b8824165b597d7a2c2f608057537dd2cee058569114be0", "aaaa").
		Storage("00000000000000000000000000000000000000f5", "e9018287c0d9d38524c16f7450cf3ed7ca7b2a466a4746910462343626cb7e9b", "bbbb").
		Storage("00000000000000000000000000000000000000f5", "e5635458dccace734b0f3fe6bae307a6d23282dae083218bd0db7ecf8b784b41", "cccc").
		Storage("00000000000000000000000000000000000000f5", "0a1c82a16bce90d07e4aed8d44cb584b25f39d8d8dd61dea068f144e985326a2", "dddd").
		Storage("00000000000000000000000000000000000000f5", "778e0ba7ae9d62a62b883cfb447343673f37854d335595b4934b2c20ff936a5f", "eeee").
		Storage("00000000000000000000000000000000000000f5", "787ec6ab994586c0f3116e311c61479d4a171287ef1b4a97afcce56044d698dc", "ffff").
		Storage("00000000000000000000000000000000000000f5", "1bf6be2031cd9a8e204ffae1fea4dcfef0c85fb20d189a0a7b0880ef9b7bb3c7", "0000").
		Storage("00000000000000000000000000000000000000f5", "ab4756ebb7abc2631dddf5f362155e571c947465add47812794d8641ff04c283", "1111").
		Storage("00000000000000000000000000000000000000f5", "f094bf04ad37fc7aa047784f3346e12ed72b799fc7dc70c9d8eac296829c592e", "2222").
		Storage("00000000000000000000000000000000000000f5", "c88ebea9f05008643aa43f6f610eec0f81c3d736c3a85b12a09034359d744021", "4444").
		Storage("00000000000000000000000000000000000000f5", "58a60d4461d743243c8d77a05708351bde842bf3702dfb3276a6a948603dca7d", "ffff").
		Storage("00000000000000000000000000000000000000f5", "377c067adec6f257f25dff4bc98fd74800df84974189199801ed8b560c805a95", "aaaa").
		Storage("00000000000000000000000000000000000000f5", "c8a1d3e638914407d095a9a0f785d5dac4ad580bca47c924d6864e1431b74a23", "eeee").
		Storage("00000000000000000000000000000000000000f5", "1f00000000000000000000000000000000000000f5", "00000000000000000000000000000000000000f5").
		Build()

	trieOne := NewHexPatriciaHashed(length.Addr, msOne)
	plainKeys, updates = sortUpdatesByHashIncrease(t, trieOne, plainKeys, updates)

	//rnd := rand.New(rand.NewSource(345))
	//noise := make([]byte, 32)
	//prefixes := make(map[string][][]byte)
	//prefixesCnt := make(map[string]int)
	//for i := 0; i < 5000000; i++ {
	//	rnd.Read(noise)
	//	//hashed := trieOne.KeyToHexNibbleHash(noise)
	//	trieOne.keccak.Reset()
	//	trieOne.keccak.Write(noise)
	//	hashed := make([]byte, 32)
	//	trieOne.keccak.Read(hashed)
	//	prefixesCnt[string(hashed[:5])]++
	//	if c := prefixesCnt[string(hashed[:5])]; c < 5 {
	//		prefixes[string(hashed[:5])] = append(prefixes[string(hashed[:5])], common.Copy(noise))
	//	}
	//}
	//
	//count := 0
	//for pref, cnt := range prefixesCnt {
	//	if cnt > 1 {
	//		for _, noise := range prefixes[pref] {
	//			fmt.Printf("%x %x\n", pref, noise)
	//			count++
	//		}
	//	}
	//}
	//fmt.Printf("total %d\n", count)

	trieTwo := NewHexPatriciaHashed(length.Addr, msTwo)

	trieOne.SetTrace(false)
	trieTwo.SetTrace(false)

	var rSeq, rBatch []byte
	{
		fmt.Printf("1. Trie sequential update (%d updates)\n", len(updates))
		for i := 0; i < len(updates); i++ {
			err := msOne.applyPlainUpdates(plainKeys[i:i+1], updates[i:i+1])
			require.NoError(t, err)

			updsOne := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys[i:i+1], updates[i:i+1])

			sequentialRoot, err := trieOne.Process(ctx, updsOne, "")
			require.NoError(t, err)

			t.Logf("sequential root @%d hash %x\n", i, sequentialRoot)
			rSeq = common.Copy(sequentialRoot)

			updsOne.Close()
		}
	}
	{
		err := msTwo.applyPlainUpdates(plainKeys, updates)
		require.NoError(t, err)

		updsTwo := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)

		fmt.Printf("\n2. Trie batch update (%d updates)\n", len(updates))
		rh, err := trieTwo.Process(ctx, updsTwo, "")
		require.NoError(t, err)
		t.Logf("batch of %d root hash %x\n", len(updates), rh)

		updsTwo.Close()

		rBatch = common.Copy(rh)
	}
	require.Equal(t, rBatch, rSeq, "sequential and batch root should match")
}

// longer prefixLen - harder to find required keys
func generatePlainKeysWithSameHashPrefix(tb testing.TB, constPrefix []byte, keyLen int, prefixLen int, keyCount int) (plainKeys [][]byte, hashedKeys [][]byte) {
	tb.Helper()
	plainKeys = make([][]byte, 0, keyCount)
	hashedKeys = make([][]byte, 0, keyCount)
	for {
		key := make([]byte, keyLen)
		if constPrefix != nil {
			copy(key, constPrefix)
		}
		rand.Read(key[len(constPrefix):])

		hashed := KeyToHexNibbleHash(key)
		if len(plainKeys) == 0 {
			plainKeys = append(plainKeys, key)
			hashedKeys = append(hashedKeys, hashed)
			if keyCount == 1 {
				break
			}
			continue
		}
		if bytes.Equal(hashed[:prefixLen], hashedKeys[0][:prefixLen]) {
			plainKeys = append(plainKeys, key)
			hashedKeys = append(hashedKeys, hashed)
		}
		if len(plainKeys) == keyCount {
			break
		}
	}
	return plainKeys, hashedKeys
}

// longer prefixLen - harder to find required keys. Use up to 6 bytes for common prefix for quick pass.
//
// constPrefix is used when need to generate storage keys for same account address correctly. If constPrefix is not nil, keyLen must be > than len of constPrefix.
// So final key in this case will be constPrefix + random bytes, producing hased key like hash(constPrefix)+hash(random bytes) and checking that hash of random bytes has common prefix of prefixLen
func sortUpdatesByHashIncrease(t *testing.T, hph *HexPatriciaHashed, plainKeys [][]byte, updates []Update) ([][]byte, []Update) {
	t.Helper()

	ku := make([]*KeyUpdate, len(plainKeys))
	for i, pk := range plainKeys {
		ku[i] = &KeyUpdate{plainKey: string(pk), hashedKey: KeyToHexNibbleHash(pk), update: &updates[i]}
	}

	sort.Slice(updates, func(i, j int) bool {
		return bytes.Compare(ku[i].hashedKey, ku[j].hashedKey) < 0
	})

	pks := make([][]byte, len(updates))
	upds := make([]Update, len(updates))
	for i, u := range ku {
		pks[i] = []byte(u.plainKey)
		upds[i] = *u.update
		fmt.Printf("%x -> %x\n", u.plainKey, u.hashedKey)
	}
	return pks, upds
}

func Test_WitnessTrie_GenerateWitness(t *testing.T) {
	//t.Parallel()

	buildTrieAndWitness := func(t *testing.T, builder *UpdateBuilder, addrToWitness []byte) {
		t.Helper()

		ctx := context.Background()
		ms := NewMockState(t)
		hph := NewHexPatriciaHashed(length.Addr, ms)
		hph.SetTrace(false)

		plainKeys, updates := builder.Build()
		err := ms.applyPlainUpdates(plainKeys, updates)
		require.NoError(t, err)

		toProcess := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
		defer toProcess.Close()

		hph.trace = true
		root, err := hph.Process(ctx, toProcess, "")
		require.NoError(t, err)

		toWitness := NewUpdates(ModeDirect, "", KeyToHexNibbleHash)
		defer toWitness.Close()
		toWitness.TouchPlainKey(string(addrToWitness), nil, toProcess.TouchAccount)

		witnessTrie, rootWitness, err := hph.GenerateWitness(context.Background(), toWitness, nil, root, "")
		require.NoError(t, err)
		_ = witnessTrie
		require.NotNil(t, witnessTrie, "witness trie should not be nil")
		require.NotNil(t, rootWitness, "root witness should not be nil")
		require.Equal(t, root, rootWitness, "root witness should have the same root hash as trie")
	}

	//t.Run("JustRoot", func(t *testing.T) {
	//	plainKeysList, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 0, 1)
	//
	//	addrWithSingleton := common.Copy(plainKeysList[0])
	//	builder := NewUpdateBuilder()
	//	for i := 0; i < len(plainKeysList); i++ {
	//		builder.Balance(common.Bytes2Hex(plainKeysList[i]), uint64(i))
	//	}
	//
	//	buildTrieAndWitness(t, builder, addrWithSingleton)
	//})
	t.Run("RandomAccountsOnly", func(t *testing.T) {
		plainKeysList, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 0, 5)

		addrWithSingleton := common.Copy(plainKeysList[0])
		builder := NewUpdateBuilder()
		for i := 0; i < len(plainKeysList); i++ {
			builder.Balance(common.Bytes2Hex(plainKeysList[i]), uint64(i))
		}

		buildTrieAndWitness(t, builder, addrWithSingleton)
	})
	t.Run("RandomAccountsOnlyWithCPrefix", func(t *testing.T) {
		plainKeysList, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 4, 5)

		addrWithSingleton := common.Copy(plainKeysList[0])
		builder := NewUpdateBuilder()
		for i := 0; i < len(plainKeysList); i++ {
			builder.Balance(common.Bytes2Hex(plainKeysList[i]), uint64(i))
		}

		buildTrieAndWitness(t, builder, addrWithSingleton)
	})

	t.Run("RandomAccountsOnly-Many", func(t *testing.T) {
		plainKeysList, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 0, 25)

		addrWithSingleton := common.Copy(plainKeysList[0])
		builder := NewUpdateBuilder()
		for i := 0; i < len(plainKeysList); i++ {
			builder.Balance(common.Bytes2Hex(plainKeysList[i]), uint64(i))
		}

		buildTrieAndWitness(t, builder, addrWithSingleton)
	})
	t.Run("StorageSingleton", func(t *testing.T) {
		plainKeysList, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 0, 2)

		addrWithSingleton := common.Copy(plainKeysList[0])
		//storageKeysList, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Hash, 4, 2)

		builder := NewUpdateBuilder()
		for i := 0; i < len(plainKeysList); i++ {
			builder.Balance(common.Bytes2Hex(plainKeysList[i]), uint64(i))
		}
		// add just a single storage slot to address
		builder.Storage(common.Bytes2Hex(addrWithSingleton), "00044c45500c49b2a2a5dde8dfc7d1e71c894b7b9081866bfd33d5552deed470", "00044c45500c49b2a2a5dde8dfc7d1e71c894b7b9081866bfd33d5552deed470")

		//for sl := 0; sl < len(storageKeysList); sl++ {
		//	builder.Storage(common.Bytes2Hex(addrWithSingleton), common.Bytes2Hex(storageKeysList[sl]), common.Bytes2Hex(storageKeysList[sl]))
		//}
		// fmt.Printf("addrWithSingleton %x\n", addrWithSingleton)
		// builder.Storage(common.Bytes2Hex(addrWithSingleton), "01044c45500c49b2a2a5dde8dfc7d1e71c894b7b9081866bfd33d5552deed470", "00044c45500c49b2a2a5dde8dfc7d1e71c894b7b9081866bfd33d5552deed470")

		buildTrieAndWitness(t, builder, addrWithSingleton)
	})

	t.Run("StorageSubtrieWithCommonPrefix", func(t *testing.T) {
		t.Skip("flaky test with partially fixed edge case")
		t.Logf("StorageSubtrieWithCommonPrefix")
		plainKeysList, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Addr, 0, 2)

		addrWithSingleton := common.Copy(plainKeysList[0])
		// generate 2 storage slots HAVING common prefix of len >=4
		storageKeysList, _ := generatePlainKeysWithSameHashPrefix(t, nil, length.Hash, 4, 2)

		builder := NewUpdateBuilder()
		for i := 0; i < len(plainKeysList); i++ {
			builder.Balance(common.Bytes2Hex(plainKeysList[i]), uint64(i))
			fmt.Printf("addr %x\n", plainKeysList[i])
		}

		for sl := 0; sl < len(storageKeysList); sl++ {
			builder.Storage(common.Bytes2Hex(addrWithSingleton), common.Bytes2Hex(storageKeysList[sl]), common.Bytes2Hex(storageKeysList[sl]))
			fmt.Printf("storage %x -> %x\n", storageKeysList[sl], storageKeysList[sl])
		}

		buildTrieAndWitness(t, builder, addrWithSingleton)
	})
}
