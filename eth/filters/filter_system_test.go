// Copyright 2016 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package filters

/*
var (
	deadline = 5 * time.Minute
)

// TestBlockSubscription tests if a block subscription returns block hashes for posted chain events.
// It creates multiple subscriptions:
// - one at the start and should receive all posted chain events and a second (blockHashes)
// - one that is created after a cutoff moment and uninstalled after a second cutoff moment (blockHashes[cutoff1:cutoff2])
// - one that is created after the second cutoff moment (blockHashes[cutoff2:])
func TestBlockSubscription(t *testing.T) {
	t.Parallel()

	db := ethdb.NewTestDB(t)
	var (
		backend     = &testBackend{db: db}
		api         = NewPublicFilterAPI(backend, deadline)
		genesis     = (&core.Genesis{Config: params.TestChainConfig}).MustCommitDeprecated(db)
		chain, _    = core.GenerateChain(params.TestChainConfig, genesis, ethash.NewFaker(), db.RwKV(), 10, func(i int, gen *core.BlockGen) {}, false)
		chainEvents = []core.ChainEvent{}
	)

	for _, blk := range chain.Blocks {
		chainEvents = append(chainEvents, core.ChainEvent{Hash: blk.Hash(), Block: blk})
	}

	chan0 := make(chan *types.Header)
	sub0 := api.events.SubscribeNewHeads(chan0)
	chan1 := make(chan *types.Header)
	sub1 := api.events.SubscribeNewHeads(chan1)

	go func() { // simulate client
		i1, i2 := 0, 0
		for i1 != len(chainEvents) || i2 != len(chainEvents) {
			select {
			case header := <-chan0:
				if chainEvents[i1].Hash != header.Hash() {
					t.Errorf("sub0 received invalid hash on index %d, want %x, got %x", i1, chainEvents[i1].Hash, header.Hash())
				}
				i1++
			case header := <-chan1:
				if chainEvents[i2].Hash != header.Hash() {
					t.Errorf("sub1 received invalid hash on index %d, want %x, got %x", i2, chainEvents[i2].Hash, header.Hash())
				}
				i2++
			}
		}

		sub0.Unsubscribe()
		sub1.Unsubscribe()
	}()

	time.Sleep(1 * time.Second)
	for _, e := range chainEvents {
		backend.chainFeed.Send(e)
	}

	<-sub0.Err()
	<-sub1.Err()
}

// TestPendingTxFilter tests whether pending tx filters retrieve all pending transactions that are posted to the event mux.
func TestPendingTxFilter(t *testing.T) {
	t.Parallel()

	db := ethdb.NewTestDB(t)
	var (
		backend = &testBackend{db: db}
		api     = NewPublicFilterAPI(backend, deadline)

		transactions = []types.Transaction{
			types.NewTransaction(0, libcommon.HexToAddress("0xb794f5ea0ba39494ce83a213fffba74279579268"), new(uint256.Int), 0, new(uint256.Int), nil),
			types.NewTransaction(1, libcommon.HexToAddress("0xb794f5ea0ba39494ce83a213fffba74279579268"), new(uint256.Int), 0, new(uint256.Int), nil),
			types.NewTransaction(2, libcommon.HexToAddress("0xb794f5ea0ba39494ce83a213fffba74279579268"), new(uint256.Int), 0, new(uint256.Int), nil),
			types.NewTransaction(3, libcommon.HexToAddress("0xb794f5ea0ba39494ce83a213fffba74279579268"), new(uint256.Int), 0, new(uint256.Int), nil),
			types.NewTransaction(4, libcommon.HexToAddress("0xb794f5ea0ba39494ce83a213fffba74279579268"), new(uint256.Int), 0, new(uint256.Int), nil),
		}

		hashes []libcommon.Hash
	)

	fid0 := api.NewPendingTransactionFilter()

	time.Sleep(1 * time.Second)
	backend.txFeed.Send(core.NewTxsEvent{Txs: transactions})

	timeout := time.Now().Add(1 * time.Second)
	for {
		results, err := api.GetFilterChanges(fid0)
		if err != nil {
			t.Fatalf("Unable to retrieve logs: %v", err)
		}

		h := results.([]libcommon.Hash)
		hashes = append(hashes, h...)
		if len(hashes) >= len(transactions) {
			break
		}
		// check timeout
		if time.Now().After(timeout) {
			break
		}

		time.Sleep(100 * time.Millisecond)
	}

	if len(hashes) != len(transactions) {
		t.Errorf("invalid number of transactions, want %d transactions(s), got %d", len(transactions), len(hashes))
		return
	}
	for i := range hashes {
		if hashes[i] != transactions[i].Hash() {
			t.Errorf("hashes[%d] invalid, want %x, got %x", i, transactions[i].Hash(), hashes[i])
		}
	}
}

// TestLogFilterCreation test whether a given filter criteria makes sense.
// If not it must return an error.
func TestLogFilterCreation(t *testing.T) {
	db := ethdb.NewTestDB(t)
	var (
		backend = &testBackend{db: db}
		api     = NewPublicFilterAPI(backend, deadline)

		testCases = []struct {
			crit    FilterCriteria
			success bool
		}{
			// defaults
			{FilterCriteria{}, true},
			// valid block number range
			{FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(2)}, true},
			// "mined" block range to pending
			{FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(rpc.LatestBlockNumber.Int64())}, true},
			// new mined and pending blocks
			{FilterCriteria{FromBlock: big.NewInt(rpc.LatestBlockNumber.Int64()), ToBlock: big.NewInt(rpc.PendingBlockNumber.Int64())}, true},
			// from block "higher" than to block
			{FilterCriteria{FromBlock: big.NewInt(2), ToBlock: big.NewInt(1)}, false},
			// from block "higher" than to block
			{FilterCriteria{FromBlock: big.NewInt(rpc.LatestBlockNumber.Int64()), ToBlock: big.NewInt(100)}, false},
			// from block "higher" than to block
			{FilterCriteria{FromBlock: big.NewInt(rpc.PendingBlockNumber.Int64()), ToBlock: big.NewInt(100)}, false},
			// from block "higher" than to block
			{FilterCriteria{FromBlock: big.NewInt(rpc.PendingBlockNumber.Int64()), ToBlock: big.NewInt(rpc.LatestBlockNumber.Int64())}, false},
		}
	)

	for i, test := range testCases {
		_, err := api.NewFilter(test.crit)
		if test.success && err != nil {
			t.Errorf("expected filter creation for case %d to success, got %v", i, err)
		}
		if !test.success && err == nil {
			t.Errorf("expected testcase %d to fail with an error", i)
		}
	}
}

// TestInvalidLogFilterCreation tests whether invalid filter log criteria results in an error
// when the filter is created.
func TestInvalidLogFilterCreation(t *testing.T) {
	t.Parallel()
	db := ethdb.NewTestDB(t)
	var (
		backend = &testBackend{db: db}
		api     = NewPublicFilterAPI(backend, deadline)
	)

	// different situations where log filter creation should fail.
	// Reason: fromBlock > toBlock
	testCases := []FilterCriteria{
		0: {FromBlock: big.NewInt(rpc.PendingBlockNumber.Int64()), ToBlock: big.NewInt(rpc.LatestBlockNumber.Int64())},
		1: {FromBlock: big.NewInt(rpc.PendingBlockNumber.Int64()), ToBlock: big.NewInt(100)},
		2: {FromBlock: big.NewInt(rpc.LatestBlockNumber.Int64()), ToBlock: big.NewInt(100)},
	}

	for i, test := range testCases {
		if _, err := api.NewFilter(test); err == nil {
			t.Errorf("Expected NewFilter for case #%d to fail", i)
		}
	}
}

func TestInvalidGetLogsRequest(t *testing.T) {
	db := ethdb.NewTestDB(t)
	var (
		backend   = &testBackend{db: db}
		api       = NewPublicFilterAPI(backend, deadline)
		blockHash = libcommon.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	)

	// Reason: Cannot specify both BlockHash and FromBlock/ToBlock)
	testCases := []FilterCriteria{
		0: {BlockHash: &blockHash, FromBlock: big.NewInt(100)},
		1: {BlockHash: &blockHash, ToBlock: big.NewInt(500)},
		2: {BlockHash: &blockHash, FromBlock: big.NewInt(rpc.LatestBlockNumber.Int64())},
	}

	for i, test := range testCases {
		if _, err := api.GetLogs(context.Background(), test); err == nil {
			t.Errorf("Expected Logs for case #%d to fail", i)
		}
	}
}

// TestLogFilter tests whether log filters match the correct logs that are posted to the event feed.
func TestLogFilter(t *testing.T) {
	t.Skip("Erigon doesn't have public API, move this test to RPCDaemon")
	t.Parallel()

	db := ethdb.NewTestDB(t)
	var (
		backend = &testBackend{db: db}
		api     = NewPublicFilterAPI(backend, deadline)

		firstAddr      = libcommon.HexToAddress("0x1111111111111111111111111111111111111111")
		secondAddr     = libcommon.HexToAddress("0x2222222222222222222222222222222222222222")
		thirdAddress   = libcommon.HexToAddress("0x3333333333333333333333333333333333333333")
		notUsedAddress = libcommon.HexToAddress("0x9999999999999999999999999999999999999999")
		firstTopic     = libcommon.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
		secondTopic    = libcommon.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
		notUsedTopic   = libcommon.HexToHash("0x9999999999999999999999999999999999999999999999999999999999999999")

		// posted twice, once as regular logs and once as pending logs.
		allLogs = []*types.Log{
			{Address: firstAddr},
			{Address: firstAddr, Topics: []libcommon.Hash{firstTopic}, BlockNumber: 1},
			{Address: secondAddr, Topics: []libcommon.Hash{firstTopic}, BlockNumber: 1},
			{Address: thirdAddress, Topics: []libcommon.Hash{secondTopic}, BlockNumber: 2},
			{Address: thirdAddress, Topics: []libcommon.Hash{secondTopic}, BlockNumber: 3},
		}

		expectedCase7  = []*types.Log{allLogs[3], allLogs[4], allLogs[0], allLogs[1], allLogs[2], allLogs[3], allLogs[4]}
		expectedCase11 = []*types.Log{allLogs[1], allLogs[2], allLogs[1], allLogs[2]}

		testCases = []struct {
			crit     FilterCriteria
			expected []*types.Log
			id       rpc.ID
		}{
			// match all
			0: {FilterCriteria{}, allLogs, ""},
			// match none due to no matching addresses
			1: {FilterCriteria{Addresses: []libcommon.Address{{}, notUsedAddress}, Topics: [][]libcommon.Hash{nil}}, []*types.Log{}, ""},
			// match logs based on addresses, ignore topics
			2: {FilterCriteria{Addresses: []libcommon.Address{firstAddr}}, allLogs[:2], ""},
			// match none due to no matching topics (match with address)
			3: {FilterCriteria{Addresses: []libcommon.Address{secondAddr}, Topics: [][]libcommon.Hash{{notUsedTopic}}}, []*types.Log{}, ""},
			// match logs based on addresses and topics
			4: {FilterCriteria{Addresses: []libcommon.Address{thirdAddress}, Topics: [][]libcommon.Hash{{firstTopic, secondTopic}}}, allLogs[3:5], ""},
			// match logs based on multiple addresses and "or" topics
			5: {FilterCriteria{Addresses: []libcommon.Address{secondAddr, thirdAddress}, Topics: [][]libcommon.Hash{{firstTopic, secondTopic}}}, allLogs[2:5], ""},
			// logs in the pending block
			6: {FilterCriteria{Addresses: []libcommon.Address{firstAddr}, FromBlock: big.NewInt(rpc.PendingBlockNumber.Int64()), ToBlock: big.NewInt(rpc.PendingBlockNumber.Int64())}, allLogs[:2], ""},
			// mined logs with block num >= 2 or pending logs
			7: {FilterCriteria{FromBlock: big.NewInt(2), ToBlock: big.NewInt(rpc.PendingBlockNumber.Int64())}, expectedCase7, ""},
			// all "mined" logs with block num >= 2
			8: {FilterCriteria{FromBlock: big.NewInt(2), ToBlock: big.NewInt(rpc.LatestBlockNumber.Int64())}, allLogs[3:], ""},
			// all "mined" logs
			9: {FilterCriteria{ToBlock: big.NewInt(rpc.LatestBlockNumber.Int64())}, allLogs, ""},
			// all "mined" logs with 1>= block num <=2 and topic secondTopic
			10: {FilterCriteria{FromBlock: big.NewInt(1), ToBlock: big.NewInt(2), Topics: [][]libcommon.Hash{{secondTopic}}}, allLogs[3:4], ""},
			// all "mined" and pending logs with topic firstTopic
			11: {FilterCriteria{FromBlock: big.NewInt(rpc.LatestBlockNumber.Int64()), ToBlock: big.NewInt(rpc.PendingBlockNumber.Int64()), Topics: [][]libcommon.Hash{{firstTopic}}}, expectedCase11, ""},
			// match all logs due to wildcard topic
			12: {FilterCriteria{Topics: [][]libcommon.Hash{nil}}, allLogs[1:], ""},
		}
	)

	// create all filters
	for i := range testCases {
		testCases[i].id, _ = api.NewFilter(testCases[i].crit)
	}

	// raise events
	time.Sleep(1 * time.Second)
	if nsend := backend.logsFeed.Send(allLogs); nsend == 0 {
		t.Fatal("Logs event not delivered")
	}
	if nsend := backend.pendingLogsFeed.Send(allLogs); nsend == 0 {
		t.Fatal("Pending logs event not delivered")
	}

	for i, tt := range testCases {
		var fetched []*types.Log
		timeout := time.Now().Add(1 * time.Second)
		for { // fetch all expected logs
			results, err := api.GetFilterChanges(tt.id)
			if err != nil {
				t.Fatalf("Unable to fetch logs: %v", err)
			}

			fetched = append(fetched, results.([]*types.Log)...)
			if len(fetched) >= len(tt.expected) {
				break
			}
			// check timeout
			if time.Now().After(timeout) {
				break
			}

			time.Sleep(100 * time.Millisecond)
		}

		if len(fetched) != len(tt.expected) {
			t.Errorf("invalid number of logs for case %d, want %d log(s), got %d", i, len(tt.expected), len(fetched))
			return
		}

		for l := range fetched {
			if fetched[l].Removed {
				t.Errorf("expected log not to be removed for log %d in case %d", l, i)
			}
			if !reflect.DeepEqual(fetched[l], tt.expected[l]) {
				t.Errorf("invalid log on index %d for case %d", l, i)
			}
		}
	}
}

// TestPendingLogsSubscription tests if a subscription receives the correct pending logs that are posted to the event feed.
func TestPendingLogsSubscription(t *testing.T) {
	t.Parallel()

	db := ethdb.NewTestDB(t)
	var (
		backend = &testBackend{db: db}
		api     = NewPublicFilterAPI(backend, deadline)

		firstAddr      = libcommon.HexToAddress("0x1111111111111111111111111111111111111111")
		secondAddr     = libcommon.HexToAddress("0x2222222222222222222222222222222222222222")
		thirdAddress   = libcommon.HexToAddress("0x3333333333333333333333333333333333333333")
		notUsedAddress = libcommon.HexToAddress("0x9999999999999999999999999999999999999999")
		firstTopic     = libcommon.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
		secondTopic    = libcommon.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
		thirdTopic     = libcommon.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333")
		fourthTopic    = libcommon.HexToHash("0x4444444444444444444444444444444444444444444444444444444444444444")
		notUsedTopic   = libcommon.HexToHash("0x9999999999999999999999999999999999999999999999999999999999999999")

		allLogs = [][]*types.Log{
			{{Address: firstAddr, Topics: []libcommon.Hash{}, BlockNumber: 0}},
			{{Address: firstAddr, Topics: []libcommon.Hash{firstTopic}, BlockNumber: 1}},
			{{Address: secondAddr, Topics: []libcommon.Hash{firstTopic}, BlockNumber: 2}},
			{{Address: thirdAddress, Topics: []libcommon.Hash{secondTopic}, BlockNumber: 3}},
			{{Address: thirdAddress, Topics: []libcommon.Hash{secondTopic}, BlockNumber: 4}},
			{
				{Address: thirdAddress, Topics: []libcommon.Hash{firstTopic}, BlockNumber: 5},
				{Address: thirdAddress, Topics: []libcommon.Hash{thirdTopic}, BlockNumber: 5},
				{Address: thirdAddress, Topics: []libcommon.Hash{fourthTopic}, BlockNumber: 5},
				{Address: firstAddr, Topics: []libcommon.Hash{firstTopic}, BlockNumber: 5},
			},
		}

		testCases = []struct {
			crit     ethereum.FilterQuery
			expected []*types.Log
			c        chan []*types.Log
			sub      *Subscription
		}{
			// match all
			{
				ethereum.FilterQuery{}, flattenLogs(allLogs),
				nil, nil,
			},
			// match none due to no matching addresses
			{
				ethereum.FilterQuery{Addresses: []libcommon.Address{{}, notUsedAddress}, Topics: [][]libcommon.Hash{nil}},
				nil,
				nil, nil,
			},
			// match logs based on addresses, ignore topics
			{
				ethereum.FilterQuery{Addresses: []libcommon.Address{firstAddr}},
				append(flattenLogs(allLogs[:2]), allLogs[5][3]),
				nil, nil,
			},
			// match none due to no matching topics (match with address)
			{
				ethereum.FilterQuery{Addresses: []libcommon.Address{secondAddr}, Topics: [][]libcommon.Hash{{notUsedTopic}}},
				nil, nil, nil,
			},
			// match logs based on addresses and topics
			{
				ethereum.FilterQuery{Addresses: []libcommon.Address{thirdAddress}, Topics: [][]libcommon.Hash{{firstTopic, secondTopic}}},
				append(flattenLogs(allLogs[3:5]), allLogs[5][0]),
				nil, nil,
			},
			// match logs based on multiple addresses and "or" topics
			{
				ethereum.FilterQuery{Addresses: []libcommon.Address{secondAddr, thirdAddress}, Topics: [][]libcommon.Hash{{firstTopic, secondTopic}}},
				append(flattenLogs(allLogs[2:5]), allLogs[5][0]),
				nil,
				nil,
			},
			// block numbers are ignored for filters created with New***Filter, these return all logs that match the given criteria when the state changes
			{
				ethereum.FilterQuery{Addresses: []libcommon.Address{firstAddr}, FromBlock: big.NewInt(2), ToBlock: big.NewInt(3)},
				append(flattenLogs(allLogs[:2]), allLogs[5][3]),
				nil, nil,
			},
			// multiple pending logs, should match only 2 topics from the logs in block 5
			{
				ethereum.FilterQuery{Addresses: []libcommon.Address{thirdAddress}, Topics: [][]libcommon.Hash{{firstTopic, fourthTopic}}},
				[]*types.Log{allLogs[5][0], allLogs[5][2]},
				nil, nil,
			},
		}
	)

	// create all subscriptions, this ensures all subscriptions are created before the events are posted.
	// on slow machines this could otherwise lead to missing events when the subscription is created after
	// (some) events are posted.
	for i := range testCases {
		testCases[i].c = make(chan []*types.Log)
		testCases[i].sub, _ = api.events.SubscribeLogs(testCases[i].crit, testCases[i].c)
	}

	for n, test := range testCases {
		i := n
		tt := test
		go func() {
			var fetched []*types.Log
		fetchLoop:
			for {
				logs := <-tt.c
				fetched = append(fetched, logs...)
				if len(fetched) >= len(tt.expected) {
					break fetchLoop
				}
			}

			if len(fetched) != len(tt.expected) {
				panic(fmt.Sprintf("invalid number of logs for case %d, want %d log(s), got %d", i, len(tt.expected), len(fetched)))
			}

			for l := range fetched {
				if fetched[l].Removed {
					panic(fmt.Sprintf("expected log not to be removed for log %d in case %d", l, i))
				}
				if !reflect.DeepEqual(fetched[l], tt.expected[l]) {
					panic(fmt.Sprintf("invalid log on index %d for case %d", l, i))
				}
			}
		}()
	}

	// raise events
	time.Sleep(1 * time.Second)
	for _, ev := range allLogs {
		backend.pendingLogsFeed.Send(ev)
	}
}

// TestPendingTxFilterDeadlock tests if the event loop hangs when pending
// txes arrive at the same time that one of multiple filters is timing out.
// Please refer to #22131 for more details.
func TestPendingTxFilterDeadlock(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("fix me on win please")
	}
	t.Parallel()
	timeout := 100 * time.Millisecond

	var (
		db      = ethdb.NewTestDB(t)
		backend = &testBackend{db: db}
		api     = NewPublicFilterAPI(backend, timeout)
		done    = make(chan struct{})
	)

	go func() {
		// Bombard feed with txes until signal was received to stop
		i := uint64(0)
		for {
			select {
			case <-done:
				return
			default:
			}

			tx := types.NewTransaction(i, libcommon.HexToAddress("0xb794f5ea0ba39494ce83a213fffba74279579268"), uint256.NewInt(0), 0, uint256.NewInt(0), nil)
			backend.txFeed.Send(core.NewTxsEvent{Txs: []types.Transaction{tx}})
			i++
		}
	}()

	// Create a bunch of filters that will
	// timeout either in 100ms or 200ms
	fids := make([]rpc.ID, 20)
	for i := 0; i < len(fids); i++ {
		fid := api.NewPendingTransactionFilter()
		fids[i] = fid
		// Wait for at least one tx to arrive in filter
		for {
			hashes, err := api.GetFilterChanges(fid)
			if err != nil {
				t.Fatalf("Filter should exist: %v\n", err)
			}
			if len(hashes.([]libcommon.Hash)) > 0 {
				break
			}
			runtime.Gosched()
		}
	}

	// Wait until filters have timed out
	time.Sleep(3 * timeout)

	// If tx loop doesn't consume `done` after a second
	// it's hanging.
	select {
	case done <- struct{}{}:
		// Check that all filters have been uninstalled
		for _, fid := range fids {
			if _, err := api.GetFilterChanges(fid); err == nil {
				t.Errorf("Filter %s should have been uninstalled\n", fid)
			}
		}
	case <-time.After(1 * time.Second):
		t.Error("Tx sending loop hangs")
	}
}
func flattenLogs(pl [][]*types.Log) []*types.Log {
	//nolint: prealloc
	var logs []*types.Log
	for _, l := range pl {
		logs = append(logs, l...)
	}
	return logs
}
*/
