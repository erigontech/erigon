package commands

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
)

var (
	blockRangeTestsLatestBlock     = uint64(10)
	blockRangeTestsValidHashNumber = uint64(4)
	blockRangeTestsValidHash       = common.HexToHash("0xbd552cf35ebdcf5369d610542910ecef1c741110d439a9782b3580809f2f05d4")
	blockRangeTestsInvalidHash     = common.HexToHash("0xbd552cf35ebdcf5369d610542910ecef1c741110d439a9782b3580809f2f06d4")
)

var blockRangeTests = []struct {
	name      string
	crit      filters.FilterCriteria
	wantBegin uint64
	wantEnd   uint64
	wantErr   error
}{
	{"All nil", filters.FilterCriteria{FromBlock: nil, ToBlock: nil, BlockHash: nil}, blockRangeTestsLatestBlock, blockRangeTestsLatestBlock, nil},
	{"FromBlock -1", filters.FilterCriteria{FromBlock: big.NewInt(-1), ToBlock: nil, BlockHash: nil}, blockRangeTestsLatestBlock, blockRangeTestsLatestBlock, nil},
	{"ToBlock -1", filters.FilterCriteria{FromBlock: nil, ToBlock: big.NewInt(-1), BlockHash: nil}, blockRangeTestsLatestBlock, blockRangeTestsLatestBlock, nil},
	{"Both -1", filters.FilterCriteria{FromBlock: big.NewInt(-1), ToBlock: big.NewInt(-1), BlockHash: nil}, blockRangeTestsLatestBlock, blockRangeTestsLatestBlock, nil},
	{"FromBlock negative and not -1", filters.FilterCriteria{FromBlock: big.NewInt(-100), ToBlock: nil, BlockHash: nil}, 0, 0, fmt.Errorf("negative value for FromBlock: %v", big.NewInt(-100))},
	{"ToBlock negative and not -1", filters.FilterCriteria{FromBlock: nil, ToBlock: big.NewInt(-100), BlockHash: nil}, 0, 0, fmt.Errorf("negative value for ToBlock: %v", big.NewInt(-100))},
	{"From 3 to 5", filters.FilterCriteria{FromBlock: big.NewInt(3), ToBlock: big.NewInt(5), BlockHash: nil}, 3, 5, nil},
	{"Valid hash", filters.FilterCriteria{FromBlock: nil, ToBlock: nil, BlockHash: &blockRangeTestsValidHash}, blockRangeTestsValidHashNumber, blockRangeTestsValidHashNumber, nil},
	{"Invalid hash", filters.FilterCriteria{FromBlock: nil, ToBlock: nil, BlockHash: &blockRangeTestsInvalidHash}, 0, 0, fmt.Errorf("block not found: %x", &blockRangeTestsInvalidHash)},
}

func TestBlockRangeFromFilter(t *testing.T) {
	db := rpcdaemontest.CreateTestKV(t)
	defer db.Close()

	stateCache := kvcache.New(kvcache.DefaultCoherentConfig)
	api := NewEthAPI(
		NewBaseApi(nil, stateCache, snapshotsync.NewBlockReader(), false),
		db, nil, nil, nil, 5000000)

	tx, _ := api.db.BeginRo(context.Background())
	defer tx.Rollback()

	for _, test := range blockRangeTests {
		t.Run(test.name, func(t *testing.T) {
			begin, end, err := blockRangeFromFilter(tx, test.crit)

			if begin != test.wantBegin {
				t.Errorf("have begin = %v, expected begin = %v", begin, test.wantBegin)
			}

			if end != test.wantEnd {
				t.Errorf("have end = %v, expected end = %v", end, test.wantEnd)
			}

			if err != nil && test.wantErr == nil {
				t.Errorf("unexpected err occured = %v", err)
			}

			if err == nil && test.wantErr != nil {
				t.Errorf("err is nil, but expected err = %v", test.wantErr)
			}

			if err != nil && test.wantErr != nil && err.Error() != test.wantErr.Error() {
				t.Errorf("have err = %v, expected err = %v", err.Error(), test.wantErr.Error())
			}
		})
	}
}
