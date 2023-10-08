package jsonrpc

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common/hexutil"
)

type ContractListResult struct {
	BlocksSummary map[hexutil.Uint64]*BlockSummary `json:"blocksSummary"`
	Results       []interface{}                    `json:"results"`
}

func (api *Otterscan2APIImpl) genericMatchingList(ctx context.Context, tx kv.Tx, matchTable, counterTable string, idx, count uint64) ([]AddrMatch, error) {
	if count > MAX_MATCH_COUNT {
		return nil, fmt.Errorf("maximum allowed results: %v", MAX_MATCH_COUNT)
	}

	if tx == nil {
		var err error
		tx, err = api.db.BeginRo(ctx)
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	c, err := tx.Cursor(counterTable)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	startIdx := idx + 1
	counterK, blockNumV, err := c.Seek(hexutility.EncodeTs(startIdx))
	if err != nil {
		return nil, err
	}
	if counterK == nil {
		return nil, nil
	}

	prevCounterK, _, err := c.Prev()
	if err != nil {
		return nil, err
	}
	prevTotal := uint64(0)
	if prevCounterK != nil {
		prevTotal = binary.BigEndian.Uint64(prevCounterK)
	}

	contracts, err := tx.CursorDupSort(matchTable)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	kk, vv, err := contracts.SeekExact(blockNumV)
	if err != nil {
		return nil, err
	}
	if kk == nil {
		// DB corrupted
		return nil, fmt.Errorf("couldn't find exact block %v for counter index: %v, counter key: %v", binary.BigEndian.Uint64(blockNumV), startIdx, binary.BigEndian.Uint64(counterK))
	}

	// Position cursor at the first match
	for i := uint64(0); i < startIdx-prevTotal-1; i++ {
		kk, vv, err = contracts.NextDup()
		if err != nil {
			return nil, err
		}
	}

	matches := make([]AddrMatch, 0, count)
	for i := uint64(0); i < count && kk != nil; i++ {
		blockNum := hexutil.Uint64(binary.BigEndian.Uint64(kk))
		addr := common.BytesToAddress(vv)
		matches = append(matches, AddrMatch{Block: &blockNum, Address: &addr})

		kk, vv, err = contracts.NextDup()
		if err != nil {
			return nil, err
		}
		if kk == nil {
			kk, vv, err = contracts.NextNoDup()
			if err != nil {
				return nil, err
			}
			if kk == nil {
				break
			}
		}
	}

	return matches, nil
}

func (api *Otterscan2APIImpl) genericMatchingCounter(ctx context.Context, counterTable string) (uint64, error) {
	tx, err := api.db.BeginRo(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	ct, err := tx.Cursor(counterTable)
	if err != nil {
		return 0, err
	}
	defer ct.Close()

	k, _, err := ct.Last()
	if err != nil {
		return 0, err
	}
	if k == nil {
		return 0, nil
	}

	counter := binary.BigEndian.Uint64(k)
	return counter, nil
}
