package state

import (
	"fmt"

	common2 "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

// HistoryReaderV3 Implements StateReader and StateWriter
type HistoryReaderV3 struct {
	ac    *libstate.Aggregator22Context
	txNum uint64
	trace bool
	tx    kv.Tx
	ttx   kv.TemporalTx
}

func NewHistoryReaderV3() *HistoryReaderV3 {
	return &HistoryReaderV3{}
}

func (hr *HistoryReaderV3) SetAc(ac *libstate.Aggregator22Context) {
	hr.ac = ac
}
func (hr *HistoryReaderV3) SetTx(tx kv.Tx) {
	if ttx, casted := tx.(kv.TemporalTx); casted {
		hr.ttx = ttx
	}
	hr.tx = tx
}
func (hr *HistoryReaderV3) SetTxNum(txNum uint64) { hr.txNum = txNum }
func (hr *HistoryReaderV3) SetTrace(trace bool)   { hr.trace = trace }

func (hr *HistoryReaderV3) ReadAccountData(address common.Address) (*accounts.Account, error) {
	var enc []byte
	var ok bool
	var err error
	if hr.ttx != nil {
		enc, ok, err = hr.ttx.DomainGet(temporal.AccountsDomain, address.Bytes(), nil, hr.txNum)
		if err != nil || !ok || len(enc) == 0 {
			if hr.trace {
				fmt.Printf("ReadAccountData [%x] => []\n", address)
			}
			return nil, err
		}
		var a accounts.Account
		if err := a.DecodeForStorage(enc); err != nil {
			return nil, fmt.Errorf("ReadAccountData(%x): %w", address, err)
		}
		if hr.trace {
			fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x]\n", address, a.Nonce, &a.Balance, a.CodeHash)
		}
		return &a, nil
	}
	enc, ok, err = hr.ac.ReadAccountDataNoStateWithRecent(address.Bytes(), hr.txNum)
	if err != nil {
		return nil, err
	}
	if ok {
		if len(enc) == 0 {
			if hr.trace {
				fmt.Printf("ReadAccountData [%x] => []\n", address)
			}
			return nil, nil
		}
		var a accounts.Account
		if err := accounts.DeserialiseV3(&a, enc); err != nil {
			return nil, fmt.Errorf("ReadAccountData(%x): %w", address, err)
		}
		if hr.trace {
			fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x]\n", address, a.Nonce, &a.Balance, a.CodeHash)
		}
		return &a, nil
	}
	enc, err = hr.tx.GetOne(kv.PlainState, address.Bytes())
	if err != nil {
		return nil, err
	}

	if len(enc) == 0 {
		if hr.trace {
			fmt.Printf("ReadAccountData [%x] => []\n", address)
		}
		return nil, nil
	}
	var a accounts.Account
	if err := a.DecodeForStorage(enc); err != nil {
		return nil, fmt.Errorf("ReadAccountData(%x): %w", address, err)
	}

	if hr.trace {
		fmt.Printf("ReadAccountData [%x] => [nonce: %d, balance: %d, codeHash: %x]\n", address, a.Nonce, &a.Balance, a.CodeHash)
	}
	return &a, nil
}

func (hr *HistoryReaderV3) ReadAccountStorage(address common.Address, incarnation uint64, key *common.Hash) ([]byte, error) {
	if hr.ttx != nil {
		enc, _, err := hr.ttx.DomainGet(temporal.StorageDomain, append(address.Bytes(), common2.EncodeTs(incarnation)...), key.Bytes(), hr.txNum)
		return enc, err
	}

	enc, ok, err := hr.ac.ReadAccountStorageNoStateWithRecent(address.Bytes(), key.Bytes(), hr.txNum)
	if err != nil {
		return nil, err
	}
	if !ok {
		k := dbutils.PlainGenerateCompositeStorageKey(address[:], incarnation, key.Bytes())
		enc, err = hr.tx.GetOne(kv.PlainState, k)
		if err != nil {
			return nil, err
		}
	}
	if hr.trace {
		if enc == nil {
			fmt.Printf("ReadAccountStorage [%x] [%x] => []\n", address, key.Bytes())
		} else {
			fmt.Printf("ReadAccountStorage [%x] [%x] => [%x]\n", address, key.Bytes(), enc)
		}
	}
	if enc == nil {
		return nil, nil
	}
	return enc, nil
}

func (hr *HistoryReaderV3) ReadAccountCode(address common.Address, incarnation uint64, codeHash common.Hash) ([]byte, error) {
	if codeHash == emptyCodeHashH {
		return nil, nil
	}
	if hr.ttx != nil {
		enc, _, err := hr.ttx.DomainGet(temporal.CodeDomain, address.Bytes(), codeHash.Bytes(), hr.txNum)
		return enc, err
	}

	enc, ok, err := hr.ac.ReadAccountCodeNoStateWithRecent(address.Bytes(), hr.txNum)
	if err != nil {
		return nil, err
	}
	if !ok {
		enc, err = hr.tx.GetOne(kv.Code, codeHash[:])
		if err != nil {
			return nil, err
		}
	}
	if hr.trace {
		fmt.Printf("ReadAccountCode [%x %x] => [%x]\n", address, codeHash, enc)
	}
	return enc, nil
}

func (hr *HistoryReaderV3) ReadAccountCodeSize(address common.Address, incarnation uint64, codeHash common.Hash) (int, error) {
	if hr.ttx != nil {
		enc, _, err := hr.ttx.DomainGet(temporal.CodeDomain, address.Bytes(), codeHash.Bytes(), hr.txNum)
		return len(enc), err
	}
	enc, ok, err := hr.ac.ReadAccountCodeNoStateWithRecent(address.Bytes(), hr.txNum)
	size := len(enc)
	if err != nil {
		return 0, err
	}
	if !ok {
		enc, err := hr.tx.GetOne(kv.Code, codeHash[:])
		if err != nil {
			return 0, err
		}
		size = len(enc)
	}
	if err != nil {
		return 0, err
	}
	if hr.trace {
		fmt.Printf("ReadAccountCodeSize [%x] => [%d]\n", address, size)
	}
	return size, nil
}

func (hr *HistoryReaderV3) ReadAccountIncarnation(address common.Address) (uint64, error) {
	return 0, nil
}

/*
func (s *HistoryReaderV3) ForEachStorage(addr common.Address, startLocation common.Hash, cb func(key, seckey common.Hash, value uint256.Int) bool, maxResults int) error {
	acc, err := s.ReadAccountData(addr)
	if err != nil {
		return err
	}

	var k [length.Addr + length.Incarnation + length.Hash]byte
	copy(k[:], addr[:])
	binary.BigEndian.PutUint64(k[length.Addr:], acc.Incarnation)
	copy(k[length.Addr+length.Incarnation:], startLocation[:])

	//toKey := make([]byte, 4)
	//bigCurrent.FillBytes(toKey)
	//
	//bigStep := big.NewInt(0x100000000)
	//bigStep.Div(bigStep, bigCount)
	//bigCurrent.Add(bigCurrent, bigStep)
	//toKey = make([]byte, 4)
	//bigCurrent.FillBytes(toKey)

	st := btree.New(16)
	min := &storageItem{key: startLocation}
	overrideCounter := 0
	if t, ok := s.storage[addr]; ok {
		t.AscendGreaterOrEqual(min, func(i btree.Item) bool {
			item := i.(*storageItem)
			st.ReplaceOrInsert(item)
			if !item.value.IsZero() {
				copy(lastKey[:], item.key[:])
				// Only count non-zero items
				overrideCounter++
			}
			return overrideCounter < maxResults
		})
	}
	numDeletes := st.Len() - overrideCounter

	var lastKey common.Hash
	iterator := s.ac.IterateStorageHistory(startLocation.Bytes(), nil, s.txNum)
	for iterator.HasNext() {
		k, vs, p := iterator.Next()
		if len(vs) == 0 {
			// Skip deleted entries
			continue
		}
		kLoc := k[20:]
		keyHash, err1 := common.HashData(kLoc)
		if err1 != nil {
			return err1
		}
		//fmt.Printf("seckey: %x\n", seckey)
		si := storageItem{}
		copy(si.key[:], kLoc)
		copy(si.seckey[:], keyHash[:])
		if st.Has(&si) {
			continue
		}
		si.value.SetBytes(vs)
		st.ReplaceOrInsert(&si)
		if bytes.Compare(kLoc, lastKey[:]) > 0 {
			// Beyond overrides
			if st.Len() < maxResults+numDeletes {
				continue
			}
			break
		}

	}

	results := 0
	var innerErr error
	st.AscendGreaterOrEqual(min, func(i btree.Item) bool {
		item := i.(*storageItem)
		if !item.value.IsZero() {
			// Skip if value == 0
			cb(item.key, item.seckey, item.value)
			results++
		}
		return results < maxResults
	})
	return innerErr
}
*/

/*
func (s *PlainState) ForEachStorage(addr common.Address, startLocation common.Hash, cb func(key, seckey common.Hash, value uint256.Int) bool, maxResults int) error {
	st := btree.New(16)
	var k [length.Addr + length.Incarnation + length.Hash]byte
	copy(k[:], addr[:])
	accData, err := GetAsOf(s.tx, s.accHistoryC, s.accChangesC, false , addr[:], s.blockNr)
	if err != nil {
		return err
	}
	var acc accounts.Account
	if err := acc.DecodeForStorage(accData); err != nil {
		log.Error("Error decoding account", "err", err)
		return err
	}
	binary.BigEndian.PutUint64(k[length.Addr:], acc.Incarnation)
	copy(k[length.Addr+length.Incarnation:], startLocation[:])
	var lastKey common.Hash
	overrideCounter := 0
	min := &storageItem{key: startLocation}
	if t, ok := s.storage[addr]; ok {
		t.AscendGreaterOrEqual(min, func(i btree.Item) bool {
			item := i.(*storageItem)
			st.ReplaceOrInsert(item)
			if !item.value.IsZero() {
				copy(lastKey[:], item.key[:])
				// Only count non-zero items
				overrideCounter++
			}
			return overrideCounter < maxResults
		})
	}
	numDeletes := st.Len() - overrideCounter
	if err := WalkAsOfStorage(s.tx, addr, acc.Incarnation, startLocation, s.blockNr, func(kAddr, kLoc, vs []byte) (bool, error) {
		if !bytes.Equal(kAddr, addr[:]) {
			return false, nil
		}
		if len(vs) == 0 {
			// Skip deleted entries
			return true, nil
		}
		keyHash, err1 := common.HashData(kLoc)
		if err1 != nil {
			return false, err1
		}
		//fmt.Printf("seckey: %x\n", seckey)
		si := storageItem{}
		copy(si.key[:], kLoc)
		copy(si.seckey[:], keyHash[:])
		if st.Has(&si) {
			return true, nil
		}
		si.value.SetBytes(vs)
		st.ReplaceOrInsert(&si)
		if bytes.Compare(kLoc, lastKey[:]) > 0 {
			// Beyond overrides
			return st.Len() < maxResults+numDeletes, nil
		}
		return st.Len() < maxResults+overrideCounter+numDeletes, nil
	}); err != nil {
		log.Error("ForEachStorage walk error", "err", err)
		return err
	}
	results := 0
	var innerErr error
	st.AscendGreaterOrEqual(min, func(i btree.Item) bool {
		item := i.(*storageItem)
		if !item.value.IsZero() {
			// Skip if value == 0
			cb(item.key, item.seckey, item.value)
			results++
		}
		return results < maxResults
	})
	return innerErr
}
*/
