package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
)

type DirectAccessDomains struct {
	BufferedSharedDomains
	rwTx             kv.RwTx
	lastTouchedTxNum uint64
}

func NewDirectAccessDomains(tx kv.RwTx, logger log.Logger) (*DirectAccessDomains, error) {
	sd, _ := NewSharedDomains(tx, logger)
	dad := &DirectAccessDomains{
		BufferedSharedDomains: *sd,
		rwTx:                  tx,
		lastTouchedTxNum:      0,
		//trace:   true,
	}

	// re-set the pointer to the current object
	dad.sdCtx.sharedDomains = dad
	return dad, nil
}

// // TODO JG: large values tables
// // TODO JG: index tables

func (sd *DirectAccessDomains) ObjectInfo() string {
	return "DirectAccessDomains " + fmt.Sprintf("-%d", sd.objectNum)
}

func (sd *DirectAccessDomains) GetLatest(domain kv.Domain, k []byte) (v []byte, step uint64, err error) {
	if domain != kv.AccountsDomain && domain != kv.CodeDomain && domain != kv.StorageDomain {
		return sd.BufferedSharedDomains.GetLatest(domain, k)
	}
	v, step, _, err = sd.aggTx.GetLatest(domain, k, sd.roTx)
	if err != nil {
		return nil, 0, fmt.Errorf("storage %x read error: %w", k, err)
	}
	// // fmt.Println("JG", sd.ObjectInfo(), "GetLatest", domain.String(), hexutil.Encode(k), hexutil.Encode(v), step)
	return v, step, nil
}

func (sd *DirectAccessDomains) DomainPut(domain kv.Domain, k1, k2 []byte, val, prevVal []byte, prevStep uint64) error {
	if domain != kv.AccountsDomain && domain != kv.CodeDomain && domain != kv.StorageDomain {
		return sd.BufferedSharedDomains.DomainPut(domain, k1, k2, val, prevVal, prevStep)
	}
	// // fmt.Println("JG", sd.ObjectInfo(), "DomainPut", domain.String(), hexutil.Encode(k1), hexutil.Encode(k2), hexutil.Encode(val), hexutil.Encode(prevVal), prevStep)
	if val == nil {
		return fmt.Errorf("DomainPut: %s, trying to put nil value. not allowed", domain)
	}
	if prevVal == nil {
		var err error
		prevVal, prevStep, err = sd.GetLatest(domain, k1)
		if err != nil {
			return err
		}
	}
	sd.sdCtx.TouchKey(domain, string(append(k1, k2...)), val)
	if bytes.Equal(prevVal, val) {
		return nil
	}
	return sd.updateValue(k1, k2, val, domain)
}

func (sd *DirectAccessDomains) updateHistory(k1 []byte, k2 []byte, val []byte, domain kv.Domain) error {
	var histStepPrefix [8]byte
	binary.BigEndian.PutUint64(histStepPrefix[:], ^(sd.txNum / sd.aggTx.a.aggregationStep))
	var txNumBytes [8]byte
	binary.BigEndian.PutUint64(txNumBytes[:], sd.txNum)
	k := append(k1, k2...)
	v := append(txNumBytes[:], val...)
	return sd.rwTx.AppendDup(sd.aggTx.d[domain].ht.h.valuesTable, k, v)
	//TODO: JG Test append dup for sorted table
	//TODO: JG History Keys table
}

func (sd *DirectAccessDomains) updateValue(k1 []byte, k2 []byte, val []byte, domain kv.Domain) error {
	var histStepPrefix [8]byte
	binary.BigEndian.PutUint64(histStepPrefix[:], ^(sd.txNum / sd.aggTx.a.aggregationStep))
	var k, v []byte
	if sd.aggTx.d[domain].d.largeValues {
		k = append(append(k1, k2...), histStepPrefix[:]...)
		v = val
		if err := sd.rwTx.Put(sd.aggTx.d[domain].d.valuesTable, k, v); err != nil {
			// // fmt.Println("JG", sd.ObjectInfo(), "DomainPut", "Put large value", err)
			return err
		}
		return nil
	}
	k = append(k1, k2...)
	v = append(histStepPrefix[:], val...)
	valuesCursor, err := sd.rwTx.RwCursorDupSort(sd.aggTx.d[domain].d.valuesTable)
	if err != nil {
		// // fmt.Println("JG", sd.ObjectInfo(), "DomainPut", "RwCursorDupSort", err)
		return err
	}
	defer valuesCursor.Close()
	foundVal, err := valuesCursor.SeekBothRange(k, v[:8])
	if err != nil {
		// fmt.Println("JG", sd.ObjectInfo(), "DomainPut", "SeekBothRange", err)
		return err
	}
	if len(foundVal) == 0 || !bytes.Equal(foundVal[:8], v[:8]) {
		if err := valuesCursor.Put(k, v); err != nil {
			// fmt.Println("JG", sd.ObjectInfo(), "DomainPut", "Put", err)
			return err
		}
		return nil
	}
	if err := valuesCursor.DeleteCurrent(); err != nil {
		// fmt.Println("JG", sd.ObjectInfo(), "DomainPut", "DeleteCurrent", err)
		return err
	}
	if err := valuesCursor.Put(k, v); err != nil {
		// fmt.Println("JG", sd.ObjectInfo(), "DomainPut", "Put2", err)
		return err
	}
	return nil
}

func (sd *DirectAccessDomains) DomainDel(domain kv.Domain, k1, k2 []byte, prevVal []byte, prevStep uint64) error {
	if domain != kv.AccountsDomain && domain != kv.CodeDomain && domain != kv.StorageDomain {
		return sd.BufferedSharedDomains.DomainDel(domain, k1, k2, prevVal, prevStep)
	}
	fmt.Println("JG", sd.ObjectInfo(), "DomainDel", domain.String(), hexutil.Encode(k1), hexutil.Encode(k2), hexutil.Encode(prevVal), prevStep)
	k := append(k1, k2...)
	v := prevVal
	if v == nil {
		var err error
		v, prevStep, err = sd.GetLatest(domain, k)
		if err != nil {
			return err
		}
	}

	if v == nil || len(v) == 0 {
		fmt.Println("JG", sd.ObjectInfo(), "DomainDel", "value not found", hexutil.Encode(k))
		return nil
	}
	if sd.aggTx.d[domain].d.largeValues {
		valuesCursor, err := sd.rwTx.RwCursor(sd.aggTx.d[domain].d.valuesTable)
		if err != nil {
			return err
		}
		defer valuesCursor.Close()
		return valuesCursor.Delete(k)
	}
	valuesCursor, err := sd.rwTx.RwCursorDupSort(sd.aggTx.d[domain].d.valuesTable)
	if err != nil {
		return err
	}
	defer valuesCursor.Close()
	foundVal, err := valuesCursor.SeekBothRange(k, v[:8])
	if err != nil {
		return err
	}
	if len(foundVal) > 0 {
		if err := valuesCursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	return nil
}

func (sd *DirectAccessDomains) SetTxNum(txNum uint64) {
	sd.BufferedSharedDomains.SetTxNum(txNum)
	if sd.lastTouchedTxNum == 0 {
		sd.lastTouchedTxNum = txNum - 1
	}
}

func (sd *DirectAccessDomains) touchHistory(historyTableName string) error {
	var txKey [8]byte
	binary.BigEndian.PutUint64(txKey[:], sd.lastTouchedTxNum+1)
	valCursor, err := sd.rwTx.CursorDupSort(historyTableName)
	if err != nil {
		return err
	}
	defer valCursor.Close()
	for hk, hv, err := valCursor.Seek(txKey[:]); hv != nil; hk, hv, err = valCursor.Next() {
		if err != nil {
			return err
		}
		if binary.BigEndian.Uint64(hk[:]) > sd.txNum {
			break
		}
		// fmt.Println("JG", historyTableName, "seek", hexutil.Encode(hk), hexutil.Encode(hv))
		sd.TouchKey(kv.AccountsDomain, string(hv), nil)
	}
	return nil
}

func (sd *DirectAccessDomains) ComputeCommitment(ctx context.Context, saveStateAfter bool, blockNum uint64, logPrefix string) (rootHash []byte, err error) {
	// fmt.Println("JG", sd.ObjectInfo(), "ComputeCommitment for block", blockNum)
	if err := sd.touchHistory(kv.TblAccountHistoryKeys); err != nil {
		return nil, err
	}
	if err := sd.touchHistory(kv.TblCodeHistoryKeys); err != nil {
		return nil, err
	}
	if err := sd.touchHistory(kv.TblStorageHistoryKeys); err != nil {
		return nil, err
	}
	sd.lastTouchedTxNum = sd.txNum
	return sd.BufferedSharedDomains.ComputeCommitment(ctx, saveStateAfter, blockNum, logPrefix)
}

func (sd *DirectAccessDomains) Close() {
	sd.BufferedSharedDomains.Close()
}
