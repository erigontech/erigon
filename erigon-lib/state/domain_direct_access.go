package state

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/commitment"
	"github.com/erigontech/erigon-lib/kv"
)

type DirectAccessDomains struct {
	SharedDomains

	rwTx kv.RwTx
}

func NewDirectAccessDomains(tx kv.RwTx, logger log.Logger) (*DirectAccessDomains, error) {
	fmt.Println("NewDirectAccessDomains")
	sd := &SharedDomains{
		logger: logger,
		// storage: btree2.NewMap[string, dataWithPrevStep](128),
		//trace:   true,
	}
	sd.SetTx(tx)

	sd.aggTx.a.DiscardHistory(kv.CommitmentDomain)

	for id, ii := range sd.aggTx.iis {
		sd.iiWriters[id] = ii.NewWriter()
	}

	for id, d := range sd.aggTx.d {
		if kv.Domain(id) == kv.CommitmentDomain {
			sd.domainWriters[id] = d.NewWriter()
		}
	}

	sd.SetTxNum(0)
	sd.sdCtx = NewSharedDomainsCommitmentContext(sd, commitment.ModeDirect, commitment.VariantHexPatriciaTrie)

	if _, err := sd.SeekCommitment(context.Background(), tx); err != nil {
		return nil, err
	}
	return &DirectAccessDomains{
		SharedDomains: *sd,
		rwTx:          tx,
	}, nil
}

// TODO JG: large values tables
// TODO JG: index tables

func (sd *DirectAccessDomains) ObjectInfo() string {
	return "DirectAccessDomains"
}

// TemporalDomain satisfaction
func (sd *DirectAccessDomains) GetLatest(domain kv.Domain, k, k2 []byte) (v []byte, step uint64, err error) {
	if domain == kv.CommitmentDomain {
		fmt.Println("JG GetLatest", domain.String(), "LatestCommitment")
		return sd.SharedDomains.LatestCommitment(k)
	}

	if k2 != nil {
		k = append(k, k2...)
	}
	v, step, _, err = sd.aggTx.GetLatest(domain, k, nil, sd.roTx)
	if err != nil {
		fmt.Println("JG DAD.GetLatest", domain.String(), err)
		return nil, 0, fmt.Errorf("storage %x read error: %w", k, err)
	}
	fmt.Println("JG GetLatest", sd.ObjectInfo(), domain.String(), hexutility.Encode(k), hexutility.Encode(v), step)
	return v, step, nil
}

func (sd *DirectAccessDomains) DomainPut(domain kv.Domain, k1, k2 []byte, val, prevVal []byte, prevStep uint64) error {
	if val == nil {
		return fmt.Errorf("DomainPut: %s, trying to put nil value. not allowed", domain)
	}
	if prevVal == nil {
		var err error
		prevVal, prevStep, err = sd.GetLatest(domain, k1, k2)
		if err != nil {
			return err
		}
	}

	switch domain {
	default:
		if bytes.Equal(prevVal, val) {
			return nil
		}

		err := sd.updateHistory(k1, k2, val, domain)
		if err != nil {
			return err
		}

		return sd.updateValue(k1, k2, val, domain)
	}
}

func (sd *DirectAccessDomains) updateHistory(k1 []byte, k2 []byte, val []byte, domain kv.Domain) error {
	var histStepPrefix [8]byte
	binary.BigEndian.PutUint64(histStepPrefix[:], ^(sd.txNum / sd.aggTx.a.aggregationStep))
	var txNumBytes [8]byte
	binary.BigEndian.PutUint64(txNumBytes[:], sd.txNum)

	k := append(k1, k2...)
	v := append(txNumBytes[:], val...)

	err := sd.rwTx.AppendDup(sd.aggTx.d[domain].d.hist.valuesTable, k, v)
	if err != nil {
		return err
	}
	return nil

	//TODO: JG Test append dup for sorted table
	//TODO: JG History Keys table
}

func (sd *DirectAccessDomains) updateValue(k1 []byte, k2 []byte, val []byte, domain kv.Domain) error {
	var histStepPrefix [8]byte
	binary.BigEndian.PutUint64(histStepPrefix[:], ^(sd.txNum / sd.aggTx.a.aggregationStep))

	var k []byte
	var v []byte

	if sd.aggTx.d[domain].d.largeValues {
		k = append(append(k1, k2...), histStepPrefix[:]...)
		v = val
	} else {
		k = append(k1, k2...)
		v = append(histStepPrefix[:], val...)

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
	if len(foundVal) == 0 || !bytes.Equal(foundVal[:8], v[:8]) {
		if err := valuesCursor.Put(k, v); err != nil {
			return err
		}
		return nil
	}
	if err := valuesCursor.DeleteCurrent(); err != nil {
		return err
	}
	if err := valuesCursor.Put(k, v); err != nil {
		return err
	}
	return nil
}

func (sd *DirectAccessDomains) DomainDel(domain kv.Domain, k1, k2 []byte, prevVal []byte, prevStep uint64) error {
	if prevVal == nil {
		var err error
		prevVal, prevStep, err = sd.GetLatest(domain, k1, k2)
		if err != nil {
			return err
		}
	}

	k := append(k1, k2...)
	v := prevVal

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

func (sd *DirectAccessDomains) Close() {
	fmt.Println("JG", sd.ObjectInfo(), "Close")
	sd.SetBlockNum(0)
	if sd.aggTx != nil {
		sd.SetTxNum(0)

		//sd.walLock.Lock()
		//defer sd.walLock.Unlock()
		for _, d := range sd.domainWriters {
			d.close()
		}
		for _, iiWriter := range sd.iiWriters {
			iiWriter.close()
		}
	}

	if sd.sdCtx != nil {
		sd.sdCtx.Close()
	}
}
