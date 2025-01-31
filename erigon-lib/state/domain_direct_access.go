package state

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/kv"
)

type DirectAccessDomains struct {
	SharedDomains

	rwTx kv.RwTx
}

func NewDirectAccessDomains(tx kv.RwTx, logger log.Logger) (*DirectAccessDomains, error) {
	// fmt.Println("NewDirectAccessDomains")

	sub, _ := NewSharedDomains(tx, logger)

	sd := &DirectAccessDomains{
		SharedDomains: *sub,
		rwTx:          tx,
		//trace:   true,
	}

	sd.sdCtx.sharedDomains = &sd.SharedDomains
	// sd.SetTx(tx)

	// sd.aggTx.a.DiscardHistory(kv.CommitmentDomain)

	// for id, ii := range sd.aggTx.iis {
	// 	sd.iiWriters[id] = ii.NewWriter()
	// }

	// for id, d := range sd.aggTx.d {
	// 	sd.domains[id] = map[string]dataWithPrevStep{}
	// 	sd.domainWriters[id] = d.NewWriter()
	// }

	// sd.SetTxNum(0)
	// sd.sdCtx = NewSharedDomainsCommitmentContext(sub, commitment.ModeDirect, commitment.VariantHexPatriciaTrie)

	// if _, err := sd.SeekCommitment(context.Background(), tx); err != nil {
	// 	return nil, err
	// }
	return sd, nil
}

// TODO JG: large values tables
// TODO JG: index tables

func (sd *DirectAccessDomains) ObjectInfo() string {
	return "DirectAccessDomains"
}

// TemporalDomain satisfaction
func (sd *DirectAccessDomains) GetLatest(domain kv.Domain, k []byte) (v []byte, step uint64, err error) {
	if domain == kv.CommitmentDomain {
		fmt.Println("JG GetLatest", "LatestCommitment")
		return sd.SharedDomains.LatestCommitment(k)
	}

	v, step, _, err = sd.aggTx.GetLatest(domain, k, sd.roTx)
	if err != nil {
		return nil, 0, fmt.Errorf("storage %x read error: %w", k, err)
	}
	fmt.Println("JG GetLatest", domain.String(), hexutility.Encode(k), hexutility.Encode(v), step)
	return v, step, nil
}

func (sd *DirectAccessDomains) DomainPut(domain kv.Domain, k1, k2 []byte, val, prevVal []byte, prevStep uint64) error {
	fmt.Println("JG DomainPut", domain.String(), hexutility.Encode(k1), hexutility.Encode(k2), hexutility.Encode(val), prevStep)
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

	// sd.sdCtx.TouchKey()

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

	err := sd.rwTx.AppendDup(sd.aggTx.d[domain].ht.h.valuesTable, k, v)
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

	sd.sdCtx.TouchKey(domain, string(k), v)

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
		prevVal, prevStep, err = sd.GetLatest(domain, k1)
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
	sd.SharedDomains.Close()
}
