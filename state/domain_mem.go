package state

import (
	"encoding/binary"
	"sync"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/etl"
)

type DomainMem struct {
	*Domain

	etl    *etl.Collector
	mu     sync.RWMutex
	values *KVList
	latest map[string][]byte
}

type KVList struct {
	Keys []string
	Vals [][]byte
}

func (l *KVList) Put(k, v []byte) {
	ks := *(*string)(unsafe.Pointer(&k))
	l.Keys = append(l.Keys, ks)
	l.Vals = append(l.Vals, v)
}

func (l *KVList) Apply(f func(k, v []byte) error) error {
	for i := range l.Keys {
		if err := f([]byte(l.Keys[i]), l.Vals[i]); err != nil {
			return err
		}
	}
	return nil
}

func (l *KVList) Reset() {
	l.Keys = l.Keys[:0]
	l.Vals = l.Vals[:0]
}

func NewDomainMem(d *Domain, tmpdir string) *DomainMem {
	return &DomainMem{
		Domain: d,
		latest: make(map[string][]byte, 128),
		etl:    etl.NewCollector(d.valsTable, tmpdir, etl.NewSortableBuffer(WALCollectorRam)),
		//values: &KVList{
		//	Keys: make([]string, 0, 1000),
		//	Vals: make([][]byte, 0, 1000),
		//},
	}
}

func (d *DomainMem) Get(k1, k2 []byte) ([]byte, error) {
	key := common.Append(k1, k2)

	d.mu.RLock()
	value, _ := d.latest[string(key)]
	d.mu.RUnlock()

	return value, nil
}

func (d *DomainMem) Put(k1, k2, value []byte) error {
	key := common.Append(k1, k2)
	ks := *(*string)(unsafe.Pointer(&key))

	invertedStep := ^(d.txNum / d.aggregationStep)
	keySuffix := make([]byte, len(key)+8)
	copy(keySuffix, key)
	binary.BigEndian.PutUint64(keySuffix[len(key):], invertedStep)

	if err := d.etl.Collect(keySuffix, value); err != nil {
		return err
	}

	d.mu.Lock()
	//d.values.Put(keySuffix, value)
	prev, existed := d.latest[ks]
	_ = existed
	d.latest[ks] = value
	d.mu.Unlock()
	if !existed {
		d.defaultDc.readFromFiles()
	}
	d.Get()

	d.wal.addPrevValue()

	return d.PutWitPrev(k1, k2, value, prev)
}

func (d *DomainMem) Delete(k1, k2 []byte) error {
	key := common.Append(k1, k2)

	d.mu.Lock()
	prev, existed := d.latest[string(key)]
	if existed {
		delete(d.latest, string(key))
	}
	d.mu.Unlock()

	return d.DeleteWithPrev(k1, k2, prev)
}

func (d *DomainMem) Reset() {
	d.mu.Lock()
	d.latest = make(map[string][]byte)
	d.values.Reset()
	d.mu.Unlock()
}
