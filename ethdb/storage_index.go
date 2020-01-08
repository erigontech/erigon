package ethdb

import (
	"bytes"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ugorji/go/codec"
)

func NewStorageIndex() StorageIndex  {
	return make(StorageIndex)
}
type StorageIndex map[common.Hash]*HistoryIndex


func (si StorageIndex) Encode() ([]byte, error) {
	var w bytes.Buffer
	var handle codec.CborHandle
	//handle.WriterBufferSize = 1024
	encoder := codec.NewEncoder(&w, &handle)
	err:=encoder.Encode(si)
	if err!=nil {
		return nil, err
	}
	return w.Bytes(), nil
}

func (si StorageIndex) Decode(s []byte) error {
	if len(s) == 0 {
		return nil
	}
	var handle codec.CborHandle
	decoder:=codec.NewDecoder(bytes.NewBuffer(s), &handle)
	return decoder.Decode(si)
}

func (si StorageIndex) Append(key common.Hash, val uint64) {
	if _,ok:=si[key]; !ok {
		si[key] = new(HistoryIndex)
	}
	si[key] = si[key].Append(val)
}

//most common operation is remove one from the tail
func (si StorageIndex) Remove(key common.Hash, val uint64) {
	if v,ok:=si[key]; ok&&v!=nil {
		v = v.Remove(val)
		if len(*v) ==0 {
			delete(si,key)
		} else {
			si[key] = v
		}
	}
}

func (si StorageIndex) Search(key common.Hash, val uint64) (uint64, bool) {
	if v,ok:=si[key]; ok&&v!=nil {
		return v.Search(val)
	}
	return 0,false
}

func AppendToStorageIndex(b []byte, key []byte, timestamp uint64) ([]byte, error) {
	v := NewStorageIndex()

	if err := v.Decode(b); err != nil {
		return nil, err
	}

	v.Append(common.BytesToHash(key), timestamp)
	return v.Encode()
}
func RemoveFromStorageIndex(b []byte, timestamp uint64) ([]byte, bool, error) {
	v := NewStorageIndex()

	if err := v.Decode(b); err != nil {
		return nil, false, err
	}

	for key:=range v {
		v.Remove(key, timestamp)
	}

	res, err := v.Encode()
	if len(v) == 0 {
		return res, true, err
	}
	return res, false, err
}
