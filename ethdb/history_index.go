package ethdb

import (
	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"sort"
)

//
type HistoryIndex []uint64

func (hi *HistoryIndex) Encode() ([]byte,error)  {
	return rlp.EncodeToBytes(hi)
}

func (hi *HistoryIndex) Decode(s []byte) error  {
	if len(s)==0 {
		return nil
	}
	return rlp.DecodeBytes(s, &hi)
}

func (hi *HistoryIndex) Append(v uint64) *HistoryIndex  {
	*hi=append(*hi, v)
	if !sort.SliceIsSorted(*hi, func(i, j int) bool {
		return (*hi)[i] <= (*hi)[j]
	}) {
		sort.Slice(*hi, func(i, j int) bool {
			return (*hi)[i] <= (*hi)[j]
		})
	}

	return hi
}

//most common operation is remove one from the tail
func (hi *HistoryIndex) Remove(v uint64) *HistoryIndex  {
	for i := len(*hi)-1; i >= 0; i-- {
		if (*hi)[i] == v {
			*hi = append((*hi)[:i], (*hi)[i+1:]...)
		}
	}
	return hi
}

func (hi *HistoryIndex) Search(v uint64) (uint64, bool)  {
	ln:=len(*hi)-1
	////fixme it's could be a bug
	//i:=sort.Search(ln, func(i int) bool {
	//	return (*hi)[i]>=v
	//})
	//if i<ln {
	//	return (*hi)[i], true
	//}

	if (*hi)[ln]<v {
		return 0,false
	}
	for i:=ln;i>=0;i-- {
		if v==(*hi)[i] {
			return v, true
		}

		if (*hi)[i]<v {
			return (*hi)[i+1], true
		}
	}
	return 0, false
}

func AppendToIndex(b []byte, timestamp uint64) ([]byte, error)  {
	v:=new(HistoryIndex)

	if err:= v.Decode(b);err!=nil {
		return nil, err
	}

	v.Append(timestamp)
	return v.Encode()
}
func RemoveFromIndex(b []byte, timestamp uint64) ([]byte, bool, error)  {
	v:=new(HistoryIndex)

	if err:= v.Decode(b);err!=nil {
		return nil, false, err
	}

	v.Remove(timestamp)
	res, err:=v.Encode()
	if len(*v) ==0 {
		return res, true, err
	}
	return res, false, err
}

func BoltDBFindByHistory(tx *bolt.Tx, hBucket []byte, key []byte, timestamp uint64) ([]byte, error) {
	//check
	hB := tx.Bucket(hBucket)
	if hB == nil {
		return nil, ErrKeyNotFound
	}
	v,_:=hB.Get(key)
	index:= new(HistoryIndex)

	err:=index.Decode(v)
	if err!=nil {
		return nil, err
	}

	changeSetBlock, ok:=index.Search(timestamp)
	if !ok {
		return nil, ErrKeyNotFound
	}

	csB := tx.Bucket(dbutils.ChangeSetBucket)
	if csB == nil {
		return nil, ErrKeyNotFound
	}
	changeSetData,_:=csB.Get(dbutils.CompositeChangeSetKey(dbutils.EncodeTimestamp(changeSetBlock), hBucket))
	cs,err:=dbutils.DecodeChangeSet(changeSetData)
	if err!=nil {
		return nil, err
	}

	var data []byte
	data, err =cs.FindLast(key)
	if err!=nil {
		return nil, ErrKeyNotFound
	}
	return data, nil

}
