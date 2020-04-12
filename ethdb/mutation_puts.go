package ethdb

import (
	"errors"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/debug"
	"sort"
)


type puts struct {
	mp map[string]putsBucket //map[bucket]putsBucket
	chunkIDs map[string][]uint64 //map[key_of_chunk_without_id]chunkIDs
}


func newPuts() puts {
	return puts{
		mp:make(map[string]putsBucket),
		chunkIDs: make(map[string][]uint64),
	}
}

func (p puts) set(bucket, key, value []byte) {
	var bucketPuts putsBucket
	var ok bool
	if bucketPuts, ok = p.mp[string(bucket)]; !ok {
		bucketPuts = make(putsBucket)
		p.mp[string(bucket)] = bucketPuts
	}
	if debug.IsThinHistory() && dbutils.IsIndexBucket(bucket) {
		//keyWithoutID, chunkID:=keyAndChunkID(key)
		//v,err:=p.ChunkByIDOrLast(bucket, keyWithoutID, chunkID)
		keyWithoutID, ID:=keyAndChunkID(key)
		p.addChunkID(keyWithoutID, ID)
	}

	bucketPuts[string(key)] = value
}

func (p puts) get(bucket, key []byte) ([]byte, bool) {
	var bucketPuts putsBucket
	var ok bool
	if bucketPuts, ok = p.mp[string(bucket)]; !ok {
		return nil, false
	}
	return bucketPuts.Get(key)
}

func (p puts) Delete(bucket, key []byte) {
	p.set(bucket, key, nil)
}

func (p puts) Size() int {
	var size int
	for _, put := range p.mp {
		size += len(put)
	}
	return size
}


func (p puts) ChunkByIDOrLast(bucket, key []byte, timestamp uint64) ([]byte,error)  {
	chunkIDs,ok:=p.chunkIDs[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}

	for _,chunk:=range chunkIDs {
		if timestamp == chunk {
			index,ok:=p.get(bucket, dbutils.IndexChunkKey(key, chunkIDs[len(chunkIDs)-1]))
			if !ok {
				return nil, errors.New("incorrect mapping")
			}
			return index, nil
		}
	}

	//inverted
	if timestamp > chunkIDs[len(chunkIDs)-1] {
		index,ok:=p.get(bucket, dbutils.IndexChunkKey(key, chunkIDs[len(chunkIDs)-1]))
		if !ok {
			return nil, errors.New("incorrect mapping")
		}
		return index, nil

	}

	return nil, ErrKeyNotFound
}


//key without blockNumber
func (p puts) Put(mp putsBucket, key []byte, index *dbutils.HistoryIndexBytes) error   {
	indexKey,err:=index.Key(key)
	if err!=nil {
		return err
	}

	ID, ok:=index.FirstElement()
	if !ok {
		return errors.New("incorrect index")
	}

	mp[string(indexKey)] = *index
	p.addChunkID(key, ID)

	return nil
}

func (p puts) addChunkID(key []byte, id uint64) {
	chunkIDs,ok:=p.chunkIDs[string(key)]
	if !ok {
		p.chunkIDs[string(key)] = []uint64{id}
		return
	}

	for _, v:=range chunkIDs {
		if v == id {
			return
		}
	}

	chunkIDs = append(chunkIDs, id)
	sort.Slice(chunkIDs, func(i, j int) bool {
		return chunkIDs[i]<chunkIDs[j]
	})

	p.chunkIDs[string(key)] = chunkIDs
}




type putsBucket map[string][]byte //map[key]value

func (pb putsBucket) Get(key []byte) ([]byte, bool) {
	value, ok := pb[string(key)]
	if !ok {
		return nil, false
	}

	if value == nil {
		return nil, true
	}

	return value, true
}

func (pb putsBucket) GetStr(key string) ([]byte, bool) {
	value, ok := pb[key]
	if !ok {
		return nil, false
	}

	if value == nil {
		return nil, true
	}

	return value, true
}
