package ethdb

import (
)

type puts struct {
	mp       map[string]putsBucket //map[bucket]putsBucket
	size 	int
}

func newPuts() puts {
	return puts{
		mp:       make(map[string]putsBucket),
	}
}

func (p puts) set(bucket, key, value []byte) {
	var bucketPuts putsBucket
	var ok bool
	if bucketPuts, ok = p.mp[string(bucket)]; !ok {
		bucketPuts = make(putsBucket)
		p.mp[string(bucket)] = bucketPuts
	}
	skey := string(key)
	if oldVal, ok := bucketPuts[skey]; ok {
		p.size -= len(oldVal)
	} else {
		p.size += len(skey) + 32 // Add fixed overhead per key
	}
	bucketPuts[string(key)] = value
	p.size += len(value)
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
	return p.size
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
