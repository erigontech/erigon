package ethdb

type puts struct {
	mp   map[string]putsBucket //map[dbi]putsBucket
	size int
	len  int
}

func newPuts() *puts {
	return &puts{
		mp: make(map[string]putsBucket),
	}
}

func (p *puts) set(bucket string, key, value []byte) {
	var bucketPuts putsBucket
	var ok bool
	if bucketPuts, ok = p.mp[bucket]; !ok {
		bucketPuts = make(putsBucket)
		p.mp[bucket] = bucketPuts
	}
	oldLen := len(bucketPuts)
	skey := string(key)
	if oldVal, ok := bucketPuts[skey]; ok {
		p.size -= len(oldVal)
	} else {
		p.size += len(skey) + 32 // Add fixed overhead per key
	}
	bucketPuts[skey] = value
	p.size += len(value)
	p.len += len(bucketPuts) - oldLen
}

func (p *puts) get(bucket string, key []byte) ([]byte, bool) {
	var bucketPuts putsBucket
	var ok bool
	if bucketPuts, ok = p.mp[bucket]; !ok {
		return nil, false
	}
	return bucketPuts.Get(key)
}

func (p *puts) Delete(bucket string, key []byte) {
	p.set(bucket, key, nil)
}

func (p *puts) Len() int {
	return p.len
}

func (p *puts) Size() int {
	return p.size
}

type putsBucket map[string][]byte //map[key]value

func (pb putsBucket) Len() int {
	return len(pb)
}

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
