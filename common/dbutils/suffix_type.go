package dbutils

import "encoding/binary"

type Suffix []byte

func ToSuffix(b []byte) Suffix {
	return b
}

func (s Suffix) Add(key []byte) Suffix {
	var l int
	if s == nil {
		l = 4
	} else {
		l = len(s)
	}
	dv := make([]byte, l+1+len(key))
	copy(dv, s)
	binary.BigEndian.PutUint32(dv, 1+s.KeyCount()) // Increment the counter of keys
	dv[l] = byte(len(key))
	copy(dv[l+1:], key)
	return dv
}
func (s Suffix) MultiAdd(keys [][]byte) Suffix {
	var l int
	if s == nil {
		l = 4
	} else {
		l = len(s)
	}
	newLen := len(keys)
	for _, key := range keys {
		newLen += len(key)
	}
	dv := make([]byte, l+newLen)
	copy(dv, s)
	binary.BigEndian.PutUint32(dv, uint32(len(keys))+s.KeyCount())
	i := l
	for _, key := range keys {
		dv[i] = byte(len(key))
		i++
		copy(dv[i:], key)
		i += len(key)
	}
	return dv
}

func (s Suffix) KeyCount() uint32 {
	if len(s) < 4 {
		return 0
	}
	return binary.BigEndian.Uint32(s)
}

func (s Suffix) Walk(f func(k []byte) error) error {
	keyCount := int(s.KeyCount())
	for i, ki := 4, 0; ki < keyCount; ki++ {
		l := int(s[i])
		i++
		kk := make([]byte, l)
		copy(kk, s[i:i+l])
		err := f(kk)
		if err != nil {
			return err
		}
		i += l
	}
	return nil
}
