package dbutils

import (
	"bytes"
	"github.com/ledgerwatch/turbo-geth/rlp"
)
//
//type Suffix []byte
//
//func ToSuffix(b []byte) Suffix {
//	return Suffix(b)
//}
//
//func (s Suffix) Add(key []byte) Suffix {
//	var l int
//	if s == nil {
//		l = 4
//	} else {
//		l = len(s)
//	}
//	dv := make([]byte, l+1+len(key))
//	copy(dv, s)
//	binary.BigEndian.PutUint32(dv, 1+s.KeyCount()) // Increment the counter of keys
//	dv[l] = byte(len(key))
//	copy(dv[l+1:], key)
//	return Suffix(dv)
//}
//func (s Suffix) MultiAdd(keys [][]byte) Suffix {
//	var l int
//	if s == nil {
//		l = 4
//	} else {
//		l = len(s)
//	}
//	newLen := len(keys)
//	for _, key := range keys {
//		newLen += len(key)
//	}
//	dv := make([]byte, l+newLen)
//	copy(dv, s)
//	binary.BigEndian.PutUint32(dv, uint32(len(keys))+s.KeyCount())
//	i := l
//	for _, key := range keys {
//		dv[i] = byte(len(key))
//		i++
//		copy(dv[i:], key)
//		i += len(key)
//	}
//	return Suffix(dv)
//}
//
//func (s Suffix) KeyCount() uint32 {
//	if len(s) < 4 {
//		return 0
//	}
//	return binary.BigEndian.Uint32(s)
//}
//
//func (s Suffix) Walk(f func(k []byte) error) error {
//	keyCount := int(s.KeyCount())
//	for i, ki := 4, 0; ki < keyCount; ki++ {
//		l := int(s[i])
//		i++
//		kk := make([]byte, l)
//		copy(kk, s[i:i+l])
//		err := f(kk)
//		if err != nil {
//			return err
//		}
//		i += l
//	}
//	return nil
//}



func Decode(b []byte) (SuffixHistory,error) {
	if len(b)==0 {
		return SuffixHistory{
			Changes: make([]Change,0),
		}, nil
	}
	buf:=bytes.NewBuffer(b)
	h:=SuffixHistory{}
	err:=rlp.Decode(buf, &h)
	if err!=nil {
		return SuffixHistory{}, err
	}
	return h, nil
}

func Encode(sh SuffixHistory) ([]byte,error) {
	buf:=new(bytes.Buffer)
	err:=rlp.Encode(buf,sh)
	if err!=nil {
		return nil, err
	}
	return buf.Bytes(), nil
}


type Change struct {
	Key []byte
	Value []byte
}

type SuffixHistory struct {
	Changes []Change
}


func (s SuffixHistory) Add(key []byte, value []byte) SuffixHistory {
	s.Changes =append(s.Changes, Change{
		Key:key,
		Value:value,
	})
	return s
}
func (s SuffixHistory) MultiAdd(changes []Change) SuffixHistory {
	s.Changes = append(s.Changes, changes...)
	return s
}

func (s SuffixHistory) KeyCount() uint32 {
	return uint32(len(s.Changes))
}

func (s SuffixHistory) Walk(f func(k, v []byte) error) error {
	for i:=range s.Changes {
		err:=f(s.Changes[i].Key, s.Changes[i].Value)
		if err != nil {
			return err
		}
	}
	return nil
}

