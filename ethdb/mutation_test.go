package ethdb

import (
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/petar/GoLLRB/llrb"
	"sort"
	"testing"
)

func TestOne(t *testing.T)  {
	var ht *llrb.LLRB
	ht = llrb.New()
	ht.ReplaceOrInsert(&PutItem{key: []byte("some bytes"), value: []byte{1}})

	a:=&PutItem{key: []byte("some bytes")}
	spew.Dump(ht.Get(a))
	spew.Dump("-----------")
	a2:=ht.Get(&PutItem{key: []byte("some other bytes")})
	t.Log("a=", a2==nil)


}

func TestTwo(t *testing.T)  {
	timestamp:=uint64(11)
	index:=[]uint64{ 1, 5, 10,12,14}
	//index:=[]uint64{ 14,13, 12, 10, 5,1}
	i:=sort.Search(len(index), func(i int) bool {
		fmt.Println(i)
		return index[i]>=timestamp
	})
	fmt.Println(i)
	t.Log(i, index[i])
}