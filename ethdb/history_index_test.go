package ethdb

import "testing"

func TestHistoryIndex_Search(t *testing.T) {
	index:=&HistoryIndex{3,5,8}
	_,b:=index.Search(1)
	if b {
		t.FailNow()
	}
	v,b:=index.Search(3)
	if !b && v!=3 {
		t.FailNow()
	}

	v,b=index.Search(4)
	if !b && v!=3 {
		t.FailNow()
	}

	v,b=index.Search(5)
	if !b && v!=5 {
		t.FailNow()
	}
}