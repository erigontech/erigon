package ethdb

import "testing"

func TestHistoryIndex_Search(t *testing.T) {
	index:=&HistoryIndex{3,5,8}
	v,_:=index.Search(1)
	if v!=3 {
		t.Fatal("must be 3")
	}
	v,_=index.Search(3)
	if v!=3 {
		t.Fatal("must be 3")
	}

	v,_=index.Search(4)
	if v!=5 {
		t.Fatal("must be 5")
	}

	v,_=index.Search(5)
	if v!=5 {
		t.Fatal("must be 5")
	}
	v,_=index.Search(7)
	if v!=8 {
		t.Fatal("must be 8")
	}
	_,b:=index.Search(9)
	if b {
		t.Fatal("must be not found")
	}
}

func TestHistoryIndex_Search2(t *testing.T) {
		index:=&HistoryIndex{}
		_,b:=index.Search(1)
		if b {
			t.FailNow()
		}
	}