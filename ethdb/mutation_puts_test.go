package ethdb

//
//func TestIndexMapper(t *testing.T) {
//	im:=indexMapper{
//		cache:    make(map[string]*dbutils.HistoryIndexBytes),
//		chunkIDs: make(map[string][]uint64),
//	}
//
//	key:=[]byte("some key")
//	if err:=im.Put(key,dbutils.NewHistoryIndex().Append(1)); err!=nil {
//		t.Fatal(1, err)
//	}
//	index,err:=im.LastChunk(key)
//	if err!=nil {
//		t.Fatal(err)
//	}
//	if v,ok:= index.FirstElement(); !ok || v!=1 {
//		t.Fatal(v, ok,im)
//	}
//
//	if err:=im.Put(key,dbutils.NewHistoryIndex().Append(2)); err!=nil {
//		t.Fatal(2, err)
//	}
//
//	index,err=im.LastChunk(key)
//	if err!=nil {
//		t.Fatal(err)
//	}
//
//	if v,ok:= index.FirstElement(); !ok || v!=2 {
//		t.Fatal(v, ok, im)
//	}
//}
