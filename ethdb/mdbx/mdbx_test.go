package mdbx

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
)

func TestTest1(t *testing.T) {
	env, err1 := NewEnv()
	if err1 != nil {
		t.Fatalf("Cannot create environment: %s", err1)
	}
	err1 = env.SetGeometry(-1, -1, 1024*1024, -1, -1, 4096)
	if err1 != nil {
		t.Fatalf("Cannot set mapsize: %s", err1)
	}
	path, err1 := ioutil.TempDir("", "mdb_test")
	if err1 != nil {
		t.Fatalf("Cannot create temporary directory")
	}
	err1 = os.MkdirAll(path, 0770)
	defer os.RemoveAll(path)
	if err1 != nil {
		t.Fatalf("Cannot create directory: %s", path)
	}
	err1 = env.Open(path, 0, 0664)
	defer env.Close()
	if err1 != nil {
		t.Fatalf("Cannot open environment: %s", err1)
	}

	var db DBI
	numEntries := 10
	var data = map[string]string{}
	var key string
	var val string
	for i := 0; i < numEntries; i++ {
		key = fmt.Sprintf("Key-%d", i)
		val = fmt.Sprintf("Val-%d", i)
		data[key] = val
	}
	if err := env.Update(func(txn *Txn) (err error) {
		db, err = txn.OpenRoot(0)
		if err != nil {
			return err
		}

		for k, v := range data {
			err = txn.Put(db, []byte(k), []byte(v), NoOverwrite)
			if err != nil {
				return fmt.Errorf("put: %v", err)
			}
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	stat, err1 := env.Stat()
	if err1 != nil {
		t.Fatalf("Cannot get stat %s", err1)
	} else if stat.Entries != uint64(numEntries) {
		t.Errorf("Less entry in the database than expected: %d <> %d", stat.Entries, numEntries)
	}
	t.Logf("%#v", stat)

	if err := env.View(func(txn *Txn) error {
		cursor, err := txn.OpenCursor(db)
		if err != nil {
			cursor.Close()
			return fmt.Errorf("cursor: %v", err)
		}
		var bkey, bval []byte
		var bNumVal int
		for {
			bkey, bval, err = cursor.Get(nil, nil, Next)
			if IsNotFound(err) {
				break
			}
			if err != nil {
				return fmt.Errorf("cursor get: %v", err)
			}
			bNumVal++
			skey := string(bkey)
			sval := string(bval)
			t.Logf("Val: %s", sval)
			t.Logf("Key: %s", skey)
			var d string
			var ok bool
			if d, ok = data[skey]; !ok {
				return fmt.Errorf("cursor get: key does not exist %q", skey)
			}
			if d != sval {
				return fmt.Errorf("cursor get: value %q does not match %q", sval, d)
			}
		}
		if bNumVal != numEntries {
			t.Errorf("cursor iterated over %d entries when %d expected", bNumVal, numEntries)
		}
		cursor.Close()
		bval, err = txn.Get(db, []byte("Key-0"))
		if err != nil {
			return fmt.Errorf("get: %v", err)
		}
		if string(bval) != "Val-0" {
			return fmt.Errorf("get: value %q does not match %q", bval, "Val-0")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

//func TestVersion(t *testing.T) {
//	maj, min, patch, str := Version()
//	if maj < 0 || min < 0 || patch < 0 {
//		t.Error("invalid version number: ", maj, min, patch)
//	}
//	if maj == 0 && min == 0 && patch == 0 {
//		t.Error("invalid version number: ", maj, min, patch)
//	}
//	if str == "" {
//		t.Error("empty version string")
//	}
//
//	str = VersionString()
//	if str == "" {
//		t.Error("empty version string")
//	}
//}
