package ethdb

import (
	//"fmt"
	"testing"
)

var testbucket = []byte("B")

// multiWalkAsOf(db Getter, varKeys bool, bucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) (bool, error))
func TestEmptyWalk(t *testing.T) {
	db := NewMemDatabase()
	count := 0
	err := db.MultiWalkAsOf(testbucket, [][]byte{}, []uint{}, 0, func(i int, k []byte, v []byte) (bool, error) {
		count++
		return true, nil
	})
	if err != nil {
		t.Errorf("Error while walking: %v", err)
	}
	if count != 0 {
		t.Errorf("Expected no keys, got %d", count)
	}
}

func TestWalk2(t *testing.T) {
	db := NewMemDatabase()
	db.PutS(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	count := 0
	err := db.MultiWalkAsOf(
		testbucket,
		[][]byte{[]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")},
		[]uint{10},
		0,
		func(i int, k []byte, v []byte) (bool, error) {
			if k != nil {
				count++
			}
			return true, nil
		},
	)
	if err != nil {
		t.Errorf("Error while walking: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 keys, got %d", count)
	}
}

func TestWalkN(t *testing.T) {
	db := NewMemDatabase()

	db.PutS(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)

	db.PutS(testbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("bbaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)

	count := make(map[int]int)
	err := db.MultiWalkAsOf(
		testbucket,
		[][]byte{[]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")},
		[]uint{40, 16},
		0,
		func(i int, k []byte, v []byte) (bool, error) {
			if k != nil {
				count[i]++
			}
			return true, nil
		},
	)
	if err != nil {
		t.Errorf("Error while walking: %v", err)
	}
	if len(count) != 2 {
		t.Errorf("Expected 2 key groups, got %d", len(count))
	}
	if count[0] != 2 {
		t.Errorf("Expected 2 keys in group 0, got %d", count[0])
	}
	if count[1] != 3 {
		t.Errorf("Expected 3 keys in group 1, got %d", count[1])
	}
}

func TestRewindData1Bucket(t *testing.T) {
	db := NewMemDatabase()

	db.PutS(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)

	db.PutS(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"), 1)
	db.PutS(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"), 1)
	db.PutS(testbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 1)
	db.PutS(testbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 1)

	db.PutS(testbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxzzzzzzzzzzzzzzzzzzzzzzzz"), 2)
	db.PutS(testbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)
	db.PutS(testbucket, []byte("bbaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)
	db.PutS(testbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxaaaaaaaaaaaaaaaaaaaaaaaaa"), 2)
	db.PutS(testbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)

	db.PutS(testbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), nil, 3)
	db.PutS(testbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 3)

	count := 0
	err := rewindData(db, 3, 2, func(bucket, key, value []byte) error {
		count++
		//fmt.Printf("bucket: %s, key: %s, value: %s\n", string(bucket), string(key), string(value))
		return nil
	})
	if err != nil {
		t.Errorf("Could not rewind 3->2 %v", err)
	}
	if count != 2 {
		t.Errorf("Expected %d items in rewind data, got %d", 2, count)
	}

	count = 0
	err = rewindData(db, 3, 0, func(bucket, key, value []byte) error {
		count++
		//fmt.Printf("bucket: %s, key: %s, value: %s\n", string(bucket), string(key), string(value))
		return nil
	})
	if err != nil {
		t.Errorf("Could not rewind 3->0 %v", err)
	}
	if count != 7 {
		t.Errorf("Expected %d items in rewind data, got %d", 7, count)
	}
}

func TestRewindData2Bucket(t *testing.T) {
	db := NewMemDatabase()

	otherbucket := []byte("OB")

	db.PutS(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)
	db.PutS(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 0)

	db.PutS(testbucket, []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"), 1)
	db.PutS(testbucket, []byte("aaaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyyyyyyyy"), 1)
	db.PutS(testbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 1)
	db.PutS(testbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 1)

	db.PutS(otherbucket, []byte("baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxzzzzzzzzzzzzzzzzzzzzzzzz"), 2)
	db.PutS(otherbucket, []byte("bbaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)
	db.PutS(otherbucket, []byte("bbaaaccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)
	db.PutS(otherbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxaaaaaaaaaaaaaaaaaaaaaaaaa"), 2)
	db.PutS(otherbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 2)

	db.PutS(testbucket, []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), nil, 3)
	db.PutS(testbucket, []byte("bccccccccccccccccccccccccccccccc"), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"), 3)

	count := 0
	err := rewindData(db, 3, 2, func(bucket, key, value []byte) error {
		count++
		//fmt.Printf("bucket: %s, key: %s, value: %s\n", string(bucket), string(key), string(value))
		return nil
	})
	if err != nil {
		t.Errorf("Could not rewind 3->2 %v", err)
	}
	if count != 2 {
		t.Errorf("Expected %d items in rewind data, got %d", 2, count)
	}

	count = 0
	err = rewindData(db, 3, 0, func(bucket, key, value []byte) error {
		count++
		//fmt.Printf("bucket: %s, key: %s, value: %s\n", string(bucket), string(key), string(value))
		return nil
	})
	if err != nil {
		t.Errorf("Could not rewind 3->0 %v", err)
	}
	if count != 11 {
		t.Errorf("Expected %d items in rewind data, got %d", 11, count)
	}
}
