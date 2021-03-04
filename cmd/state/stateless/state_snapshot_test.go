package stateless

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

type testData map[string][]byte

func generateData(prefix string) testData {
	data := make(map[string][]byte)
	for i := 0; i < 100; i++ {
		data[fmt.Sprintf("key:%s-%d", prefix, i)] = []byte(fmt.Sprintf("val:%s-%d", prefix, i))
	}

	return testData(data)
}

func writeDataToDb(t *testing.T, db ethdb.Database, bucket string, data testData) {
	for k, v := range data {
		err := db.Put(bucket, []byte(k), v)
		if err != nil {
			t.Errorf("error while forming the source db: %v", err)
		}
	}
}

func checkDataInDb(t *testing.T, db ethdb.Database, bucket string, data testData) {
	for k, v := range data {
		val, err := db.Get(bucket, []byte(k))
		if err != nil {
			t.Errorf("error while requesting the dest db: %v", err)
		}

		if !bytes.Equal(v, val) {
			t.Errorf("unexpected value for the key %s (got: %s expected: %x)", k, string(val), string(v))
		}
	}

	err := db.Walk(bucket, nil, 0, func(k, v []byte) (bool, error) {
		val, ok := data[string(k)]
		if !ok {
			t.Errorf("unexpected key in the database (not in the data): %s", string(k))
		}

		if !bytes.Equal(v, val) {
			t.Errorf("unexpected value for the key %s (got: %s expected: %x)", k, string(val), string(v))
		}

		return true, nil
	})

	if err != nil {
		t.Errorf("error while walking the dest db: %v", err)
	}
}

func TestCopyDatabase(t *testing.T) {
	doTestcase(t, map[string]testData{})

	doTestcase(t, map[string]testData{
		dbutils.HashedAccountsBucket: generateData(dbutils.HashedAccountsBucket),
		dbutils.HashedStorageBucket:  generateData(dbutils.HashedStorageBucket),
	})

	doTestcase(t, map[string]testData{
		dbutils.CodeBucket: generateData(dbutils.CodeBucket),
	})

	doTestcase(t, map[string]testData{
		dbutils.HashedAccountsBucket: generateData(dbutils.HashedAccountsBucket),
		dbutils.HashedStorageBucket:  generateData(dbutils.HashedStorageBucket),
		dbutils.CodeBucket:           generateData(dbutils.CodeBucket),
	})

	doTestcase(t, map[string]testData{
		dbutils.HashedAccountsBucket: generateData(dbutils.HashedAccountsBucket),
		dbutils.HashedStorageBucket:  generateData(dbutils.HashedStorageBucket),
		dbutils.CodeBucket:           generateData(dbutils.CodeBucket),
	})

	doTestcase(t, map[string]testData{
		dbutils.HashedAccountsBucket: generateData(dbutils.HashedAccountsBucket),
		dbutils.HashedStorageBucket:  generateData(dbutils.HashedStorageBucket),
		dbutils.CodeBucket:           generateData(dbutils.CodeBucket),
	})

	doTestcase(t, map[string]testData{
		dbutils.HashedAccountsBucket: generateData(dbutils.HashedAccountsBucket),
		dbutils.HashedStorageBucket:  generateData(dbutils.HashedStorageBucket),
		dbutils.CodeBucket:           generateData(dbutils.CodeBucket),
		dbutils.DatabaseInfoBucket:   generateData(dbutils.DatabaseInfoBucket),
	})

}

func doTestcase(t *testing.T, testCase map[string]testData) {
	sourceDb := ethdb.NewMemDatabase()
	defer sourceDb.Close()

	destDb := ethdb.NewMemDatabase()
	defer destDb.Close()

	for bucket, data := range testCase {
		writeDataToDb(t, sourceDb, bucket, data)
	}

	err := copyDatabase(sourceDb, destDb)

	if err != nil {
		t.Errorf("error while copying the db: %v", err)
	}

	for bucket, data := range testCase {
		checkDataInDb(t, destDb, bucket, data)
	}
}
