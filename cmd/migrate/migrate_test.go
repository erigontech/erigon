package main

import (
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

func TestMigrate(t *testing.T) {
	db := ethdb.NewMemDatabase()

	err := db.Put(dbutils.AccountsBucket, common.Hex2Bytes("03601462093b5945d1676df093446790fd31b20e7b12a2e8e5e09d068109616b"), common.Hex2Bytes("c8808502540be40080"))
	if err != nil {
		t.Errorf(err.Error())
	}

	err = db.Put(dbutils.AccountsBucket, common.Hex2Bytes("0fbc62ba90dec43ec1d6016f9dd39dc324e967f2a3459a78281d1f4b2ba962a6"), common.Hex2Bytes("f845806480a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a04f1593970e8f030c0a2c39758181a447774eae7c65653c4e6440e8c18dad69bc"))
	if err != nil {
		t.Errorf(err.Error())
	}

	err = convertDatabaseToCBOR(db.DB(), 1)
	if err != nil {
		t.Errorf(err.Error())
	}

	val1, err := db.Get(dbutils.AccountsBucket, common.Hex2Bytes("03601462093b5945d1676df093446790fd31b20e7b12a2e8e5e09d068109616b"))
	if err != nil {
		t.Errorf(err.Error())
	}

	val2, err := db.Get(dbutils.AccountsBucket, common.Hex2Bytes("0fbc62ba90dec43ec1d6016f9dd39dc324e967f2a3459a78281d1f4b2ba962a6"))
	if err != nil {
		t.Errorf(err.Error())
	}

	expected1 := "020502540be400"
	expected2 := "120164204f1593970e8f030c0a2c39758181a447774eae7c65653c4e6440e8c18dad69bc"

	if common.Bytes2Hex(val1) != expected1 {
		t.Errorf("Expected %s, got %s", expected1, common.Bytes2Hex(val1))
	}

	if common.Bytes2Hex(val2) != expected2 {
		t.Errorf("Expected %s, got %s", expected2, common.Bytes2Hex(val2))
	}
}
