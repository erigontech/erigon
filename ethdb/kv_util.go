package ethdb

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
)

func Walk(c kv.Cursor, startkey []byte, fixedbits int, walker func(k, v []byte) (bool, error)) error {
	fixedbytes, mask := Bytesmask(fixedbits)
	k, v, err := c.Seek(startkey)
	if err != nil {
		return err
	}
	for k != nil && len(k) >= fixedbytes && (fixedbits == 0 || bytes.Equal(k[:fixedbytes-1], startkey[:fixedbytes-1]) && (k[fixedbytes-1]&mask) == (startkey[fixedbytes-1]&mask)) {
		goOn, err := walker(k, v)
		if err != nil {
			return err
		}
		if !goOn {
			break
		}
		k, v, err = c.Next()
		if err != nil {
			return err
		}
	}
	return nil
}

// todo: return TEVM code and use it
func GetCheckTEVM(db kv.Getter) func(contractHash common.Hash) (bool, error) {
	checked := map[common.Hash]struct{}{}
	var ok bool

	return func(contractHash common.Hash) (bool, error) {
		if contractHash == (common.Hash{}) {
			return true, nil
		}

		if _, ok = checked[contractHash]; ok {
			return true, nil
		}

		ok, err := db.Has(kv.ContractTEVMCode, contractHash.Bytes())
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return false, fmt.Errorf("can't check TEVM bucket by contract %q hash: %w",
				contractHash.String(), err)
		}

		if !ok {
			checked[contractHash] = struct{}{}
		}

		return ok, nil
	}
}

func Bytesmask(fixedbits int) (fixedbytes int, mask byte) {
	fixedbytes = (fixedbits + 7) / 8
	shiftbits := fixedbits & 7
	mask = byte(0xff)
	if shiftbits != 0 {
		mask = 0xff << (8 - shiftbits)
	}
	return fixedbytes, mask
}
