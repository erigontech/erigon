package state

import "fmt"

func (r *PlainStateReader) ForEach(table string, fromPrefix []byte, walker func(k, v []byte) error) error {
	return r.db.ForEach(table, fromPrefix, walker)
}

func (r *PlainStateReader) GetTxCount() (uint64, error) {

	return 0, fmt.Errorf("not implemented")

}
