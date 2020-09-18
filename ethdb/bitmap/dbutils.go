package bitmap

import (
	"bytes"
	"github.com/RoaringBitmap/roaring"
	"github.com/ledgerwatch/turbo-geth/ethdb"
)

// PutOr - puts bitmap into database. If database already has such key - does merge by OR.
func PutOr(c ethdb.Cursor, k []byte, bm *roaring.Bitmap) error {
	v, err := c.SeekExact(k)
	if err != nil {
		panic(err)
	}

	if len(v) > 0 {
		exisintg := roaring.New()
		_, err = exisintg.ReadFrom(bytes.NewReader(v))
		if err != nil {
			panic(err)
		}

		bm.Or(exisintg)
	}

	bufBytes, err := c.Reserve(k, int(bm.GetSerializedSizeInBytes()))
	if err != nil {
		panic(err)
	}

	buf := bytes.NewBuffer(bufBytes[:0])
	_, err = bm.WriteTo(buf)
	if err != nil {
		return err
	}
	return nil
}
