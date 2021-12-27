package parallelcompress

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/log/v3"
)

func Decompress(logPrefix, fileName string) error {
	d, err := compress.NewDecompressor(fileName + ".seg")
	if err != nil {
		return err
	}
	defer d.Close()
	logEvery := time.NewTicker(20 * time.Second)
	defer logEvery.Stop()
	var df *os.File
	if df, err = os.Create(fileName + ".decompressed.dat"); err != nil {
		return err
	}
	dw := bufio.NewWriterSize(df, etl.BufIOSize)
	var word = make([]byte, 0, 256)
	numBuf := make([]byte, binary.MaxVarintLen64)
	var decodeTime time.Duration
	g := d.MakeGetter()
	start := time.Now()
	wc := 0
	for g.HasNext() {
		word, _ = g.Next(word[:0])
		decodeTime += time.Since(start)
		n := binary.PutUvarint(numBuf, uint64(len(word)))
		if _, e := dw.Write(numBuf[:n]); e != nil {
			return e
		}
		if len(word) > 0 {
			if _, e := dw.Write(word); e != nil {
				return e
			}
		}
		wc++
		select {
		default:
		case <-logEvery.C:
			log.Info(fmt.Sprintf("[%s] Decompress", logPrefix), "millions", wc/1_000_000)
		}
		start = time.Now()
	}
	log.Info(fmt.Sprintf("[%s] Average decoding time", logPrefix), "per word", time.Duration(int64(decodeTime)/int64(wc)))
	if err = dw.Flush(); err != nil {
		return err
	}
	if err = df.Close(); err != nil {
		return err
	}
	return nil
}
