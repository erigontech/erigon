package state

// import "github.com/erigontech/erigon-lib/seg"

// // tooling for creating indexes

// type IndexCreationInputFactory interface {
// 	Create() *DataIterator
// }

// type DataIterator interface {
// 	Next() (key []byte, offset uint64)
// 	HasNext() bool
// 	Close()
// }

// type ResettableDataIterator interface {
// 	DataIterator
// 	Reset(offset uint64)
// }

// type DecompressorDataIteratorFactory struct {
// 	pathName string
// }

// func (d *DecompressorDataIteratorFactory) Create() *DecompressorDataIterator {
// 	dec, _ := seg.NewDecompressor(d.pathName)
// 	return &DecompressorDataIterator{
// 		dec: dec,
// 		g:   dec.MakeGetter(),
// 		buf: []byte{},
// 	}
// }

// type DecompressorData struct {
// 	Word   []byte
// 	Offset uint64
// }

// type DecompressorDataIterator struct {
// 	g   *seg.Getter
// 	dec *seg.Decompressor
// 	buf []byte
// }

// func (d *DecompressorDataIterator) Next() ([]byte, uint64) {
// 	return d.g.Next(d.buf[:0])
// }

// func (d *DecompressorDataIterator) HasNext() bool {
// 	return d.g.HasNext()
// }

// func (d *DecompressorDataIterator) Reset(offset uint64) {
// 	d.g.Reset(offset)
// }

// func (d *DecompressorDataIterator) Close() {
// 	d.dec.Close()
// }

// // func (d *DecompressorDataIterator) Testing() {
// // 	var di DataIterator[DecompressorData]
// // 	di = d
// // 	di.Close()
// // }

// // double seg data iterator

// type DoubleDecompressorData struct {
// 	Word1    []byte
// 	BlockNum []byte
// 	Offset   uint64
// }

// type DoubleDecompressorDataIterator struct {
// 	g1   *seg.Getter
// 	g2   *seg.Getter
// 	dec1 *seg.Decompressor
// 	dec2 *seg.Decompressor
// }

// func (d *DoubleDecompressorDataIterator) Next() DoubleDecompressorData {
// 	data1, offset1 := d.g1.Next(nil)
// 	data2, _ := d.g2.Next(nil)
// 	return DoubleDecompressorData{
// 		Word1:    data1,
// 		BlockNum: data2,
// 		Offset:   offset1,
// 	}
// }

// func (d *DoubleDecompressorDataIterator) HasNext() bool {
// 	return d.g1.HasNext()
// }

// func (d *DoubleDecompressorDataIterator) Reset(offset uint64) {
// 	d.g1.Reset(offset)
// 	d.g2.Reset(offset)
// }

// func (d *DoubleDecompressorDataIterator) Close() {
// 	d.dec1.Close()
// 	d.dec2.Close()
// }
