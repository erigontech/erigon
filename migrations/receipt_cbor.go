package migrations

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/cbor"
	"github.com/ledgerwatch/erigon/log"

	"errors"
	pkg2_big "math/big"
	"runtime"
	"strconv"

	pkg1_common "github.com/ledgerwatch/erigon/common"
	codec1978 "github.com/ugorji/go/codec"
)

// OldReceipt is receipt structure before introduction of Type field
// to be able to read old records
type OldReceipt struct {
	// Consensus fields: These fields are defined by the Yellow Paper
	PostState         []byte `json:"root" codec:"1"`
	Status            uint64 `json:"status" codec:"2"`
	CumulativeGasUsed uint64 `json:"cumulativeGasUsed" gencodec:"required" codec:"3"`
}

type OldReceipts []*OldReceipt

var ReceiptCbor = Migration{
	Name: "receipt_cbor",
	Up: func(db ethdb.Database, tmpdir string, progress []byte, CommitProgress etl.LoadCommitHandler) (err error) {
		var tx ethdb.RwTx
		if hasTx, ok := db.(ethdb.HasTx); ok {
			tx = hasTx.Tx().(ethdb.RwTx)
		} else {
			return fmt.Errorf("no transaction")
		}
		genesisBlock, err := rawdb.ReadBlockByNumber(tx, 0)
		if err != nil {
			return err
		}
		if genesisBlock == nil {
			// Empty database check
			return CommitProgress(db, nil, true)
		}
		chainConfig, cerr := rawdb.ReadChainConfig(tx, genesisBlock.Hash())
		if cerr != nil {
			return cerr
		}
		logInterval := 30 * time.Second
		logEvery := time.NewTicker(logInterval)
		defer logEvery.Stop()
		var buf bytes.Buffer
		var key [8]byte
		var v []byte
		var to uint64
		if to, err = stages.GetStageProgress(tx, stages.Execution); err != nil {
			return err
		}
		for blockNum := uint64(1); blockNum <= to; blockNum++ {
			binary.BigEndian.PutUint64(key[:], blockNum)
			if v, err = tx.GetOne(dbutils.BlockReceiptsPrefix, key[:]); err != nil {
				return err
			}
			if v == nil {
				continue
			}
			select {
			default:
			case <-logEvery.C:
				log.Info("Scanned receipts up to", "block", blockNum)
			}
			var receipts types.Receipts
			var oldReceipts OldReceipts
			if err = cbor.Unmarshal(&oldReceipts, bytes.NewReader(v)); err != nil {
				continue
			}

			var blockHash common.Hash
			if blockHash, err = rawdb.ReadCanonicalHash(tx, blockNum); err != nil {
				return err
			}
			var body *types.Body
			if chainConfig.IsBerlin(blockNum) {
				body = rawdb.ReadBody(tx, blockHash, blockNum)
			}
			receipts = make(types.Receipts, len(oldReceipts))
			for i, oldReceipt := range oldReceipts {
				receipts[i] = new(types.Receipt)
				receipts[i].PostState = oldReceipt.PostState
				receipts[i].Status = oldReceipt.Status
				receipts[i].CumulativeGasUsed = oldReceipt.CumulativeGasUsed
				if body != nil {
					receipts[i].Type = body.Transactions[i].Type()
				}
			}
			buf.Reset()
			if err = cbor.Marshal(&buf, receipts); err != nil {
				return err
			}
			if err = tx.Put(dbutils.BlockReceiptsPrefix, common.CopyBytes(key[:]), common.CopyBytes(buf.Bytes())); err != nil {
				return err
			}
		}
		return CommitProgress(db, nil, true)
	},
}

const (
	// ----- value types used ----
	codecSelferValueTypeArray2 = 10
	codecSelferValueTypeMap2   = 9
	codecSelferValueTypeNil2   = 1
)

var (
	errCodecSelferOnlyMapOrArrayEncodeToStruct2 = errors.New(`only encoded map or array can be decoded into a struct`)
)

type codecSelfer2 struct{}

func init() {
	if codec1978.GenVersion != 19 {
		_, file, _, _ := runtime.Caller(0)
		ver := strconv.FormatInt(int64(codec1978.GenVersion), 10)
		panic(errors.New("codecgen version mismatch: current: 19, need " + ver + ". Re-generate file: " + file))
	}
	if false { // reference the types, but skip this branch at build/run time
		var _ pkg1_common.Address
		var _ pkg2_big.Int
	}
}

func (x *OldReceipt) CodecEncodeSelf(e *codec1978.Encoder) {
	var h codecSelfer2
	z, r := codec1978.GenHelperEncoder(e)
	_, _, _ = h, z, r
	if x == nil {
		r.EncodeNil()
	} else {
		yy2arr2 := z.EncBasicHandle().StructToArray
		_ = yy2arr2
		z.EncWriteArrayStart(3)
		z.EncWriteArrayElem()
		if x.PostState == nil {
			r.EncodeNil()
		} else {
			r.EncodeStringBytesRaw([]byte(x.PostState))
		} // end block: if x.PostState slice == nil
		z.EncWriteArrayElem()
		r.EncodeUint(uint64(x.Status))
		z.EncWriteArrayElem()
		r.EncodeUint(uint64(x.CumulativeGasUsed))
		z.EncWriteArrayEnd()
	}
}

func (x *OldReceipt) CodecDecodeSelf(d *codec1978.Decoder) {
	var h codecSelfer2
	z, r := codec1978.GenHelperDecoder(d)
	_, _, _ = h, z, r
	yyct2 := r.ContainerType()
	if yyct2 == codecSelferValueTypeNil2 {
		*(x) = OldReceipt{}
	} else if yyct2 == codecSelferValueTypeMap2 {
		yyl2 := z.DecReadMapStart()
		if yyl2 == 0 {
		} else {
			x.codecDecodeSelfFromMap(yyl2, d)
		}
		z.DecReadMapEnd()
	} else if yyct2 == codecSelferValueTypeArray2 {
		yyl2 := z.DecReadArrayStart()
		if yyl2 != 0 {
			x.codecDecodeSelfFromArray(yyl2, d)
		}
		z.DecReadArrayEnd()
	} else {
		panic(errCodecSelferOnlyMapOrArrayEncodeToStruct2)
	}
}

func (x *OldReceipt) codecDecodeSelfFromMap(l int, d *codec1978.Decoder) {
	var h codecSelfer2
	z, r := codec1978.GenHelperDecoder(d)
	_, _, _ = h, z, r
	var yyhl3 bool = l >= 0
	for yyj3 := 0; ; yyj3++ {
		if yyhl3 {
			if yyj3 >= l {
				break
			}
		} else {
			if z.DecCheckBreak() {
				break
			}
		}
		z.DecReadMapElemKey()
		yys3 := z.StringView(r.DecodeStringAsBytes())
		z.DecReadMapElemValue()
		switch yys3 {
		case "1":
			x.PostState = r.DecodeBytes(([]byte)(x.PostState), false)
		case "2":
			x.Status = (uint64)(r.DecodeUint64())
		case "3":
			x.CumulativeGasUsed = (uint64)(r.DecodeUint64())
		default:
			z.DecStructFieldNotFound(-1, yys3)
		} // end switch yys3
	} // end for yyj3
}

func (x *OldReceipt) codecDecodeSelfFromArray(l int, d *codec1978.Decoder) {
	var h codecSelfer2
	z, r := codec1978.GenHelperDecoder(d)
	_, _, _ = h, z, r
	var yyj8 int
	var yyb8 bool
	var yyhl8 bool = l >= 0
	yyj8++
	if yyhl8 {
		yyb8 = yyj8 > l
	} else {
		yyb8 = z.DecCheckBreak()
	}
	if yyb8 {
		z.DecReadArrayEnd()
		return
	}
	z.DecReadArrayElem()
	x.PostState = r.DecodeBytes(([]byte)(x.PostState), false)
	yyj8++
	if yyhl8 {
		yyb8 = yyj8 > l
	} else {
		yyb8 = z.DecCheckBreak()
	}
	if yyb8 {
		z.DecReadArrayEnd()
		return
	}
	z.DecReadArrayElem()
	x.Status = (uint64)(r.DecodeUint64())
	yyj8++
	if yyhl8 {
		yyb8 = yyj8 > l
	} else {
		yyb8 = z.DecCheckBreak()
	}
	if yyb8 {
		z.DecReadArrayEnd()
		return
	}
	z.DecReadArrayElem()
	x.CumulativeGasUsed = (uint64)(r.DecodeUint64())
	for {
		yyj8++
		if yyhl8 {
			yyb8 = yyj8 > l
		} else {
			yyb8 = z.DecCheckBreak()
		}
		if yyb8 {
			break
		}
		z.DecReadArrayElem()
		z.DecStructFieldNotFound(yyj8-1, "")
	}
}

func (x *OldReceipt) IsCodecEmpty() bool {
	return !(len(x.PostState) != 0 && x.Status != 0 && x.CumulativeGasUsed != 0 && true)
}

func (x OldReceipts) CodecEncodeSelf(e *codec1978.Encoder) {
	var h codecSelfer2
	z, r := codec1978.GenHelperEncoder(e)
	_, _, _ = h, z, r
	if x == nil {
		r.EncodeNil()
	} else {
		h.encReceipts((OldReceipts)(x), e)
	} // end block: if x slice == nil
}

func (x *OldReceipts) CodecDecodeSelf(d *codec1978.Decoder) {
	var h codecSelfer2
	z, r := codec1978.GenHelperDecoder(d)
	_, _, _ = h, z, r
	h.decReceipts((*OldReceipts)(x), d)
}

func (x codecSelfer2) encReceipts(v OldReceipts, e *codec1978.Encoder) {
	var h codecSelfer2
	z, r := codec1978.GenHelperEncoder(e)
	_, _, _ = h, z, r
	if v == nil {
		r.EncodeNil()
		return
	}
	z.EncWriteArrayStart(len(v))
	for _, yyv1 := range v {
		z.EncWriteArrayElem()
		if yyv1 == nil {
			r.EncodeNil()
		} else {
			yyv1.CodecEncodeSelf(e)
		}
	}
	z.EncWriteArrayEnd()
}

func (x codecSelfer2) decReceipts(v *OldReceipts, d *codec1978.Decoder) {
	var h codecSelfer2
	z, r := codec1978.GenHelperDecoder(d)
	_, _, _ = h, z, r

	yyv1 := *v
	yyh1, yyl1 := z.DecSliceHelperStart()
	var yyc1 bool
	_ = yyc1
	if yyh1.IsNil {
		if yyv1 != nil {
			yyv1 = nil
			yyc1 = true
		}
	} else if yyl1 == 0 {
		if yyv1 == nil {
			yyv1 = []*OldReceipt{}
			yyc1 = true
		} else if len(yyv1) != 0 {
			yyv1 = yyv1[:0]
			yyc1 = true
		}
	} else {
		yyhl1 := yyl1 > 0
		var yyrl1 int
		_ = yyrl1
		if yyhl1 {
			if yyl1 > cap(yyv1) {
				yyrl1 = z.DecInferLen(yyl1, z.DecBasicHandle().MaxInitLen, 8)
				if yyrl1 <= cap(yyv1) {
					yyv1 = yyv1[:yyrl1]
				} else {
					yyv1 = make([]*OldReceipt, yyrl1)
				}
				yyc1 = true
			} else if yyl1 != len(yyv1) {
				yyv1 = yyv1[:yyl1]
				yyc1 = true
			}
		}
		var yyj1 int
		for yyj1 = 0; (yyhl1 && yyj1 < yyl1) || !(yyhl1 || z.DecCheckBreak()); yyj1++ { // bounds-check-elimination
			if yyj1 == 0 && yyv1 == nil {
				if yyhl1 {
					yyrl1 = z.DecInferLen(yyl1, z.DecBasicHandle().MaxInitLen, 8)
				} else {
					yyrl1 = 8
				}
				yyv1 = make([]*OldReceipt, yyrl1)
				yyc1 = true
			}
			yyh1.ElemContainerState(yyj1)
			var yydb1 bool
			if yyj1 >= len(yyv1) {
				yyv1 = append(yyv1, nil)
				yyc1 = true
			}
			if yydb1 {
				z.DecSwallow()
			} else {
				if r.TryNil() {
					yyv1[yyj1] = nil
				} else {
					if yyv1[yyj1] == nil {
						yyv1[yyj1] = new(OldReceipt)
					}
					yyv1[yyj1].CodecDecodeSelf(d)
				}
			}
		}
		if yyj1 < len(yyv1) {
			yyv1 = yyv1[:yyj1]
			yyc1 = true
		} else if yyj1 == 0 && yyv1 == nil {
			yyv1 = make([]*OldReceipt, 0)
			yyc1 = true
		}
	}
	yyh1.End()
	if yyc1 {
		*v = yyv1
	}
}
