// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package types

import (
	"bytes"
	"encoding/json"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/rlp"
)

func BenchmarkHeaderRLP(b *testing.B) {
	tr := NewTRand()
	header := tr.RandHeader()
	var buf bytes.Buffer
	b.Run(`Encode`, func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			buf.Reset()
			header.EncodeRLP(&buf)
		}
	})
	b.Run(`Decode`, func(b *testing.B) {
		b.ReportAllocs()
		buf.Reset()
		header.EncodeRLP(&buf)
		var v Header
		for b.Loop() {
			rlp.DecodeBytes(buf.Bytes(), &v)
		}
	})
}

func BenchmarkLegacyTxRLP(b *testing.B) {
	tr := NewTRand()
	txn := tr.RandTransaction(LegacyTxType)
	var buf bytes.Buffer

	for b.Loop() {
		buf.Reset()
		txn.EncodeRLP(&buf)
	}
}

func BenchmarkAccessListTxRLP(b *testing.B) {
	tr := NewTRand()
	txn := tr.RandTransaction(AccessListTxType)
	var buf bytes.Buffer

	for b.Loop() {
		buf.Reset()
		txn.EncodeRLP(&buf)
	}
}

func BenchmarkDynamicFeeTxRLP(b *testing.B) {
	tr := NewTRand()
	txn := tr.RandTransaction(DynamicFeeTxType)
	var buf bytes.Buffer

	for b.Loop() {
		buf.Reset()
		txn.EncodeRLP(&buf)
	}
}

func BenchmarkBlobTxRLP(b *testing.B) {
	tr := NewTRand()
	txn := tr.RandTransaction(BlobTxType)
	var buf bytes.Buffer

	for b.Loop() {
		buf.Reset()
		txn.EncodeRLP(&buf)
	}
}

func BenchmarkSetCodeTxRLP(b *testing.B) {
	tr := NewTRand()
	txn := tr.RandTransaction(SetCodeTxType)
	var buf bytes.Buffer

	for b.Loop() {
		buf.Reset()
		txn.EncodeRLP(&buf)
	}
}

// BenchmarkDecodeTransactionRLP covers the encoding transactions have inside a
// block body and in snapshot files: typed transactions are wrapped in an RLP
// string envelope.
func BenchmarkDecodeTransactionRLP(b *testing.B) {
	for _, tt := range benchTxTypes {
		b.Run(tt.name, func(b *testing.B) {
			enc := encodeTxRLP(b, randCallTransaction(b, tt.txType))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := DecodeTransaction(enc); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkUnmarshalTransactionFromBinary covers the canonical EIP-2718
// encoding used on the wire and by the RPC API: no envelope.
func BenchmarkUnmarshalTransactionFromBinary(b *testing.B) {
	for _, tt := range benchTxTypes {
		b.Run(tt.name, func(b *testing.B) {
			enc := encodeTxBinary(b, randCallTransaction(b, tt.txType))
			b.ReportAllocs()
			for b.Loop() {
				if _, err := UnmarshalTransactionFromBinary(enc, false); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDecodeOptionalAddress(b *testing.B) {
	run := func(b *testing.B, addr *common.Address) {
		b.Helper()
		var buf bytes.Buffer
		if err := EncodeOptionalAddress(addr, &buf, make([]byte, 21)); err != nil {
			b.Fatal(err)
		}
		enc := buf.Bytes()
		b.ReportAllocs()
		var dst *common.Address
		for b.Loop() {
			s := rlp.NewBytesStream(enc)
			if err := DecodeOptionalAddress(&dst, s); err != nil {
				b.Fatal(err)
			}
			rlp.PutStream(s)
		}
	}
	b.Run("Address", func(b *testing.B) {
		addr := common.HexToAddress("0x000000000000000000000000000000000000dEaD")
		run(b, &addr)
	})
	b.Run("Nil", func(b *testing.B) { run(b, nil) })
}

func BenchmarkWithdrawalRLP(b *testing.B) {
	tr := NewTRand()
	w := tr.RandWithdrawal()
	var buf bytes.Buffer

	for b.Loop() {
		buf.Reset()
		w.EncodeRLP(&buf)
	}
}

func BenchmarkLogRLP(b *testing.B) {
	tr := NewTRand()
	log := tr.RandLogFixed()
	var buf bytes.Buffer

	b.Run(`Encode`, func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			buf.Reset()
			logStorage := (*LogForStorage)(log)
			logStorage.EncodeRLP(&buf)
		}
	})

	b.Run(`Decode`, func(b *testing.B) {
		b.ReportAllocs()
		buf.Reset()
		logStorage := (*LogForStorage)(log)
		logStorage.EncodeRLP(&buf)
		var decoded LogForStorage
		for b.Loop() {
			rlp.DecodeBytes(buf.Bytes(), &decoded)
		}
	})
}

func BenchmarkDeriveFieldsV4ForCachedReceipt(b *testing.B) {
	tr := NewTRand()
	receipt := tr.RandReceiptFixed()
	blockHash, txnHash := tr.RandHash(), tr.RandHash()

	b.Run(`bloom`, func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			receipt.DeriveFieldsV4ForCachedReceipt(blockHash, 123456, txnHash, true)
		}
	})
	b.Run(`no-bloom`, func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			receipt.DeriveFieldsV4ForCachedReceipt(blockHash, 123456, txnHash, false)
		}
	})
}

func BenchmarkReceiptRLP(b *testing.B) {
	tr := NewTRand()
	receipt := tr.RandReceiptFixed()
	var buf bytes.Buffer

	b.Run(`Encode`, func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			buf.Reset()
			receiptStorage := (*ReceiptForStorage)(receipt)
			receiptStorage.EncodeRLP(&buf)
		}
	})

	b.Run(`Decode`, func(b *testing.B) {
		b.ReportAllocs()
		buf.Reset()
		receiptStorage := (*ReceiptForStorage)(receipt)
		receiptStorage.EncodeRLP(&buf)
		var decoded ReceiptForStorage
		for b.Loop() {
			rlp.DecodeBytes(buf.Bytes(), &decoded)
		}
	})
}

func BenchmarkLogJSON(b *testing.B) {
	tr := NewTRand()

	mkLog := func() *Log {
		l := tr.RandLogFixed()
		l.BlockNumber = hexutil.Uint64(tr.rnd.Uint64())
		l.TxHash = tr.RandHash()
		l.TxIndex = hexutil.Uint(tr.rnd.Intn(200))
		l.BlockHash = tr.RandHash()
		l.Index = hexutil.Uint(tr.rnd.Intn(500))
		return l
	}

	log := mkLog()

	b.Run("Log/Single", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchJSONSink, _ = json.Marshal(log)
		}
	})

	logs := make([]*Log, 100)
	for i := range logs {
		logs[i] = mkLog()
	}
	b.Run("Log/Batch100", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchJSONSink, _ = json.Marshal(logs)
		}
	})

	rpcLog := &RPCLog{
		Log:            *log,
		BlockTimestamp: hexutil.Uint64(1700000000),
	}
	b.Run("RPCLog/Single", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			benchJSONSink, _ = json.Marshal(rpcLog)
		}
	})
}

func BenchmarkLogJSONUnmarshal(b *testing.B) {
	tr := NewTRand()

	mkLog := func() *Log {
		l := tr.RandLogFixed()
		l.BlockNumber = hexutil.Uint64(tr.rnd.Uint64())
		l.TxHash = tr.RandHash()
		l.TxIndex = hexutil.Uint(tr.rnd.Intn(200))
		l.BlockHash = tr.RandHash()
		l.Index = hexutil.Uint(tr.rnd.Intn(500))
		return l
	}

	log := mkLog()
	encoded, err := json.Marshal(log)
	require.NoError(b, err)

	b.Run("Log/Single", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = json.Unmarshal(encoded, &benchLogSink)
		}
	})

	rpcLog := &RPCLog{Log: *log, BlockTimestamp: hexutil.Uint64(1700000000)}
	rpcEncoded, err := json.Marshal(rpcLog)
	require.NoError(b, err)
	var rpcSink RPCLog
	b.Run("RPCLog/Single", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = json.Unmarshal(rpcEncoded, &rpcSink)
		}
	})
}

// randCallTransaction returns a transaction of the given type that has a non-nil
// To, i.e. a call rather than a contract creation. Blob transactions cannot
// encode without one. The seed is fixed so that payload sizes stay comparable
// across runs.
func randCallTransaction(b *testing.B, txType int) Transaction {
	b.Helper()
	tr := &TRand{rnd: rand.New(rand.NewSource(1))}
	for range 100 {
		if txn := tr.RandTransaction(txType); txn.GetTo() != nil {
			return txn
		}
	}
	b.Fatalf("no transaction of type %d with a non-nil To", txType)
	return nil
}

func encodeTxRLP(b *testing.B, txn Transaction) []byte {
	b.Helper()
	var buf bytes.Buffer
	if err := txn.EncodeRLP(&buf); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

func encodeTxBinary(b *testing.B, txn Transaction) []byte {
	b.Helper()
	var buf bytes.Buffer
	if err := txn.MarshalBinary(&buf); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

var benchTxTypes = []struct {
	name   string
	txType int
}{
	{"Legacy", LegacyTxType},
	{"AccessList", AccessListTxType},
	{"DynamicFee", DynamicFeeTxType},
	{"Blob", BlobTxType},
	{"SetCode", SetCodeTxType},
}

var benchJSONSink []byte

var benchLogSink Log
