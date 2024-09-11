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

package freezeblocks

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/hexutility"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/dbutils"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit"
	"github.com/erigontech/erigon/core/rawdb"
	coresnaptype "github.com/erigontech/erigon/core/snaptype"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	borsnaptype "github.com/erigontech/erigon/polygon/bor/snaptype"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/services"
)

var ErrSpanNotFound = errors.New("span not found")
var ErrMilestoneNotFound = errors.New("milestone not found")
var ErrCheckpointNotFound = errors.New("checkpoint not found")

type RemoteBlockReader struct {
	client remote.ETHBACKENDClient
}

func (r *RemoteBlockReader) CanPruneTo(uint64) uint64 {
	panic("not implemented")
}
func (r *RemoteBlockReader) CurrentBlock(db kv.Tx) (*types.Block, error) {
	headHash := rawdb.ReadHeadBlockHash(db)
	headNumber, err := r.HeaderNumber(context.Background(), db, headHash)
	if err != nil {
		return nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if headNumber == nil {
		return nil, nil
	}
	block, _, err := r.BlockWithSenders(context.Background(), db, headHash, *headNumber)
	return block, err
}
func (r *RemoteBlockReader) RawTransactions(ctx context.Context, tx kv.Getter, fromBlock, toBlock uint64) (txs [][]byte, err error) {
	panic("not implemented")
}

func (r *RemoteBlockReader) FirstTxnNumNotInSnapshots() uint64 {
	panic("not implemented")
}

func (r *RemoteBlockReader) ReadAncestor(db kv.Getter, hash common.Hash, number, ancestor uint64, maxNonCanonical *uint64) (common.Hash, uint64) {
	panic("not implemented")
}
func (r *RemoteBlockReader) HeadersRange(ctx context.Context, walker func(header *types.Header) error) error {
	panic("not implemented")
}

func (r *RemoteBlockReader) Integrity(_ context.Context) error {
	panic("not implemented")
}

func (r *RemoteBlockReader) BadHeaderNumber(ctx context.Context, tx kv.Getter, hash common.Hash) (blockHeight *uint64, err error) {
	return rawdb.ReadBadHeaderNumber(tx, hash)
}

func (r *RemoteBlockReader) BlockByNumber(ctx context.Context, db kv.Tx, number uint64) (*types.Block, error) {
	hash, err := r.CanonicalHash(ctx, db, number)
	if err != nil {
		return nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if hash == (common.Hash{}) {
		return nil, nil
	}
	block, _, err := r.BlockWithSenders(ctx, db, hash, number)
	return block, err
}
func (r *RemoteBlockReader) BlockByHash(ctx context.Context, db kv.Tx, hash common.Hash) (*types.Block, error) {
	number, err := r.HeaderNumber(ctx, db, hash)
	if err != nil {
		return nil, err
	}
	if number == nil {
		return nil, nil
	}
	block, _, err := r.BlockWithSenders(ctx, db, hash, *number)
	return block, err
}
func (r *RemoteBlockReader) HeaderByNumber(ctx context.Context, tx kv.Getter, blockHeight uint64) (*types.Header, error) {
	canonicalHash, err := r.CanonicalHash(ctx, tx, blockHeight)
	if err != nil {
		return nil, err
	}
	block, _, err := r.BlockWithSenders(ctx, tx, canonicalHash, blockHeight)
	if err != nil {
		return nil, err
	}
	return block.Header(), nil
}
func (r *RemoteBlockReader) Snapshots() services.BlockSnapshots    { panic("not implemented") }
func (r *RemoteBlockReader) BorSnapshots() services.BlockSnapshots { panic("not implemented") }
func (r *RemoteBlockReader) AllTypes() []snaptype.Type             { panic("not implemented") }
func (r *RemoteBlockReader) FrozenBlocks() uint64                  { panic("not supported") }
func (r *RemoteBlockReader) FrozenBorBlocks() uint64               { panic("not supported") }
func (r *RemoteBlockReader) FrozenFiles() (list []string)          { panic("not supported") }
func (r *RemoteBlockReader) FreezingCfg() ethconfig.BlocksFreezing { panic("not supported") }

func (r *RemoteBlockReader) HeaderByHash(ctx context.Context, tx kv.Getter, hash common.Hash) (*types.Header, error) {
	blockNum, err := r.HeaderNumber(ctx, tx, hash)
	if err != nil {
		return nil, err
	}
	if blockNum == nil {
		return nil, nil
	}
	block, _, err := r.BlockWithSenders(ctx, tx, hash, *blockNum)
	if err != nil {
		return nil, err
	}
	return block.Header(), nil
}

func (r *RemoteBlockReader) CanonicalHash(ctx context.Context, tx kv.Getter, blockHeight uint64) (common.Hash, error) {
	resp, err := r.client.CanonicalHash(ctx, &remote.CanonicalHashRequest{BlockNumber: blockHeight})
	if err != nil {
		return common.Hash{}, err
	}
	return gointerfaces.ConvertH256ToHash(resp.Hash), nil
}

var _ services.FullBlockReader = &RemoteBlockReader{}

func NewRemoteBlockReader(client remote.ETHBACKENDClient) *RemoteBlockReader {
	return &RemoteBlockReader{client}
}

func (r *RemoteBlockReader) TxnLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, bool, error) {
	reply, err := r.client.TxnLookup(ctx, &remote.TxnLookupRequest{TxnHash: gointerfaces.ConvertHashToH256(txnHash)})
	if err != nil {
		return 0, false, err
	}
	if reply == nil {
		return 0, false, nil
	}
	return reply.BlockNumber, true, nil
}

func (r *RemoteBlockReader) TxnByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, i int) (txn types.Transaction, err error) {
	canonicalHash, err := r.CanonicalHash(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	b, err := r.BodyWithTransactions(ctx, tx, canonicalHash, blockNum)
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, nil
	}
	if i < 0 {
		return nil, nil
	}
	if len(b.Transactions) <= i {
		return nil, nil
	}
	return b.Transactions[i], nil
}

func (r *RemoteBlockReader) HasSenders(ctx context.Context, _ kv.Getter, hash common.Hash, blockHeight uint64) (bool, error) {
	panic("HasSenders is low-level method, don't use it in RPCDaemon")
}

func (r *RemoteBlockReader) BlockWithSenders(ctx context.Context, _ kv.Getter, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error) {
	reply, err := r.client.Block(ctx, &remote.BlockRequest{BlockHash: gointerfaces.ConvertHashToH256(hash), BlockHeight: blockHeight})
	if err != nil {
		return nil, nil, err
	}

	block = &types.Block{}
	err = rlp.Decode(bytes.NewReader(reply.BlockRlp), block)
	if err != nil {
		return nil, nil, err
	}
	senders = make([]common.Address, len(reply.Senders)/20)
	for i := range senders {
		senders[i].SetBytes(reply.Senders[i*20 : (i+1)*20])
	}
	if len(senders) == block.Transactions().Len() { //it's fine if no senders provided - they can be lazy recovered
		block.SendersToTxs(senders)
	}
	return block, senders, nil
}

func (r *RemoteBlockReader) IterateFrozenBodies(_ func(blockNum uint64, baseTxNum uint64, txCount uint64) error) error {
	panic("not implemented")
}

func (r *RemoteBlockReader) Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (*types.Header, error) {
	block, _, err := r.BlockWithSenders(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return block.Header(), nil
}
func (r *RemoteBlockReader) Body(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, txCount uint32, err error) {
	block, _, err := r.BlockWithSenders(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, 0, err
	}
	if block == nil {
		return nil, 0, nil
	}
	return block.Body(), uint32(len(block.Body().Transactions)), nil
}
func (r *RemoteBlockReader) IsCanonical(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (bool, error) {
	expected, err := r.CanonicalHash(ctx, tx, blockHeight)
	if err != nil {
		return false, err
	}
	return expected == hash, nil
}
func (r *RemoteBlockReader) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
	block, _, err := r.BlockWithSenders(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return block.Body(), nil
}
func (r *RemoteBlockReader) HeaderNumber(ctx context.Context, tx kv.Getter, hash common.Hash) (*uint64, error) {
	resp, err := r.client.HeaderNumber(ctx, &remote.HeaderNumberRequest{Hash: gointerfaces.ConvertHashToH256(hash)})
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, nil
	}
	return resp.Number, nil
}
func (r *RemoteBlockReader) BodyRlp(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (bodyRlp rlp.RawValue, err error) {
	body, err := r.BodyWithTransactions(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	bodyRlp, err = rlp.EncodeToBytes(body)
	if err != nil {
		return nil, err
	}
	return bodyRlp, nil
}

func (r *RemoteBlockReader) LastEventId(ctx context.Context, tx kv.Tx) (uint64, bool, error) {
	return 0, false, errors.New("not implemented")
}

func (r *RemoteBlockReader) EventLookup(ctx context.Context, tx kv.Getter, borTxnHash common.Hash) (uint64, bool, error) {
	reply, err := r.client.BorTxnLookup(ctx, &remote.BorTxnLookupRequest{BorTxHash: gointerfaces.ConvertHashToH256(borTxnHash)})
	if err != nil {
		return 0, false, err
	}
	if reply == nil {
		return 0, false, nil
	}
	return reply.BlockNumber, reply.Present, nil
}

func (r *RemoteBlockReader) EventsByBlock(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) ([]rlp.RawValue, error) {
	reply, err := r.client.BorEvents(ctx, &remote.BorEventsRequest{BlockHash: gointerfaces.ConvertHashToH256(hash), BlockNum: blockHeight})
	if err != nil {
		return nil, err
	}
	result := make([]rlp.RawValue, len(reply.EventRlps))
	for i, r := range reply.EventRlps {
		result[i] = r
	}
	return result, nil
}
func (r *RemoteBlockReader) BorStartEventID(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (uint64, error) {
	panic("not implemented")
}

func (r *RemoteBlockReader) LastFrozenEventId() uint64 {
	panic("not implemented")
}

func (r *RemoteBlockReader) LastFrozenEventBlockNum() uint64 {
	panic("not implemented")
}

func (r *RemoteBlockReader) Span(_ context.Context, _ kv.Getter, _ uint64) ([]byte, error) {
	panic("not implemented")
}

func (r *RemoteBlockReader) LastSpanId(_ context.Context, _ kv.Tx) (uint64, bool, error) {
	panic("not implemented")
}

func (r *RemoteBlockReader) LastFrozenSpanId() uint64 {
	panic("not implemented")
}

func (r *RemoteBlockReader) LastMilestoneId(ctx context.Context, tx kv.Tx) (uint64, bool, error) {
	return 0, false, errors.New("not implemented")
}

func (r *RemoteBlockReader) Milestone(ctx context.Context, tx kv.Getter, spanId uint64) ([]byte, error) {
	return nil, nil
}

func (r *RemoteBlockReader) LastCheckpointId(ctx context.Context, tx kv.Tx) (uint64, bool, error) {
	return 0, false, errors.New("not implemented")
}
func (r *RemoteBlockReader) CanonicalBodyForStorage(ctx context.Context, tx kv.Getter, blockNum uint64) (body *types.BodyForStorage, err error) {
	bdRaw, err := r.client.CanonicalBodyForStorage(ctx, &remote.CanonicalBodyForStorageRequest{BlockNumber: blockNum})
	if err != nil {
		return nil, err
	}
	if len(bdRaw.Body) == 0 {
		return nil, nil
	}
	body = &types.BodyForStorage{}
	err = rlp.Decode(bytes.NewReader(bdRaw.Body), body)
	if err != nil {
		return nil, err
	}
	return body, nil
}
func (r *RemoteBlockReader) Checkpoint(ctx context.Context, tx kv.Getter, spanId uint64) ([]byte, error) {
	return nil, nil
}

// BlockReader can read blocks from db and snapshots
type BlockReader struct {
	sn    *RoSnapshots
	borSn *BorRoSnapshots
}

func NewBlockReader(snapshots services.BlockSnapshots, borSnapshots services.BlockSnapshots) *BlockReader {
	borSn, _ := borSnapshots.(*BorRoSnapshots)
	sn, _ := snapshots.(*RoSnapshots)
	return &BlockReader{sn: sn, borSn: borSn}
}

func (r *BlockReader) CanPruneTo(currentBlockInDB uint64) uint64 {
	return CanDeleteTo(currentBlockInDB, r.sn.BlocksAvailable())
}
func (r *BlockReader) Snapshots() services.BlockSnapshots { return r.sn }
func (r *BlockReader) BorSnapshots() services.BlockSnapshots {
	if r.borSn != nil {
		return r.borSn
	}

	return nil
}

func (r *BlockReader) AllTypes() []snaptype.Type {
	var types []snaptype.Type
	types = append(types, r.sn.Types()...)
	if r.borSn != nil {
		types = append(types, r.borSn.Types()...)
	}
	return types
}

func (r *BlockReader) FrozenBlocks() uint64 { return r.sn.BlocksAvailable() }
func (r *BlockReader) FrozenBorBlocks() uint64 {
	if r.borSn != nil {
		return r.borSn.BlocksAvailable()
	}
	return 0
}
func (r *BlockReader) FrozenFiles() []string {
	files := r.sn.Files()
	if r.borSn != nil {
		files = append(files, r.borSn.Files()...)
	}
	sort.Strings(files)
	return files
}
func (r *BlockReader) FreezingCfg() ethconfig.BlocksFreezing { return r.sn.Cfg() }

func (r *BlockReader) HeadersRange(ctx context.Context, walker func(header *types.Header) error) error {
	return ForEachHeader(ctx, r.sn, walker)
}

func (r *BlockReader) HeaderByNumber(ctx context.Context, tx kv.Getter, blockHeight uint64) (h *types.Header, err error) {
	var dbgPrefix string
	dbgLogs := dbg.Enabled(ctx)
	if dbgLogs {
		dbgPrefix = fmt.Sprintf("[dbg] BlockReader(idxMax=%d,segMax=%d).HeaderByNumber(blk=%d) -> ", r.sn.idxMax.Load(), r.sn.segmentsMax.Load(), blockHeight)
	}

	maxBlockNumInFiles := r.sn.BlocksAvailable()
	if maxBlockNumInFiles == 0 || blockHeight > maxBlockNumInFiles {
		if tx != nil {
			blockHash, err := rawdb.ReadCanonicalHash(tx, blockHeight)
			if err != nil {
				return nil, err
			}
			// if no canonical marker - still can try read from files
			if blockHash != emptyHash {
				h = rawdb.ReadHeader(tx, blockHash, blockHeight)
				if h != nil {
					return h, nil
				} else {
					if dbgLogs {
						log.Info(dbgPrefix + "not found in db")
					}
				}
			} else {
				if dbgLogs {
					log.Info(dbgPrefix + "canonical hash is empty")
				}
			}
		} else {
			if dbgLogs {
				log.Info(dbgPrefix + "tx is nil")
			}
		}
		return nil, nil
	}

	seg, ok, release := r.sn.ViewSingleFile(coresnaptype.Headers, blockHeight)
	if !ok {
		if dbgLogs {
			log.Info(dbgPrefix + "not found file for such blockHeight")
		}
		return
	}
	defer release()

	h, _, err = r.headerFromSnapshot(blockHeight, seg, nil)
	if err != nil {
		return nil, err
	}
	if h == nil {
		if dbgLogs {
			log.Info(dbgPrefix + "got nil from file")
		}
	}
	return h, nil
}
func (r *BlockReader) HeaderNumber(ctx context.Context, tx kv.Getter, hash common.Hash) (*uint64, error) {
	ret := rawdb.ReadHeaderNumber(tx, hash)
	if ret != nil {
		return ret, nil
	}

	h, err := r.HeaderByHash(ctx, tx, hash)
	if err != nil {
		return nil, err
	}
	if h == nil {
		return nil, nil
	}
	ret = new(uint64)
	*ret = h.Number.Uint64()
	return ret, nil
}
func (r *BlockReader) IsCanonical(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (bool, error) {
	expected, ok, err := r.CanonicalHash(ctx, tx, blockHeight)
	if err != nil {
		return false, err
	}
	return ok && expected == hash, nil
}

// HeaderByHash - will search header in all snapshots starting from recent
func (r *BlockReader) HeaderByHash(ctx context.Context, tx kv.Getter, hash common.Hash) (h *types.Header, err error) {
	h, err = rawdb.ReadHeaderByHash(tx, hash)
	if err != nil {
		return nil, err
	}
	if h != nil {
		return h, nil
	}

	segments, release := r.sn.ViewType(coresnaptype.Headers)
	defer release()

	buf := make([]byte, 128)
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].Index() == nil {
			continue
		}

		h, err = r.headerFromSnapshotByHash(hash, segments[i], buf)
		if err != nil {
			return nil, err
		}
		if h != nil {
			break
		}
	}
	return h, nil
}

var emptyHash = common.Hash{}

func (r *BlockReader) CanonicalHash(ctx context.Context, tx kv.Getter, blockHeight uint64) (h common.Hash, ok bool, err error) {
	h, err = rawdb.ReadCanonicalHash(tx, blockHeight)
	if err != nil {
		return emptyHash, false, err
	}
	if h != emptyHash {
		return h, true, nil
	}

	seg, ok, release := r.sn.ViewSingleFile(coresnaptype.Headers, blockHeight)
	if !ok {
		return h, false, nil
	}
	defer release()

	header, _, err := r.headerFromSnapshot(blockHeight, seg, nil)
	if err != nil {
		return h, false, err
	}
	if header == nil {
		return h, false, nil
	}
	h = header.Hash()
	return h, true, nil
}

func (r *BlockReader) Header(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (h *types.Header, err error) {
	//TODO: investigate why code blolow causing getting error `Could not set forkchoice                 app=caplin stage=ForkChoice err="execution Client RPC failed to retrieve ForkChoiceUpdate response, err: unknown ancestor"`
	//maxBlockNumInFiles := r.sn.BlocksAvailable()
	//if maxBlockNumInFiles == 0 || blockHeight > maxBlockNumInFiles {
	//	if tx == nil {
	//		return nil, nil
	//	}
	//	h = rawdb.ReadHeader(tx, hash, blockHeight)
	//	return h, nil
	//}
	if tx != nil {
		h = rawdb.ReadHeader(tx, hash, blockHeight)
		if h != nil {
			return h, nil
		}
	}

	seg, ok, release := r.sn.ViewSingleFile(coresnaptype.Headers, blockHeight)
	if !ok {
		return
	}
	defer release()

	h, _, err = r.headerFromSnapshot(blockHeight, seg, nil)
	if err != nil {
		return h, err
	}
	return h, nil
}

func (r *BlockReader) BodyWithTransactions(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, err error) {
	var dbgPrefix string
	dbgLogs := dbg.Enabled(ctx)
	if dbgLogs {
		dbgPrefix = fmt.Sprintf("[dbg] BlockReader(idxMax=%d,segMax=%d).BodyWithTransactions(hash=%x,blk=%d) -> ", r.sn.idxMax.Load(), r.sn.segmentsMax.Load(), hash, blockHeight)
	}

	maxBlockNumInFiles := r.sn.BlocksAvailable()
	if maxBlockNumInFiles == 0 || blockHeight > maxBlockNumInFiles {
		if tx == nil {
			if dbgLogs {
				log.Info(dbgPrefix + "RoTx is nil")
			}
			return nil, nil
		}
		body, err = rawdb.ReadBodyWithTransactions(tx, hash, blockHeight)
		if err != nil {
			return nil, err
		}
		if body != nil {
			return body, nil
		}
		if dbgLogs {
			log.Info(dbgPrefix + "found in db=false")
		}
	}

	seg, ok, release := r.sn.ViewSingleFile(coresnaptype.Bodies, blockHeight)
	if !ok {
		if dbgLogs {
			log.Info(dbgPrefix + "no bodies file for this block num")
		}
		return nil, nil
	}
	defer release()

	var baseTxnID uint64
	var txCount uint32
	var buf []byte
	body, baseTxnID, txCount, buf, err = r.bodyFromSnapshot(blockHeight, seg, buf)
	if err != nil {
		return nil, err
	}
	release()

	if body == nil {
		if dbgLogs {
			log.Info(dbgPrefix + "got nil body from file")
		}
		return nil, nil
	}

	txnSeg, ok, release := r.sn.ViewSingleFile(coresnaptype.Transactions, blockHeight)
	if !ok {
		if dbgLogs {
			log.Info(dbgPrefix+"no transactions file for this block num", "r.sn.BlocksAvailable()", r.sn.BlocksAvailable(), "r.sn.idxMax", r.sn.idxMax.Load(), "r.sn.segmetntsMax", r.sn.segmentsMax.Load())
		}
		return nil, nil
	}
	defer release()

	txs, senders, err := r.txsFromSnapshot(baseTxnID, txCount, txnSeg, buf)
	if err != nil {
		return nil, err
	}
	release()

	if txs == nil {
		if dbgLogs {
			log.Info(dbgPrefix + "got nil txs from file")
		}
		return nil, nil
	}
	if dbgLogs {
		log.Info(dbgPrefix+"got non-nil txs from file", "len(txs)", len(txs))
	}
	body.Transactions = txs
	body.SendersToTxs(senders)
	return body, nil
}

func (r *BlockReader) BodyRlp(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (bodyRlp rlp.RawValue, err error) {
	body, err := r.BodyWithTransactions(ctx, tx, hash, blockHeight)
	if err != nil {
		return nil, err
	}
	bodyRlp, err = rlp.EncodeToBytes(body)
	if err != nil {
		return nil, err
	}
	return bodyRlp, nil
}

func (r *BlockReader) Body(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (body *types.Body, txCount uint32, err error) {
	maxBlockNumInFiles := r.sn.BlocksAvailable()
	if maxBlockNumInFiles == 0 || blockHeight > maxBlockNumInFiles {
		if tx == nil {
			return nil, 0, nil
		}
		body, _, txCount = rawdb.ReadBody(tx, hash, blockHeight)
		return body, txCount, nil
	}

	seg, ok, release := r.sn.ViewSingleFile(coresnaptype.Bodies, blockHeight)
	if !ok {
		return
	}
	defer release()

	body, _, txCount, _, err = r.bodyFromSnapshot(blockHeight, seg, nil)
	if err != nil {
		return nil, 0, err
	}
	return body, txCount, nil
}

func (r *BlockReader) HasSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (bool, error) {
	maxBlockNumInFiles := r.sn.BlocksAvailable()
	if maxBlockNumInFiles == 0 || blockHeight > maxBlockNumInFiles {
		return rawdb.HasSenders(tx, hash, blockHeight)
	}
	return true, nil
}

func (r *BlockReader) BlockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64) (block *types.Block, senders []common.Address, err error) {
	return r.blockWithSenders(ctx, tx, hash, blockHeight, false)
}
func (r *BlockReader) CanonicalBodyForStorage(ctx context.Context, tx kv.Getter, blockNum uint64) (body *types.BodyForStorage, err error) {
	bodySeg, ok, release := r.sn.ViewSingleFile(coresnaptype.Bodies, blockNum)
	if !ok {
		hash, ok, err := r.CanonicalHash(ctx, tx, blockNum)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, nil
		}
		return rawdb.ReadBodyForStorageByKey(tx, dbutils.BlockBodyKey(blockNum, hash))
	}
	defer release()

	var buf []byte
	b, _, err := r.bodyForStorageFromSnapshot(blockNum, bodySeg, buf)
	return b, err
}
func (r *BlockReader) blockWithSenders(ctx context.Context, tx kv.Getter, hash common.Hash, blockHeight uint64, forceCanonical bool) (block *types.Block, senders []common.Address, err error) {
	var dbgPrefix string
	dbgLogs := dbg.Enabled(ctx)
	if dbgLogs {
		dbgPrefix = fmt.Sprintf("[dbg] BlockReader(idxMax=%d,segMax=%d).blockWithSenders(hash=%x,blk=%d) -> ", r.sn.idxMax.Load(), r.sn.segmentsMax.Load(), hash, blockHeight)
	}

	maxBlockNumInFiles := r.sn.BlocksAvailable()
	if maxBlockNumInFiles == 0 || blockHeight > maxBlockNumInFiles {
		if tx == nil {
			if dbgLogs {
				log.Info(dbgPrefix + "RoTx is nil")
			}
			return nil, nil, nil
		}
		if forceCanonical {
			canonicalHash, ok, err := r.CanonicalHash(ctx, tx, blockHeight)
			if err != nil {
				return nil, nil, fmt.Errorf("requested non-canonical hash %x. canonical=%x", hash, canonicalHash)
			}
			if !ok || canonicalHash != hash {
				if dbgLogs {
					log.Info(dbgPrefix + fmt.Sprintf("this hash is not canonical now. current one is %x", canonicalHash))
				}
				return nil, nil, nil
			}
		}

		block, senders, err = rawdb.ReadBlockWithSenders(tx, hash, blockHeight)
		if err != nil {
			return nil, nil, err
		}
		if dbgLogs {
			log.Info(dbgPrefix + fmt.Sprintf("found_in_db=%t", block != nil))
		}
		return block, senders, nil
	}

	if r.sn == nil {
		if dbgLogs {
			log.Info(dbgPrefix + "no files")
		}
		return
	}

	seg, ok, release := r.sn.ViewSingleFile(coresnaptype.Headers, blockHeight)
	if !ok {
		if dbgLogs {
			log.Info(dbgPrefix + "no header files for this block num")
		}
		return
	}
	defer release()

	var buf []byte
	h, buf, err := r.headerFromSnapshot(blockHeight, seg, buf)
	if err != nil {
		return nil, nil, err
	}
	if h == nil {
		if dbgLogs {
			log.Info(dbgPrefix + "got nil header from file")
		}
		return
	} else {
		hash = h.Hash()
	}
	release()

	var b *types.Body
	var baseTxnId uint64
	var txCount uint32
	bodySeg, ok, release := r.sn.ViewSingleFile(coresnaptype.Bodies, blockHeight)
	if !ok {
		if dbgLogs {
			log.Info(dbgPrefix + "no bodies file for this block num")
		}
		return
	}
	defer release()

	b, baseTxnId, txCount, buf, err = r.bodyFromSnapshot(blockHeight, bodySeg, buf)
	if err != nil {
		return nil, nil, err
	}
	release()

	if b == nil {
		if dbgLogs {
			log.Info(dbgPrefix + "got nil body from file")
		}
		return
	}
	if txCount == 0 {
		block = types.NewBlockFromStorage(hash, h, nil, b.Uncles, b.Withdrawals, b.Requests)
		if len(senders) != block.Transactions().Len() {
			if dbgLogs {
				log.Info(dbgPrefix + fmt.Sprintf("found block with %d transactions, but %d senders", block.Transactions().Len(), len(senders)))
			}
			return block, senders, nil // no senders is fine - will recover them on the fly
		}
		block.SendersToTxs(senders)
		return block, senders, nil
	}

	txnSeg, ok, release := r.sn.ViewSingleFile(coresnaptype.Transactions, blockHeight)
	if !ok {
		if dbgLogs {
			log.Info(dbgPrefix+"no transactions file for this block num", "r.sn.BlocksAvailable()", r.sn.BlocksAvailable(), "r.sn.indicesReady", r.sn.indicesReady.Load())
		}
		return
	}
	defer release()
	var txs []types.Transaction
	txs, senders, err = r.txsFromSnapshot(baseTxnId, txCount, txnSeg, buf)
	if err != nil {
		return nil, nil, err
	}
	release()

	block = types.NewBlockFromStorage(hash, h, txs, b.Uncles, b.Withdrawals, b.Requests)
	if len(senders) != block.Transactions().Len() {
		if dbgLogs {
			log.Info(dbgPrefix + fmt.Sprintf("found block with %d transactions, but %d senders", block.Transactions().Len(), len(senders)))
		}
		return block, senders, nil // no senders is fine - will recover them on the fly
	}
	block.SendersToTxs(senders)
	return block, senders, nil
}

func (r *BlockReader) headerFromSnapshot(blockHeight uint64, sn *Segment, buf []byte) (*types.Header, []byte, error) {
	index := sn.Index()

	if index == nil {
		return nil, buf, nil
	}
	headerOffset := index.OrdinalLookup(blockHeight - index.BaseDataID())
	gg := sn.MakeGetter()
	gg.Reset(headerOffset)
	if !gg.HasNext() {
		return nil, buf, nil
	}
	buf, _ = gg.Next(buf[:0])
	if len(buf) == 0 {
		return nil, buf, nil
	}
	h := &types.Header{}
	if err := rlp.DecodeBytes(buf[1:], h); err != nil {
		return nil, buf, err
	}
	return h, buf, nil
}

// headerFromSnapshotByHash - getting header by hash AND ensure that it has correct hash
// because HeaderByHash method will search header in all snapshots - and may request header which doesn't exists
// but because our indices are based on PerfectHashMap, no way to know is given key exists or not, only way -
// to make sure is to fetch it and compare hash
func (r *BlockReader) headerFromSnapshotByHash(hash common.Hash, sn *Segment, buf []byte) (*types.Header, error) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Errorf("%+v, snapshot: %d-%d, trace: %s", rec, sn.from, sn.to, dbg.Stack()))
		}
	}() // avoid crash because Erigon's core does many things

	index := sn.Index()

	if index == nil {
		return nil, nil
	}

	reader := recsplit.NewIndexReader(index)
	localID, ok := reader.Lookup(hash[:])
	if !ok {
		return nil, nil
	}
	headerOffset := index.OrdinalLookup(localID)
	gg := sn.MakeGetter()
	gg.Reset(headerOffset)
	if !gg.HasNext() {
		return nil, nil
	}
	buf, _ = gg.Next(buf[:0])
	if len(buf) > 1 && hash[0] != buf[0] {
		return nil, nil
	}

	h := &types.Header{}
	if err := rlp.DecodeBytes(buf[1:], h); err != nil {
		return nil, err
	}
	if h.Hash() != hash {
		return nil, nil
	}
	return h, nil
}

func (r *BlockReader) bodyFromSnapshot(blockHeight uint64, sn *Segment, buf []byte) (*types.Body, uint64, uint32, []byte, error) {
	b, buf, err := r.bodyForStorageFromSnapshot(blockHeight, sn, buf)
	if err != nil {
		return nil, 0, 0, buf, err
	}
	if b == nil {
		return nil, 0, 0, buf, nil
	}
	body := new(types.Body)
	body.Uncles = b.Uncles
	body.Withdrawals = b.Withdrawals
	body.Requests = b.Requests
	var txCount uint32
	if b.TxCount >= 2 {
		txCount = b.TxCount - 2
	}
	return body, b.BaseTxnID.First(), txCount, buf, nil // empty txs in the beginning and end of block
}

func (r *BlockReader) bodyForStorageFromSnapshot(blockHeight uint64, sn *Segment, buf []byte) (*types.BodyForStorage, []byte, error) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Errorf("%+v, snapshot: %d-%d, trace: %s", rec, sn.from, sn.to, dbg.Stack()))
		}
	}() // avoid crash because Erigon's core does many things

	index := sn.Index()

	if index == nil {
		return nil, buf, nil
	}

	bodyOffset := index.OrdinalLookup(blockHeight - index.BaseDataID())

	gg := sn.MakeGetter()
	gg.Reset(bodyOffset)
	if !gg.HasNext() {
		return nil, buf, nil
	}
	buf, _ = gg.Next(buf[:0])
	if len(buf) == 0 {
		return nil, buf, nil
	}
	b := &types.BodyForStorage{}
	reader := bytes.NewReader(buf)
	if err := rlp.Decode(reader, b); err != nil {
		return nil, buf, err
	}

	return b, buf, nil
}

func (r *BlockReader) txsFromSnapshot(baseTxnID uint64, txCount uint32, txsSeg *Segment, buf []byte) (txs []types.Transaction, senders []common.Address, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Errorf("%+v, snapshot: %d-%d, trace: %s", rec, txsSeg.from, txsSeg.to, dbg.Stack()))
		}
	}() // avoid crash because Erigon's core does many things

	idxTxnHash := txsSeg.Index(coresnaptype.Indexes.TxnHash)

	if idxTxnHash == nil {
		return nil, nil, nil
	}
	if baseTxnID < idxTxnHash.BaseDataID() {
		return nil, nil, fmt.Errorf(".idx file has wrong baseDataID? %d<%d, %s", baseTxnID, idxTxnHash.BaseDataID(), txsSeg.FilePath())
	}

	txs = make([]types.Transaction, txCount)
	senders = make([]common.Address, txCount)
	if txCount == 0 {
		return txs, senders, nil
	}
	txnOffset := idxTxnHash.OrdinalLookup(baseTxnID - idxTxnHash.BaseDataID())
	gg := txsSeg.MakeGetter()
	gg.Reset(txnOffset)
	for i := uint32(0); i < txCount; i++ {
		if !gg.HasNext() {
			return nil, nil, nil
		}
		buf, _ = gg.Next(buf[:0])
		if len(buf) < 1+20 {
			return nil, nil, fmt.Errorf("segment %s has too short record: len(buf)=%d < 21", txsSeg.FilePath(), len(buf))
		}
		senders[i].SetBytes(buf[1 : 1+20])
		txRlp := buf[1+20:]
		txs[i], err = types.DecodeTransaction(txRlp)
		if err != nil {
			return nil, nil, err
		}
		txs[i].SetSender(senders[i])
	}

	return txs, senders, nil
}

func (r *BlockReader) txnByID(txnID uint64, sn *Segment, buf []byte) (txn types.Transaction, err error) {
	idxTxnHash := sn.Index(coresnaptype.Indexes.TxnHash)

	offset := idxTxnHash.OrdinalLookup(txnID - idxTxnHash.BaseDataID())
	gg := sn.MakeGetter()
	gg.Reset(offset)
	if !gg.HasNext() {
		return nil, nil
	}
	buf, _ = gg.Next(buf[:0])
	sender, txnRlp := buf[1:1+20], buf[1+20:]

	txn, err = types.DecodeTransaction(txnRlp)
	if err != nil {
		return
	}
	txn.SetSender(*(*common.Address)(sender)) // see: https://tip.golang.org/ref/spec#Conversions_from_slice_to_array_pointer
	return
}

func (r *BlockReader) txnByHash(txnHash common.Hash, segments []*Segment, buf []byte) (types.Transaction, uint64, bool, error) {
	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]

		idxTxnHash := sn.Index(coresnaptype.Indexes.TxnHash)
		idxTxnHash2BlockNum := sn.Index(coresnaptype.Indexes.TxnHash2BlockNum)

		if idxTxnHash == nil || idxTxnHash2BlockNum == nil {
			continue
		}

		reader := recsplit.NewIndexReader(idxTxnHash)
		txnId, ok := reader.Lookup(txnHash[:])
		if !ok {
			continue
		}
		offset := idxTxnHash.OrdinalLookup(txnId)
		gg := sn.MakeGetter()
		gg.Reset(offset)
		// first byte txnHash check - reducing false-positives 256 times. Allows don't store and don't calculate full hash of entity - when checking many snapshots.
		if !gg.MatchPrefix([]byte{txnHash[0]}) {
			continue
		}
		buf, _ = gg.Next(buf[:0])
		senderByte, txnRlp := buf[1:1+20], buf[1+20:]
		sender := (common.Address)(senderByte)

		txn, err := types.DecodeTransaction(txnRlp)
		if err != nil {
			return nil, 0, false, err
		}

		txn.SetSender(sender) // see: https://tip.golang.org/ref/spec#Conversions_from_slice_to_array_pointer

		reader2 := recsplit.NewIndexReader(idxTxnHash2BlockNum)
		blockNum, ok := reader2.Lookup(txnHash[:])
		if !ok {
			continue
		}

		// final txnHash check  - completely avoid false-positives
		if txn.Hash() == txnHash {
			return txn, blockNum, true, nil
		}
	}

	return nil, 0, false, nil
}

// TxnByIdxInBlock - doesn't include system-transactions in the begin/end of block
// return nil if 0 < i < body.txCount
func (r *BlockReader) TxnByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, txIdxInBlock int) (txn types.Transaction, err error) {
	maxBlockNumInFiles := r.sn.BlocksAvailable()
	if maxBlockNumInFiles == 0 || blockNum > maxBlockNumInFiles {
		canonicalHash, ok, err := r.CanonicalHash(ctx, tx, blockNum)
		if err != nil {
			return nil, err
		}
		if !ok {
			return
		}
		return rawdb.TxnByIdxInBlock(tx, canonicalHash, blockNum, txIdxInBlock)
	}

	seg, ok, release := r.sn.ViewSingleFile(coresnaptype.Bodies, blockNum)
	if !ok {
		return
	}
	defer release()

	var b *types.BodyForStorage
	b, _, err = r.bodyForStorageFromSnapshot(blockNum, seg, nil)
	if err != nil {
		return nil, err
	}
	release()
	if b == nil {
		return
	}

	// if block has no transactions, or requested txNum out of non-system transactions length
	if b.TxCount == 2 || txIdxInBlock == -1 || txIdxInBlock >= int(b.TxCount-2) {
		return nil, nil
	}

	txnSeg, ok, release := r.sn.ViewSingleFile(coresnaptype.Transactions, blockNum)
	if !ok {
		return
	}
	defer release()

	// +1 because block has system-txn in the beginning of block
	return r.txnByID(b.BaseTxnID.At(txIdxInBlock), txnSeg, nil)
}

// TxnLookup - find blockNumber and txnID by txnHash
func (r *BlockReader) TxnLookup(_ context.Context, tx kv.Getter, txnHash common.Hash) (uint64, bool, error) {
	n, err := rawdb.ReadTxLookupEntry(tx, txnHash)
	if err != nil {
		return 0, false, err
	}

	if n != nil {
		return *n, true, nil
	}

	txns, release := r.sn.ViewType(coresnaptype.Transactions)
	defer release()
	_, blockNum, ok, err := r.txnByHash(txnHash, txns, nil)
	if err != nil {
		return 0, false, err
	}

	return blockNum, ok, nil
}

func (r *BlockReader) FirstTxnNumNotInSnapshots() uint64 {
	sn, ok, release := r.sn.ViewSingleFile(coresnaptype.Transactions, r.sn.BlocksAvailable())
	if !ok {
		return 0
	}
	defer release()

	lastTxnID := sn.Index(coresnaptype.Indexes.TxnHash).BaseDataID() + uint64(sn.Count())
	return lastTxnID
}

func (r *BlockReader) IterateFrozenBodies(f func(blockNum, baseTxNum, txCount uint64) error) error {
	view := r.sn.View()
	defer view.Close()
	for _, sn := range view.Bodies() {
		sn := sn
		defer sn.EnableReadAhead().DisableReadAhead()

		var buf []byte
		g := sn.MakeGetter()
		blockNum := sn.from
		var b types.BodyForStorage
		for g.HasNext() {
			buf, _ = g.Next(buf[:0])
			if err := rlp.DecodeBytes(buf, &b); err != nil {
				return err
			}
			if err := f(blockNum, b.BaseTxnID.U64(), uint64(b.TxCount)); err != nil {
				return err
			}
			blockNum++
		}
	}
	return nil
}

func (r *BlockReader) IntegrityTxnID(failFast bool) error {
	defer log.Info("[integrity] IntegrityTxnID done")
	view := r.sn.View()
	defer view.Close()

	var expectedFirstTxnID uint64
	for _, snb := range view.Bodies() {
		firstBlockNum := snb.Index().BaseDataID()
		sn, _ := view.TxsSegment(firstBlockNum)
		b, _, err := r.bodyForStorageFromSnapshot(firstBlockNum, snb, nil)
		if err != nil {
			return err
		}
		if b.BaseTxnID.U64() != expectedFirstTxnID {
			err := fmt.Errorf("[integrity] IntegrityTxnID: bn=%d, baseID=%d, cnt=%d, expectedFirstTxnID=%d", firstBlockNum, b.BaseTxnID, sn.Count(), expectedFirstTxnID)
			if failFast {
				return err
			} else {
				log.Error(err.Error())
			}
		}
		expectedFirstTxnID = expectedFirstTxnID + uint64(sn.Count())
	}
	return nil
}

func (r *BlockReader) BadHeaderNumber(ctx context.Context, tx kv.Getter, hash common.Hash) (blockHeight *uint64, err error) {
	return rawdb.ReadBadHeaderNumber(tx, hash)
}
func (r *BlockReader) BlockByNumber(ctx context.Context, db kv.Tx, number uint64) (*types.Block, error) {
	hash := emptyHash
	maxBlockNumInFiles := r.sn.BlocksAvailable()
	if maxBlockNumInFiles == 0 || number > maxBlockNumInFiles {
		var err error
		hash, err = rawdb.ReadCanonicalHash(db, number)
		if err != nil {
			return nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
		}
		if hash == emptyHash {
			return nil, nil
		}
	}
	block, _, err := r.BlockWithSenders(ctx, db, hash, number)
	return block, err
}
func (r *BlockReader) BlockByHash(ctx context.Context, db kv.Tx, hash common.Hash) (*types.Block, error) {
	number, err := r.HeaderNumber(ctx, db, hash)
	if err != nil {
		return nil, fmt.Errorf("failed HeaderNumber: %w", err)
	}
	if number == nil {
		return nil, nil
	}
	block, _, err := r.BlockWithSenders(ctx, db, hash, *number)
	return block, err
}
func (r *BlockReader) CurrentBlock(db kv.Tx) (*types.Block, error) {
	headHash := rawdb.ReadHeadBlockHash(db)
	headNumber, err := r.HeaderNumber(context.Background(), db, headHash)
	if err != nil {
		return nil, fmt.Errorf("failed HeaderNumber: %w", err)
	}
	block, _, err := r.blockWithSenders(context.Background(), db, headHash, *headNumber, true)
	return block, err
}

func (r *BlockReader) RawTransactions(ctx context.Context, tx kv.Getter, fromBlock, toBlock uint64) (txs [][]byte, err error) {
	return rawdb.RawTransactionsRange(tx, fromBlock, toBlock)
}

func (r *BlockReader) ReadAncestor(db kv.Getter, hash common.Hash, number, ancestor uint64, maxNonCanonical *uint64) (common.Hash, uint64) {
	if ancestor > number {
		return common.Hash{}, 0
	}
	if ancestor == 1 {
		header, err := r.Header(context.Background(), db, hash, number)
		if err != nil {
			panic(err)
		}
		// in this case it is cheaper to just read the header
		if header != nil {
			return header.ParentHash, number - 1
		}
		return common.Hash{}, 0
	}
	for ancestor != 0 {
		h, ok, err := r.CanonicalHash(context.Background(), db, number)
		if err != nil {
			panic(err)
		}
		if ok && h == hash {
			ancestorHash, ok1, err := r.CanonicalHash(context.Background(), db, number-ancestor)
			if err != nil {
				panic(err)
			}
			h, ok2, err := r.CanonicalHash(context.Background(), db, number)
			if err != nil {
				panic(err)
			}
			if ok1 && ok2 && h == hash {
				number -= ancestor
				return ancestorHash, number
			}
		}
		if *maxNonCanonical == 0 {
			return common.Hash{}, 0
		}
		*maxNonCanonical--
		ancestor--
		header, err := r.Header(context.Background(), db, hash, number)
		if err != nil {
			panic(err)
		}
		if header == nil {
			return common.Hash{}, 0
		}
		hash = header.ParentHash
		number--
	}
	return hash, number
}

func (r *BlockReader) EventLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, bool, error) {
	n, err := rawdb.ReadBorTxLookupEntry(tx, txnHash)
	if err != nil {
		return 0, false, err
	}
	if n != nil {
		return *n, true, nil
	}

	if r.borSn == nil {
		return 0, false, nil
	}

	segs, release := r.borSn.ViewType(borsnaptype.BorEvents)
	defer release()

	blockNum, ok, err := r.borBlockByEventHash(txnHash, segs, nil)
	if err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil
	}
	return blockNum, true, nil
}

func (r *BlockReader) borBlockByEventHash(txnHash common.Hash, segments []*Segment, buf []byte) (blockNum uint64, ok bool, err error) {
	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		idxBorTxnHash := sn.Index()

		if idxBorTxnHash == nil {
			continue
		}
		if idxBorTxnHash.KeyCount() == 0 {
			continue
		}
		reader := recsplit.NewIndexReader(idxBorTxnHash)
		blockEventId, exists := reader.Lookup(txnHash[:])
		if !exists {
			continue
		}
		offset := idxBorTxnHash.OrdinalLookup(blockEventId)
		gg := sn.MakeGetter()
		gg.Reset(offset)
		if !gg.MatchPrefix(txnHash[:]) {
			continue
		}
		buf, _ = gg.Next(buf[:0])
		blockNum = binary.BigEndian.Uint64(buf[length.Hash:])
		ok = true
		return
	}
	return
}

func (r *BlockReader) BorStartEventID(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) (uint64, error) {
	maxBlockNumInFiles := r.FrozenBorBlocks()
	if maxBlockNumInFiles == 0 || blockHeight > maxBlockNumInFiles {
		v, err := tx.GetOne(kv.BorEventNums, hexutility.EncodeTs(blockHeight))
		if err != nil {
			return 0, err
		}
		if len(v) == 0 {
			return 0, fmt.Errorf("BorStartEventID(%d) not found", blockHeight)
		}
		startEventId := binary.BigEndian.Uint64(v)
		return startEventId, nil
	}

	borTxHash := bortypes.ComputeBorTxHash(blockHeight, hash)

	segments, release := r.borSn.ViewType(borsnaptype.BorEvents)
	defer release()

	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		if sn.from > blockHeight {
			continue
		}
		if sn.to <= blockHeight {
			break
		}

		idxBorTxnHash := sn.Index()

		if idxBorTxnHash == nil {
			continue
		}
		if idxBorTxnHash.KeyCount() == 0 {
			continue
		}
		reader := recsplit.NewIndexReader(idxBorTxnHash)
		blockEventId, found := reader.Lookup(borTxHash[:])
		if !found {
			return 0, fmt.Errorf("borTxHash %x not found in snapshot %s", borTxHash, sn.FilePath())
		}
		return idxBorTxnHash.BaseDataID() + blockEventId, nil
	}
	return 0, nil
}

func (r *BlockReader) EventsByBlock(ctx context.Context, tx kv.Tx, hash common.Hash, blockHeight uint64) ([]rlp.RawValue, error) {
	maxBlockNumInFiles := r.FrozenBorBlocks()
	if tx != nil && (maxBlockNumInFiles == 0 || blockHeight > maxBlockNumInFiles) {
		c, err := tx.Cursor(kv.BorEventNums)
		if err != nil {
			return nil, err
		}
		defer c.Close()
		var k, v []byte
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], blockHeight)
		result := []rlp.RawValue{}
		if k, v, err = c.Seek(buf[:]); err != nil {
			return nil, err
		}
		if !bytes.Equal(k, buf[:]) {
			return result, nil
		}
		endEventId := binary.BigEndian.Uint64(v)
		var startEventId uint64
		if k, v, err = c.Prev(); err != nil {
			return nil, err
		}
		if k == nil {
			startEventId = 1
		} else {
			startEventId = binary.BigEndian.Uint64(v) + 1
		}
		c1, err := tx.Cursor(kv.BorEvents)
		if err != nil {
			return nil, err
		}
		defer c1.Close()
		binary.BigEndian.PutUint64(buf[:], startEventId)
		for k, v, err = c1.Seek(buf[:]); err == nil && k != nil; k, v, err = c1.Next() {
			eventId := binary.BigEndian.Uint64(k)
			if eventId > endEventId {
				break
			}
			result = append(result, common.Copy(v))
		}
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	borTxHash := bortypes.ComputeBorTxHash(blockHeight, hash)

	segments, release := r.borSn.ViewType(borsnaptype.BorEvents)
	defer release()

	var buf []byte
	result := []rlp.RawValue{}
	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		if sn.from > blockHeight {
			continue
		}
		if sn.to <= blockHeight {
			break
		}

		idxBorTxnHash := sn.Index()

		if idxBorTxnHash == nil {
			continue
		}
		if idxBorTxnHash.KeyCount() == 0 {
			continue
		}
		reader := recsplit.NewIndexReader(idxBorTxnHash)
		blockEventId, ok := reader.Lookup(borTxHash[:])
		if !ok {
			continue
		}
		offset := idxBorTxnHash.OrdinalLookup(blockEventId)
		gg := sn.MakeGetter()
		gg.Reset(offset)
		for gg.HasNext() && gg.MatchPrefix(borTxHash[:]) {
			buf, _ = gg.Next(buf[:0])
			result = append(result, rlp.RawValue(common.Copy(buf[length.Hash+length.BlockNum+8:])))
		}
	}
	return result, nil
}

// EventsByIdFromSnapshot returns the list of records limited by time, or the number of records along with a bool value to signify if the records were limited by time
func (r *BlockReader) EventsByIdFromSnapshot(from uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, bool, error) {
	segments, release := r.borSn.ViewType(borsnaptype.BorEvents)
	defer release()

	var buf []byte
	var result []*heimdall.EventRecordWithTime
	maxTime := false

	for _, sn := range segments {
		idxBorTxnHash := sn.Index()

		if idxBorTxnHash == nil || idxBorTxnHash.KeyCount() == 0 {
			continue
		}

		offset := idxBorTxnHash.OrdinalLookup(0)
		gg := sn.MakeGetter()
		gg.Reset(offset)
		for gg.HasNext() {
			buf, _ = gg.Next(buf[:0])

			raw := rlp.RawValue(common.Copy(buf[length.Hash+length.BlockNum+8:]))
			var event heimdall.EventRecordWithTime
			if err := event.UnmarshallBytes(raw); err != nil {
				return nil, false, err
			}

			if event.ID < from {
				continue
			}
			if event.Time.After(to) {
				maxTime = true
				return result, maxTime, nil
			}

			result = append(result, &event)

			if len(result) == limit {
				return result, maxTime, nil
			}
		}
	}

	return result, maxTime, nil
}

func (r *BlockReader) LastEventId(_ context.Context, tx kv.Tx) (uint64, bool, error) {
	cursor, err := tx.Cursor(kv.BorEvents)
	if err != nil {
		return 0, false, err
	}

	defer cursor.Close()
	k, _, err := cursor.Last()
	if err != nil {
		return 0, false, err
	}

	var lastEventId uint64
	var ok bool
	if k != nil {
		lastEventId = binary.BigEndian.Uint64(k)
		ok = true
	}

	snapshotLastEventId := r.LastFrozenEventId()
	if snapshotLastEventId > lastEventId {
		return snapshotLastEventId, true, nil
	}

	return lastEventId, ok, nil
}

func (r *BlockReader) LastFrozenEventId() uint64 {
	if r.borSn == nil {
		return 0
	}

	segments, release := r.borSn.ViewType(borsnaptype.BorEvents)
	defer release()

	if len(segments) == 0 {
		return 0
	}
	// find the last segment which has a built index
	var lastSegment *Segment
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].Index() != nil {
			gg := segments[i].MakeGetter()
			if gg.HasNext() {
				lastSegment = segments[i]
				break
			}
		}
	}
	if lastSegment == nil {
		return 0
	}
	var lastEventID uint64
	gg := lastSegment.MakeGetter()
	var buf []byte
	for gg.HasNext() {
		buf, _ = gg.Next(buf[:0])
		lastEventID = binary.BigEndian.Uint64(buf[length.Hash+length.BlockNum : length.Hash+length.BlockNum+8])
	}
	return lastEventID
}

func (r *BlockReader) LastFrozenEventBlockNum() uint64 {
	if r.borSn == nil {
		return 0
	}

	segments, release := r.borSn.ViewType(borsnaptype.BorEvents)
	defer release()

	if len(segments) == 0 {
		return 0
	}
	// find the last segment which has a built index
	var lastSegment *Segment
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].Index() != nil {
			gg := segments[i].MakeGetter()
			if gg.HasNext() {
				lastSegment = segments[i]
				break
			}
		}
	}
	if lastSegment == nil {
		return 0
	}
	var lastBlockNum uint64
	var buf []byte
	gg := lastSegment.MakeGetter()
	for gg.HasNext() {
		buf, _ = gg.Next(buf[:0])
		lastBlockNum = binary.BigEndian.Uint64(buf[length.Hash : length.Hash+length.BlockNum])
	}

	return lastBlockNum
}

func lastId(ctx context.Context, tx kv.Tx, db string) (uint64, bool, error) {
	var last uint64
	var ok bool

	if tx != nil {
		sCursor, err := tx.Cursor(db)
		if err != nil {
			return 0, false, err
		}

		defer sCursor.Close()
		k, _, err := sCursor.Last()
		if err != nil {
			return 0, false, err
		}

		if k != nil {
			ok = true
			last = binary.BigEndian.Uint64(k)
		}
	}

	return last, ok, nil
}

func (r *BlockReader) LastFrozenSpanId() uint64 {
	if r.borSn == nil {
		return 0
	}

	segments, release := r.borSn.ViewType(borsnaptype.BorSpans)
	defer release()

	if len(segments) == 0 {
		return 0
	}
	// find the last segment which has a built index
	var lastSegment *Segment
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].Index() != nil {
			lastSegment = segments[i]
			break
		}
	}
	if lastSegment == nil {
		return 0
	}

	lastSpanID := heimdall.SpanIdAt(lastSegment.to)
	if lastSpanID > 0 {
		lastSpanID--
	}
	return uint64(lastSpanID)
}

func (r *BlockReader) Span(ctx context.Context, tx kv.Getter, spanId uint64) ([]byte, error) {
	var endBlock uint64
	if spanId > 0 {
		endBlock = heimdall.SpanEndBlockNum(heimdall.SpanId(spanId))
	}
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], spanId)
	maxBlockNumInFiles := r.FrozenBorBlocks()
	if tx != nil && (maxBlockNumInFiles == 0 || endBlock > maxBlockNumInFiles) {
		v, err := tx.GetOne(kv.BorSpans, buf[:])
		if err != nil {
			return nil, err
		}
		if v == nil {
			err := fmt.Errorf("span %d not found (db), frozenBlocks=%d", spanId, maxBlockNumInFiles)
			return nil, fmt.Errorf("%w: %w", ErrSpanNotFound, err)
		}
		return common.Copy(v), nil
	}
	segments, release := r.borSn.ViewType(borsnaptype.BorSpans)
	defer release()

	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		idx := sn.Index()

		if idx == nil {
			continue
		}
		spanFrom := uint64(heimdall.SpanIdAt(sn.from))
		if spanId < spanFrom {
			continue
		}
		spanTo := uint64(heimdall.SpanIdAt(sn.to))
		if spanId >= spanTo {
			continue
		}
		if idx.KeyCount() == 0 {
			continue
		}
		offset := idx.OrdinalLookup(spanId - idx.BaseDataID())
		gg := sn.MakeGetter()
		gg.Reset(offset)
		result, _ := gg.Next(nil)
		return common.Copy(result), nil
	}
	err := fmt.Errorf("span %d not found (snapshots)", spanId)
	return nil, fmt.Errorf("%w: %w", ErrSpanNotFound, err)
}

func (r *BlockReader) LastSpanId(_ context.Context, tx kv.Tx) (uint64, bool, error) {
	var lastSpanId uint64
	var k []byte
	if tx != nil {
		sCursor, err := tx.Cursor(kv.BorSpans)
		if err != nil {
			return 0, false, err
		}

		defer sCursor.Close()
		k, _, err = sCursor.Last()
		if err != nil {
			return 0, false, err
		}

		if k != nil {
			lastSpanId = binary.BigEndian.Uint64(k)
		}
	}

	snapshotLastSpanId := r.LastFrozenSpanId()
	if snapshotLastSpanId > lastSpanId {
		return snapshotLastSpanId, true, nil
	}

	return lastSpanId, k != nil, nil
}

func (r *BlockReader) LastMilestoneId(ctx context.Context, tx kv.Tx) (uint64, bool, error) {
	return lastId(ctx, tx, kv.BorMilestones)
}

func (r *BlockReader) Milestone(ctx context.Context, tx kv.Getter, milestoneId uint64) ([]byte, error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], milestoneId)
	v, err := tx.GetOne(kv.BorMilestones, buf[:])

	if err != nil {
		return nil, err
	}

	if v == nil {
		return nil, fmt.Errorf("%w, id: %d (db)", ErrMilestoneNotFound, milestoneId)
	}

	return common.Copy(v), nil
}

func (r *BlockReader) LastCheckpointId(ctx context.Context, tx kv.Tx) (uint64, bool, error) {
	lastCheckpointId, ok, err := lastId(ctx, tx, kv.BorCheckpoints)

	snapshotLastCheckpointId := r.LastFrozenCheckpointId()

	if snapshotLastCheckpointId > lastCheckpointId {
		return snapshotLastCheckpointId, true, nil
	}

	return lastCheckpointId, ok, err
}

func (r *BlockReader) Checkpoint(ctx context.Context, tx kv.Getter, checkpointId uint64) ([]byte, error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], checkpointId)
	v, err := tx.GetOne(kv.BorCheckpoints, buf[:])

	if err != nil {
		return nil, err
	}

	if v != nil {
		return common.Copy(v), nil
	}

	segments, release := r.borSn.ViewType(borsnaptype.BorCheckpoints)
	defer release()

	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]
		index := sn.Index()

		if index == nil || index.KeyCount() == 0 || checkpointId < index.BaseDataID() {
			continue
		}

		offset := index.OrdinalLookup(checkpointId - index.BaseDataID())
		gg := sn.MakeGetter()
		gg.Reset(offset)
		result, _ := gg.Next(nil)
		return common.Copy(result), nil
	}

	return nil, fmt.Errorf("%w, id: %d (db)", ErrCheckpointNotFound, checkpointId)
}

func (r *BlockReader) LastFrozenCheckpointId() uint64 {
	if r.borSn == nil {
		return 0
	}

	segments, release := r.borSn.ViewType(borsnaptype.BorCheckpoints)
	defer release()
	if len(segments) == 0 {
		return 0
	}
	// find the last segment which has a built index
	var lastSegment *Segment
	for i := len(segments) - 1; i >= 0; i-- {
		if segments[i].Index() != nil {
			lastSegment = segments[i]
			break
		}
	}

	if lastSegment == nil {
		return 0
	}

	index := lastSegment.Index()

	return index.BaseDataID() + index.KeyCount() - 1
}

// ---- Data Integrity part ----

func (r *BlockReader) ensureHeaderNumber(n uint64, seg *Segment) error {
	h, _, err := r.headerFromSnapshot(n, seg, nil)
	if err != nil {
		return err
	}
	if h == nil {
		return fmt.Errorf("ensureHeaderNumber: not found header: %d", n)
	}
	if h.Number.Uint64() != n {
		return fmt.Errorf("ensureHeaderNumber: requested header: %d, got: %d", n, h.Number.Uint64())
	}
	return nil
}

func (r *BlockReader) Integrity(ctx context.Context) error {
	view := r.sn.View()
	defer view.Close()
	for _, seg := range view.Headers() {
		if err := r.ensureHeaderNumber(seg.from, seg); err != nil {
			return err
		}
		if err := r.ensureHeaderNumber(seg.to-1, seg); err != nil {
			return err
		}
	}
	return nil
}

func ReadTxNumFuncFromBlockReader(ctx context.Context, r services.FullBlockReader) rawdbv3.ReadTxNumFunc {
	return func(tx kv.Tx, c kv.Cursor, blockNum uint64) (maxTxNum uint64, ok bool, err error) {
		maxTxNum, ok, err = rawdbv3.DefaultReadTxNumFunc(tx, c, blockNum)
		if err != nil {
			return
		}
		if ok || r == nil {
			return
		}
		b, err := r.CanonicalBodyForStorage(ctx, tx, blockNum)
		if err != nil {
			return 0, false, err
		}
		if b == nil {
			return 0, false, nil
		}
		ret := b.BaseTxnID.U64() + uint64(b.TxCount) - 1
		return ret, true, nil
	}

}
