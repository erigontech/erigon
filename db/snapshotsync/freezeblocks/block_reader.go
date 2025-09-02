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
	"context"
	"fmt"
	"sort"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/recsplit"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/turbo/services"
)

type RemoteBlockReader struct {
	client       remote.ETHBACKENDClient
	txBlockIndex *txBlockIndexWithBlockReader
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
	hash, ok, err := r.CanonicalHash(ctx, db, number)
	if err != nil {
		return nil, fmt.Errorf("failed ReadCanonicalHash: %w", err)
	}
	if !ok {
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
	canonicalHash, ok, err := r.CanonicalHash(ctx, tx, blockHeight)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	block, _, err := r.BlockWithSenders(ctx, tx, canonicalHash, blockHeight)
	if err != nil {
		return nil, err
	}
	return block.Header(), nil
}
func (r *RemoteBlockReader) Snapshots() snapshotsync.BlockSnapshots    { panic("not implemented") }
func (r *RemoteBlockReader) BorSnapshots() snapshotsync.BlockSnapshots { panic("not implemented") }
func (r *RemoteBlockReader) AllTypes() []snaptype.Type                 { panic("not implemented") }
func (r *RemoteBlockReader) FrozenBlocks() uint64                      { panic("not supported") }
func (r *RemoteBlockReader) FrozenBorBlocks(align bool) uint64         { panic("not supported") }
func (r *RemoteBlockReader) FrozenFiles() (list []string)              { panic("not supported") }
func (r *RemoteBlockReader) FreezingCfg() ethconfig.BlocksFreezing     { panic("not supported") }

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

func (r *RemoteBlockReader) CanonicalHash(ctx context.Context, tx kv.Getter, blockHeight uint64) (h common.Hash, ok bool, err error) {
	reply, err := r.client.CanonicalHash(ctx, &remote.CanonicalHashRequest{BlockNumber: blockHeight})
	if err != nil {
		return common.Hash{}, false, err
	}
	if reply == nil || reply.Hash == nil {
		return h, false, nil
	}
	h = gointerfaces.ConvertH256ToHash(reply.Hash)
	return h, h != emptyHash, nil
}

func (r *RemoteBlockReader) BlockForTxNum(ctx context.Context, tx kv.Tx, txnNum uint64) (blockNum uint64, ok bool, err error) {
	reply, err := r.client.BlockForTxNum(ctx, &remote.BlockForTxNumRequest{Txnum: txnNum})
	if err != nil {
		return 0, false, err
	}
	if reply == nil {
		return 0, false, nil
	}
	return reply.BlockNumber, reply.Present, nil
}

var _ services.FullBlockReader = &RemoteBlockReader{}

func NewRemoteBlockReader(client remote.ETHBACKENDClient) *RemoteBlockReader {
	br := &RemoteBlockReader{
		client: client,
	}
	txnumReader := TxBlockIndexFromBlockReader(context.Background(), br).(*txBlockIndexWithBlockReader)
	br.txBlockIndex = txnumReader
	return br
}

func (r *RemoteBlockReader) TxnLookup(ctx context.Context, tx kv.Getter, txnHash common.Hash) (uint64, uint64, bool, error) {
	reply, err := r.client.TxnLookup(ctx, &remote.TxnLookupRequest{TxnHash: gointerfaces.ConvertHashToH256(txnHash)})
	if err != nil {
		return 0, 0, false, err
	}
	if reply == nil {
		return 0, 0, false, nil
	}

	// Not a perfect solution, assumes there are no transactions in block 0 (same check on server side)
	if reply.BlockNumber == 0 && reply.TxNumber == 0 {
		return reply.BlockNumber, reply.TxNumber, false, nil
	}

	return reply.BlockNumber, reply.TxNumber, true, nil
}

func (r *RemoteBlockReader) TxnByIdxInBlock(ctx context.Context, tx kv.Getter, blockNum uint64, i int) (txn types.Transaction, err error) {
	canonicalHash, ok, err := r.CanonicalHash(ctx, tx, blockNum)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
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
	if len(reply.BlockRlp) == 0 {
		// block not found
		return nil, nil, nil
	}

	block = &types.Block{}
	err = rlp.DecodeBytes(reply.BlockRlp, block)
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
	expected, ok, err := r.CanonicalHash(ctx, tx, blockHeight)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
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

func (r *RemoteBlockReader) Ready(ctx context.Context) <-chan error {
	// TODO this should probably check with the remote connection, at
	// the moment it just returns the ctx err to be non blocking
	ch := make(chan error, 1)
	ch <- ctx.Err()
	return ch
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
	err = rlp.DecodeBytes(bdRaw.Body, body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (r *RemoteBlockReader) TxnumReader(ctx context.Context) rawdbv3.TxNumsReader {
	if r == nil {
		// tests
		txnumReader := TxBlockIndexFromBlockReader(ctx, nil).(*txBlockIndexWithBlockReader)
		return rawdbv3.TxNums.WithCustomReadTxNumFunc(txnumReader)
	}
	return rawdbv3.TxNums.WithCustomReadTxNumFunc(r.txBlockIndex.CopyWithContext(ctx))
}

// BlockReader can read blocks from db and snapshots
type BlockReader struct {
	sn           *RoSnapshots
	borSn        *heimdall.RoSnapshots
	txBlockIndex *txBlockIndexWithBlockReader

	//files are immutable: no reorgs, on updates - means no invalidation needed
	headerByNumCache *lru.Cache[uint64, *types.Header]
}

var headerByNumCacheSize = dbg.EnvInt("RPC_HEADER_BY_NUM_LRU", 1_000)

func NewBlockReader(snapshots snapshotsync.BlockSnapshots, borSnapshots snapshotsync.BlockSnapshots) *BlockReader {
	borSn, _ := borSnapshots.(*heimdall.RoSnapshots)
	sn, _ := snapshots.(*RoSnapshots)
	br := &BlockReader{sn: sn, borSn: borSn}
	br.headerByNumCache, _ = lru.New[uint64, *types.Header](headerByNumCacheSize)
	txnumReader := TxBlockIndexFromBlockReader(context.Background(), br).(*txBlockIndexWithBlockReader)
	br.txBlockIndex = txnumReader
	return br
}

func (r *BlockReader) CanPruneTo(currentBlockInDB uint64) uint64 {
	return CanDeleteTo(currentBlockInDB, r.sn.BlocksAvailable())
}
func (r *BlockReader) Snapshots() snapshotsync.BlockSnapshots { return r.sn }
func (r *BlockReader) BorSnapshots() snapshotsync.BlockSnapshots {
	if r.borSn != nil {
		return r.borSn
	}

	return nil
}

func (r *BlockReader) Ready(ctx context.Context) <-chan error {
	if r.borSn == nil {
		return r.sn.Ready(ctx)
	}

	ch := make(chan error)

	go func() {
		if err := <-r.sn.Ready(ctx); err != nil {
			ch <- err
			return
		}

		if r.borSn != nil {
			if err := <-r.borSn.Ready(ctx); err != nil {
				ch <- err
				return
			}
		}
		ch <- nil
	}()

	return ch
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
func (r *BlockReader) FrozenBorBlocks(align bool) uint64 {
	if r.borSn == nil {
		return 0
	}

	frozen := r.borSn.BlocksAvailable()

	if !align {
		return frozen
	}

	for _, t := range r.borSn.Types() {

		available := r.borSn.VisibleBlocksAvailable(t.Enum())

		if available < frozen {
			frozen = available
		}
	}

	return frozen
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
		dbgPrefix = fmt.Sprintf("[dbg] BlockReader(idxMax=%d,segMax=%d).HeaderByNumber(blk=%d) -> ", r.sn.IndicesMax(), r.sn.SegmentsMax(), blockHeight)
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

	seg, ok, release := r.sn.ViewSingleFile(snaptype2.Headers, blockHeight)
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

	segmentRotx := r.sn.ViewType(snaptype2.Headers)
	defer segmentRotx.Close()

	buf := make([]byte, 128)
	segments := segmentRotx.Segments
	for i := len(segments) - 1; i >= 0; i-- {
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

	seg, ok, release := r.sn.ViewSingleFile(snaptype2.Headers, blockHeight)
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

	seg, ok, release := r.sn.ViewSingleFile(snaptype2.Headers, blockHeight)
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
		dbgPrefix = fmt.Sprintf("[dbg] BlockReader(idxMax=%d,segMax=%d).BodyWithTransactions(hash=%x,blk=%d) -> ", r.sn.IndicesMax(), r.sn.SegmentsMax(), hash, blockHeight)
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

	seg, ok, release := r.sn.ViewSingleFile(snaptype2.Bodies, blockHeight)
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

	txnSeg, ok, release := r.sn.ViewSingleFile(snaptype2.Transactions, blockHeight)
	if !ok {
		if dbgLogs {
			log.Info(dbgPrefix+"no transactions file for this block num", "r.sn.BlocksAvailable()", r.sn.BlocksAvailable(), "r.sn.idxMax", r.sn.IndicesMax(), "r.sn.segmetntsMax", r.sn.SegmentsMax())
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

	seg, ok, release := r.sn.ViewSingleFile(snaptype2.Bodies, blockHeight)
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
	bodySeg, ok, release := r.sn.ViewSingleFile(snaptype2.Bodies, blockNum)
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
		dbgPrefix = fmt.Sprintf("[dbg] BlockReader(idxMax=%d,segMax=%d).blockWithSenders(hash=%x,blk=%d) -> ", r.sn.IndicesMax(), r.sn.SegmentsMax(), hash, blockHeight)
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

	seg, ok, release := r.sn.ViewSingleFile(snaptype2.Headers, blockHeight)
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
	bodySeg, ok, release := r.sn.ViewSingleFile(snaptype2.Bodies, blockHeight)
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

	var txs []types.Transaction
	if txCount != 0 {
		txnSeg, ok, release := r.sn.ViewSingleFile(snaptype2.Transactions, blockHeight)
		if !ok {
			err = fmt.Errorf("no transactions snapshot file for blockNum=%d, BlocksAvailable=%d", blockHeight, r.sn.BlocksAvailable())
			return nil, nil, err
		}
		defer release()
		txs, senders, err = r.txsFromSnapshot(baseTxnId, txCount, txnSeg, buf)
		if err != nil {
			return nil, nil, err
		}
		release()
	}

	if h.WithdrawalsHash == nil && len(b.Withdrawals) == 0 {
		// Hack for Issue 12297
		// Apparently some snapshots have pre-Shapella blocks with empty rather than nil withdrawals
		b.Withdrawals = nil
	}
	block = types.NewBlockFromStorage(hash, h, txs, b.Uncles, b.Withdrawals)
	if len(senders) != block.Transactions().Len() {
		if dbgLogs {
			log.Info(dbgPrefix + fmt.Sprintf("found block with %d transactions, but %d senders", block.Transactions().Len(), len(senders)))
		}
		return block, senders, nil // no senders is fine - will recover them on the fly
	}
	block.SendersToTxs(senders)
	return block, senders, nil
}

func (r *BlockReader) headerFromSnapshot(blockHeight uint64, sn *snapshotsync.VisibleSegment, buf []byte) (*types.Header, []byte, error) {
	if h, ok := r.headerByNumCache.Get(blockHeight); ok {
		return h, buf, nil
	}

	index := sn.Src().Index()
	if index == nil {
		return nil, buf, nil
	}
	headerOffset := index.OrdinalLookup(blockHeight - index.BaseDataID())
	gg := sn.Src().MakeGetter()
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

	r.headerByNumCache.Add(blockHeight, h)
	return h, buf, nil
}

// headerFromSnapshotByHash - getting header by hash AND ensure that it has correct hash
// because HeaderByHash method will search header in all snapshots - and may request header which doesn't exists
// but because our indices are based on PerfectHashMap, no way to know is given key exists or not, only way -
// to make sure is to fetch it and compare hash
func (r *BlockReader) headerFromSnapshotByHash(hash common.Hash, sn *snapshotsync.VisibleSegment, buf []byte) (*types.Header, error) {
	defer func() {
		if rec := recover(); rec != nil {
			fname := "src=nil"
			if sn.Src() != nil {
				fname = sn.Src().FileName()
			}
			panic(fmt.Errorf("%+v, snapshot: %d-%d fname: %s, trace: %s", rec, sn.From(), sn.To(), fname, dbg.Stack()))
		}
	}() // avoid crash because Erigon's core does many things

	index := sn.Src().Index()

	if index == nil {
		return nil, nil
	}

	reader := recsplit.NewIndexReader(index)
	headerOffset, ok := reader.TwoLayerLookup(hash[:])
	if !ok {
		return nil, nil
	}

	gg := sn.Src().MakeGetter()
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

func (r *BlockReader) bodyFromSnapshot(blockHeight uint64, sn *snapshotsync.VisibleSegment, buf []byte) (*types.Body, uint64, uint32, []byte, error) {
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
	var txCount uint32
	if b.TxCount >= 2 {
		txCount = b.TxCount - 2
	}
	return body, b.BaseTxnID.First(), txCount, buf, nil // empty txs in the beginning and end of block
}

func (r *BlockReader) bodyForStorageFromSnapshot(blockHeight uint64, sn *snapshotsync.VisibleSegment, buf []byte) (*types.BodyForStorage, []byte, error) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Errorf("%+v, snapshot: %d-%d, trace: %s", rec, sn.From(), sn.To(), dbg.Stack()))
		}
	}() // avoid crash because Erigon's core does many things

	return BodyForStorageFromSnapshot(blockHeight, sn, buf)
}

func BodyForTxnFromSnapshot(blockHeight uint64, sn *snapshotsync.VisibleSegment, buf []byte) (*types.BodyOnlyTxn, []byte, error) {
	index := sn.Src().Index()

	if index == nil {
		return nil, buf, nil
	}

	bodyOffset := index.OrdinalLookup(blockHeight - index.BaseDataID())

	gg := sn.Src().MakeGetter()
	gg.Reset(bodyOffset)
	if !gg.HasNext() {
		return nil, buf, nil
	}
	buf, _ = gg.Next(buf[:0])
	if len(buf) == 0 {
		return nil, buf, nil
	}
	b := &types.BodyOnlyTxn{}
	if err := rlp.DecodeBytesPartial(buf, b); err != nil {
		return nil, buf, err
	}

	return b, buf, nil
}

func BodyForStorageFromSnapshot(blockHeight uint64, sn *snapshotsync.VisibleSegment, buf []byte) (*types.BodyForStorage, []byte, error) {
	index := sn.Src().Index()

	if index == nil {
		return nil, buf, nil
	}

	bodyOffset := index.OrdinalLookup(blockHeight - index.BaseDataID())

	gg := sn.Src().MakeGetter()
	gg.Reset(bodyOffset)
	if !gg.HasNext() {
		return nil, buf, nil
	}
	buf, _ = gg.Next(buf[:0])
	if len(buf) == 0 {
		return nil, buf, nil
	}
	b := &types.BodyForStorage{}
	if err := rlp.DecodeBytes(buf, b); err != nil {
		return nil, buf, err
	}

	return b, buf, nil
}

func (r *BlockReader) txsFromSnapshot(baseTxnID uint64, txCount uint32, txsSeg *snapshotsync.VisibleSegment, buf []byte) (txs []types.Transaction, senders []common.Address, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			panic(fmt.Errorf("%+v, snapshot: %d-%d, trace: %s", rec, txsSeg.From(), txsSeg.To(), dbg.Stack()))
		}
	}() // avoid crash because Erigon's core does many things

	idxTxnHash := txsSeg.Src().Index(snaptype2.Indexes.TxnHash)

	if idxTxnHash == nil {
		return nil, nil, nil
	}
	if baseTxnID < idxTxnHash.BaseDataID() {
		return nil, nil, fmt.Errorf(".idx file has wrong baseDataID? %d<%d, %s", baseTxnID, idxTxnHash.BaseDataID(), txsSeg.Src().FileName())
	}

	txs = make([]types.Transaction, txCount)
	senders = make([]common.Address, txCount)
	if txCount == 0 {
		return txs, senders, nil
	}
	txnOffset := idxTxnHash.OrdinalLookup(baseTxnID - idxTxnHash.BaseDataID())
	if txsSeg.Src() == nil {
		return nil, nil, nil
	}
	gg := txsSeg.Src().MakeGetter()
	gg.Reset(txnOffset)
	for i := uint32(0); i < txCount; i++ {
		if !gg.HasNext() {
			return nil, nil, nil
		}
		buf, _ = gg.Next(buf[:0])
		if len(buf) < 1+20 {
			return nil, nil, fmt.Errorf("segment %s has too short record: len(buf)=%d < 21", txsSeg.Src().FileName(), len(buf))
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

func (r *BlockReader) txnByID(txnID uint64, sn *snapshotsync.VisibleSegment, buf []byte) (txn types.Transaction, err error) {
	idxTxnHash := sn.Src().Index(snaptype2.Indexes.TxnHash)

	offset := idxTxnHash.OrdinalLookup(txnID - idxTxnHash.BaseDataID())
	gg := sn.Src().MakeGetter()
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

func (r *BlockReader) txnByHash(txnHash common.Hash, segments []*snapshotsync.VisibleSegment, buf []byte) (types.Transaction, uint64, uint64, bool, error) {
	for i := len(segments) - 1; i >= 0; i-- {
		sn := segments[i]

		idxTxnHash := sn.Src().Index(snaptype2.Indexes.TxnHash)
		idxTxnHash2BlockNum := sn.Src().Index(snaptype2.Indexes.TxnHash2BlockNum)

		if idxTxnHash == nil || idxTxnHash2BlockNum == nil {
			continue
		}

		reader := recsplit.NewIndexReader(idxTxnHash)
		txNumInFile, ok := reader.Lookup(txnHash[:])
		if !ok {
			continue
		}
		offset := idxTxnHash.OrdinalLookup(txNumInFile)
		gg := sn.Src().MakeGetter()
		gg.Reset(offset)
		// first byte txnHash check - reducing false-positives 256 times. Allows don't store and don't calculate full hash of entity - when checking many snapshots.
		if !gg.MatchPrefix([]byte{txnHash[0]}) {
			continue
		}
		buf, _ = gg.Next(buf[:0])
		sender, txnRlp := buf[1:1+20], buf[1+20:]

		txn, err := types.DecodeTransaction(txnRlp)
		if err != nil {
			return nil, 0, 0, false, err
		}

		txn.SetSender((common.Address)(sender)) // see: https://tip.golang.org/ref/spec#Conversions_from_slice_to_array_pointer

		reader2 := recsplit.NewIndexReader(idxTxnHash2BlockNum)
		blockNum, ok := reader2.Lookup(txnHash[:])
		if !ok {
			continue
		}

		// final txnHash check  - completely avoid false-positives
		if txn.Hash() == txnHash {
			return txn, blockNum, idxTxnHash.BaseDataID() + txNumInFile, true, nil
		}
	}

	return nil, 0, 0, false, nil
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
			return nil, nil
		}
		return rawdb.TxnByIdxInBlock(tx, canonicalHash, blockNum, txIdxInBlock)
	}

	seg, ok, release := r.sn.ViewSingleFile(snaptype2.Bodies, blockNum)
	if !ok {
		return
	}
	defer release()

	var b *types.BodyOnlyTxn
	b, _, err = BodyForTxnFromSnapshot(blockNum, seg, nil)
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

	txnSeg, ok, release := r.sn.ViewSingleFile(snaptype2.Transactions, blockNum)
	if !ok {
		return
	}
	defer release()

	// +1 because block has system-txn in the beginning of block
	return r.txnByID(b.BaseTxnID.At(txIdxInBlock), txnSeg, nil)
}

// TxnLookup - find blockNumber and txnID by txnHash
func (r *BlockReader) TxnLookup(_ context.Context, tx kv.Getter, txnHash common.Hash) (blockNum uint64, txNum uint64, ok bool, err error) {
	blockNumPointer, txNumPointer, err := rawdb.ReadTxLookupEntry(tx, txnHash)
	if err != nil {
		return 0, 0, false, err
	}

	if blockNumPointer != nil && txNumPointer != nil {
		return *blockNumPointer, *txNumPointer, true, nil
	}

	txns := r.sn.ViewType(snaptype2.Transactions)
	defer txns.Close()
	_, blockNum, txNum, ok, err = r.txnByHash(txnHash, txns.Segments, nil)
	if err != nil {
		return 0, 0, false, err
	}
	return blockNum, txNum, ok, nil
}

func (r *BlockReader) FirstTxnNumNotInSnapshots() uint64 {
	sn, ok, close := r.sn.ViewSingleFile(snaptype2.Transactions, r.sn.BlocksAvailable())
	if !ok {
		return 0
	}
	defer close()

	lastTxnID := sn.Src().Index(snaptype2.Indexes.TxnHash).BaseDataID() + uint64(sn.Src().Count())
	return lastTxnID
}

func (r *BlockReader) IterateFrozenBodies(f func(blockNum, baseTxNum, txCount uint64) error) error {
	view := r.sn.View()
	defer view.Close()
	for _, sn := range view.Bodies() {
		sn := sn
		defer sn.Src().MadvSequential().DisableReadAhead()

		var buf []byte
		g := sn.Src().MakeGetter()
		blockNum := sn.From()
		var b types.BodyOnlyTxn
		for g.HasNext() {
			buf, _ = g.Next(buf[:0])
			if err := rlp.DecodeBytesPartial(buf, &b); err != nil {
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
	defer log.Info("[integrity] BlocksTxnID done")
	view := r.sn.View()
	defer view.Close()

	var expectedFirstTxnID uint64
	for _, snb := range view.Bodies() {
		if snb.Src() == nil {
			continue
		}
		firstBlockNum := snb.Src().Index().BaseDataID()
		sn, _ := view.TxsSegment(firstBlockNum)
		b, _, err := BodyForTxnFromSnapshot(firstBlockNum, snb, nil)
		if err != nil {
			return err
		}
		if b.BaseTxnID.U64() != expectedFirstTxnID {
			err := fmt.Errorf("[integrity] BlocksTxnID: bn=%d, baseID=%d, cnt=%d, expectedFirstTxnID=%d", firstBlockNum, b.BaseTxnID, sn.Src().Count(), expectedFirstTxnID)
			if failFast {
				return err
			} else {
				log.Error(err.Error())
			}
		}
		expectedFirstTxnID = expectedFirstTxnID + uint64(sn.Src().Count())
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

// ---- Data Integrity part ----

func (r *BlockReader) ensureHeaderNumber(n uint64, seg *snapshotsync.VisibleSegment) error {
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
		if err := r.ensureHeaderNumber(seg.From(), seg); err != nil {
			return err
		}
		if err := r.ensureHeaderNumber(seg.To()-1, seg); err != nil {
			return err
		}
	}
	return nil
}

func (r *BlockReader) TxnumReader(ctx context.Context) rawdbv3.TxNumsReader {
	if r == nil {
		// tests
		txnumReader := TxBlockIndexFromBlockReader(ctx, nil).(*txBlockIndexWithBlockReader)
		return rawdbv3.TxNums.WithCustomReadTxNumFunc(txnumReader)
	}
	return rawdbv3.TxNums.WithCustomReadTxNumFunc(r.txBlockIndex.CopyWithContext(ctx))
}

func (r *BlockReader) BlockForTxNum(ctx context.Context, tx kv.Tx, txnNum uint64) (blockNum uint64, ok bool, err error) {
	return r.TxnumReader(ctx).FindBlockNum(tx, txnNum)
}

func TxBlockIndexFromBlockReader(ctx context.Context, r services.FullBlockReader) rawdbv3.TxBlockIndex {
	return &txBlockIndexWithBlockReader{
		r:     r,
		ctx:   ctx,
		cache: NewBlockTxNumLookupCache(20),
	}
}

type txBlockIndexWithBlockReader struct {
	r     services.FullBlockReader
	ctx   context.Context
	cache *BlockTxNumLookupCache
}

func (t *txBlockIndexWithBlockReader) CopyWithContext(ctx context.Context) *txBlockIndexWithBlockReader {
	return &txBlockIndexWithBlockReader{
		r:     t.r,
		ctx:   ctx,
		cache: t.cache,
	}
}

func (t *txBlockIndexWithBlockReader) MaxTxNum(tx kv.Tx, c kv.Cursor, blockNum uint64) (maxTxNum uint64, ok bool, err error) {
	maxTxNum, ok, err = rawdbv3.DefaultTxBlockIndexInstance.MaxTxNum(tx, c, blockNum)
	if err != nil {
		return
	}
	r := t.r
	if ok || r == nil {
		return
	}
	b, err := r.CanonicalBodyForStorage(t.ctx, tx, blockNum)
	if err != nil {
		return 0, false, err
	}
	if b == nil {
		return 0, false, nil
	}
	ret := b.BaseTxnID.U64() + uint64(b.TxCount) - 1
	return ret, true, nil
}

func (t *txBlockIndexWithBlockReader) BlockNumber(tx kv.Tx, txNum uint64) (blockNum uint64, ok bool, err error) {
	if _, ok := t.r.(*BlockReader); !ok {
		return t.r.BlockForTxNum(t.ctx, tx, txNum)
	}
	var buf []byte
	var b *types.BodyOnlyTxn
	view := t.r.Snapshots().(*RoSnapshots).View()
	defer view.Close()

	getMaxTxNum := func(seg *snapshotsync.VisibleSegment) GetMaxTxNum {
		return func(i uint64) (uint64, error) {
			b, buf, err = BodyForTxnFromSnapshot(i, seg, buf)
			if err != nil {
				return 0, err
			}
			if b == nil {
				return 0, fmt.Errorf("BlockReader.BlockNumber: empty block: %d in file bodies.seg(%d-%d)", i, seg.From(), seg.To())
			}
			return b.BaseTxnID.U64() + uint64(b.TxCount) - 1, nil
		}
	}

	// find the file
	bodies := view.Bodies()
	cache := t.cache
	var maxTxNum uint64

	blockIndex := sort.Search(len(bodies), func(i int) bool {
		if err != nil {
			return true
		}
		seg := bodies[i]
		ran := seg.Range
		maxTxNum, err = cache.GetLastMaxTxNum(ran, getMaxTxNum(seg))
		if err != nil {
			return true
		}
		return maxTxNum >= txNum
	})

	if err != nil {
		return 0, false, err
	}

	if blockIndex == len(bodies) {
		// not in snapshots
		blockNum, ok, err = rawdbv3.DefaultTxBlockIndexInstance.BlockNumber(tx, txNum)
		if err != nil {
			return
		}
		r := t.r
		if ok || r == nil {
			return
		}

		return 0, false, nil
	}

	seg := bodies[blockIndex]
	ran := seg.Range

	blkNum, err := cache.Find(ran, txNum, getMaxTxNum(seg))
	if err != nil {
		return 0, false, err
	}

	if blkNum >= ran.To() {
		return 0, false, fmt.Errorf("BlockReader.BlockNumber: not found block: %d in file (%d-%d), searching for %d", blkNum, ran.From(), ran.To(), txNum)
	}

	return blkNum, true, nil
}
