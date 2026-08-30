// Copyright 2026 The Erigon Authors
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

package payloadoptimizer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/protocol/misc"
	protocolparams "github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/txnprovider"
)

var (
	ErrSessionClosed            = errors.New("payload optimizer session is closed")
	ErrBackendBusy              = errors.New("payload optimizer backend is busy")
	ErrUnknownPayload           = errors.New("payload optimizer backend no longer has the payload")
	ErrPayloadNotReady          = errors.New("payload optimizer backend returned no payload")
	ErrCandidateContextMismatch = errors.New("payload optimizer candidate does not match its build context")
)

type Backend interface {
	AssembleBlock(context.Context, *builder.Parameters) (execmodule.AssembleBlockResult, error)
	GetAssembledBlock(context.Context, uint64) (execmodule.AssembledBlockResult, error)
	DiscardAssembledBlock(uint64)
}

type PayloadOptimizer struct {
	backend Backend
}

func New(backend Backend) *PayloadOptimizer {
	return &PayloadOptimizer{backend: backend}
}

func (o *PayloadOptimizer) Open(ctx context.Context, buildContext BuildContext) (*Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if o == nil || isNilInterface(o.backend) {
		return nil, errors.New("payload optimizer requires a backend")
	}
	if buildContext.params == nil {
		return nil, errors.New("payload optimizer requires a valid build context")
	}
	sessionCtx, cancel := context.WithCancel(ctx)
	return &Session{
		backend:      o.backend,
		buildContext: buildContext.clone(),
		ctx:          sessionCtx,
		cancel:       cancel,
		applyPermit:  makeApplyPermit(),
	}, nil
}

func makeApplyPermit() chan struct{} {
	permit := make(chan struct{}, 1)
	permit <- struct{}{}
	return permit
}

type OrderflowUpdate struct {
	transactions types.Transactions
}

func NewOrderflowUpdate(transactions types.Transactions) (update OrderflowUpdate, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			update = OrderflowUpdate{}
			err = fmt.Errorf("copy orderflow transactions: %v", recovered)
		}
	}()
	if slices.ContainsFunc(transactions, isNilTransaction) {
		return OrderflowUpdate{}, errors.New("payload optimizer orderflow contains a nil transaction")
	}
	for i, transaction := range transactions {
		if transaction.Type() != types.BlobTxType {
			continue
		}
		wrapper, ok := transaction.(*types.BlobTxWrapper)
		if !ok {
			return OrderflowUpdate{}, fmt.Errorf("payload optimizer orderflow blob transaction %d has no sidecar", i)
		}
		if err := wrapper.ValidateBlobTransactionWrapper(); err != nil {
			return OrderflowUpdate{}, fmt.Errorf("payload optimizer orderflow blob transaction %d: %w", i, err)
		}
	}
	if _, err := types.MarshalTransactionsBinary(transactions); err != nil {
		return OrderflowUpdate{}, fmt.Errorf("copy orderflow transactions: %w", err)
	}
	return OrderflowUpdate{transactions: copyTransactions(transactions)}, nil
}

func isNilTransaction(transaction types.Transaction) bool {
	return isNilInterface(transaction)
}

func isNilInterface(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func (u OrderflowUpdate) Transactions() types.Transactions {
	return copyTransactions(u.transactions)
}

func copyTransactions(transactions types.Transactions) types.Transactions {
	owned := types.CopyTxs(transactions)
	for i, transaction := range transactions {
		if sender, ok := transaction.GetSender(); ok {
			owned[i].SetSender(sender)
		}
	}
	return owned
}

type Candidate struct {
	buildContext BuildContext
	block        *types.BlockWithReceipts
	value        uint256.Int
}

func (c *Candidate) Context() BuildContext {
	if c == nil {
		return BuildContext{}
	}
	return c.buildContext.clone()
}

func (c *Candidate) Block() *types.BlockWithReceipts {
	if c == nil {
		return nil
	}
	return copyBlockWithReceipts(c.block)
}

func (c *Candidate) Value() *uint256.Int {
	if c == nil {
		return nil
	}
	return new(uint256.Int).Set(&c.value)
}

type Session struct {
	mu sync.RWMutex

	backend      Backend
	buildContext BuildContext
	ctx          context.Context
	cancel       context.CancelFunc
	applyPermit  chan struct{}
	closed       bool
	best         *Candidate
}

const (
	maxBusyAttempts  = 6
	initialBusyDelay = time.Millisecond
	maxBusyDelay     = 8 * time.Millisecond
)

func (s *Session) Apply(ctx context.Context, update OrderflowUpdate) (*Candidate, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.sessionError(); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.ctx.Done():
		return nil, s.sessionError()
	case <-s.applyPermit:
	}
	defer func() { s.applyPermit <- struct{}{} }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.sessionError(); err != nil {
		return nil, err
	}
	applyCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer func() {
		stop()
		cancel()
	}()

	params := s.buildContext.Parameters()
	params.CustomTxnProvider = &updateProvider{transactions: update.Transactions()}
	assembled, err := s.backend.AssembleBlock(applyCtx, params)
	if assembled.PayloadID != 0 {
		defer s.backend.DiscardAssembledBlock(assembled.PayloadID)
	}
	if err != nil {
		return nil, fmt.Errorf("start cold payload build: %w", err)
	}
	if assembled.Busy {
		if err := applyCtx.Err(); err != nil {
			return nil, err
		}
		return nil, ErrBackendBusy
	}
	if err := applyCtx.Err(); err != nil {
		return nil, err
	}

	var result execmodule.AssembledBlockResult
	busyDelay := initialBusyDelay
	for attempt := 0; ; attempt++ {
		result, err = s.backend.GetAssembledBlock(applyCtx, assembled.PayloadID)
		if err != nil {
			return nil, fmt.Errorf("collect cold payload build: %w", err)
		}
		if err := applyCtx.Err(); err != nil {
			return nil, err
		}
		if !result.Busy {
			break
		}
		if attempt+1 >= maxBusyAttempts {
			return nil, ErrBackendBusy
		}
		timer := time.NewTimer(busyDelay)
		select {
		case <-applyCtx.Done():
			timer.Stop()
			return nil, applyCtx.Err()
		case <-timer.C:
		}
		busyDelay = min(busyDelay*2, maxBusyDelay)
	}
	if result.Unknown {
		return nil, ErrUnknownPayload
	}
	if result.Block == nil {
		return nil, ErrPayloadNotReady
	}
	if err := validateCandidate(s.buildContext, result); err != nil {
		return nil, err
	}
	candidate, err := newCandidate(s.buildContext, result)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}
	if err := applyCtx.Err(); err != nil {
		return nil, err
	}
	if s.closed {
		return nil, ErrSessionClosed
	}
	if s.best != nil && candidate.value.Cmp(&s.best.value) <= 0 {
		return nil, nil
	}
	s.best = candidate
	return candidate.copy(), nil
}

func (s *Session) sessionError() error {
	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return ErrSessionClosed
	}
	return s.ctx.Err()
}

func validateCandidate(buildContext BuildContext, result execmodule.AssembledBlockResult) (err error) {
	mismatch := func(field string) error {
		return fmt.Errorf("%w: %s", ErrCandidateContextMismatch, field)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			err = mismatch(fmt.Sprintf("malformed result: %v", recovered))
		}
	}()
	if result.Block == nil || result.Block.Block == nil || result.Block.Block.HeaderNoCopy() == nil {
		return ErrPayloadNotReady
	}
	if result.BlockValue == nil {
		return mismatch("block value")
	}
	params := buildContext.params
	headerView := result.Block.Block.HeaderNoCopy()
	if headerView.GasLimit < protocolparams.MinBlockGasLimit || headerView.GasLimit > protocolparams.MaxBlockGasLimit {
		return mismatch("gas limit bounds")
	}
	if uint64(len(headerView.Extra)) > protocolparams.MaximumExtraDataSize {
		return mismatch("extra data bounds")
	}
	header := result.Block.Block.Header()
	if header.ParentHash != params.ParentHash {
		return mismatch("parent hash")
	}
	if header.Time != params.Timestamp {
		return mismatch("timestamp")
	}
	if header.MixDigest != params.PrevRandao {
		return mismatch("prev randao")
	}
	if header.Coinbase != params.SuggestedFeeRecipient {
		return mismatch("fee recipient")
	}
	if !reflect.DeepEqual(header.ParentBeaconBlockRoot, params.ParentBeaconBlockRoot) {
		return mismatch("parent beacon block root")
	}
	if !reflect.DeepEqual(header.SlotNumber, params.SlotNumber) {
		return mismatch("slot number")
	}
	if params.TargetGasLimit != nil && !misc.IsGasLimitTargetCompatible(buildContext.parentGasLimit, header.GasLimit, *params.TargetGasLimit) {
		return mismatch("target gas limit")
	}
	if params.TargetGasLimit == nil && header.GasLimit != buildContext.parentGasLimit {
		return mismatch("target gas limit")
	}
	if !reflect.DeepEqual(header.Extra, params.ExtraData) {
		return mismatch("extra data")
	}
	transactions := result.Block.Block.Transactions()
	if slices.ContainsFunc(transactions, isNilTransaction) {
		return mismatch("nil transaction")
	}
	var blobCount uint64
	for _, transaction := range transactions {
		if transaction.Type() == types.BlobTxType {
			if _, ok := transaction.(*types.BlobTxWrapper); !ok {
				return mismatch("blob transaction sidecar")
			}
		}
		transactionBlobs := uint64(len(transaction.GetBlobHashes()))
		if transactionBlobs > ^uint64(0)-blobCount {
			return mismatch("blob count")
		}
		blobCount += transactionBlobs
	}
	if params.MaxBlobsPerBlock == nil || blobCount > *params.MaxBlobsPerBlock {
		return mismatch("blob count")
	}
	if blobCount > ^uint64(0)/protocolparams.GasPerBlob {
		return mismatch("blob gas")
	}
	expectedBlobGas := blobCount * protocolparams.GasPerBlob
	if blobCount == 0 {
		if header.BlobGasUsed != nil && *header.BlobGasUsed != 0 {
			return mismatch("blob gas")
		}
	} else if header.BlobGasUsed == nil || *header.BlobGasUsed != expectedBlobGas {
		return mismatch("blob gas")
	}
	for _, transaction := range transactions {
		wrapper, ok := transaction.(*types.BlobTxWrapper)
		if !ok {
			continue
		}
		if err := wrapper.ValidateBlobTransactionWrapper(); err != nil {
			return mismatch("blob transaction sidecar: " + err.Error())
		}
	}
	for _, uncle := range result.Block.Block.Uncles() {
		if uncle == nil {
			return mismatch("nil uncle")
		}
	}
	withdrawals := result.Block.Block.Withdrawals()
	for _, withdrawal := range withdrawals {
		if withdrawal == nil {
			return mismatch("nil withdrawal")
		}
	}
	if !reflect.DeepEqual(withdrawals, types.Withdrawals(params.Withdrawals)) {
		return mismatch("withdrawals")
	}
	if len(result.Block.Receipts) != len(transactions) {
		return mismatch("receipt cardinality")
	}
	for _, receipt := range result.Block.Receipts {
		if receipt == nil {
			return mismatch("nil receipt")
		}
		if receipt.BlockNumber == nil {
			return mismatch("receipt block number")
		}
		for _, log := range receipt.Logs {
			if log == nil {
				return mismatch("nil receipt log")
			}
		}
	}
	if types.DeriveSha(result.Block.Receipts) != header.ReceiptHash {
		return mismatch("receipt root")
	}
	if body := result.Block.Block.Body(); body.MatchesHeader(header) != nil {
		return mismatch("block body roots")
	}
	if !reflect.DeepEqual(result.Block.Requests.Hash(), header.RequestsHash) {
		return mismatch("execution requests hash")
	}
	if buildContext.executionRequests != nil && !reflect.DeepEqual(result.Block.Requests, buildContext.executionRequests) {
		return mismatch("execution requests")
	}
	if err := validateBlockAccessList(result.Block, header); err != nil {
		return mismatch("block access list: " + err.Error())
	}
	return nil
}

func validateBlockAccessList(result *types.BlockWithReceipts, header *types.Header) error {
	if err := result.BlockAccessList.ValidateForBlock(header.GasLimit); err != nil {
		return err
	}
	sidecar := result.Block.BlockAccessListSidecar()
	if header.BlockAccessListHash == nil {
		if sidecar != nil {
			return errors.New("sidecar without header hash")
		}
		return nil
	}
	if sidecar == nil {
		return errors.New("header hash without sidecar")
	}
	if err := sidecar.ValidateForBlock(header.GasLimit); err != nil {
		return err
	}
	hash, err := sidecar.Hash()
	if err != nil {
		return err
	}
	if hash != *header.BlockAccessListHash {
		return errors.New("sidecar hash mismatch")
	}
	if !reflect.DeepEqual(sidecar.BlockAccessList(), result.BlockAccessList) {
		return errors.New("sidecar representation mismatch")
	}
	return nil
}

func (s *Session) Best() (*Candidate, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.ctx.Err() != nil || s.best == nil {
		return nil, false
	}
	return s.best.copy(), true
}

func (s *Session) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		s.best = nil
		s.cancel()
	}
	s.mu.Unlock()
	<-s.applyPermit
	s.applyPermit <- struct{}{}
	return nil
}

func newCandidate(buildContext BuildContext, result execmodule.AssembledBlockResult) (candidate *Candidate, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			candidate = nil
			err = fmt.Errorf("%w: copy malformed result: %v", ErrCandidateContextMismatch, recovered)
		}
	}()
	candidate = &Candidate{
		buildContext: buildContext.clone(),
		block:        copyBlockWithReceipts(result.Block),
	}
	candidate.value.Set(result.BlockValue)
	return candidate, nil
}

func (c *Candidate) copy() *Candidate {
	if c == nil {
		return nil
	}
	copy := &Candidate{
		buildContext: c.buildContext.clone(),
		block:        copyBlockWithReceipts(c.block),
	}
	copy.value.Set(&c.value)
	return copy
}

func copyBlockWithReceipts(block *types.BlockWithReceipts) *types.BlockWithReceipts {
	if block == nil {
		return nil
	}
	return &types.BlockWithReceipts{
		Block:           block.Block.Copy(),
		Receipts:        block.Receipts.Copy(),
		Requests:        copyRequests(block.Requests),
		BlockAccessList: block.BlockAccessList.Copy(),
	}
}

type updateProvider struct {
	mu           sync.Mutex
	transactions types.Transactions
	next         int
}

var _ txnprovider.TxnProvider = (*updateProvider)(nil)
var _ builder.RetainedTxnProvider = (*updateProvider)(nil)

func (p *updateProvider) ProvideTxns(ctx context.Context, opts ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	batch, err := p.ProvideRetainedTxns(ctx, opts...)
	return batch.Transactions, err
}

func (p *updateProvider) ProvideRetainedTxns(ctx context.Context, opts ...txnprovider.ProvideOption) (builder.RetainedTxnBatch, error) {
	if err := ctx.Err(); err != nil {
		return builder.RetainedTxnBatch{}, err
	}
	options := txnprovider.ApplyProvideOptions(opts...)
	amount := options.Amount
	if amount <= 0 {
		return builder.RetainedTxnBatch{}, nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return builder.RetainedTxnBatch{}, err
	}
	provided := make(types.Transactions, 0, min(amount, len(p.transactions)-p.next))
	newlyYielded := make([][32]byte, 0, cap(provided))
	remainingBlobGas := options.GasTarget.Blob
	remainingRlpSpace := options.AvailableRlpSpace
	for p.next < len(p.transactions) && len(provided) < amount {
		if err := ctx.Err(); err != nil {
			return builder.RetainedTxnBatch{}, err
		}
		transaction := p.transactions[p.next]
		p.next++
		if options.TxnIdsFilter != nil && options.TxnIdsFilter.Contains([32]byte(transaction.Hash())) {
			continue
		}
		blobCount := uint64(len(transaction.GetBlobHashes()))
		if blobCount > remainingBlobGas/protocolparams.GasPerBlob {
			continue
		}
		encodingSize := transaction.EncodingSize()
		encodingSize += rlp.ListPrefixLen(encodingSize)
		if encodingSize > remainingRlpSpace {
			continue
		}
		provided = append(provided, transaction)
		if options.TxnIdsFilter != nil {
			hash := [32]byte(transaction.Hash())
			options.TxnIdsFilter.Add(hash)
			newlyYielded = append(newlyYielded, hash)
		}
		remainingBlobGas -= blobCount * protocolparams.GasPerBlob
		remainingRlpSpace -= encodingSize
	}
	passComplete := p.next == len(p.transactions)
	if passComplete {
		p.next = 0
	}
	return builder.RetainedTxnBatch{
		Transactions:       copyTransactions(provided),
		NewlyYieldedTxnIDs: newlyYielded,
		PassComplete:       passComplete,
	}, nil
}
