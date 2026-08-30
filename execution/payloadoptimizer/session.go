package payloadoptimizer

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/execmodule"
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
	if o == nil || o.backend == nil {
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
	}, nil
}

type OrderflowUpdate struct {
	transactions types.Transactions
}

func NewOrderflowUpdate(transactions types.Transactions) (OrderflowUpdate, error) {
	if _, err := types.MarshalTransactionsBinary(transactions); err != nil {
		return OrderflowUpdate{}, fmt.Errorf("copy orderflow transactions: %w", err)
	}
	return OrderflowUpdate{transactions: types.CopyTxs(transactions)}, nil
}

func (u OrderflowUpdate) Transactions() types.Transactions {
	return types.CopyTxs(u.transactions)
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
	applyMu sync.Mutex
	mu      sync.RWMutex

	backend      Backend
	buildContext BuildContext
	ctx          context.Context
	cancel       context.CancelFunc
	closed       bool
	best         *Candidate
}

func (s *Session) Apply(ctx context.Context, update OrderflowUpdate) (*Candidate, error) {
	s.applyMu.Lock()
	defer s.applyMu.Unlock()

	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		return nil, ErrSessionClosed
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
	if err != nil {
		return nil, fmt.Errorf("start cold payload build: %w", err)
	}
	if err := applyCtx.Err(); err != nil {
		return nil, err
	}
	if assembled.Busy {
		return nil, ErrBackendBusy
	}
	result, err := s.backend.GetAssembledBlock(applyCtx, assembled.PayloadID)
	if err != nil {
		return nil, fmt.Errorf("collect cold payload build: %w", err)
	}
	if err := applyCtx.Err(); err != nil {
		return nil, err
	}
	if result.Busy {
		return nil, ErrBackendBusy
	}
	if result.Unknown {
		return nil, ErrUnknownPayload
	}
	if result.Block == nil {
		return nil, ErrPayloadNotReady
	}
	if err := validateCandidate(s.buildContext, result.Block); err != nil {
		return nil, err
	}
	candidate := newCandidate(s.buildContext, result)

	s.mu.Lock()
	defer s.mu.Unlock()
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

func validateCandidate(buildContext BuildContext, result *types.BlockWithReceipts) error {
	if result == nil || result.Block == nil || result.Block.HeaderNoCopy() == nil {
		return ErrPayloadNotReady
	}
	params := buildContext.params
	header := result.Block.Header()
	mismatch := func(field string) error {
		return fmt.Errorf("%w: %s", ErrCandidateContextMismatch, field)
	}
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
	if params.TargetGasLimit != nil && header.GasLimit == 0 {
		return mismatch("target gas limit")
	}
	if params.ExtraData != nil && !reflect.DeepEqual(header.Extra, params.ExtraData) {
		return mismatch("extra data")
	}
	if !reflect.DeepEqual(result.Block.Withdrawals(), types.Withdrawals(params.Withdrawals)) {
		return mismatch("withdrawals")
	}
	if !reflect.DeepEqual(result.Requests, buildContext.executionRequests) {
		return mismatch("execution requests")
	}
	return nil
}

func (s *Session) Best() (*Candidate, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.best == nil {
		return nil, false
	}
	return s.best.copy(), true
}

func (s *Session) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.best = nil
	s.cancel()
	s.mu.Unlock()
	return nil
}

func newCandidate(buildContext BuildContext, result execmodule.AssembledBlockResult) *Candidate {
	candidate := &Candidate{
		buildContext: buildContext.clone(),
		block:        copyBlockWithReceipts(result.Block),
	}
	if result.BlockValue != nil {
		candidate.value.Set(result.BlockValue)
	}
	return candidate
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
	done         atomic.Bool
	transactions types.Transactions
}

var _ txnprovider.TxnProvider = (*updateProvider)(nil)

func (p *updateProvider) ProvideTxns(ctx context.Context, _ ...txnprovider.ProvideOption) ([]types.Transaction, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !p.done.CompareAndSwap(false, true) {
		return nil, nil
	}
	return types.CopyTxs(p.transactions), nil
}
