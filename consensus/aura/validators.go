package aura

import (
	"container/list"
	"fmt"
	"sort"
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/aura/aurainterfaces"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/log"
	"go.uber.org/atomic"
)

/// Kind of SystemOrCodeCall, this is either an on-chain address, or code.
type SystemOrCodeCallKind uint8

const (
	SystemCallOnChain SystemOrCodeCallKind = 0
	CallHardCodedCode SystemOrCodeCallKind = 1
)

//nolint
type CallResults struct {
	data      []byte
	proof     [][]byte
	execError string
}

// Type alias for a function we can make calls through synchronously.
// Returns the call result and state proof for each call.
type Call func(common.Address, []byte) (CallResults, error)

// A system-calling closure. Enacts calls on a block's state from the system address.
type SystemCall func(common.Address, []byte) (CallResults, error)

type client interface {
	CallAtBlockHash(common.Hash, common.Address, []byte) (CallResults, error)
	CallAtLatestBlock(common.Address, []byte) (CallResults, error)
	SystemCallAtBlockHash(blockHash common.Hash, contract common.Address, data []byte) (CallResults, error)
}

type ValidatorSet interface {

	// Get the default "Call" helper, for use in general operation.
	// TODO [keorn]: this is a hack intended to migrate off of
	// a strict dependency on state always being available.
	defaultCaller(blockHash common.Hash) (Call, error)

	// Called for each new block this node is creating.  If this block is
	// the first block of an epoch, this is called *after* `on_epoch_begin()`,
	// but with the same parameters.
	//
	// Returns a list of contract calls to be pushed onto the new block.
	//func generateEngineTransactions(_first bool, _header *types.Header, _call SystemCall) -> Result<Vec<(Address, Bytes)>, EthcoreError>

	// Called on the close of every block.
	onCloseBlock(_header *types.Header, _address common.Address) error

	// Draws an validator nonce modulo number of validators.
	getWithCaller(parentHash common.Hash, nonce uint, caller Call) (common.Address, error)

	// Recover the validator set from the given proof, the block number, and
	// whether this header is first in its set.
	//
	// May fail if the given header doesn't kick off an epoch or
	// the proof is invalid.
	//
	// Returns the set, along with a flag indicating whether finality of a specific
	// hash should be proven.
	epochSet(first bool, num uint64, proof []byte) (SimpleList, *common.Hash, error)
	/*
	 // Returns the current number of validators.
	    fn count(&self, parent: &H256) -> usize {
	        let default = self.default_caller(BlockId::Hash(*parent));
	        self.count_with_caller(parent, &*default)
	    }

	    // Signalling that a new epoch has begun.
	    //
	    // All calls here will be from the `SYSTEM_ADDRESS`: 2^160 - 2
	    // and will have an effect on the block's state.
	    // The caller provided here may not generate proofs.
	    //
	    // `first` is true if this is the first block in the set.
	    fn on_epoch_begin(
	        &self,
	        _first: bool,
	        _header: &Header,
	        _call: &mut SystemCall,
	    ) -> Result<(), ::error::Error> {
	        Ok(())
	    }

	    // Extract genesis epoch data from the genesis state and header.
	    fn genesis_epoch_data(&self, _header: &Header, _call: &Call) -> Result<Vec<u8>, String> {
	        Ok(Vec::new())
	    }

	    // Whether this block is the last one in its epoch.
	    //
	    // Indicates that the validator set changed at the given block in a manner
	    // that doesn't require finality.
	    //
	    // `first` is true if this is the first block in the set.
	    fn is_epoch_end(&self, first: bool, chain_head: &Header) -> Option<Vec<u8>>;

	    // Whether the given block signals the end of an epoch, but change won't take effect
	    // until finality.
	    //
	    // Engine should set `first` only if the header is genesis. Multiplexing validator
	    // sets can set `first` to internal changes.
	    fn signals_epoch_end(
	        &self,
	        first: bool,
	        header: &Header,
	        aux: AuxiliaryData,
	    ) -> ::engines::EpochChange<EthereumMachine>;

	    // Recover the validator set from the given proof, the block number, and
	    // whether this header is first in its set.
	    //
	    // May fail if the given header doesn't kick off an epoch or
	    // the proof is invalid.
	    //
	    // Returns the set, along with a flag indicating whether finality of a specific
	    // hash should be proven.
	    fn epoch_set(
	        &self,
	        first: bool,
	        machine: &EthereumMachine,
	        number: BlockNumber,
	        proof: &[u8],
	    ) -> Result<(SimpleList, Option<H256>), ::error::Error>;

	    // Checks if a given address is a validator, with the given function
	    // for executing synchronous calls to contracts.
	    fn contains_with_caller(
	        &self,
	        parent_block_hash: &H256,
	        address: &Address,
	        caller: &Call,
	    ) -> bool;

	    // Draws an validator nonce modulo number of validators.
	    fn get_with_caller(&self, parent_block_hash: &H256, nonce: usize, caller: &Call) -> Address;

	    // Returns the current number of validators.
	    fn count_with_caller(&self, parent_block_hash: &H256, caller: &Call) -> usize;

	    // Notifies about malicious behaviour.
	    fn report_malicious(
	        &self,
	        _validator: &Address,
	        _set_block: BlockNumber,
	        _block: BlockNumber,
	        _proof: Bytes,
	    ) {
	    }
	    // Notifies about benign misbehaviour.
	    fn report_benign(&self, _validator: &Address, _set_block: BlockNumber, _block: BlockNumber) {}
	*/
}

func get(s ValidatorSet, h common.Hash, nonce uint) (common.Address, error) {
	d, err := s.defaultCaller(h)
	if err != nil {
		return common.Address{}, err
	}
	return s.getWithCaller(h, nonce, d)
}

//nolint
type MultiItem struct {
	num  uint64
	hash common.Hash
	set  ValidatorSet
}

type Multi struct {
	sorted []MultiItem
	parent func(common.Hash) *types.Header
}

func (s *Multi) Less(i, j int) bool { return s.sorted[i].num < s.sorted[j].num }
func (s *Multi) Len() int           { return len(s.sorted) }
func (s *Multi) Swap(i, j int)      { s.sorted[i], s.sorted[j] = s.sorted[j], s.sorted[i] }

func NewMulti(m map[uint64]ValidatorSet) *Multi {
	if _, ok := m[0]; !ok {
		panic("ValidatorSet has to be specified from block 0")
	}
	list := make([]MultiItem, len(m))
	i := 0
	for n, v := range m {
		list[i] = MultiItem{num: n, set: v}
		i++
	}
	multi := &Multi{sorted: list}
	sort.Sort(multi)
	return multi
}

func (s *Multi) defaultCaller(blockHash common.Hash) (Call, error) {
	set, ok := s.correctSet(blockHash)
	if !ok {
		return nil, fmt.Errorf("no validator set for given blockHash: %x", blockHash)
	}
	return set.defaultCaller(blockHash)
}

func (s *Multi) getWithCaller(parentHash common.Hash, nonce uint, caller Call) (common.Address, error) {
	panic("not implemented")
}

func (s *Multi) correctSet(blockHash common.Hash) (ValidatorSet, bool) {
	parent := s.parent(blockHash)
	if parent == nil {
		return nil, false
	}
	_, set := s.correctSetByNumber(parent.Number.Uint64())
	return set, set != nil
}

func (s *Multi) correctSetByNumber(parentNumber uint64) (uint64, ValidatorSet) {
	// get correct set by block number, along with block number at which
	// this set was activated.
	for i := len(s.sorted); i >= 0; i-- {
		if s.sorted[i].num <= parentNumber+1 {
			return s.sorted[i].num, s.sorted[i].set
		}
	}
	panic("constructor validation ensures that there is at least one validator set for block 0; block 0 is less than any uint; qed")
}

func (s *Multi) get(num uint64) (first bool, set ValidatorSet) {
	block, set := s.correctSetByNumber(num)
	first = block == num
	return first, set
}

func (s *Multi) onCloseBlock(header *types.Header, address common.Address) error {
	_, set := s.get(header.Number.Uint64())
	return set.onCloseBlock(header, address)
}

// TODO: do we need add `proof` argument?
//nolint
func (s *Multi) epochSet(first bool, num uint64, proof []byte) (SimpleList, *common.Hash, error) {
	setBlock, set := s.correctSetByNumber(num)
	first = setBlock == num
	return set.epochSet(first, num, proof)
}

//func (s *Multi) onEpochBegin(first bool, header *types.Header, call SysCall) error {
//	first, set := s.get(header.Number.Uint64())
//	return set.onEpochBegin(first,header, address)
//}

type SimpleList struct {
	validators []common.Address
}

func (s *SimpleList) epochSet(first bool, num uint64, proof []byte) (SimpleList, *common.Hash, error) {
	return *s, nil, nil
}
func (s *SimpleList) onCloseBlock(_header *types.Header, _address common.Address) error { return nil }
func (s *SimpleList) defaultCaller(blockHash common.Hash) (Call, error) {
	return nil, fmt.Errorf("simple list doesn't require calls")
}
func (s *SimpleList) getWithCaller(parentHash common.Hash, nonce uint, caller Call) (common.Address, error) {
	if len(s.validators) == 0 {
		return common.Address{}, fmt.Errorf("cannot operate with an empty validator set")
	}
	return s.validators[nonce%uint(len(s.validators))], nil
}

// Draws an validator nonce modulo number of validators.

func NewSimpleList(validators []common.Address) *SimpleList {
	return &SimpleList{validators: validators}
}

//nolint
type ReportQueueItem struct {
	addr     common.Address
	blockNum uint64
	data     []byte
}
type ReportQueue struct {
	sync.RWMutex
	list *list.List
}

//nolint
func (q *ReportQueue) push(addr common.Address, blockNum uint64, data []byte) {
	q.Lock()
	defer q.Unlock()
	q.list.PushBack(&ReportQueueItem{addr: addr, blockNum: blockNum, data: data})
}

// Filters reports of validators that have already been reported or are banned.

func (q *ReportQueue) filter(abi aurainterfaces.ValidatorSetABI, client client, ourAddr, contractAddr common.Address) error {
	q.Lock()
	defer q.Unlock()
	for e := q.list.Front(); e != nil; e = e.Next() {
		el := e.Value.(*ReportQueueItem)
		// Check if the validator should be reported.
		maliciousValidatorAddress := el.addr
		data, decoder := abi.ShouldValidatorReport(ourAddr, maliciousValidatorAddress, el.blockNum)
		res, err := client.CallAtLatestBlock(contractAddr, data)
		if err != nil {
			return err
		}
		if res.execError != "" {
			log.Warn("Failed to query report status, dropping pending report.", "reason", res.execError)
			continue
		}
		var shouldReport bool
		err = decoder(res.data, &res)
		if err != nil {
			return err
		}
		if !shouldReport {
			q.list.Remove(e)
		}
	}
	return nil
}

// Removes reports from the queue if it contains more than `MAX_QUEUED_REPORTS` entries.
func (q *ReportQueue) truncate() {
	// The maximum number of reports to keep queued.
	const MaxQueuedReports = 10

	q.RLock()
	defer q.RUnlock()
	// Removes reports from the queue if it contains more than `MAX_QUEUED_REPORTS` entries.
	if q.list.Len() > MaxQueuedReports {
		log.Warn("Removing reports from report cache, even though it has not been finalized", "amount", q.list.Len()-MaxQueuedReports)
	}
	i := 0
	for e := q.list.Front(); e != nil; e = e.Next() {
		if i > MaxQueuedReports {
			q.list.Remove(e)
		}
		i++
	}
}

// The validator contract should have the following interface:
//nolint
type ValidatorSafeContract struct {
	contractAddress common.Address
	validators      *lru.Cache  // RwLock<MemoryLruCache<H256, SimpleList>>,
	reportQueue     ReportQueue //Mutex<ReportQueue>,
	// The block number where we resent the queued reports last time.
	resentReportsInBlock atomic.Uint64
	// If set, this is the block number at which the consensus engine switches from AuRa to AuRa
	// with POSDAO modifications.
	posdaoTransition *uint64

	abi    aurainterfaces.ValidatorSetABI
	client client
}

func NewValidatorSafeContract(contractAddress common.Address, posdaoTransition *uint64, abi aurainterfaces.ValidatorSetABI, client client) *ValidatorSafeContract {
	const MemoizeCapacity = 500
	c, err := lru.New(MemoizeCapacity)
	if err != nil {
		panic("error creating ValidatorSafeContract cache")
	}
	return &ValidatorSafeContract{contractAddress: contractAddress, posdaoTransition: posdaoTransition, validators: c, client: client}
}

// Called for each new block this node is creating.  If this block is
// the first block of an epoch, this is called *after* `on_epoch_begin()`,
// but with the same parameters.
//
// Returns a list of contract calls to be pushed onto the new block.
//func generateEngineTransactions(_first bool, _header *types.Header, _call SystemCall) -> Result<Vec<(Address, Bytes)>, EthcoreError>

func (s *ValidatorSafeContract) epochSet(first bool, num uint64, proof []byte) (SimpleList, *common.Hash, error) {
	return SimpleList{}, nil, fmt.Errorf("ValidatorSafeContract.epochSet not implemented")
	/*
		    fn epoch_set(
		        &self,
		        first: bool,
		        machine: &EthereumMachine,
		        _number: ::types::BlockNumber,
		        proof: &[u8],
		    ) -> Result<(SimpleList, Option<H256>), ::error::Error> {
		        let rlp = Rlp::new(proof);

		        if first {
		            trace!(target: "engine", "Recovering initial epoch set");

		            let (old_header, state_items) = decode_first_proof(&rlp)?;
		            let number = old_header.number();
		            let old_hash = old_header.hash();
		            let addresses =
		                check_first_proof(machine, self.contract_address, old_header, &state_items)
		                    .map_err(::engines::EngineError::InsufficientProof)?;

		            trace!(target: "engine", "extracted epoch set at #{}: {} addresses",
						number, addresses.len());

		            Ok((SimpleList::new(addresses), Some(old_hash)))
		        } else {
		            let (old_header, receipts) = decode_proof(&rlp)?;

		            // ensure receipts match header.
		            // TODO: optimize? these were just decoded.
		            let found_root = ::triehash::ordered_trie_root(receipts.iter().map(|r| r.encode()));
		            if found_root != *old_header.receipts_root() {
		                return Err(::error::BlockError::InvalidReceiptsRoot(Mismatch {
		                    expected: *old_header.receipts_root(),
		                    found: found_root,
		                })
		                .into());
		            }

		            let bloom = self.expected_bloom(&old_header);

		            match self.extract_from_event(bloom, &old_header, &receipts) {
		                Some(list) => Ok((list, Some(old_header.hash()))),
		                None => Err(::engines::EngineError::InsufficientProof(
		                    "No log event in proof.".into(),
		                )
		                .into()),
		            }
		        }
		    }
	*/
}

func (s *ValidatorSafeContract) defaultCaller(blockHash common.Hash) (Call, error) {
	return func(addr common.Address, data []byte) (CallResults, error) {
		return s.client.CallAtBlockHash(blockHash, addr, data)
	}, nil
}
func (s *ValidatorSafeContract) getWithCaller(blockHash common.Hash, nonce uint, caller Call) (common.Address, error) {
	set, ok := s.validators.Get(blockHash)
	if ok {
		return get(set.(ValidatorSet), blockHash, nonce)
	}

	list, ok := s.getList(caller)
	if !ok {
		return common.Address{}, nil
	}
	return get(list, blockHash, nonce)
}

func (s *ValidatorSafeContract) getList(caller Call) (*SimpleList, bool) {
	code, decoder := s.abi.GetValidators()
	callResult, err := caller(s.contractAddress, code)
	if err != nil {
		log.Debug("Set of validators could not be updated: ", "err", err)
		return nil, false
	}
	if callResult.execError != "" {
		log.Debug("Set of validators could not be updated: ", "err", callResult.execError)
		return nil, false
	}
	var res []common.Address
	err = decoder(callResult.data, &res)
	if err != nil {
		log.Debug("Set of validators could not be updated: ", "err", err)
		return nil, false
	}
	return NewSimpleList(res), true
}

func (s *ValidatorSafeContract) onCloseBlock(header *types.Header, ourAddress common.Address) error {
	// Skip the rest of the function unless there has been a transition to POSDAO AuRa.
	if s.posdaoTransition != nil && header.Number.Uint64() < *s.posdaoTransition {
		log.Trace("Skipping resending of queued malicious behavior reports")
		return nil
	}
	err := s.reportQueue.filter(s.abi, s.client, ourAddress, s.contractAddress)
	if err != nil {
		return err
	}
	s.reportQueue.truncate()
	/*
	   let mut resent_reports_in_block = self.resent_reports_in_block.lock();

	   // Skip at least one block after sending malicious reports last time.
	   if header.number() > *resent_reports_in_block + REPORTS_SKIP_BLOCKS {
	       *resent_reports_in_block = header.number();
	       let mut nonce = client.latest_nonce(our_address);
	       for (address, block, data) in report_queue.iter() {
	           debug!(target: "engine", "Retrying to report validator {} for misbehavior on block {} with nonce {}.",
	              address, block, nonce);
	           while match self.transact(data.clone(), nonce) {
	               Ok(()) => false,
	               Err(EthcoreError(
	                   EthcoreErrorKind::Transaction(transaction::Error::Old),
	                   _,
	               )) => true,
	               Err(err) => {
	                   warn!(target: "engine", "Cannot report validator {} for misbehavior on block {}: {}",
	                     address, block, err);
	                   false
	               }
	           } {
	               warn!(target: "engine", "Nonce {} already used. Incrementing.", nonce);
	               nonce += U256::from(1);
	           }
	           nonce += U256::from(1);
	       }
	   }

	   Ok(())

	*/
	return nil
}

// ValidatorContract a validator contract with reporting.
type ValidatorContract struct {
	contractAddress  common.Address
	validators       ValidatorSafeContract
	posdaoTransition *uint64
}

func (s *ValidatorContract) epochSet(first bool, num uint64, proof []byte) (SimpleList, *common.Hash, error) {
	return s.validators.epochSet(first, num, proof)
}

func (s *ValidatorContract) defaultCaller(blockHash common.Hash) (Call, error) {
	return s.validators.defaultCaller(blockHash)
}

func (s *ValidatorContract) getWithCaller(parentHash common.Hash, nonce uint, caller Call) (common.Address, error) {
	return s.validators.getWithCaller(parentHash, nonce, caller)
}
func (s *ValidatorContract) onCloseBlock(header *types.Header, address common.Address) error {
	return s.validators.onCloseBlock(header, address)
}
