package aura

import (
	"container/list"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	lru2 "github.com/hashicorp/golang-lru/v2"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/aura/auraabi"
	"github.com/ledgerwatch/erigon/consensus/aura/aurainterfaces"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/rlp"
)

// nolint
type CallResults struct {
	data      []byte
	proof     [][]byte
	execError string
}

// Type alias for a function we can make calls through synchronously.
// Returns the call result and state proof for each call.
type Call func(libcommon.Address, []byte) (CallResults, error)

// A system-calling closure. Enacts calls on a block's state from the system address.
type SystemCall func(libcommon.Address, []byte) (CallResults, error)

type client interface {
	CallAtBlockHash(libcommon.Hash, libcommon.Address, []byte) (CallResults, error)
	CallAtLatestBlock(libcommon.Address, []byte) (CallResults, error)
	SystemCallAtBlockHash(blockHash libcommon.Hash, contract libcommon.Address, data []byte) (CallResults, error)
}

type ValidatorSet interface {

	// Get the default "Call" helper, for use in general operation.
	// TODO [keorn]: this is a hack intended to migrate off of
	// a strict dependency on state always being available.
	defaultCaller(blockHash libcommon.Hash) (Call, error)

	// Called for each new block this node is creating.  If this block is
	// the first block of an epoch, this is called *after* `on_epoch_begin()`,
	// but with the same parameters.
	//
	// Returns a list of contract calls to be pushed onto the new block.
	//func generateEngineTransactions(_firstInEpoch bool, _header *types.Header, _call SystemCall) -> Result<Vec<(Address, Bytes)>, EthcoreError>

	// Signalling that a new epoch has begun.
	//
	// All calls here will be from the `SYSTEM_ADDRESS`: 2^160 - 2
	// and will have an effect on the block's state.
	// The caller provided here may not generate proofs.
	//
	// `first` is true if this is the first block in the set.
	onEpochBegin(firstInEpoch bool, header *types.Header, caller consensus.SystemCall) error

	// Called on the close of every block.
	onCloseBlock(_header *types.Header, _address libcommon.Address) error

	// Draws an validator nonce modulo number of validators.
	getWithCaller(parentHash libcommon.Hash, nonce uint, caller consensus.Call) (libcommon.Address, error)
	// Returns the current number of validators.
	countWithCaller(parentHash libcommon.Hash, caller consensus.Call) (uint64, error)

	// Recover the validator set from the given proof, the block number, and
	// whether this header is first in its set.
	//
	// May fail if the given header doesn't kick off an epoch or
	// the proof is invalid.
	//
	// Returns the set, along with a flag indicating whether finality of a specific
	// hash should be proven.
	epochSet(firstInEpoch bool, num uint64, setProof []byte, call consensus.SystemCall) (SimpleList, libcommon.Hash, error)

	// Extract genesis epoch data from the genesis state and header.
	genesisEpochData(header *types.Header, call consensus.SystemCall) ([]byte, error)

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
	*/
	// Whether this block is the last one in its epoch.
	//
	// Indicates that the validator set changed at the given block in a manner
	// that doesn't require finality.
	//
	// `first` is true if this is the first block in the set.
	signalEpochEnd(firstInEpoch bool, header *types.Header, receipts types.Receipts) ([]byte, error)
	/*
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

func get(s ValidatorSet, h libcommon.Hash, nonce uint, call consensus.Call) (libcommon.Address, error) {
	//d, err := s.defaultCaller(h)
	//if err != nil {
	//	return libcommon.Address{}, err
	//}
	return s.getWithCaller(h, nonce, call)
}
func count(s ValidatorSet, h libcommon.Hash, call consensus.Call) (uint64, error) {
	//d, err := s.defaultCaller(h)
	//if err != nil {
	//	return 0, err
	//}
	return s.countWithCaller(h, call)
}

// nolint
type MultiItem struct {
	num  uint64
	hash libcommon.Hash
	set  ValidatorSet
}

type Multi struct {
	sorted []MultiItem
	parent func(libcommon.Hash) *types.Header
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

func (s *Multi) defaultCaller(blockHash libcommon.Hash) (Call, error) {
	set, ok := s.correctSet(blockHash)
	if !ok {
		return nil, fmt.Errorf("no validator set for given blockHash: %x", blockHash)
	}
	return set.defaultCaller(blockHash)
}

func (s *Multi) getWithCaller(parentHash libcommon.Hash, nonce uint, caller consensus.Call) (libcommon.Address, error) {
	panic("not implemented")
}
func (s *Multi) countWithCaller(parentHash libcommon.Hash, caller consensus.Call) (uint64, error) {
	set, ok := s.correctSet(parentHash)
	if !ok {
		return math.MaxUint64, nil
	}
	return set.countWithCaller(parentHash, caller)
}

func (s *Multi) correctSet(blockHash libcommon.Hash) (ValidatorSet, bool) {
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
	for i := len(s.sorted) - 1; i >= 0; i-- {
		if s.sorted[i].num <= parentNumber+1 {
			return s.sorted[i].num, s.sorted[i].set
		}
	}
	panic("constructor validation ensures that there is at least one validator set for block 0; block 0 is less than any uint; qed")
}

func (s *Multi) get(num uint64) (firstInEpoch bool, set ValidatorSet) {
	block, set := s.correctSetByNumber(num)
	firstInEpoch = block == num
	return firstInEpoch, set
}

func (s *Multi) onCloseBlock(header *types.Header, address libcommon.Address) error {
	_, set := s.get(header.Number.Uint64())
	return set.onCloseBlock(header, address)
}

// TODO: do we need add `proof` argument?
// nolint
func (s *Multi) epochSet(firstInEpoch bool, num uint64, proof []byte, call consensus.SystemCall) (SimpleList, libcommon.Hash, error) {
	setBlock, set := s.correctSetByNumber(num)
	firstInEpoch = setBlock == num
	return set.epochSet(firstInEpoch, num, proof, call)
}
func (s *Multi) genesisEpochData(header *types.Header, call consensus.SystemCall) ([]byte, error) {
	_, set := s.correctSetByNumber(0)
	return set.genesisEpochData(header, call)
}

func (s *Multi) onEpochBegin(_ bool, header *types.Header, caller consensus.SystemCall) error {
	setTransition, set := s.correctSetByNumber(header.Number.Uint64())
	return set.onEpochBegin(setTransition == header.Number.Uint64(), header, caller)
}
func (s *Multi) signalEpochEnd(_ bool, header *types.Header, r types.Receipts) ([]byte, error) {
	num := header.Number.Uint64()
	setBlock, set := s.correctSetByNumber(num)
	first := setBlock == num
	return set.signalEpochEnd(first, header, r)
}

type SimpleList struct {
	validators []libcommon.Address
}

func (s *SimpleList) epochSet(firstInEpoch bool, num uint64, proof []byte, call consensus.SystemCall) (SimpleList, libcommon.Hash, error) {
	return *s, libcommon.Hash{}, nil
}
func (s *SimpleList) onEpochBegin(firstInEpoch bool, header *types.Header, caller consensus.SystemCall) error {
	return nil
}
func (s *SimpleList) onCloseBlock(_header *types.Header, _address libcommon.Address) error {
	return nil
}
func (s *SimpleList) defaultCaller(blockHash libcommon.Hash) (Call, error) {
	return nil, nil //simple list doesn't require calls
}
func (s *SimpleList) getWithCaller(parentHash libcommon.Hash, nonce uint, caller consensus.Call) (libcommon.Address, error) {
	if len(s.validators) == 0 {
		return libcommon.Address{}, fmt.Errorf("cannot operate with an empty validator set")
	}
	return s.validators[nonce%uint(len(s.validators))], nil
}
func (s *SimpleList) countWithCaller(parentHash libcommon.Hash, caller consensus.Call) (uint64, error) {
	return uint64(len(s.validators)), nil
}
func (s *SimpleList) genesisEpochData(header *types.Header, call consensus.SystemCall) ([]byte, error) {
	return []byte{}, nil
}

func (s *SimpleList) signalEpochEnd(_ bool, header *types.Header, r types.Receipts) ([]byte, error) {
	return nil, nil
}

// Draws an validator nonce modulo number of validators.

func NewSimpleList(validators []libcommon.Address) *SimpleList {
	return &SimpleList{validators: validators}
}

// nolint
type ReportQueueItem struct {
	addr     libcommon.Address
	blockNum uint64
	data     []byte
}

// nolint
type ReportQueue struct {
	mu   sync.RWMutex
	list *list.List
}

// nolint
func (q *ReportQueue) push(addr libcommon.Address, blockNum uint64, data []byte) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.list.PushBack(&ReportQueueItem{addr: addr, blockNum: blockNum, data: data})
}

// Filters reports of validators that have already been reported or are banned.
// nolint
func (q *ReportQueue) filter(abi aurainterfaces.ValidatorSetABI, client client, ourAddr, contractAddr libcommon.Address) error {
	q.mu.Lock()
	defer q.mu.Unlock()
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
// nolint
func (q *ReportQueue) truncate() {
	// The maximum number of reports to keep queued.
	const MaxQueuedReports = 10

	q.mu.RLock()
	defer q.mu.RUnlock()
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
// nolint
type ValidatorSafeContract struct {
	contractAddress libcommon.Address
	validators      *lru2.Cache[libcommon.Hash, *SimpleList] // RwLock<MemoryLruCache<H256, SimpleList>>,
	reportQueue     ReportQueue                              //Mutex<ReportQueue>,
	// The block number where we resent the queued reports last time.
	resentReportsInBlock atomic.Uint64
	// If set, this is the block number at which the consensus engine switches from AuRa to AuRa
	// with POSDAO modifications.
	posdaoTransition *uint64

	abi    abi.ABI
	client client
}

func NewValidatorSafeContract(contractAddress libcommon.Address, posdaoTransition *uint64, client client) *ValidatorSafeContract {
	const MemoizeCapacity = 500
	c, err := lru2.New[libcommon.Hash, *SimpleList](MemoizeCapacity)
	if err != nil {
		panic("error creating ValidatorSafeContract cache")
	}

	parsed, err := abi.JSON(strings.NewReader(auraabi.ValidatorSetABI))
	if err != nil {
		panic(err)
	}
	return &ValidatorSafeContract{contractAddress: contractAddress, posdaoTransition: posdaoTransition, validators: c, abi: parsed}
}

// Called for each new block this node is creating.  If this block is
// the first block of an epoch, this is called *after* `on_epoch_begin()`,
// but with the same parameters.
//
// Returns a list of contract calls to be pushed onto the new block.
// func generateEngineTransactions(_firstInEpoch bool, _header *types.Header, _call SystemCall) -> Result<Vec<(Address, Bytes)>, EthcoreError>
func (s *ValidatorSafeContract) epochSet(firstInEpoch bool, num uint64, setProof []byte, call consensus.SystemCall) (SimpleList, libcommon.Hash, error) {
	if firstInEpoch {
		var proof FirstValidatorSetProof
		if err := rlp.DecodeBytes(setProof, &proof); err != nil {
			return SimpleList{}, libcommon.Hash{}, fmt.Errorf("[ValidatorSafeContract.epochSet] %w", err)
		}

		if num == 0 {
			return *NewSimpleList([]libcommon.Address{proof.Header.Coinbase}), proof.Header.ParentHash, nil
		}
		l, ok := s.getListSyscall(call)
		if !ok {
			panic(1)
		}

		//addresses, err := checkFirstValidatorSetProof(s.contractAddress, oldHeader, state_items)
		//if err != nil {
		//	panic(err)
		//	return SimpleList{}, libcommon.Hash{}, fmt.Errorf("insufitient proof: block=%d,%x: %w", oldHeader.Number.Uint64(), oldHeader.Hash(), err)
		//}

		//fmt.Printf("aaaa: %x,%x\n", libcommon.HexToAddress("0xe8ddc5c7a2d2f0d7a9798459c0104fdf5e987aca"), params.SokolGenesisHash)
		//fmt.Printf("bbbbb: %x,%x\n", proof.ContractAddress, proof.Header.Hash())
		return *l, proof.Header.ParentHash, nil
	}
	var proof ValidatorSetProof
	if err := rlp.DecodeBytes(setProof, &proof); err != nil {
		return SimpleList{}, libcommon.Hash{}, fmt.Errorf("[ValidatorSafeContract.epochSet] %w", err)
	}

	if num > DEBUG_LOG_FROM {
		fmt.Printf("epoch_set1: %d,%d,%d\n", proof.Header.Number.Uint64(), len(setProof), len(proof.Receipts))
	}
	ll, ok := s.extractFromEvent(proof.Header, proof.Receipts)
	if !ok {
		panic(1)
	}

	// ensure receipts match header.
	// TODO: optimize? these were just decoded.
	/*
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
	*/
	return *ll, libcommon.Hash{}, nil
	/*
		setProof, err := decodeValidatorSetProof(proof.SetProof)
		if err != nil {
			return SimpleList{}, libcommon.Hash{}, err
		}
		_ = setProof
	*/

}

// check a first proof: fetch the validator set at the given block.
// nolint
func checkFirstValidatorSetProof(contract_address libcommon.Address, oldHeader *types.Header, dbItems [][]byte) ([]libcommon.Address, error) {
	/*
		fn check_first_proof(
		    machine: &EthereumMachine,
		    contract_address: Address,
		    old_header: Header,
		    state_items: &[DBValue],
		) -> Result<Vec<Address>, String> {
		    use types::transaction::{Action, Transaction, TypedTransaction};

		    // TODO: match client contract_call_tx more cleanly without duplication.
		    const PROVIDED_GAS: u64 = 50_000_000;

		    let env_info = ::vm::EnvInfo {
		        number: old_header.number(),
		        author: *old_header.author(),
		        difficulty: *old_header.difficulty(),
		        gas_limit: PROVIDED_GAS.into(),
		        timestamp: old_header.timestamp(),
		        last_hashes: {
		            // this will break if we don't inclue all 256 last hashes.
		            let mut last_hashes: Vec<_> = (0..256).map(|_| H256::default()).collect();
		            last_hashes[255] = *old_header.parent_hash();
		            Arc::new(last_hashes)
		        },
		        gas_used: 0.into(),
		    };

		    // check state proof using given machine.
		    let number = old_header.number();
		    let (data, decoder) = validator_set::functions::get_validators::call();

		    let from = Address::default();
		    let tx = TypedTransaction::Legacy(Transaction {
		        nonce: machine.account_start_nonce(number),
		        action: Action::Call(contract_address),
		        gas: PROVIDED_GAS.into(),
		        gas_price: U256::default(),
		        value: U256::default(),
		        data,
		    })
		    .fake_sign(from);

		    let res = ::state::check_proof(
		        state_items,
		        *old_header.state_root(),
		        &tx,
		        machine,
		        &env_info,
		    );

		    match res {
		        ::state::ProvedExecution::BadProof => Err("Bad proof".into()),
		        ::state::ProvedExecution::Failed(e) => Err(format!("Failed call: {}", e)),
		        ::state::ProvedExecution::Complete(e) => {
		            decoder.decode(&e.output).map_err(|e| e.to_string())
		        }
		    }
		}
	*/
	return nil, nil
}

// inter-contract proofs are a header and receipts.
// checking will involve ensuring that the receipts match the header and
// extracting the validator set from the receipts.
// nolint
func (s *ValidatorSafeContract) defaultCaller(blockHash libcommon.Hash) (Call, error) {
	return func(addr libcommon.Address, data []byte) (CallResults, error) {
		return s.client.CallAtBlockHash(blockHash, addr, data)
	}, nil
}
func (s *ValidatorSafeContract) getWithCaller(blockHash libcommon.Hash, nonce uint, caller consensus.Call) (libcommon.Address, error) {
	set, ok := s.validators.Get(blockHash)
	if ok {
		return get(set, blockHash, nonce, caller)
	}

	list, ok := s.getList(caller)
	if !ok {
		return libcommon.Address{}, nil
	}
	s.validators.Add(blockHash, list)
	return get(list, blockHash, nonce, caller)
}
func (s *ValidatorSafeContract) countWithCaller(parentHash libcommon.Hash, caller consensus.Call) (uint64, error) {
	set, ok := s.validators.Get(parentHash)
	if ok {
		return count(set, parentHash, caller)
	}
	list, ok := s.getList(caller)
	if !ok {
		return math.MaxUint64, nil
	}
	s.validators.Add(parentHash, list)
	return count(list, parentHash, caller)
}

func (s *ValidatorSafeContract) getList(caller consensus.Call) (*SimpleList, bool) {
	packed, err := s.abi.Pack("getValidators")
	if err != nil {
		panic(err)
	}
	out, err := caller(s.contractAddress, packed)
	if err != nil {
		panic(err)
	}
	res, err := s.abi.Unpack("getValidators", out)
	if err != nil {
		panic(err)
	}
	out0 := *abi.ConvertType(res[0], new([]libcommon.Address)).(*[]libcommon.Address)
	return NewSimpleList(out0), true
}

func (s *ValidatorSafeContract) getListSyscall(caller consensus.SystemCall) (*SimpleList, bool) {
	packed, err := s.abi.Pack("getValidators")
	if err != nil {
		panic(err)
	}
	out, err := caller(s.contractAddress, packed)
	if err != nil {
		panic(err)
	}
	res, err := s.abi.Unpack("getValidators", out)
	if err != nil {
		panic(err)
	}
	out0 := *abi.ConvertType(res[0], new([]libcommon.Address)).(*[]libcommon.Address)
	return NewSimpleList(out0), true
}

func (s *ValidatorSafeContract) genesisEpochData(header *types.Header, call consensus.SystemCall) ([]byte, error) {
	return proveInitial(s, s.contractAddress, header, call)
}

func (s *ValidatorSafeContract) onEpochBegin(firstInEpoch bool, header *types.Header, caller consensus.SystemCall) error {
	data := common.FromHex("75286211") // s.abi.Pack("finalizeChange")
	_, err := caller(s.contractAddress, data)
	if err != nil {
		return err
	}

	/*
	   let data = validator_set::functions::finalize_change::encode_input();
	   caller(self.contract_address, data)
	       .map(|_| ())
	       .map_err(::engines::EngineError::FailedSystemCall)
	       .map_err(Into::into)
	*/
	return nil
}

func (s *ValidatorSafeContract) signalEpochEnd(firstInEpoch bool, header *types.Header, r types.Receipts) ([]byte, error) {
	if header.Number.Uint64() >= DEBUG_LOG_FROM {
		fmt.Printf("signalEpochEnd: %d,%t\n", header.Number.Uint64(), firstInEpoch)
	}
	// transition to the first block of a contract requires finality but has no log event.
	if firstInEpoch {
		/*
		   let state_proof = Arc::new(FirstValidatorSetProof {
		       contract_address: self.contract_address,
		       header: header.clone(),
		   });
		   return ::engines::EpochChange::Yes(::engines::Proof::WithState(state_proof as Arc<_>));
		*/
		return rlp.EncodeToBytes(FirstValidatorSetProof{Header: header, ContractAddress: s.contractAddress})
	}

	// otherwise, we're checking for logs.
	//let bloom = self.expected_bloom(header);
	//let header_bloom = header.log_bloom();
	//if &bloom & header_bloom != bloom {
	//	return ::engines::EpochChange::No;
	//}

	_, ok := s.extractFromEvent(header, r)
	if !ok {
		if header.Number.Uint64() >= DEBUG_LOG_FROM {
			fmt.Printf("signalEpochEnd: no-no-no %d,%d\n", header.Number.Uint64(), len(r))
		}
		return nil, nil
	}
	proof, err := rlp.EncodeToBytes(ValidatorSetProof{Header: header, Receipts: r})
	if err != nil {
		return nil, err
	}
	if header.Number.Uint64() >= DEBUG_LOG_FROM {
		fmt.Printf("signalEpochEnd: %d,%d, proofLen=%d\n", header.Number.Uint64(), len(r), len(proof))
	}
	return proof, nil
}

func (s *ValidatorSafeContract) extractFromEvent(header *types.Header, receipts types.Receipts) (*SimpleList, bool) {
	if len(receipts) == 0 {
		if header.Number.Uint64() >= DEBUG_LOG_FROM {
			fmt.Printf("extractFromEvent1: %d\n", header.Number.Uint64())
		}
		return nil, false
	}
	if header.Number.Uint64() >= DEBUG_LOG_FROM {
		fmt.Printf("extractFromEvent111: %d,%d\n", header.Number.Uint64(), len(receipts))
	}

	// iterate in reverse because only the _last_ change in a given
	// block actually has any effect.
	// the contract should only increment the nonce once.
	for j := len(receipts) - 1; j >= 0; j-- {
		logs := receipts[j].Logs
		/*
			TODO: skipped next bloom check (is it required?)
					expectedBloom := expected_bloom(&self, header: &Header) -> Bloom {
				        let topics = vec![*EVENT_NAME_HASH, *header.parent_hash()];

				        debug!(target: "engine", "Expected topics for header {}: {:?}",
							header.hash(), topics);

				        LogEntry {
				            address: self.contract_address,
				            topics: topics,
				            data: Vec::new(), // irrelevant for bloom.
				        }
				        .bloom()
				    }
					if !r.log_bloom.contains_bloom(&bloom){
						continue
					}
		*/
		for i := 0; i < len(logs); i++ {
			l := logs[i]
			if header.Number.Uint64() >= DEBUG_LOG_FROM {
				fmt.Printf("extractFromEvent3: %d\n", header.Number.Uint64())
			}
			if len(l.Topics) != 2 {
				continue
			}
			found := l.Address == s.contractAddress && l.Topics[0] == EVENT_NAME_HASH && l.Topics[1] == header.ParentHash
			if !found {
				if header.Number.Uint64() >= DEBUG_LOG_FROM {
					fmt.Printf("extractFromEvent4: %d\n", header.Number.Uint64())
				}
				continue
			}

			contract := bind.NewBoundContract(l.Address, s.abi, nil, nil, nil)
			event := new(auraabi.ValidatorSetInitiateChange)
			if err := contract.UnpackLog(event, "InitiateChange", *l); err != nil {
				panic(err)
			}
			if header.Number.Uint64() >= DEBUG_LOG_FROM {
				fmt.Printf("extractFromEvent5: %d\n", header.Number.Uint64())
			}

			// only one last log is taken into account
			return NewSimpleList(event.NewSet), true
		}
	}
	/*
					  let check_log = |log: &LogEntry| {
		            log.address == self.contract_address
		                && log.topics.len() == 2
		                && log.topics[0] == *EVENT_NAME_HASH
		                && log.topics[1] == *header.parent_hash()
		        };

		        //// iterate in reverse because only the _last_ change in a given
		        //// block actually has any effect.
		        //// the contract should only increment the nonce once.
		        let mut decoded_events = receipts
		            .iter()
		            .rev()
		            .filter(|r| r.log_bloom.contains_bloom(&bloom))
		            .flat_map(|r| r.logs.iter())
		            .filter(move |l| check_log(l))
		            .filter_map(|log| {
		                validator_set::events::initiate_change::parse_log(
		                    (log.topics.clone(), log.data.clone()).into(),
		                )
		                .ok()
		            });

		        // only last log is taken into account
		        decoded_events.next().map(|matched_event| {
		            let l = SimpleList::new(matched_event.new_set);
		            println!("matched_event: {:?}", l);
		            l
		        })
	*/
	return nil, false
}

const EVENT_NAME = "InitiateChange(bytes32,address[])"

var EVENT_NAME_HASH = crypto.Keccak256Hash([]byte(EVENT_NAME))

func (s *ValidatorSafeContract) onCloseBlock(header *types.Header, ourAddress libcommon.Address) error {
	// Skip the rest of the function unless there has been a transition to POSDAO AuRa.
	if s.posdaoTransition != nil && header.Number.Uint64() < *s.posdaoTransition {
		log.Trace("Skipping resending of queued malicious behavior reports")
		return nil
	}
	/*
		err := s.reportQueue.filter(s.abi, s.client, ourAddress, s.contractAddress)
		if err != nil {
			return err
		}
		s.reportQueue.truncate()
	*/

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
	contractAddress  libcommon.Address
	validators       *ValidatorSafeContract
	posdaoTransition *uint64
}

func (s *ValidatorContract) epochSet(firstInEpoch bool, num uint64, proof []byte, call consensus.SystemCall) (SimpleList, libcommon.Hash, error) {
	return s.validators.epochSet(firstInEpoch, num, proof, call)
}
func (s *ValidatorContract) defaultCaller(blockHash libcommon.Hash) (Call, error) {
	return s.validators.defaultCaller(blockHash)
}
func (s *ValidatorContract) getWithCaller(parentHash libcommon.Hash, nonce uint, caller consensus.Call) (libcommon.Address, error) {
	return s.validators.getWithCaller(parentHash, nonce, caller)
}
func (s *ValidatorContract) countWithCaller(parentHash libcommon.Hash, caller consensus.Call) (uint64, error) {
	return s.validators.countWithCaller(parentHash, caller)
}
func (s *ValidatorContract) onEpochBegin(firstInEpoch bool, header *types.Header, caller consensus.SystemCall) error {
	return s.validators.onEpochBegin(firstInEpoch, header, caller)
}
func (s *ValidatorContract) onCloseBlock(header *types.Header, address libcommon.Address) error {
	return s.validators.onCloseBlock(header, address)
}
func (s *ValidatorContract) genesisEpochData(header *types.Header, call consensus.SystemCall) ([]byte, error) {
	return s.validators.genesisEpochData(header, call)
}
func (s *ValidatorContract) signalEpochEnd(firstInEpoch bool, header *types.Header, r types.Receipts) ([]byte, error) {
	return s.validators.signalEpochEnd(firstInEpoch, header, r)
}

func proveInitial(s *ValidatorSafeContract, contractAddr libcommon.Address, header *types.Header, caller consensus.SystemCall) ([]byte, error) {
	return rlp.EncodeToBytes(FirstValidatorSetProof{Header: header, ContractAddress: s.contractAddress})
	//list, err := s.getList(caller)
	//fmt.Printf("aaa: %x,%t\n", list, err)

	//return common.FromHex("0xf91a84f9020da00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a0fad4af258fd11939fae0c6c6eec9d340b1caac0b0196fd9a1bc3f489c5bf00b3a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000830200008083663be080808080b8410000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000f91871b914c26060604052600436106100fc576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff16806303aca79214610101578063108552691461016457806340a141ff1461019d57806340c9cdeb146101d65780634110a489146101ff57806345199e0a1461025757806349285b58146102c15780634d238c8e14610316578063752862111461034f578063900eb5a8146103645780639a573786146103c7578063a26a47d21461041c578063ae4b1b5b14610449578063b3f05b971461049e578063b7ab4db5146104cb578063d3e848f114610535578063fa81b2001461058a578063facd743b146105df575b600080fd5b341561010c57600080fd5b6101226004808035906020019091905050610630565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561016f57600080fd5b61019b600480803573ffffffffffffffffffffffffffffffffffffffff1690602001909190505061066f565b005b34156101a857600080fd5b6101d4600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610807565b005b34156101e157600080fd5b6101e9610bb7565b6040518082815260200191505060405180910390f35b341561020a57600080fd5b610236600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610bbd565b60405180831515151581526020018281526020019250505060405180910390f35b341561026257600080fd5b61026a610bee565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b838110156102ad578082015181840152602081019050610292565b505050509050019250505060405180910390f35b34156102cc57600080fd5b6102d4610c82565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561032157600080fd5b61034d600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610d32565b005b341561035a57600080fd5b610362610fcc565b005b341561036f57600080fd5b61038560048080359060200190919050506110fc565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156103d257600080fd5b6103da61113b565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561042757600080fd5b61042f6111eb565b604051808215151515815260200191505060405180910390f35b341561045457600080fd5b61045c6111fe565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156104a957600080fd5b6104b1611224565b604051808215151515815260200191505060405180910390f35b34156104d657600080fd5b6104de611237565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b83811015610521578082015181840152602081019050610506565b505050509050019250505060405180910390f35b341561054057600080fd5b6105486112cb565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561059557600080fd5b61059d6112f1565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156105ea57600080fd5b610616600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050611317565b604051808215151515815260200191505060405180910390f35b60078181548110151561063f57fe5b90600052602060002090016000915054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460029054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161415156106cb57600080fd5b600460019054906101000a900460ff161515156106e757600080fd5b600073ffffffffffffffffffffffffffffffffffffffff168173ffffffffffffffffffffffffffffffffffffffff161415151561072357600080fd5b80600a60006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055506001600460016101000a81548160ff0219169083151502179055507f600bcf04a13e752d1e3670a5a9f1c21177ca2a93c6f5391d4f1298d098097c22600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff16604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390a150565b600080600061081461113b565b73ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff1614151561084d57600080fd5b83600960008273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff1615156108a957600080fd5b600960008673ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101549350600160078054905003925060078381548110151561090857fe5b906000526020600020900160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1691508160078581548110151561094657fe5b906000526020600020900160006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff16021790555083600960008473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101819055506007838154811015156109e557fe5b906000526020600020900160006101000a81549073ffffffffffffffffffffffffffffffffffffffff02191690556000600780549050111515610a2757600080fd5b6007805480919060019003610a3c9190611370565b506000600960008773ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101819055506000600960008773ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160006101000a81548160ff0219169083151502179055506000600460006101000a81548160ff0219169083151502179055506001430340600019167f55252fa6eee4741b4e24a74a70e9c11fd2c2281df8d6ea13126ff845f7825c89600760405180806020018281038252838181548152602001915080548015610ba257602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610b58575b50509250505060405180910390a25050505050565b60085481565b60096020528060005260406000206000915090508060000160009054906101000a900460ff16908060010154905082565b610bf661139c565b6007805480602002602001604051908101604052809291908181526020018280548015610c7857602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610c2e575b5050505050905090565b6000600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff166349285b586000604051602001526040518163ffffffff167c0100000000000000000000000000000000000000000000000000000000028152600401602060405180830381600087803b1515610d1257600080fd5b6102c65a03f11515610d2357600080fd5b50505060405180519050905090565b610d3a61113b565b73ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff16141515610d7357600080fd5b80600960008273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff16151515610dd057600080fd5b600073ffffffffffffffffffffffffffffffffffffffff168273ffffffffffffffffffffffffffffffffffffffff1614151515610e0c57600080fd5b6040805190810160405280600115158152602001600780549050815250600960008473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060008201518160000160006101000a81548160ff0219169083151502179055506020820151816001015590505060078054806001018281610ea991906113b0565b9160005260206000209001600084909190916101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff160217905550506000600460006101000a81548160ff0219169083151502179055506001430340600019167f55252fa6eee4741b4e24a74a70e9c11fd2c2281df8d6ea13126ff845f7825c89600760405180806020018281038252838181548152602001915080548015610fba57602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610f70575b50509250505060405180910390a25050565b600560009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161480156110365750600460009054906101000a900460ff16155b151561104157600080fd5b6001600460006101000a81548160ff0219169083151502179055506007600690805461106e9291906113dc565b506006805490506008819055507f8564cd629b15f47dc310d45bcbfc9bcf5420b0d51bf0659a16c67f91d27632536110a4611237565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b838110156110e75780820151818401526020810190506110cc565b505050509050019250505060405180910390a1565b60068181548110151561110b57fe5b90600052602060002090016000915054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b6000600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16639a5737866000604051602001526040518163ffffffff167c0100000000000000000000000000000000000000000000000000000000028152600401602060405180830381600087803b15156111cb57600080fd5b6102c65a03f115156111dc57600080fd5b50505060405180519050905090565b600460019054906101000a900460ff1681565b600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460009054906101000a900460ff1681565b61123f61139c565b60068054806020026020016040519081016040528092919081815260200182805480156112c157602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311611277575b5050505050905090565b600560009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460029054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b6000600960008373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff169050919050565b81548183558181151161139757818360005260206000209182019101611396919061142e565b5b505050565b602060405190810160405280600081525090565b8154818355818115116113d7578183600052602060002091820191016113d6919061142e565b5b505050565b82805482825590600052602060002090810192821561141d5760005260206000209182015b8281111561141c578254825591600101919060010190611401565b5b50905061142a9190611453565b5090565b61145091905b8082111561144c576000816000905550600101611434565b5090565b90565b61149391905b8082111561148f57600081816101000a81549073ffffffffffffffffffffffffffffffffffffffff021916905550600101611459565b5090565b905600a165627a7a7230582036ea35935c8246b68074adece2eab70c40e69a0193c08a6277ce06e5b25188510029b8f3f8f1a08023c0d95fc2364e0bf7593f5ff32e1db8ef9f4b41c0bd474eae62d1af896e99808080a0b47b4f0b3e73b5edc8f9a9da1cbcfed562eb06bf54619b6aefeadebf5b3604c280a0da6ec08940a924cb08c947dd56cdb40076b29a6f0ea4dba4e2d02d9a9a72431b80a030cc4138c9e74b6cf79d624b4b5612c0fd888e91f55316cfee7d1694e1a90c0b80a0c5d54b915b56a888eee4e6eeb3141e778f9b674d1d322962eed900f02c29990aa017256b36ef47f907c6b1378a2636942ce894c17075e56fc054d4283f6846659e808080a03340bbaeafcda3a8672eb83099231dbbfab8dae02a1e8ec2f7180538fac207e080b86bf869a033aa5d69545785694b808840be50c182dad2ec3636dfccbe6572fb69828742c0b846f8440101a0663ce0d171e545a26aa67e4ca66f72ba96bb48287dbcc03beea282867f80d44ba01f0e7726926cb43c03a0abf48197dba78522ec8ba1b158e2aa30da7d2a2c6f9eb838f7a03868bdfa8727775661e4ccf117824a175a33f8703d728c04488fbfffcafda9f99594e8ddc5c7a2d2f0d7a9798459c0104fdf5e987acaa3e2a02052222313e28459528d920b65115c16c04f3efc82aaedc97be59f3f377c0d3f01b853f851808080a07bb75cabebdcbd1dbb4331054636d0c6d7a2b08483b9e04df057395a7434c9e080808080808080a0e61e567237b49c44d8f906ceea49027260b4010c10a547b38d8b131b9d3b6f848080808080b8d3f8d1a0dc277c93a9f9dcee99aac9b8ba3cfa4c51821998522469c37715644e8fbac0bfa0ab8cdb808c8303bb61fb48e276217be9770fa83ecf3f90f2234d558885f5abf1808080a0fe137c3a474fbde41d89a59dd76da4c55bf696b86d3af64a55632f76cf30786780808080a06301b39b2ea8a44df8b0356120db64b788e71f52e1d7a6309d0d2e5b86fee7cb80a0da5d8b08dea0c5a4799c0f44d8a24d7cdf209f9b7a5588c1ecafb5361f6b9f07a01b7779e149cadf24d4ffb77ca7e11314b8db7097e4d70b2a173493153ca2e5a0808080b853f851808080a0a87d9bb950836582673aa0eecc0ff64aac607870637a2dd2012b8b1b31981f698080a08da6d5c36a404670c553a2c9052df7cd604f04e3863c4c7b9e0027bfd54206d680808080808080808080b86bf869a02080c7b7ae81a58eb98d9c78de4a1fd7fd9535fc953ed2be602daaa41767312ab846f8448080a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"), nil
	//return common.FromHex("0xf91a84f9020da00000000000000000000000000000000000000000000000000000000000000000a01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a0fad4af258fd11939fae0c6c6eec9d340b1caac0b0196fd9a1bc3f489c5bf00b3a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421b9010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000830200008083663be080808080b8410000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000f91871b8d3f8d1a0dc277c93a9f9dcee99aac9b8ba3cfa4c51821998522469c37715644e8fbac0bfa0ab8cdb808c8303bb61fb48e276217be9770fa83ecf3f90f2234d558885f5abf1808080a0fe137c3a474fbde41d89a59dd76da4c55bf696b86d3af64a55632f76cf30786780808080a06301b39b2ea8a44df8b0356120db64b788e71f52e1d7a6309d0d2e5b86fee7cb80a0da5d8b08dea0c5a4799c0f44d8a24d7cdf209f9b7a5588c1ecafb5361f6b9f07a01b7779e149cadf24d4ffb77ca7e11314b8db7097e4d70b2a173493153ca2e5a0808080b8f3f8f1a08023c0d95fc2364e0bf7593f5ff32e1db8ef9f4b41c0bd474eae62d1af896e99808080a0b47b4f0b3e73b5edc8f9a9da1cbcfed562eb06bf54619b6aefeadebf5b3604c280a0da6ec08940a924cb08c947dd56cdb40076b29a6f0ea4dba4e2d02d9a9a72431b80a030cc4138c9e74b6cf79d624b4b5612c0fd888e91f55316cfee7d1694e1a90c0b80a0c5d54b915b56a888eee4e6eeb3141e778f9b674d1d322962eed900f02c29990aa017256b36ef47f907c6b1378a2636942ce894c17075e56fc054d4283f6846659e808080a03340bbaeafcda3a8672eb83099231dbbfab8dae02a1e8ec2f7180538fac207e080b838f7a03868bdfa8727775661e4ccf117824a175a33f8703d728c04488fbfffcafda9f99594e8ddc5c7a2d2f0d7a9798459c0104fdf5e987acaa3e2a02052222313e28459528d920b65115c16c04f3efc82aaedc97be59f3f377c0d3f01b853f851808080a0a87d9bb950836582673aa0eecc0ff64aac607870637a2dd2012b8b1b31981f698080a08da6d5c36a404670c553a2c9052df7cd604f04e3863c4c7b9e0027bfd54206d680808080808080808080b86bf869a033aa5d69545785694b808840be50c182dad2ec3636dfccbe6572fb69828742c0b846f8440101a0663ce0d171e545a26aa67e4ca66f72ba96bb48287dbcc03beea282867f80d44ba01f0e7726926cb43c03a0abf48197dba78522ec8ba1b158e2aa30da7d2a2c6f9eb86bf869a02080c7b7ae81a58eb98d9c78de4a1fd7fd9535fc953ed2be602daaa41767312ab846f8448080a056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421a0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470b853f851808080a07bb75cabebdcbd1dbb4331054636d0c6d7a2b08483b9e04df057395a7434c9e080808080808080a0e61e567237b49c44d8f906ceea49027260b4010c10a547b38d8b131b9d3b6f848080808080b914c26060604052600436106100fc576000357c0100000000000000000000000000000000000000000000000000000000900463ffffffff16806303aca79214610101578063108552691461016457806340a141ff1461019d57806340c9cdeb146101d65780634110a489146101ff57806345199e0a1461025757806349285b58146102c15780634d238c8e14610316578063752862111461034f578063900eb5a8146103645780639a573786146103c7578063a26a47d21461041c578063ae4b1b5b14610449578063b3f05b971461049e578063b7ab4db5146104cb578063d3e848f114610535578063fa81b2001461058a578063facd743b146105df575b600080fd5b341561010c57600080fd5b6101226004808035906020019091905050610630565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561016f57600080fd5b61019b600480803573ffffffffffffffffffffffffffffffffffffffff1690602001909190505061066f565b005b34156101a857600080fd5b6101d4600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610807565b005b34156101e157600080fd5b6101e9610bb7565b6040518082815260200191505060405180910390f35b341561020a57600080fd5b610236600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610bbd565b60405180831515151581526020018281526020019250505060405180910390f35b341561026257600080fd5b61026a610bee565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b838110156102ad578082015181840152602081019050610292565b505050509050019250505060405180910390f35b34156102cc57600080fd5b6102d4610c82565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561032157600080fd5b61034d600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050610d32565b005b341561035a57600080fd5b610362610fcc565b005b341561036f57600080fd5b61038560048080359060200190919050506110fc565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156103d257600080fd5b6103da61113b565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561042757600080fd5b61042f6111eb565b604051808215151515815260200191505060405180910390f35b341561045457600080fd5b61045c6111fe565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156104a957600080fd5b6104b1611224565b604051808215151515815260200191505060405180910390f35b34156104d657600080fd5b6104de611237565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b83811015610521578082015181840152602081019050610506565b505050509050019250505060405180910390f35b341561054057600080fd5b6105486112cb565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b341561059557600080fd5b61059d6112f1565b604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390f35b34156105ea57600080fd5b610616600480803573ffffffffffffffffffffffffffffffffffffffff16906020019091905050611317565b604051808215151515815260200191505060405180910390f35b60078181548110151561063f57fe5b90600052602060002090016000915054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460029054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161415156106cb57600080fd5b600460019054906101000a900460ff161515156106e757600080fd5b600073ffffffffffffffffffffffffffffffffffffffff168173ffffffffffffffffffffffffffffffffffffffff161415151561072357600080fd5b80600a60006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055506001600460016101000a81548160ff0219169083151502179055507f600bcf04a13e752d1e3670a5a9f1c21177ca2a93c6f5391d4f1298d098097c22600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff16604051808273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200191505060405180910390a150565b600080600061081461113b565b73ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff1614151561084d57600080fd5b83600960008273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff1615156108a957600080fd5b600960008673ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101549350600160078054905003925060078381548110151561090857fe5b906000526020600020900160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1691508160078581548110151561094657fe5b906000526020600020900160006101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff16021790555083600960008473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101819055506007838154811015156109e557fe5b906000526020600020900160006101000a81549073ffffffffffffffffffffffffffffffffffffffff02191690556000600780549050111515610a2757600080fd5b6007805480919060019003610a3c9190611370565b506000600960008773ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff168152602001908152602001600020600101819055506000600960008773ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160006101000a81548160ff0219169083151502179055506000600460006101000a81548160ff0219169083151502179055506001430340600019167f55252fa6eee4741b4e24a74a70e9c11fd2c2281df8d6ea13126ff845f7825c89600760405180806020018281038252838181548152602001915080548015610ba257602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610b58575b50509250505060405180910390a25050505050565b60085481565b60096020528060005260406000206000915090508060000160009054906101000a900460ff16908060010154905082565b610bf661139c565b6007805480602002602001604051908101604052809291908181526020018280548015610c7857602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610c2e575b5050505050905090565b6000600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff166349285b586000604051602001526040518163ffffffff167c0100000000000000000000000000000000000000000000000000000000028152600401602060405180830381600087803b1515610d1257600080fd5b6102c65a03f11515610d2357600080fd5b50505060405180519050905090565b610d3a61113b565b73ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff16141515610d7357600080fd5b80600960008273ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff16151515610dd057600080fd5b600073ffffffffffffffffffffffffffffffffffffffff168273ffffffffffffffffffffffffffffffffffffffff1614151515610e0c57600080fd5b6040805190810160405280600115158152602001600780549050815250600960008473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060008201518160000160006101000a81548160ff0219169083151502179055506020820151816001015590505060078054806001018281610ea991906113b0565b9160005260206000209001600084909190916101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff160217905550506000600460006101000a81548160ff0219169083151502179055506001430340600019167f55252fa6eee4741b4e24a74a70e9c11fd2c2281df8d6ea13126ff845f7825c89600760405180806020018281038252838181548152602001915080548015610fba57602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311610f70575b50509250505060405180910390a25050565b600560009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff163373ffffffffffffffffffffffffffffffffffffffff161480156110365750600460009054906101000a900460ff16155b151561104157600080fd5b6001600460006101000a81548160ff0219169083151502179055506007600690805461106e9291906113dc565b506006805490506008819055507f8564cd629b15f47dc310d45bcbfc9bcf5420b0d51bf0659a16c67f91d27632536110a4611237565b6040518080602001828103825283818151815260200191508051906020019060200280838360005b838110156110e75780820151818401526020810190506110cc565b505050509050019250505060405180910390a1565b60068181548110151561110b57fe5b90600052602060002090016000915054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b6000600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16639a5737866000604051602001526040518163ffffffff167c0100000000000000000000000000000000000000000000000000000000028152600401602060405180830381600087803b15156111cb57600080fd5b6102c65a03f115156111dc57600080fd5b50505060405180519050905090565b600460019054906101000a900460ff1681565b600a60009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460009054906101000a900460ff1681565b61123f61139c565b60068054806020026020016040519081016040528092919081815260200182805480156112c157602002820191906000526020600020905b8160009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff1681526020019060010190808311611277575b5050505050905090565b600560009054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b600460029054906101000a900473ffffffffffffffffffffffffffffffffffffffff1681565b6000600960008373ffffffffffffffffffffffffffffffffffffffff1673ffffffffffffffffffffffffffffffffffffffff16815260200190815260200160002060000160009054906101000a900460ff169050919050565b81548183558181151161139757818360005260206000209182019101611396919061142e565b5b505050565b602060405190810160405280600081525090565b8154818355818115116113d7578183600052602060002091820191016113d6919061142e565b5b505050565b82805482825590600052602060002090810192821561141d5760005260206000209182015b8281111561141c578254825591600101919060010190611401565b5b50905061142a9190611453565b5090565b61145091905b8082111561144c576000816000905550600101611434565b5090565b90565b61149391905b8082111561148f57600081816101000a81549073ffffffffffffffffffffffffffffffffffffffff021916905550600101611459565b5090565b905600a165627a7a7230582036ea35935c8246b68074adece2eab70c40e69a0193c08a6277ce06e5b25188510029"), nil
	/*

	   // given a provider and caller, generate proof. this will just be a state proof
	   // of `getValidators`.
	   fn prove_initial(
	       contract_address: Address,
	       header: &Header,
	       caller: &Call,
	   ) -> Result<Vec<u8>, String> {
	       use std::cell::RefCell;

	       let epoch_proof = RefCell::new(None);
	       let validators = {
	           let (data, decoder) = validator_set::functions::get_validators::call();
	           let (value, proof) = caller(contract_address, data)?;
	           *epoch_proof.borrow_mut() = Some(encode_first_proof(header, &proof));
	           decoder.decode(&value).map_err(|e| e.to_string())?
	       };

	       let proof = epoch_proof
	           .into_inner()
	           .expect("epoch_proof always set after call; qed");

	       trace!(target: "engine", "obtained proof for initial set: {} validators, {} bytes",
	   		validators.len(), proof.len());

	       info!(target: "engine", "Signal for switch to contract-based validator set.");
	       info!(target: "engine", "Initial contract validators: {:?}", validators);

	       Ok(proof)
	   }
	*/
}
