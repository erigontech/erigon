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

package aura

import (
	"cmp"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/abi"
	"github.com/erigontech/erigon/execution/abi/bind"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/aura/auraabi"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// nolint
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
	// func generateEngineTransactions(_firstInEpoch bool, _header *types.Header, _call SystemCall) -> Result<Vec<(Address, Bytes)>, EthcoreError>

	// Signalling that a new epoch has begun.
	//
	// All calls here will be from the `SYSTEM_ADDRESS`: 2^160 - 2
	// and will have an effect on the block's state.
	// The caller provided here may not generate proofs.
	//
	// `first` is true if this is the first block in the set.
	onEpochBegin(firstInEpoch bool, header *types.Header, caller rules.SystemCall) error

	// Called on the close of every block.
	onCloseBlock(_header *types.Header, _address common.Address) error

	// Draws an validator nonce modulo number of validators.
	getWithCaller(parentHash common.Hash, nonce uint, caller rules.Call) (common.Address, error)
	// Returns the current number of validators.
	countWithCaller(parentHash common.Hash, caller rules.Call) (uint64, error)

	// Recover the validator set from the given proof, the block number, and
	// whether this header is first in its set.
	//
	// May fail if the given header doesn't kick off an epoch or
	// the proof is invalid.
	//
	// Returns the set, along with a flag indicating whether finality of a specific
	// hash should be proven.
	epochSet(firstInEpoch bool, num uint64, setProof []byte, call rules.SystemCall) (SimpleList, common.Hash, error)

	// Extract genesis epoch data from the genesis state and header.
	genesisEpochData(header *types.Header, call rules.SystemCall) ([]byte, error)

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

func get(s ValidatorSet, h common.Hash, nonce uint, call rules.Call) (common.Address, error) {
	// d, err := s.defaultCaller(h)
	// if err != nil {
	// 	return common.Address{}, err
	// }
	return s.getWithCaller(h, nonce, call)
}
func count(s ValidatorSet, h common.Hash, call rules.Call) (uint64, error) {
	// d, err := s.defaultCaller(h)
	// if err != nil {
	// 	return 0, err
	// }
	return s.countWithCaller(h, call)
}

// nolint
type MultiItem struct {
	num  uint64
	hash common.Hash
	set  ValidatorSet
}

type Multi struct {
	sorted []MultiItem
	parent func(common.Hash) *types.Header
}

func (s *Multi) Sort() {
	slices.SortFunc(s.sorted, func(a, b MultiItem) int { return cmp.Compare(a.num, b.num) })
}

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
	multi.Sort()
	return multi
}

func (s *Multi) defaultCaller(blockHash common.Hash) (Call, error) {
	set, ok := s.correctSet(blockHash)
	if !ok {
		return nil, fmt.Errorf("no validator set for given blockHash: %x", blockHash)
	}
	return set.defaultCaller(blockHash)
}

func (s *Multi) getWithCaller(parentHash common.Hash, nonce uint, caller rules.Call) (common.Address, error) {
	panic("not implemented")
}
func (s *Multi) countWithCaller(parentHash common.Hash, caller rules.Call) (uint64, error) {
	set, ok := s.correctSet(parentHash)
	if !ok {
		return math.MaxUint64, nil
	}
	return set.countWithCaller(parentHash, caller)
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
	for _, entry := range slices.Backward(s.sorted) {
		if entry.num <= parentNumber+1 {
			return entry.num, entry.set
		}
	}
	panic("constructor validation ensures that there is at least one validator set for block 0; block 0 is less than any uint; qed")
}

func (s *Multi) get(num uint64) (firstInEpoch bool, set ValidatorSet) {
	block, set := s.correctSetByNumber(num)
	firstInEpoch = block == num
	return firstInEpoch, set
}

func (s *Multi) onCloseBlock(header *types.Header, address common.Address) error {
	_, set := s.get(header.Number.Uint64())
	return set.onCloseBlock(header, address)
}

// TODO: do we need add `proof` argument?
// nolint
func (s *Multi) epochSet(firstInEpoch bool, num uint64, proof []byte, call rules.SystemCall) (SimpleList, common.Hash, error) {
	setBlock, set := s.correctSetByNumber(num)
	firstInEpoch = setBlock == num
	return set.epochSet(firstInEpoch, num, proof, call)
}
func (s *Multi) genesisEpochData(header *types.Header, call rules.SystemCall) ([]byte, error) {
	_, set := s.correctSetByNumber(0)
	return set.genesisEpochData(header, call)
}

func (s *Multi) onEpochBegin(_ bool, header *types.Header, caller rules.SystemCall) error {
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
	validators []common.Address
}

func (s *SimpleList) epochSet(firstInEpoch bool, num uint64, proof []byte, call rules.SystemCall) (SimpleList, common.Hash, error) {
	return *s, common.Hash{}, nil
}
func (s *SimpleList) onEpochBegin(firstInEpoch bool, header *types.Header, caller rules.SystemCall) error {
	return nil
}
func (s *SimpleList) onCloseBlock(_header *types.Header, _address common.Address) error {
	return nil
}
func (s *SimpleList) defaultCaller(blockHash common.Hash) (Call, error) {
	return nil, nil // simple list doesn't require calls
}
func (s *SimpleList) getWithCaller(parentHash common.Hash, nonce uint, caller rules.Call) (common.Address, error) {
	if len(s.validators) == 0 {
		return common.Address{}, errors.New("cannot operate with an empty validator set")
	}
	return s.validators[nonce%uint(len(s.validators))], nil
}
func (s *SimpleList) countWithCaller(parentHash common.Hash, caller rules.Call) (uint64, error) {
	return uint64(len(s.validators)), nil
}
func (s *SimpleList) genesisEpochData(header *types.Header, call rules.SystemCall) ([]byte, error) {
	return []byte{}, nil
}

func (s *SimpleList) signalEpochEnd(_ bool, header *types.Header, r types.Receipts) ([]byte, error) {
	return nil, nil
}

// Draws an validator nonce modulo number of validators.

func NewSimpleList(validators []common.Address) *SimpleList {
	return &SimpleList{validators: validators}
}

// The validator contract should have the following interface:
// nolint
type ValidatorSafeContract struct {
	contractAddress common.Address
	validators      *lru.Cache[common.Hash, *SimpleList] // RwLock<MemoryLruCache<H256, SimpleList>>,
	// If set, this is the block number at which the rules engine switches from AuRa to AuRa
	// with POSDAO modifications.
	posdaoTransition *uint64

	abi    abi.ABI
	client client
}

func NewValidatorSafeContract(contractAddress common.Address, posdaoTransition *uint64, client client) *ValidatorSafeContract {
	const MemoizeCapacity = 500
	c, err := lru.New[common.Hash, *SimpleList](MemoizeCapacity)
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
func (s *ValidatorSafeContract) epochSet(firstInEpoch bool, num uint64, setProof []byte, call rules.SystemCall) (SimpleList, common.Hash, error) {
	if firstInEpoch {
		var proof FirstValidatorSetProof
		if err := rlp.DecodeBytes(setProof, &proof); err != nil {
			return SimpleList{}, common.Hash{}, fmt.Errorf("[ValidatorSafeContract.epochSet] %w", err)
		}

		if num == 0 {
			return *NewSimpleList([]common.Address{proof.Header.Coinbase}), proof.Header.ParentHash, nil
		}
		l, ok := s.getListSyscall(call)
		if !ok {
			panic(1)
		}

		// addresses, err := checkFirstValidatorSetProof(s.contractAddress, oldHeader, state_items)
		// if err != nil {
		// 	panic(err)
		// 	return SimpleList{}, common.Hash{}, fmt.Errorf("insufitient proof: block=%d,%x: %w", oldHeader.Number.Uint64(), oldHeader.Hash(), err)
		// }

		// fmt.Printf("aaaa: %x,%x\n", common.HexToAddress("0xe8ddc5c7a2d2f0d7a9798459c0104fdf5e987aca"), params.SokolGenesisHash)
		// fmt.Printf("bbbbb: %x,%x\n", proof.ContractAddress, proof.Header.Hash())
		return *l, proof.Header.ParentHash, nil
	}
	var proof ValidatorSetProof
	if err := rlp.DecodeBytes(setProof, &proof); err != nil {
		return SimpleList{}, common.Hash{}, fmt.Errorf("[ValidatorSafeContract.epochSet] %w", err)
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
	return *ll, common.Hash{}, nil
	/*
		setProof, err := decodeValidatorSetProof(proof.SetProof)
		if err != nil {
			return SimpleList{}, common.Hash{}, err
		}
		_ = setProof
	*/

}

// check a first proof: fetch the validator set at the given block.
// nolint
func checkFirstValidatorSetProof(contract_address common.Address, oldHeader *types.Header, dbItems [][]byte) ([]common.Address, error) {
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
		    let txn = TypedTransaction::Legacy(Transaction {
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
func (s *ValidatorSafeContract) defaultCaller(blockHash common.Hash) (Call, error) {
	return func(addr common.Address, data []byte) (CallResults, error) {
		return s.client.CallAtBlockHash(blockHash, addr, data)
	}, nil
}
func (s *ValidatorSafeContract) getWithCaller(blockHash common.Hash, nonce uint, caller rules.Call) (common.Address, error) {
	set, ok := s.validators.Get(blockHash)
	if ok {
		return get(set, blockHash, nonce, caller)
	}

	list, ok := s.getList(caller)
	if !ok {
		return common.Address{}, nil
	}
	s.validators.Add(blockHash, list)
	return get(list, blockHash, nonce, caller)
}
func (s *ValidatorSafeContract) countWithCaller(parentHash common.Hash, caller rules.Call) (uint64, error) {
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

func (s *ValidatorSafeContract) getList(caller rules.Call) (*SimpleList, bool) {
	packed, err := s.abi.Pack("getValidators")
	if err != nil {
		panic(err)
	}
	out, err := caller(accounts.InternAddress(s.contractAddress), packed)
	if err != nil {
		panic(err)
	}
	res, err := s.abi.Unpack("getValidators", out)
	if err != nil {
		panic(err)
	}
	out0 := *abi.ConvertType(res[0], new([]common.Address)).(*[]common.Address)
	return NewSimpleList(out0), true
}

func (s *ValidatorSafeContract) getListSyscall(caller rules.SystemCall) (*SimpleList, bool) {
	packed, err := s.abi.Pack("getValidators")
	if err != nil {
		panic(err)
	}
	out, err := caller(accounts.InternAddress(s.contractAddress), packed)
	if err != nil {
		panic(err)
	}
	res, err := s.abi.Unpack("getValidators", out)
	if err != nil {
		panic(err)
	}
	out0 := *abi.ConvertType(res[0], new([]common.Address)).(*[]common.Address)
	return NewSimpleList(out0), true
}

func (s *ValidatorSafeContract) genesisEpochData(header *types.Header, call rules.SystemCall) ([]byte, error) {
	return proveInitial(s, s.contractAddress, header, call)
}

func (s *ValidatorSafeContract) onEpochBegin(firstInEpoch bool, header *types.Header, caller rules.SystemCall) error {
	data := common.FromHex("75286211") // s.abi.Pack("finalizeChange")
	_, err := caller(accounts.InternAddress(s.contractAddress), data)
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
		return rlp.EncodeToBytes(&FirstValidatorSetProof{Header: header, ContractAddress: s.contractAddress})
	}

	// otherwise, we're checking for logs.
	// let bloom = self.expected_bloom(header);
	// let header_bloom = header.log_bloom();
	// if &bloom & header_bloom != bloom {
	// 	return ::engines::EpochChange::No;
	// }

	_, ok := s.extractFromEvent(header, r)
	if !ok {
		if header.Number.Uint64() >= DEBUG_LOG_FROM {
			fmt.Printf("signalEpochEnd: no-no-no %d,%d\n", header.Number.Uint64(), len(r))
		}
		return nil, nil
	}
	if len(r) > 0 && r[0].Bloom.IsEmpty() {
		for _, receipt := range r {
			receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
		}
	}
	proof, err := rlp.EncodeToBytes(&ValidatorSetProof{Header: header, Receipts: r})
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
	for _, receipt := range slices.Backward(receipts) {
		logs := receipt.Logs
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
		for i := range logs {
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

func (s *ValidatorSafeContract) onCloseBlock(header *types.Header, ourAddress common.Address) error {
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
	contractAddress  common.Address
	validators       *ValidatorSafeContract
	posdaoTransition *uint64
}

func (s *ValidatorContract) epochSet(firstInEpoch bool, num uint64, proof []byte, call rules.SystemCall) (SimpleList, common.Hash, error) {
	return s.validators.epochSet(firstInEpoch, num, proof, call)
}
func (s *ValidatorContract) defaultCaller(blockHash common.Hash) (Call, error) {
	return s.validators.defaultCaller(blockHash)
}
func (s *ValidatorContract) getWithCaller(parentHash common.Hash, nonce uint, caller rules.Call) (common.Address, error) {
	return s.validators.getWithCaller(parentHash, nonce, caller)
}
func (s *ValidatorContract) countWithCaller(parentHash common.Hash, caller rules.Call) (uint64, error) {
	return s.validators.countWithCaller(parentHash, caller)
}
func (s *ValidatorContract) onEpochBegin(firstInEpoch bool, header *types.Header, caller rules.SystemCall) error {
	return s.validators.onEpochBegin(firstInEpoch, header, caller)
}
func (s *ValidatorContract) onCloseBlock(header *types.Header, address common.Address) error {
	return s.validators.onCloseBlock(header, address)
}
func (s *ValidatorContract) genesisEpochData(header *types.Header, call rules.SystemCall) ([]byte, error) {
	return s.validators.genesisEpochData(header, call)
}
func (s *ValidatorContract) signalEpochEnd(firstInEpoch bool, header *types.Header, r types.Receipts) ([]byte, error) {
	return s.validators.signalEpochEnd(firstInEpoch, header, r)
}

func proveInitial(s *ValidatorSafeContract, contractAddr common.Address, header *types.Header, caller rules.SystemCall) ([]byte, error) {
	return rlp.EncodeToBytes(&FirstValidatorSetProof{Header: header, ContractAddress: contractAddr})
	// list, err := s.getList(caller)
	// fmt.Printf("aaa: %x,%t\n", list, err)

	// return common.FromHex("0x..."), nil
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
