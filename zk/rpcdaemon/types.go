package types

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/zkevm/hex"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/holiman/uint256"
)

var (

	// Earliest contains the string to represent the earliest block known.
	Earliest = "earliest"
	// Latest contains the string to represent the latest block known.
	Latest = "latest"
	// Pending contains the string to represent the pending block known.
	Pending = "pending"
	// Safe contains the string to represent the last virtualized block known.
	Safe = "safe"
	// Finalized contains the string to represent the last verified block known.
	Finalized = "finalized"
)

var (
	// ZeroHash is the hash 0x0000000000000000000000000000000000000000000000000000000000000000
	ZeroHash = common.Hash{}
	// ZeroAddress is the address 0x0000000000000000000000000000000000000000
	ZeroAddress = common.Address{}
)

// ArgUint64 helps to marshal uint64 values provided in the RPC requests
type ArgUint64 uint64

// MarshalText marshals into text
func (b ArgUint64) MarshalText() ([]byte, error) {
	buf := make([]byte, 2) //nolint:gomnd
	copy(buf, `0x`)
	buf = strconv.AppendUint(buf, uint64(b), hex.Base)
	return buf, nil
}

// UnmarshalText unmarshals from text
func (b *ArgUint64) UnmarshalText(input []byte) error {
	str := strings.TrimPrefix(string(input), "0x")
	num, err := strconv.ParseUint(str, hex.Base, hex.BitSize64)
	if err != nil {
		return err
	}
	*b = ArgUint64(num)
	return nil
}

// Hex returns a hexadecimal representation
func (b ArgUint64) Hex() string {
	bb, _ := b.MarshalText()
	return string(bb)
}

// ArgUint64Ptr returns the pointer of the provided ArgUint64
func ArgUint64Ptr(a ArgUint64) *ArgUint64 {
	return &a
}

// ArgBytes helps to marshal byte array values provided in the RPC requests
type ArgBytes []byte

// MarshalText marshals into text
func (b ArgBytes) MarshalText() ([]byte, error) {
	return encodeToHex(b), nil
}

// UnmarshalText unmarshals from text
func (b *ArgBytes) UnmarshalText(input []byte) error {
	hh, err := decodeToHex(input)
	if err != nil {
		return nil
	}
	aux := make([]byte, len(hh))
	copy(aux, hh)
	*b = aux
	return nil
}

// Hex returns a hexadecimal representation
func (b ArgBytes) Hex() string {
	bb, _ := b.MarshalText()
	return string(bb)
}

// ArgBytesPtr helps to marshal byte array values provided in the RPC requests
func ArgBytesPtr(b []byte) *ArgBytes {
	bb := ArgBytes(b)

	return &bb
}

// ArgBig helps to marshal big number values provided in the RPC requests
type ArgBig big.Int

// UnmarshalText unmarshals an instance of ArgBig into an array of bytes
func (a *ArgBig) UnmarshalText(input []byte) error {
	buf, err := decodeToHex(input)
	if err != nil {
		return err
	}

	b := new(big.Int)
	b.SetBytes(buf)
	*a = ArgBig(*b)

	return nil
}

// MarshalText marshals an array of bytes into an instance of ArgBig
func (a ArgBig) MarshalText() ([]byte, error) {
	b := (*big.Int)(&a)

	return []byte("0x" + b.Text(hex.Base)), nil
}

// Hex returns a hexadecimal representation
func (b ArgBig) Hex() string {
	bb, _ := b.MarshalText()
	return string(bb)
}

func decodeToHex(b []byte) ([]byte, error) {
	str := string(b)
	str = strings.TrimPrefix(str, "0x")
	if len(str)%2 != 0 {
		str = "0" + str
	}
	return hex.DecodeString(str)
}

func encodeToHex(b []byte) []byte {
	str := hex.EncodeToString(b)
	if len(str)%2 != 0 {
		str = "0" + str
	}
	return []byte("0x" + str)
}

// ArgHash represents a common.Hash that accepts strings
// shorter than 64 bytes, like 0x00
type ArgHash common.Hash

// UnmarshalText unmarshals from text
func (arg *ArgHash) UnmarshalText(input []byte) error {
	if !hex.IsValid(string(input)) {
		return fmt.Errorf("invalid hash, it needs to be a hexadecimal value")
	}

	str := strings.TrimPrefix(string(input), "0x")
	*arg = ArgHash(common.HexToHash(str))
	return nil
}

// Hash returns an instance of common.Hash
func (arg *ArgHash) Hash() common.Hash {
	result := common.Hash{}
	if arg != nil {
		result = common.Hash(*arg)
	}
	return result
}

// ArgAddress represents a common.Address that accepts strings
// shorter than 32 bytes, like 0x00
type ArgAddress common.Address

// UnmarshalText unmarshals from text
func (b *ArgAddress) UnmarshalText(input []byte) error {
	if !hex.IsValid(string(input)) {
		return fmt.Errorf("invalid address, it needs to be a hexadecimal value")
	}

	str := strings.TrimPrefix(string(input), "0x")
	*b = ArgAddress(common.HexToAddress(str))
	return nil
}

// Address returns an instance of common.Address
func (arg *ArgAddress) Address() common.Address {
	result := common.Address{}
	if arg != nil {
		result = common.Address(*arg)
	}
	return result
}

// TxArgs is the transaction argument for the rpc endpoints
type TxArgs struct {
	From     *common.Address
	To       *common.Address
	Gas      *ArgUint64
	GasPrice *ArgBytes
	Value    *ArgBytes
	Data     *ArgBytes
	Input    *ArgBytes
	Nonce    *ArgUint64
}

func LeftPadBytes(slice []byte, l int) []byte {
	if l <= len(slice) {
		return slice
	}

	padded := make([]byte, l)
	copy(padded[l-len(slice):], slice)

	return padded
}

// Block structure
type Block struct {
	ParentHash      common.Hash         `json:"parentHash"`
	Sha3Uncles      common.Hash         `json:"sha3Uncles"`
	Miner           common.Address      `json:"miner"`
	StateRoot       common.Hash         `json:"stateRoot"`
	TxRoot          common.Hash         `json:"transactionsRoot"`
	ReceiptsRoot    common.Hash         `json:"receiptsRoot"`
	LogsBloom       types.Bloom         `json:"logsBloom"`
	Difficulty      ArgUint64           `json:"difficulty"`
	TotalDifficulty ArgUint64           `json:"totalDifficulty"`
	Size            ArgUint64           `json:"size"`
	Number          ArgUint64           `json:"number"`
	GasLimit        ArgUint64           `json:"gasLimit"`
	GasUsed         ArgUint64           `json:"gasUsed"`
	Timestamp       ArgUint64           `json:"timestamp"`
	ExtraData       ArgBytes            `json:"extraData"`
	MixHash         common.Hash         `json:"mixHash"`
	Nonce           ArgBytes            `json:"nonce"`
	Hash            common.Hash         `json:"hash"`
	Transactions    []TransactionOrHash `json:"transactions"`
	Uncles          []common.Hash       `json:"uncles"`
}

// NewBlock creates a Block instance
func NewBlock(b *types.Block, receipts []types.Receipt, fullTx, includeReceipts bool) (*Block, error) {
	h := b.Header()

	n := big.NewInt(0).SetUint64(h.Nonce.Uint64())
	nonce := LeftPadBytes(n.Bytes(), 8) //nolint:gomnd

	var difficulty uint64
	if h.Difficulty != nil {
		difficulty = h.Difficulty.Uint64()
	} else {
		difficulty = uint64(0)
	}

	res := &Block{
		ParentHash:      h.ParentHash,
		Sha3Uncles:      h.UncleHash,
		Miner:           h.Coinbase,
		StateRoot:       h.Root,
		TxRoot:          h.TxHash,
		ReceiptsRoot:    h.ReceiptHash,
		LogsBloom:       h.Bloom,
		Difficulty:      ArgUint64(difficulty),
		TotalDifficulty: ArgUint64(difficulty),
		Size:            ArgUint64(b.Size()),
		Number:          ArgUint64(b.Number().Uint64()),
		GasLimit:        ArgUint64(h.GasLimit),
		GasUsed:         ArgUint64(h.GasUsed),
		Timestamp:       ArgUint64(h.Time),
		ExtraData:       ArgBytes(h.Extra),
		MixHash:         h.MixDigest,
		Nonce:           nonce,
		Hash:            b.Hash(),
		Transactions:    []TransactionOrHash{},
		Uncles:          []common.Hash{},
	}

	receiptsMap := make(map[common.Hash]types.Receipt, len(receipts))
	for _, receipt := range receipts {
		receiptsMap[receipt.TxHash] = receipt
	}

	for _, tx := range b.Transactions() {
		if fullTx {
			var receiptPtr *types.Receipt
			if receipt, found := receiptsMap[tx.Hash()]; found {
				receiptPtr = &receipt
			}

			rpcTx, err := NewTransaction(tx, receiptPtr, includeReceipts)
			if err != nil {
				return nil, err
			}
			res.Transactions = append(
				res.Transactions,
				TransactionOrHash{Tx: rpcTx},
			)
		} else {
			h := tx.Hash()
			res.Transactions = append(
				res.Transactions,
				TransactionOrHash{Hash: &h},
			)
		}
	}

	for _, uncle := range b.Uncles() {
		res.Uncles = append(res.Uncles, uncle.Hash())
	}

	return res, nil
}

// TransactionOrHash for union type of transaction and types.Hash
type TransactionOrHash struct {
	Hash *common.Hash
	Tx   *Transaction
}

// MarshalJSON marshals into json
func (th TransactionOrHash) MarshalJSON() ([]byte, error) {
	if th.Hash != nil {
		return json.Marshal(th.Hash)
	}
	return json.Marshal(th.Tx)
}

// UnmarshalJSON unmarshals from json
func (th *TransactionOrHash) UnmarshalJSON(input []byte) error {
	v := string(input)
	if strings.HasPrefix(v, "0x") || strings.HasPrefix(v, "\"0x") {
		var h common.Hash
		err := json.Unmarshal(input, &h)
		if err != nil {
			return err
		}
		*th = TransactionOrHash{Hash: &h}
		return nil
	}

	var t Transaction
	err := json.Unmarshal(input, &t)
	if err != nil {
		return err
	}
	*th = TransactionOrHash{Tx: &t}
	return nil
}

// BlockOrHash for union type of block and types.Hash
type BlockOrHash struct {
	Hash  *common.Hash
	Block *Block
}

// MarshalJSON marshals into json
func (bh BlockOrHash) MarshalJSON() ([]byte, error) {
	if bh.Hash != nil {
		return json.Marshal(bh.Hash)
	}
	return json.Marshal(bh.Block)
}

// UnmarshalJSON unmarshals from json
func (bh *BlockOrHash) UnmarshalJSON(input []byte) error {
	v := string(input)
	if strings.HasPrefix(v, "0x") || strings.HasPrefix(v, "\"0x") {
		var h common.Hash
		err := json.Unmarshal(input, &h)
		if err != nil {
			return err
		}
		*bh = BlockOrHash{Hash: &h}
		return nil
	}

	var b Block
	err := json.Unmarshal(input, &b)
	if err != nil {
		return err
	}
	*bh = BlockOrHash{Block: &b}
	return nil
}

// Transaction structure
type Transaction struct {
	Nonce       ArgUint64       `json:"nonce"`
	GasPrice    ArgBig          `json:"gasPrice"`
	Gas         ArgUint64       `json:"gas"`
	To          *common.Address `json:"to"`
	Value       ArgBig          `json:"value"`
	Input       ArgBytes        `json:"input"`
	V           ArgBig          `json:"v"`
	R           ArgBig          `json:"r"`
	S           ArgBig          `json:"s"`
	Hash        common.Hash     `json:"hash"`
	From        common.Address  `json:"from"`
	BlockHash   *common.Hash    `json:"blockHash"`
	BlockNumber *ArgUint64      `json:"blockNumber"`
	TxIndex     *ArgUint64      `json:"transactionIndex"`
	ChainID     *ArgBig         `json:"chainId,omitempty"`
	Type        ArgUint64       `json:"type"`
	Receipt     *Receipt        `json:"receipt,omitempty"`
	L2Hash      common.Hash     `json:"l2Hash,omitempty"`
}

// GetSender gets the sender from the transaction's signature
func GetSender(tx types.Transaction) (common.Address, error) {
	// TODO: fix the hardcoded chain config for the l2
	signer := types.MakeSigner(params.MainnetChainConfig, 0)

	sender, err := signer.Sender(tx)
	if err != nil {
		return common.Address{}, err
	}
	return sender, nil
}

// NewTransaction creates a transaction instance
func NewTransaction(
	tx types.Transaction,
	receipt *types.Receipt,
	includeReceipt bool,
) (*Transaction, error) {
	v, r, s := tx.RawSignatureValues()

	from, _ := GetSender(tx)
	hash := common.HexToHash(tx.Hash().Hex())
	cid := tx.GetChainID()
	var cidAB *ArgBig
	if cid.Cmp(uint256.NewInt(0)) != 0 {
		cidAB = (*ArgBig)(cid.ToBig())
	}
	res := &Transaction{
		Nonce:    ArgUint64(tx.GetNonce()),
		GasPrice: ArgBig(*tx.GetPrice().ToBig()),
		Gas:      ArgUint64(tx.GetGas()),
		To:       tx.GetTo(),
		Value:    ArgBig(*tx.GetValue().ToBig()),
		Input:    tx.GetData(),
		V:        ArgBig(*v.ToBig()),
		R:        ArgBig(*r.ToBig()),
		S:        ArgBig(*s.ToBig()),
		Hash:     hash,
		From:     from,
		ChainID:  cidAB,
		Type:     ArgUint64(tx.Type()),
	}

	if receipt != nil {
		bn := ArgUint64(receipt.BlockNumber.Uint64())
		res.BlockNumber = &bn
		blockHash := common.HexToHash(receipt.BlockHash.Hex())
		res.BlockHash = &blockHash
		ti := ArgUint64(receipt.TransactionIndex)
		res.TxIndex = &ti
		rpcReceipt, err := NewReceipt(tx, receipt)
		if err != nil {
			return nil, err
		}
		if includeReceipt {
			res.Receipt = &rpcReceipt
		}
	}

	return res, nil
}

// Receipt structure
type Receipt struct {
	CumulativeGasUsed ArgUint64       `json:"cumulativeGasUsed"`
	LogsBloom         types.Bloom     `json:"logsBloom"`
	Logs              []*types.Log    `json:"logs"`
	Status            ArgUint64       `json:"status"`
	TxHash            common.Hash     `json:"transactionHash"`
	TxIndex           ArgUint64       `json:"transactionIndex"`
	BlockHash         common.Hash     `json:"blockHash"`
	BlockNumber       ArgUint64       `json:"blockNumber"`
	GasUsed           ArgUint64       `json:"gasUsed"`
	FromAddr          common.Address  `json:"from"`
	ToAddr            *common.Address `json:"to"`
	ContractAddress   *common.Address `json:"contractAddress"`
	Type              ArgUint64       `json:"type"`
	EffectiveGasPrice *ArgBig         `json:"effectiveGasPrice,omitempty"`
	TransactionL2Hash common.Hash     `json:"transactionL2Hash,omitempty"`
}

// NewReceipt creates a new Receipt instance
func NewReceipt(tx types.Transaction, r *types.Receipt) (Receipt, error) {
	to := tx.GetTo()
	logs := r.Logs
	if logs == nil {
		logs = []*types.Log{}
	}

	var contractAddress *common.Address
	if r.ContractAddress != ZeroAddress {
		ca := r.ContractAddress
		contractAddress = &ca
	}

	blockNumber := ArgUint64(0)
	if r.BlockNumber != nil {
		blockNumber = ArgUint64(r.BlockNumber.Uint64())
	}

	from, err := GetSender(tx)
	if err != nil {
		return Receipt{}, err
	}
	receipt := Receipt{
		CumulativeGasUsed: ArgUint64(r.CumulativeGasUsed),
		LogsBloom:         r.Bloom,
		Logs:              logs,
		Status:            ArgUint64(r.Status),
		TxHash:            r.TxHash,
		TxIndex:           ArgUint64(r.TransactionIndex),
		BlockHash:         r.BlockHash,
		BlockNumber:       blockNumber,
		GasUsed:           ArgUint64(r.GasUsed),
		ContractAddress:   contractAddress,
		FromAddr:          from,
		ToAddr:            to,
		Type:              ArgUint64(r.Type),
	}
	//[zkevm] - TODO we don't have this field in the receipt
	// if *r.EffectiveGasPricePercentage != nil {
	// 	egp := ArgBig(*r.EffectiveGasPricePercentage)
	// 	receipt.EffectiveGasPricePercentage = &egp
	// }
	return receipt, nil
}

// Log structure
type Log struct {
	Address     common.Address `json:"address"`
	Topics      []common.Hash  `json:"topics"`
	Data        ArgBytes       `json:"data"`
	BlockNumber ArgUint64      `json:"blockNumber"`
	TxHash      common.Hash    `json:"transactionHash"`
	TxIndex     ArgUint64      `json:"transactionIndex"`
	BlockHash   common.Hash    `json:"blockHash"`
	LogIndex    ArgUint64      `json:"logIndex"`
	Removed     bool           `json:"removed"`
}

// NewLog creates a new instance of Log
func NewLog(l types.Log) Log {
	return Log{
		Address:     l.Address,
		Topics:      l.Topics,
		Data:        l.Data,
		BlockNumber: ArgUint64(l.BlockNumber),
		TxHash:      l.TxHash,
		TxIndex:     ArgUint64(l.TxIndex),
		BlockHash:   l.BlockHash,
		LogIndex:    ArgUint64(l.Index),
		Removed:     l.Removed,
	}
}

// ToBatchNumArg converts a big.Int into a batch number rpc parameter
func ToBatchNumArg(number *big.Int) string {
	if number == nil {
		return Latest
	}
	pending := big.NewInt(-1)
	if number.Cmp(pending) == 0 {
		return Pending
	}
	return hex.EncodeBig(number)
}
