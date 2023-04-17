package types

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v4"
	"github.com/ledgerwatch/erigon-lib/common"
	ericommon "github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/zkevm/hex"
	"github.com/ledgerwatch/erigon/zkevm/state"

	"github.com/holiman/uint256"
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
	copy(aux[:], hh[:])
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

// ToTransaction transforms txnArgs into a Transaction
func (args *TxArgs) ToTransaction(ctx context.Context, st StateInterface, maxCumulativeGasUsed uint64, root common.Hash, defaultSenderAddress common.Address, dbTx pgx.Tx) (common.Address, types.Transaction, error) {
	sender := defaultSenderAddress
	nonce := uint64(0)
	if args.From != nil && *args.From != state.ZeroAddress {
		sender = *args.From
		n, err := st.GetNonce(ctx, sender, root)
		if err != nil {
			return common.Address{}, nil, err
		}
		nonce = n
	}

	value := big.NewInt(0)
	if args.Value != nil {
		value.SetBytes(*args.Value)
	}

	gasPrice := big.NewInt(0)
	if args.GasPrice != nil {
		gasPrice.SetBytes(*args.GasPrice)
	}

	var data []byte
	if args.Data != nil {
		data = *args.Data
	} else if args.Input != nil {
		data = *args.Input
	} else if args.To == nil {
		return common.Address{}, nil, fmt.Errorf("contract creation without data provided")
	}

	gas := maxCumulativeGasUsed
	if args.Gas != nil && uint64(*args.Gas) > 0 && uint64(*args.Gas) < maxCumulativeGasUsed {
		gas = uint64(*args.Gas)
	}

	tx := &types.LegacyTx{
		types.CommonTx{
			Nonce: nonce,
			To:    args.To,
			Value: b2i(value),
			Gas:   gas,
			Data:  data,
		},
		b2i(gasPrice),
	}

	return sender, tx, nil
}

func b2i(s *big.Int) *uint256.Int {
	iii := &uint256.Int{}
	iii.SetBytes(s.Bytes())
	return iii
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
func NewBlock(b *types.Block, fullTx bool) *Block {
	h := b.Header()

	n := big.NewInt(0).SetUint64(h.Nonce.Uint64())
	nonce := ericommon.LeftPadBytes(n.Bytes(), 8) //nolint:gomnd

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

	for idx, txn := range b.Transactions() {
		if fullTx {
			blockHash := b.Hash()
			txIndex := uint64(idx)
			tx := NewTransaction(txn, b.Number(), &blockHash, &txIndex)
			res.Transactions = append(
				res.Transactions,
				TransactionOrHash{Tx: tx},
			)
		} else {
			h := txn.Hash()
			res.Transactions = append(
				res.Transactions,
				TransactionOrHash{Hash: &h},
			)
		}
	}

	for _, uncle := range b.Uncles() {
		res.Uncles = append(res.Uncles, uncle.Hash())
	}

	return res
}

// Batch structure
type Batch struct {
	Number              ArgUint64           `json:"number"`
	Coinbase            common.Address      `json:"coinbase"`
	StateRoot           common.Hash         `json:"stateRoot"`
	GlobalExitRoot      common.Hash         `json:"globalExitRoot"`
	MainnetExitRoot     common.Hash         `json:"mainnetExitRoot"`
	RollupExitRoot      common.Hash         `json:"rollupExitRoot"`
	LocalExitRoot       common.Hash         `json:"localExitRoot"`
	AccInputHash        common.Hash         `json:"accInputHash"`
	Timestamp           ArgUint64           `json:"timestamp"`
	SendSequencesTxHash *common.Hash        `json:"sendSequencesTxHash"`
	VerifyBatchTxHash   *common.Hash        `json:"verifyBatchTxHash"`
	Transactions        []TransactionOrHash `json:"transactions"`
}

// NewBatch creates a Batch instance
func NewBatch(batch *state.Batch, virtualBatch *state.VirtualBatch, verifiedBatch *state.VerifiedBatch, receipts []types.Receipt, fullTx bool, ger *state.GlobalExitRoot) *Batch {
	res := &Batch{
		Number:          ArgUint64(batch.BatchNumber),
		GlobalExitRoot:  batch.GlobalExitRoot,
		MainnetExitRoot: ger.MainnetExitRoot,
		RollupExitRoot:  ger.RollupExitRoot,
		AccInputHash:    batch.AccInputHash,
		Timestamp:       ArgUint64(batch.Timestamp.Unix()),
		StateRoot:       batch.StateRoot,
		Coinbase:        batch.Coinbase,
		LocalExitRoot:   batch.LocalExitRoot,
	}

	if virtualBatch != nil {
		res.SendSequencesTxHash = &virtualBatch.TxHash
	}

	if verifiedBatch != nil {
		res.VerifyBatchTxHash = &verifiedBatch.TxHash
	}

	receiptsMap := make(map[common.Hash]types.Receipt, len(receipts))
	for _, receipt := range receipts {
		receiptsMap[receipt.TxHash] = receipt
	}

	for _, tx := range batch.Transactions {
		if fullTx {
			receipt := receiptsMap[tx.Hash()]
			txIndex := uint64(receipt.TransactionIndex)
			rpcTx := NewTransaction(tx, receipt.BlockNumber, &receipt.BlockHash, &txIndex)
			res.Transactions = append(res.Transactions, TransactionOrHash{Tx: rpcTx})
		} else {
			h := tx.Hash()
			res.Transactions = append(res.Transactions, TransactionOrHash{Hash: &h})
		}
	}

	return res
}

// TransactionOrHash for union type of transaction and types.Hash
type TransactionOrHash struct {
	Hash *common.Hash
	Tx   *Transaction
}

// MarshalJSON marshals into json
func (b TransactionOrHash) MarshalJSON() ([]byte, error) {
	if b.Hash != nil {
		return json.Marshal(b.Hash)
	}
	return json.Marshal(b.Tx)
}

// UnmarshalJSON unmarshals from json
func (b *TransactionOrHash) UnmarshalJSON(input []byte) error {
	v := string(input)
	if strings.HasPrefix(v, "0x") || strings.HasPrefix(v, "\"0x") {
		var h common.Hash
		err := json.Unmarshal(input, &h)
		if err != nil {
			return err
		}
		*b = TransactionOrHash{Hash: &h}
		return nil
	}

	var t Transaction
	err := json.Unmarshal(input, &t)
	if err != nil {
		return err
	}
	*b = TransactionOrHash{Tx: &t}
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
	ChainID     ArgBig          `json:"chainId"`
	Type        ArgUint64       `json:"type"`
}

// CoreTx returns a geth core type Transaction
func (t Transaction) CoreTx() types.Transaction {
	return &types.LegacyTx{
		types.CommonTx{
			Nonce: uint64(t.Nonce),
			Gas:   uint64(t.Gas),
			To:    t.To,
			Value: b2i((*big.Int)(&t.Value)),
			Data:  t.Input,
			V:     *(b2i((*big.Int)(&t.V))),
			R:     *(b2i((*big.Int)(&t.R))),
			S:     *(b2i((*big.Int)(&t.S))),
		},
		b2i((*big.Int)(&t.GasPrice)),
	}
}

// NewTransaction creates a transaction instance
func NewTransaction(
	t types.Transaction,
	blockNumber *big.Int,
	blockHash *common.Hash,
	txIndex *uint64,
) *Transaction {
	v, r, s := t.RawSignatureValues()

	from, _ := state.GetSender(t)

	res := &Transaction{
		Nonce:    ArgUint64(t.GetNonce()),
		GasPrice: ArgBig(*(t.GetPrice().ToBig())),
		Gas:      ArgUint64(t.GetGas()),
		To:       t.GetTo(),
		Value:    ArgBig(*(t.GetValue().ToBig())),
		Input:    t.GetData(),
		V:        ArgBig(*(v.ToBig())),
		R:        ArgBig(*(r.ToBig())),
		S:        ArgBig(*(s.ToBig())),
		Hash:     t.Hash(),
		From:     from,
		ChainID:  ArgBig(*(t.GetChainID().ToBig())),
		Type:     ArgUint64(t.Type()),
	}

	if blockNumber != nil {
		bn := ArgUint64(blockNumber.Uint64())
		res.BlockNumber = &bn
	}

	res.BlockHash = blockHash

	if txIndex != nil {
		ti := ArgUint64(*txIndex)
		res.TxIndex = &ti
	}

	return res
}

// Receipt structure
type Receipt struct {
	Root              common.Hash     `json:"root"`
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
}

// NewReceipt creates a new Receipt instance
func NewReceipt(tx types.Transaction, r *types.Receipt) (Receipt, error) {
	to := tx.GetTo()
	logs := r.Logs
	if logs == nil {
		logs = []*types.Log{}
	}

	var contractAddress *common.Address
	if r.ContractAddress != state.ZeroAddress {
		ca := r.ContractAddress
		contractAddress = &ca
	}

	blockNumber := ArgUint64(0)
	if r.BlockNumber != nil {
		blockNumber = ArgUint64(r.BlockNumber.Uint64())
	}

	from, err := state.GetSender(tx)
	if err != nil {
		return Receipt{}, err
	}

	return Receipt{
		Root:              common.BytesToHash(r.PostState),
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
	}, nil
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
