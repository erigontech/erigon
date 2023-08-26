package polygon

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon/accounts/abi/bind"
	"github.com/ledgerwatch/erigon/cmd/devnet/accounts"
	"github.com/ledgerwatch/erigon/cmd/devnet/blocks"
	"github.com/ledgerwatch/erigon/cmd/devnet/contracts"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/checkpoint"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params/networkname"
)

type CheckpointBlock struct {
	Proposer        libcommon.Address `json:"proposer"`
	StartBlock      uint64            `json:"start_block"`
	EndBlock        uint64            `json:"end_block"`
	RootHash        libcommon.Hash    `json:"root_hash"`
	AccountRootHash libcommon.Hash    `json:"account_root_hash"`
	BorChainID      string            `json:"bor_chain_id"`
}

func (c CheckpointBlock) GetSigners() []byte {
	return c.Proposer[:]
}

func (c CheckpointBlock) GetSignBytes() ([]byte, error) {
	/*b, err := ModuleCdc.MarshalJSON(msg)

	if err != nil {
		nil, err
	}

	return sdk.SortJSON(b)*/
	return nil, fmt.Errorf("TODO")
}

type CheckpointAck struct {
	From       libcommon.Address `json:"from"`
	Number     uint64            `json:"number"`
	Proposer   libcommon.Address `json:"proposer"`
	StartBlock uint64            `json:"start_block"`
	EndBlock   uint64            `json:"end_block"`
	RootHash   libcommon.Hash    `json:"root_hash"`
	TxHash     libcommon.Hash    `json:"tx_hash"`
	LogIndex   uint64            `json:"log_index"`
}

var zeroHash libcommon.Hash
var zeroAddress libcommon.Address

func (c CheckpointBlock) ValidateBasic() error {

	if c.RootHash == zeroHash {
		return fmt.Errorf("Invalid rootHash %v", c.RootHash.String())
	}

	if c.Proposer == zeroAddress {
		return fmt.Errorf("Invalid proposer %v", c.Proposer.String())
	}

	if c.StartBlock >= c.EndBlock || c.EndBlock == 0 {
		return fmt.Errorf("Invalid startBlock %v or/and endBlock %v", c.StartBlock, c.EndBlock)
	}

	return nil
}

func (c CheckpointBlock) GetSideSignBytes() []byte {
	borChainID, _ := strconv.ParseUint(c.BorChainID, 10, 64)

	return appendBytes32(
		c.Proposer.Bytes(),
		(&big.Int{}).SetUint64(c.StartBlock).Bytes(),
		(&big.Int{}).SetUint64(c.EndBlock).Bytes(),
		c.RootHash.Bytes(),
		c.AccountRootHash.Bytes(),
		(&big.Int{}).SetUint64(borChainID).Bytes(),
	)
}

func appendBytes32(data ...[]byte) []byte {
	var result []byte

	for _, v := range data {
		l := len(v)

		var padded [32]byte

		if l > 0 && l <= 32 {
			copy(padded[32-l:], v)
		}

		result = append(result, padded[:]...)
	}

	return result
}

func (h *Heimdall) startChildHeaderSubscription(ctx context.Context) {

	node := devnet.SelectBlockProducer(ctx)

	var err error

	childHeaderChan := make(chan *types.Header)
	h.childHeaderSub, err = node.Subscribe(ctx, requests.Methods.ETHNewHeads, childHeaderChan)

	if err != nil {
		h.unsubscribe()
		h.logger.Error("Failed to subscribe to child chain headers", "err", err)
	}

	for childHeader := range childHeaderChan {
		if err := h.handleChildHeader(ctx, childHeader); err != nil {
			h.logger.Error("L2 header processing failed", "header", childHeader.Number, "err", err)
		}
	}
}

func (h *Heimdall) startRootHeaderBlockSubscription() {
	var err error

	rootHeaderBlockChan := make(chan *contracts.TestRootChainNewHeaderBlock)
	h.rootHeaderBlockSub, err = h.rootChainBinding.WatchNewHeaderBlock(&bind.WatchOpts{}, rootHeaderBlockChan, nil, nil, nil)

	if err != nil {
		h.unsubscribe()
		h.logger.Error("Failed to subscribe to root chain header blocks", "err", err)
	}

	for rootHeaderBlock := range rootHeaderBlockChan {
		if err := h.handleRootHeaderBlock(rootHeaderBlock); err != nil {
			h.logger.Error("L1 header block processing failed", "block", rootHeaderBlock.HeaderBlockId, "err", err)
		}
	}
}

func (h *Heimdall) handleChildHeader(ctx context.Context, header *types.Header) error {

	h.logger.Debug("no of checkpoint confirmations required", "childChainTxConfirmations", h.checkpointConfig.ChildChainTxConfirmations)

	latestConfirmedChildBlock := header.Number.Int64() - int64(h.checkpointConfig.ChildChainTxConfirmations)

	if latestConfirmedChildBlock <= 0 {
		h.logger.Error("no of blocks on childchain is less than confirmations required",
			"childChainBlocks", header.Number.Uint64(), "confirmationsRequired", h.checkpointConfig.ChildChainTxConfirmations)
		return errors.New("no of blocks on childchain is less than confirmations required")
	}

	timeStamp := uint64(time.Now().Unix())
	checkpointBufferTime := uint64(h.checkpointConfig.CheckpointBufferTime.Seconds())

	if h.pendingCheckpoint == nil {
		expectedCheckpointState, err := h.nextExpectedCheckpoint(ctx, uint64(latestConfirmedChildBlock))

		if err != nil {
			h.logger.Error("Error while calculate next expected checkpoint", "error", err)
			return err
		}

		h.pendingCheckpoint = &checkpoint.Checkpoint{
			Timestamp:  timeStamp,
			StartBlock: big.NewInt(int64(expectedCheckpointState.newStart)),
			EndBlock:   big.NewInt(int64(expectedCheckpointState.newEnd)),
		}
	}

	if header.Number.Cmp(h.pendingCheckpoint.EndBlock) < 0 {
		return nil
	}

	h.pendingCheckpoint.EndBlock = header.Number

	if !(h.pendingCheckpoint.Timestamp == 0 ||
		((timeStamp > h.pendingCheckpoint.Timestamp) && timeStamp-h.pendingCheckpoint.Timestamp >= checkpointBufferTime)) {
		h.logger.Debug("Pendiing checkpoint awaiting buffer expiry",
			"start", h.pendingCheckpoint.StartBlock,
			"end", h.pendingCheckpoint.EndBlock,
			"expiry", time.Unix(int64(h.pendingCheckpoint.Timestamp+checkpointBufferTime), 0))
		return nil
	}

	start := h.pendingCheckpoint.StartBlock.Uint64()
	end := h.pendingCheckpoint.EndBlock.Uint64()

	shouldSend, err := h.shouldSendCheckpoint(start, end)

	if err != nil {
		return err
	}

	if shouldSend {
		// TODO simulate tendermint chain stats
		txHash := libcommon.Hash{}
		blockHeight := int64(0)

		if err := h.createAndSendCheckpointToRootchain(ctx, start, end, blockHeight, txHash); err != nil {
			h.logger.Error("Error sending checkpoint to rootchain", "error", err)
			return err
		}

		h.pendingCheckpoint = nil
	}

	return nil
}

type ContractCheckpoint struct {
	newStart           uint64
	newEnd             uint64
	currentHeaderBlock *HeaderBlock
}

type HeaderBlock struct {
	start          uint64
	end            uint64
	number         *big.Int
	checkpointTime uint64
}

func (h *Heimdall) nextExpectedCheckpoint(ctx context.Context, latestChildBlock uint64) (*ContractCheckpoint, error) {

	// fetch current header block from mainchain contract
	currentHeaderBlock, err := h.currentHeaderBlock(h.checkpointConfig.ChildBlockInterval)

	if err != nil {
		h.logger.Error("Error while fetching current header block number from rootchain", "error", err)
		return nil, err
	}

	// current header block
	currentHeaderBlockNumber := big.NewInt(0).SetUint64(currentHeaderBlock)

	// get header info
	_, currentStart, currentEnd, lastCheckpointTime, _, err := h.getHeaderInfo(currentHeaderBlockNumber.Uint64())

	if err != nil {
		h.logger.Error("Error while fetching current header block object from rootchain", "error", err)
		return nil, err
	}

	// find next start/end
	var start, end uint64
	start = currentEnd

	// add 1 if start > 0
	if start > 0 {
		start = start + 1
	}

	// get diff
	diff := int(latestChildBlock - start + 1)
	// process if diff > 0 (positive)
	if diff > 0 {
		expectedDiff := diff - diff%int(h.checkpointConfig.AvgCheckpointLength)
		if expectedDiff > 0 {
			expectedDiff = expectedDiff - 1
		}
		// cap with max checkpoint length
		if expectedDiff > int(h.checkpointConfig.MaxCheckpointLength-1) {
			expectedDiff = int(h.checkpointConfig.MaxCheckpointLength - 1)
		}
		// get end result
		end = uint64(expectedDiff) + start
		h.logger.Debug("Calculating checkpoint eligibility",
			"latest", latestChildBlock,
			"start", start,
			"end", end,
		)
	}

	// Handle when block producers go down
	if end == 0 || end == start || (0 < diff && diff < int(h.checkpointConfig.AvgCheckpointLength)) {
		h.logger.Debug("Fetching last header block to calculate time")

		currentTime := time.Now().UTC().Unix()
		defaultForcePushInterval := h.checkpointConfig.MaxCheckpointLength * 2 // in seconds (1024 * 2 seconds)

		if currentTime-int64(lastCheckpointTime) > int64(defaultForcePushInterval) {
			end = latestChildBlock
			h.logger.Info("Force push checkpoint",
				"currentTime", currentTime,
				"lastCheckpointTime", lastCheckpointTime,
				"defaultForcePushInterval", defaultForcePushInterval,
				"start", start,
				"end", end,
			)
		}
	}

	return &ContractCheckpoint{
		newStart: start,
		newEnd:   end,
		currentHeaderBlock: &HeaderBlock{
			start:          currentStart,
			end:            currentEnd,
			number:         currentHeaderBlockNumber,
			checkpointTime: lastCheckpointTime,
		}}, nil
}

func (h *Heimdall) currentHeaderBlock(childBlockInterval uint64) (uint64, error) {
	currentHeaderBlock, err := h.rootChainBinding.CurrentHeaderBlock(nil)

	if err != nil {
		h.logger.Error("Could not fetch current header block from rootChain contract", "error", err)
		return 0, err
	}

	return currentHeaderBlock.Uint64() / childBlockInterval, nil
}

func (h *Heimdall) fetchDividendAccountRoot() (libcommon.Hash, error) {
	//TODO
	return crypto.Keccak256Hash([]byte("dividendaccountroot")), nil
}

func (h *Heimdall) getHeaderInfo(number uint64) (
	root libcommon.Hash,
	start uint64,
	end uint64,
	createdAt uint64,
	proposer libcommon.Address,
	err error,
) {
	// get header from rootChain
	checkpointBigInt := big.NewInt(0).Mul(big.NewInt(0).SetUint64(number), big.NewInt(0).SetUint64(h.checkpointConfig.ChildBlockInterval))

	headerBlock, err := h.rootChainBinding.HeaderBlocks(nil, checkpointBigInt)

	if err != nil {
		return root, start, end, createdAt, proposer, errors.New("unable to fetch checkpoint block")
	}

	createdAt = headerBlock.CreatedAt.Uint64()

	if createdAt == 0 {
		createdAt = uint64(h.startTime.Unix())
	}

	return headerBlock.Root,
		headerBlock.Start.Uint64(),
		headerBlock.End.Uint64(),
		createdAt,
		libcommon.BytesToAddress(headerBlock.Proposer.Bytes()),
		nil
}

func (h *Heimdall) getRootHash(ctx context.Context, start uint64, end uint64) (libcommon.Hash, error) {
	noOfBlock := end - start + 1

	if start > end {
		return libcommon.Hash{}, errors.New("start is greater than end")
	}

	if noOfBlock > h.checkpointConfig.MaxCheckpointLength {
		return libcommon.Hash{}, errors.New("number of headers requested exceeds")
	}

	return devnet.SelectBlockProducer(devnet.WithCurrentNetwork(ctx, networkname.BorDevnetChainName)).GetRootHash(start, end)
}

func (h *Heimdall) shouldSendCheckpoint(start uint64, end uint64) (bool, error) {

	// current child block from contract
	lastChildBlock, err := h.rootChainBinding.GetLastChildBlock(nil)

	if err != nil {
		h.logger.Error("Error fetching current child block", "currentChildBlock", lastChildBlock, "error", err)
		return false, err
	}

	h.logger.Debug("Fetched current child block", "currentChildBlock", lastChildBlock)

	currentChildBlock := lastChildBlock.Uint64()

	shouldSend := false
	// validate if checkpoint needs to be pushed to rootchain and submit
	h.logger.Info("Validating if checkpoint needs to be pushed", "commitedLastBlock", currentChildBlock, "startBlock", start)
	// check if we need to send checkpoint or not
	if ((currentChildBlock + 1) == start) || (currentChildBlock == 0 && start == 0) {
		h.logger.Info("Checkpoint Valid", "startBlock", start)

		shouldSend = true
	} else if currentChildBlock > start {
		h.logger.Info("Start block does not match, checkpoint already sent", "commitedLastBlock", currentChildBlock, "startBlock", start)
	} else if currentChildBlock > end {
		h.logger.Info("Checkpoint already sent", "commitedLastBlock", currentChildBlock, "startBlock", start)
	} else {
		h.logger.Info("No need to send checkpoint")
	}

	return shouldSend, nil
}

func (h *Heimdall) createAndSendCheckpointToRootchain(ctx context.Context, start uint64, end uint64, height int64, txHash libcommon.Hash) error {
	h.logger.Info("Preparing checkpoint to be pushed on chain", "height", height, "txHash", txHash, "start", start, "end", end)

	/*
		// proof
		tx, err := helper.QueryTxWithProof(cp.cliCtx, txHash)
		if err != nil {
			h.logger.Error("Error querying checkpoint tx proof", "txHash", txHash)
			return err
		}

		// fetch side txs sigs
		decoder := helper.GetTxDecoder(authTypes.ModuleCdc)

		stdTx, err := decoder(tx.Tx)
		if err != nil {
			h.logger.Error("Error while decoding checkpoint tx", "txHash", tx.Tx.Hash(), "error", err)
			return err
		}

		cmsg := stdTx.GetMsgs()[0]

		sideMsg, ok := cmsg.(hmTypes.SideTxMsg)
		if !ok {
			h.logger.Error("Invalid side-tx msg", "txHash", tx.Tx.Hash())
			return err
		}
	*/

	shouldSend, err := h.shouldSendCheckpoint(start, end)

	if err != nil {
		return err
	}

	if shouldSend {
		accountRoot, err := h.fetchDividendAccountRoot()

		if err != nil {
			return err
		}

		h.pendingCheckpoint.RootHash, err = h.getRootHash(ctx, start, end)

		if err != nil {
			return err
		}

		checkpoint := CheckpointBlock{
			Proposer:        h.checkpointConfig.CheckpointAccount.Address,
			StartBlock:      start,
			EndBlock:        end,
			RootHash:        h.pendingCheckpoint.RootHash,
			AccountRootHash: accountRoot,
			BorChainID:      h.chainConfig.ChainID.String(),
		}

		// side-tx data
		sideTxData := checkpoint.GetSideSignBytes()

		// get sigs
		sigs /*, err*/ := [][3]*big.Int{} //helper.FetchSideTxSigs(cp.httpClient, height, tx.Tx.Hash(), sideTxData)

		/*
			if err != nil {
				h.logger.Error("Error fetching votes for checkpoint tx", "height", height)
				return err
			}*/

		if err := h.sendCheckpoint(ctx, sideTxData, sigs); err != nil {
			h.logger.Info("Error submitting checkpoint to rootchain", "error", err)
			return err
		}
	}

	return nil
}

func (h *Heimdall) sendCheckpoint(ctx context.Context, signedData []byte, sigs [][3]*big.Int) error {

	s := make([]string, 0)
	for i := 0; i < len(sigs); i++ {
		s = append(s, fmt.Sprintf("[%s,%s,%s]", sigs[i][0].String(), sigs[i][1].String(), sigs[i][2].String()))
	}

	h.logger.Debug("Sending new checkpoint",
		"sigs", strings.Join(s, ","),
		"data", hex.EncodeToString(signedData),
	)

	node := devnet.SelectBlockProducer(ctx)

	auth, err := bind.NewKeyedTransactorWithChainID(accounts.SigKey(h.checkpointConfig.CheckpointAccount.Address), node.ChainID())

	if err != nil {
		h.logger.Error("Error while getting auth to submit checkpoint", "err", err)
		return err
	}

	waiter, cancel := blocks.BlockWaiter(ctx, blocks.CompletionChecker)
	defer cancel()

	tx, err := h.rootChainBinding.SubmitCheckpoint(auth, signedData, sigs)

	if err != nil {
		h.logger.Error("Error while submitting checkpoint", "err", err)
		return err
	}

	block, err := waiter.Await(tx.Hash())

	if err != nil {
		h.logger.Error("Error while submitting checkpoint", "err", err)
		return err
	}

	h.logger.Info("Submitted new checkpoint to rootchain successfully", "txHash", tx.Hash().String(), "block", block.Number)

	return nil
}

func (h *Heimdall) handleRootHeaderBlock(event *contracts.TestRootChainNewHeaderBlock) error {
	h.logger.Info("Received root header")

	checkpointNumber := big.NewInt(0).Div(event.HeaderBlockId, big.NewInt(0).SetUint64(h.checkpointConfig.ChildBlockInterval))

	h.logger.Info(
		"✅ Received checkpoint-ack for heimdall",
		"event", "NewHeaderBlock",
		"start", event.Start,
		"end", event.End,
		"reward", event.Reward,
		"root", hexutility.Bytes(event.Root[:]),
		"proposer", event.Proposer.Hex(),
		"checkpointNumber", checkpointNumber,
		"txHash", event.Raw.TxHash,
		"logIndex", uint64(event.Raw.Index),
	)

	// event checkpoint is older than or equal to latest checkpoint
	if h.latestCheckpoint != nil && h.latestCheckpoint.EndBlock >= event.End.Uint64() {
		h.logger.Debug("Checkpoint ack is already submitted", "start", event.Start, "end", event.End)
		return nil
	}

	// create msg checkpoint ack message
	ack := CheckpointAck{
		//From       libcommon.Address `json:"from"`
		Number:     checkpointNumber.Uint64(),
		Proposer:   event.Proposer,
		StartBlock: event.Start.Uint64(),
		EndBlock:   event.End.Uint64(),
		RootHash:   event.Root,
		TxHash:     event.Raw.TxHash,
		LogIndex:   uint64(event.Raw.Index),
	}

	if ack.StartBlock != h.pendingCheckpoint.StartBlock.Uint64() {
		h.logger.Error("Invalid start block", "startExpected", h.pendingCheckpoint.StartBlock, "startReceived", ack.StartBlock)
		return fmt.Errorf("Invalid Checkpoint Ack: Invalid start block")
	}

	// Return err if start and end matches but contract root hash doesn't match
	if ack.StartBlock == h.pendingCheckpoint.StartBlock.Uint64() &&
		ack.EndBlock == h.pendingCheckpoint.EndBlock.Uint64() && ack.RootHash != h.pendingCheckpoint.RootHash {
		h.logger.Error("Invalid ACK",
			"startExpected", h.pendingCheckpoint.StartBlock,
			"startReceived", ack.StartBlock,
			"endExpected", h.pendingCheckpoint.EndBlock,
			"endReceived", ack.StartBlock,
			"rootExpected", h.pendingCheckpoint.RootHash.String(),
			"rootRecieved", ack.RootHash.String(),
		)

		return fmt.Errorf("Invalid Checkpoint Ack: Invalid root hash")
	}

	h.latestCheckpoint = &ack

	h.ackWaiter.Broadcast()

	return nil
}
