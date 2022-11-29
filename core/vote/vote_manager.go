package vote

import (
	"errors"
	"time"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/event"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
)

const naturallyJustifiedDist = 15 // The distance to naturally justify a block

// VoteManager will handle the vote produced by self.
type VoteManager struct {
	notifyMiningAboutNewTxs chan struct{}
	chain                   consensus.ChainHeaderReader
	chainconfig             *params.ChainConfig

	chainHeadCh  chan core.ChainHeadEvent
	chainHeadSub event.Subscription

	pool    *VotePool
	signer  *VoteSigner
	journal *VoteJournal

	engine consensus.PoSA
}

func NewVoteManager(chainconfig *params.ChainConfig, chain consensus.ChainHeaderReader, pool *VotePool, journalPath, blsPasswordPath, blsWalletPath string, engine consensus.PoSA) (*VoteManager, error) {
	voteManager := &VoteManager{
		chain:       chain,
		chainconfig: chainconfig,
		chainHeadCh: make(chan core.ChainHeadEvent, chainHeadChanSize),

		pool:   pool,
		engine: engine,
	}

	// Create voteSigner.
	voteSigner, err := NewVoteSigner(blsPasswordPath, blsWalletPath)
	if err != nil {
		return nil, err
	}
	log.Info("Create voteSigner successfully")
	voteManager.signer = voteSigner

	// Create voteJournal
	voteJournal, err := NewVoteJournal(journalPath)
	if err != nil {
		return nil, err
	}
	log.Info("Create voteJournal successfully")
	voteManager.journal = voteJournal

	// // Subscribe to chain head event.
	// voteManager.chainHeadSub = voteManager.chain.SubscribeChainHeadEvent(voteManager.chainHeadCh)

	go voteManager.loop()

	return voteManager, nil
}

func (voteManager *VoteManager) loop() {
	log.Debug("vote manager routine loop started")

	var works bool
	var hasWork bool
	mineEvery := time.NewTicker(voteManager.pool.cfg.Recommit)
	errc := make(chan error, 1)
	newHeadCh, closeNewHeadCh := voteManager.pool.notifications.Events.AddHeaderSubscription()
	defer closeNewHeadCh()
	for {
		select {
		case <-voteManager.notifyMiningAboutNewTxs:
			log.Debug("Start mining new block based on txpool notif")
			hasWork = true
		case <-newHeadCh:
			log.Debug("Start mining new block based on new head channel")
			hasWork = true
		case <-mineEvery.C:
			log.Debug("Start mining new block based on miner.recommit")
			hasWork = true
		case err := <-errc:
			works = false
			hasWork = false
			if errors.Is(err, libcommon.ErrStopped) {
				return
			}
			if err != nil {
				log.Warn("mining", "err", err)
			}

		case cHead := <-voteManager.chainHeadCh:
			if !hasWork {
				log.Debug("hasWork flag is false, continue")
				continue
			}

			if !works && hasWork {
				works = true
			}
			if cHead.Block == nil {
				log.Debug("cHead.Block is nil, continue")
				continue
			}

			curHead := cHead.Block.Header()
			// Check if cur validator is within the validatorSet at curHead
			if !voteManager.engine.IsActiveValidatorAt(voteManager.chain, curHead) {
				log.Debug("cur validator is not within the validatorSet at curHead")
				continue
			}

			// Vote for curBlockHeader block.
			vote := &types.VoteData{
				TargetNumber: curHead.Number.Uint64(),
				TargetHash:   curHead.Hash(),
			}
			voteMessage := &types.VoteEnvelope{
				Data: vote,
			}

			// Put Vote into journal and VotesPool if we are active validator and allow to sign it.
			if ok, sourceNumber, sourceHash := voteManager.UnderRules(curHead); ok {
				log.Debug("curHead is underRules for voting")
				if sourceHash == (common.Hash{}) {
					log.Debug("sourceHash is empty")
					continue
				}

				voteMessage.Data.SourceNumber = sourceNumber
				voteMessage.Data.SourceHash = sourceHash

				if err := voteManager.signer.SignVote(voteMessage); err != nil {
					log.Error("Failed to sign vote", "err", err)
					continue
				}
				if err := voteManager.journal.WriteVote(voteMessage); err != nil {
					log.Error("Failed to write vote into journal", "err", err)
					continue
				}

				log.Debug("vote manager produced vote", "votedBlockNumber", voteMessage.Data.TargetNumber, "votedBlockHash", voteMessage.Data.TargetHash, "voteMessageHash", voteMessage.Hash())
				voteManager.pool.PutVote(voteMessage)
			}
		case <-voteManager.chainHeadSub.Err():
			log.Debug("voteManager subscribed chainHead failed")
			return
		}
	}
}

// UnderRules checks if the produced header under the following rules:
// A validator must not publish two distinct votes for the same height. (Rule 1)
// A validator must not vote within the span of its other votes . (Rule 2)
// Validators always vote for their canonical chain’s latest block. (Rule 3)
func (voteManager *VoteManager) UnderRules(header *types.Header) (bool, uint64, common.Hash) {
	justifiedHeader := voteManager.engine.GetJustifiedHeader(voteManager.chain, header)
	if justifiedHeader == nil {
		log.Error("highestJustifiedHeader at cur header is nil", "curHeader's BlockNumber", header.Number.Uint64(), "curHeader's BlockHash", header.Hash())
		return false, 0, common.Hash{}
	}

	sourceNumber := justifiedHeader.Number.Uint64()
	sourceHash := justifiedHeader.Hash()
	targetNumber := header.Number.Uint64()

	voteDataBuffer := voteManager.journal.voteDataBuffer
	//Rule 1:  A validator must not publish two distinct votes for the same height.
	if voteDataBuffer.Contains(header.Number.Uint64()) {
		log.Debug("err: A validator must not publish two distinct votes for the same height.")
		return false, 0, common.Hash{}
	}

	//Rule 2: A validator must not vote within the span of its other votes.
	for blockNumber := sourceNumber + 1; blockNumber < targetNumber; blockNumber++ {
		if voteDataBuffer.Contains(blockNumber) {
			voteData, ok := voteDataBuffer.Get(blockNumber)
			if !ok {
				log.Error("Failed to get voteData info from LRU cache.")
				continue
			}
			if voteData.(*types.VoteData).SourceNumber > sourceNumber {
				log.Debug("error: cur vote is within the span of other votes")
				return false, 0, common.Hash{}
			}
		}
	}
	for blockNumber := targetNumber; blockNumber <= targetNumber+naturallyJustifiedDist; blockNumber++ {
		if voteDataBuffer.Contains(blockNumber) {
			voteData, ok := voteDataBuffer.Get(blockNumber)
			if !ok {
				log.Error("Failed to get voteData info from LRU cache.")
				continue
			}
			if voteData.(*types.VoteData).SourceNumber < sourceNumber {
				log.Debug("error: other votes are within span of cur vote")
				return false, 0, common.Hash{}
			}
		}
	}

	// Rule 3: Validators always vote for their canonical chain’s latest block.
	// Since the header subscribed to is the canonical chain, so this rule is satisified by default.
	log.Debug("All three rules check passed")
	return true, sourceNumber, sourceHash
}
