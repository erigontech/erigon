package vote

import (
	"encoding/json"

	lru "github.com/hashicorp/golang-lru"
	"github.com/tidwall/wal"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/log/v3"
)

const (
	maxSizeOfRecentEntry = 512
)

type VoteJournal struct {
	journalPath string // file path of disk journal for saving the vote.

	walLog *wal.Log

	voteDataBuffer *lru.Cache
}

func NewVoteJournal(filePath string) (*VoteJournal, error) {
	walLog, err := wal.Open(filePath, &wal.Options{
		LogFormat:        wal.JSON,
		SegmentCacheSize: maxSizeOfRecentEntry,
	})
	if err != nil {
		log.Error("Failed to open vote journal", "err", err)
		return nil, err
	}

	voteDataBuffer, err := lru.New(maxSizeOfRecentEntry)
	if err != nil {
		return nil, err
	}

	firstIndex, err := walLog.FirstIndex()
	if err != nil {
		log.Error("Failed to get first index of votes journal", "err", err)
	}

	lastIndex, err := walLog.LastIndex()
	if err != nil {
		log.Error("Failed to get lastIndex of vote journal", "err", err)
		return nil, err
	}

	voteJournal := &VoteJournal{
		journalPath: filePath,
		walLog:      walLog,
	}

	// Reload all voteData from journal to lru memory everytime node reboot.
	for index := firstIndex; index <= lastIndex; index++ {
		if voteEnvelop, err := voteJournal.ReadVote(index); err == nil && voteEnvelop != nil {
			voteData := voteEnvelop.Data
			voteDataBuffer.Add(voteData.TargetNumber, voteData)
		}
	}
	voteJournal.voteDataBuffer = voteDataBuffer

	return voteJournal, nil
}

func (journal *VoteJournal) WriteVote(voteMessage *types.VoteEnvelope) error {
	walLog := journal.walLog

	vote, err := json.Marshal(voteMessage)
	if err != nil {
		log.Error("Failed to unmarshal vote", "err", err)
		return err
	}

	lastIndex, err := walLog.LastIndex()
	if err != nil {
		log.Error("Failed to get lastIndex of vote journal", "err", err)
		return err
	}

	lastIndex += 1
	if err = walLog.Write(lastIndex, vote); err != nil {
		log.Error("Failed to write vote journal", "err", err)
		return err
	}

	firstIndex, err := walLog.FirstIndex()
	if err != nil {
		log.Error("Failed to get first index of votes journal", "err", err)
	}

	if lastIndex-firstIndex+1 > maxSizeOfRecentEntry {
		if err := walLog.TruncateFront(lastIndex - maxSizeOfRecentEntry + 1); err != nil {
			log.Error("Failed to truncate votes journal", "err", err)
		}
	}

	journal.voteDataBuffer.Add(voteMessage.Data.TargetNumber, voteMessage.Data)
	return nil
}

func (journal *VoteJournal) ReadVote(index uint64) (*types.VoteEnvelope, error) {
	voteMessage, err := journal.walLog.Read(index)
	if err != nil && err != wal.ErrNotFound {
		log.Error("Failed to read votes journal", "err", err)
		return nil, err
	}

	var vote *types.VoteEnvelope
	if voteMessage != nil {
		vote = &types.VoteEnvelope{}
		if err := json.Unmarshal(voteMessage, vote); err != nil {
			log.Error("Failed to read vote from voteJournal", "err", err)
			return nil, err
		}
	}

	return vote, nil
}
