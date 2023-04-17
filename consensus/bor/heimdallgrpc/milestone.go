package heimdallgrpc

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/milestone"
	"github.com/ledgerwatch/log/v3"

	proto "github.com/maticnetwork/polyproto/heimdall"
	protoutils "github.com/maticnetwork/polyproto/utils"
)

func (h *HeimdallGRPCClient) FetchMilestoneCount(ctx context.Context) (int64, error) {
	log.Info("Fetching milestone count")

	res, err := h.client.FetchMilestoneCount(ctx, nil)
	if err != nil {
		return 0, err
	}

	log.Info("Fetched milestone count")

	return res.Result.Count, nil
}

func (h *HeimdallGRPCClient) FetchMilestone(ctx context.Context) (*milestone.Milestone, error) {
	log.Info("Fetching milestone")

	res, err := h.client.FetchMilestone(ctx, nil)
	if err != nil {
		return nil, err
	}

	log.Info("Fetched milestone")

	milestone := &milestone.Milestone{
		StartBlock: new(big.Int).SetUint64(res.Result.StartBlock),
		EndBlock:   new(big.Int).SetUint64(res.Result.EndBlock),
		Hash:       protoutils.ConvertH256ToHash(res.Result.RootHash),
		Proposer:   protoutils.ConvertH160toAddress(res.Result.Proposer),
		BorChainID: res.Result.BorChainID,
		Timestamp:  uint64(res.Result.Timestamp.GetSeconds()),
	}

	return milestone, nil
}

func (h *HeimdallGRPCClient) FetchLastNoAckMilestone(ctx context.Context) (string, error) {
	log.Info("Fetching latest no ack milestone Id")

	res, err := h.client.FetchLastNoAckMilestone(ctx, nil)
	if err != nil {
		return "", err
	}

	log.Info("Fetched last no-ack milestone")

	return res.Result.Result, nil
}

func (h *HeimdallGRPCClient) FetchNoAckMilestone(ctx context.Context, milestoneID string) error {
	req := &proto.FetchMilestoneNoAckRequest{
		MilestoneID: milestoneID,
	}

	log.Info("Fetching no ack milestone", "milestoneID", milestoneID)

	res, err := h.client.FetchNoAckMilestone(ctx, req)
	if err != nil {
		return err
	}

	if !res.Result.Result {
		return fmt.Errorf("Not in rejected list: milestoneID %q", milestoneID)
	}

	log.Info("Fetched no ack milestone", "milestoneID", milestoneID)

	return nil
}

func (h *HeimdallGRPCClient) FetchMilestoneID(ctx context.Context, milestoneID string) error {
	req := &proto.FetchMilestoneIDRequest{
		MilestoneID: milestoneID,
	}

	log.Info("Fetching milestone id", "milestoneID", milestoneID)

	res, err := h.client.FetchMilestoneID(ctx, req)
	if err != nil {
		return err
	}

	if !res.Result.Result {
		return fmt.Errorf("This milestoneID %q does not exist", milestoneID)
	}

	log.Info("Fetched milestone id", "milestoneID", milestoneID)

	return nil
}
