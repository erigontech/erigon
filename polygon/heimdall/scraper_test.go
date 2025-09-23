package heimdall

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/testlog"
	"github.com/erigontech/erigon/polygon/heimdall/poshttp"
)

func TestScrapper_Run_TransientErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	logger := testlog.Logger(t, log.LvlCrit)

	ctrl := gomock.NewController(t)
	store := NewMockEntityStore[*Milestone](ctrl)
	fetcher := NewMockentityFetcher[*Milestone](ctrl)
	store.EXPECT().Prepare(gomock.Any()).Return(nil).Times(1)
	store.EXPECT().Close().Return().Times(1)
	putEntitiesCount := atomic.Int32{}
	store.EXPECT().
		PutEntity(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, u uint64, milestone *Milestone) error {
			putEntitiesCount.Add(1)
			return nil
		}).
		AnyTimes()
	gomock.InOrder(
		store.EXPECT().
			LastEntityId(gomock.Any()).
			Return(0, false, nil).
			Times(1),
		store.EXPECT().
			LastEntityId(gomock.Any()).
			Return(1, true, nil).
			Times(2),
		store.EXPECT().
			LastEntityId(gomock.Any()).
			Return(2, true, nil).
			AnyTimes(),
	)
	gomock.InOrder(
		fetcher.EXPECT().
			FetchEntityIdRange(gomock.Any()).
			Return(ClosedRange{Start: 1, End: 1}, nil).
			Times(1),
		fetcher.EXPECT().
			FetchEntityIdRange(gomock.Any()).
			Return(ClosedRange{Start: 2, End: 2}, nil).
			AnyTimes(),
	)
	gomock.InOrder(
		fetcher.EXPECT().
			FetchEntitiesRange(gomock.Any(), gomock.Any()).
			Return([]*Milestone{{Id: 1}}, ErrNotInMilestoneList).
			Times(1),
		fetcher.EXPECT().
			FetchEntitiesRange(gomock.Any(), gomock.Any()).
			Return(nil, poshttp.ErrBadGateway).
			Times(1),
		fetcher.EXPECT().
			FetchEntitiesRange(gomock.Any(), gomock.Any()).
			Return([]*Milestone{{Id: 2}}, nil).
			Times(1),
	)

	transientErrs := []error{ErrNotInMilestoneList, poshttp.ErrBadGateway}
	scrapper := NewScraper[*Milestone]("test", store, fetcher, time.Millisecond, transientErrs, logger)

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return scrapper.Run(ctx)
	})

	cond := func() bool { return putEntitiesCount.Load() == 2 }
	require.Eventually(t, cond, time.Second, time.Millisecond, "expected to have persisted 2 entities")

	cancel()
	err := eg.Wait()
	require.ErrorIs(t, err, context.Canceled)
}
