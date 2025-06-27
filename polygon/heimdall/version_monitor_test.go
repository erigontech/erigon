package heimdall_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestVersioMonitorHeimdallV2(t *testing.T) {
	ctrl := gomock.NewController(t)
	heimdallClient := heimdall.NewMockClient(ctrl)

	status := &heimdall.ChainManagerStatus{}
	status.Params.ChainParams.PolTokenAddress = new(string)

	heimdallClient.
		EXPECT().
		FetchChainManagerStatus(gomock.Any()).
		Return(status, nil)

	monitor := heimdall.NewVersionMonitor(context.TODO(), heimdallClient, log.New(), time.Minute)
	resolved := monitor.Version()

	require.Equal(t, resolved, heimdall.HeimdallV2)
}

func TestVersioMonitorHeimdallV1(t *testing.T) {
	ctrl := gomock.NewController(t)
	heimdallClient := heimdall.NewMockClient(ctrl)

	status := &heimdall.ChainManagerStatus{}

	heimdallClient.
		EXPECT().
		FetchChainManagerStatus(gomock.Any()).
		Return(status, nil)

	monitor := heimdall.NewVersionMonitor(context.TODO(), heimdallClient, log.New(), time.Minute)
	resolved := monitor.Version()

	require.Equal(t, resolved, heimdall.HeimdallV1)
}

func TestVersioMonitorHeimdallUpgrade(t *testing.T) {
	ctx, clean := context.WithTimeout(context.Background(), time.Minute)
	defer clean()

	ctrl := gomock.NewController(t)
	heimdallClient := heimdall.NewMockClient(ctrl)

	timeNow := time.Now()
	var upgradeMonitoredTimes atomic.Int64

	heimdallClient.
		EXPECT().
		FetchChainManagerStatus(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (*heimdall.ChainManagerStatus, error) {
			status := &heimdall.ChainManagerStatus{}

			if time.Since(timeNow) > time.Second {
				status.Params.ChainParams.PolTokenAddress = new(string)
				upgradeMonitoredTimes.Add(1)
			}

			return status, nil
		}).AnyTimes()

	monitor := heimdall.NewVersionMonitor(ctx, heimdallClient, log.New(), 100*time.Millisecond)
	go monitor.Run()

	for {
		time.Sleep(100 * time.Millisecond)

		resolved := monitor.Version()

		switch upgradeMonitoredTimes.Load() {
		case 0:
			require.Equal(t, resolved, heimdall.HeimdallV1) // Upgrade has not been happened yet
		case 1:
			// Upgrade happened and monitored but race still possible to happen. Let's skip the check
		default:
			require.Equal(t, resolved, heimdall.HeimdallV2) // Upgrade happened and monitored twice or more -> it was updated in the monitor
			return
		}
	}
}

func TestVersioMonitorHeimdallDowngrade(t *testing.T) {
	ctx, clean := context.WithTimeout(context.Background(), time.Minute)
	defer clean()

	ctrl := gomock.NewController(t)
	heimdallClient := heimdall.NewMockClient(ctrl)

	timeNow := time.Now()
	var downgradeMonitoredTimes atomic.Int64

	heimdallClient.
		EXPECT().
		FetchChainManagerStatus(gomock.Any()).
		DoAndReturn(func(ctx context.Context) (*heimdall.ChainManagerStatus, error) {
			status := &heimdall.ChainManagerStatus{}
			status.Params.ChainParams.PolTokenAddress = new(string)

			if time.Since(timeNow) > time.Second {
				status.Params.ChainParams.PolTokenAddress = nil
				downgradeMonitoredTimes.Add(1)
			}

			return status, nil
		}).AnyTimes()

	monitor := heimdall.NewVersionMonitor(ctx, heimdallClient, log.New(), 100*time.Millisecond)
	go monitor.Run()

	for {
		time.Sleep(100 * time.Millisecond)

		resolved := monitor.Version()

		switch downgradeMonitoredTimes.Load() {
		case 0:
			require.Equal(t, resolved, heimdall.HeimdallV2) // Downgrade has not been happened yet
		case 1:
			// Downgrade happened and monitored but race still possible to happen. Let's skip the check
		default:
			require.Equal(t, resolved, heimdall.HeimdallV1) // Downgrade happened and monitored twice or more -> it was updated in the monitor
			return
		}
	}
}
