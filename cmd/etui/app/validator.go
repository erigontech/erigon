package app

import (
	"context"
	"time"

	"github.com/erigontech/erigon/cmd/etui/widgets"
)

func (a *App) pollValidatorPage(ctx context.Context, view *widgets.ValidatorPageView, headers []*widgets.HeaderView) {
	const pollInterval = 15 * time.Second

	update := func() {
		reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		snapshot := a.beacon.GetValidatorSnapshot(reqCtx)
		cancel()

		role := "Full+RPC"
		if snapshot.HasKeys {
			role = "Validator"
		}

		a.queueDashboardUpdate(func() {
			for _, header := range headers {
				header.SetRole(role)
			}
			view.SetSnapshot(snapshot)
		})
	}

	update()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			update()
		}
	}
}
