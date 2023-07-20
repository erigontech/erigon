package devnet

import context "context"

type Service interface {
	Start(context context.Context) error
	Stop()

	NodeCreated(ctx context.Context, node Node)
	NodeStarted(ctx context.Context, node Node)
}
