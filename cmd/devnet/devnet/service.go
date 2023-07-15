package devnet

import context "context"

type Service interface {
	Start(context context.Context) error
	Stop()

	NodeCreated(node Node)
	NodeStarted(node Node)
}
