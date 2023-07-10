package devnet

import go_context "context"

type Service interface {
	Start(context go_context.Context) error
	Stop()

	NodeStarted(node Node)
}
