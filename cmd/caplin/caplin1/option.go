package caplin1

import "github.com/ledgerwatch/erigon/cl/beacon/builder"

type option struct {
	builderClient builder.BuilderClient
}

type CaplinOption func(*option)

func WithBuilder(mevRelayUrl string) CaplinOption {
	return func(o *option) {
		o.builderClient = builder.NewBlockBuilderClient(mevRelayUrl)
	}
}
