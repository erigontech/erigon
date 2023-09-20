package gointerfaces

//go:generate moq -stub -out ./sentry/mocks.go ./sentry SentryServer SentryClient
//go:generate moq -stub -out ./remote/mocks.go ./remote KVClient KV_StateChangesClient
