package sentryproto

//go:generate mockgen -typed=true -destination=./sentry_client_mock.go -package=sentryproto . SentryClient
//go:generate mockgen -typed=true -destination=./sentry_server_mock.go -package=sentryproto . SentryServer
