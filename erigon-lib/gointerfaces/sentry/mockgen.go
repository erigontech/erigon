package sentry

//go:generate mockgen -typed=true -destination=./sentry_client_mock.go -package=sentry . SentryClient
//go:generate mockgen -typed=true -destination=./sentry_server_mock.go -package=sentry . SentryServer
