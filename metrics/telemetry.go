package metrics

import (
	"context"

	"github.com/urfave/cli/v2"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"

	"github.com/ledgerwatch/log/v3"
)

var OpenCollectorEndpointFlag = cli.StringFlag{
	Name:  "metrics.opencollector-endpoint",
	Value: "",
}

func SetupOpenCollector(ctx context.Context, openCollectorEndpoint string) {
	// setup open collector tracer
	res, err := resource.New(ctx,
		resource.WithAttributes(
			// the service name used to display traces in backends
			semconv.ServiceNameKey.String("erigon"),
		),
	)
	if err != nil {
		log.Error("failed to create open telemetry resource for service", "err", err)
	}

	// Set up a trace exporter
	traceExporter, err := otlptracegrpc.New(
		ctx,
		otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithEndpoint(openCollectorEndpoint),
	)
	if err != nil {
		log.Error("failed to create open telemetry resource for service", "err", err)
	}

	// Register the trace exporter with a TracerProvider, using a batch
	// span processor to aggregate spans before export.
	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)
	otel.SetTracerProvider(tracerProvider)

	// set global propagator to tracecontext (the default is no-op).
	otel.SetTextMapPropagator(propagation.TraceContext{})

	log.Info("Open collector tracing started", "address", openCollectorEndpoint)

	defer func() {
		log.Info("Stopping open collector tracer")
		if err := tracerProvider.Shutdown(context.Background()); err != nil {
			log.Error("Failed to shutdown open telemetry tracer")
		}
	}()
	<-ctx.Done()
}
