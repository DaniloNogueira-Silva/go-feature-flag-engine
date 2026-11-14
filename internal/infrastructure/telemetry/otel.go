package telemetry

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// InitMeterProvider configures and registers a global OpenTelemetry MeterProvider
// that exports metrics over OTLP/HTTP to the configured collector endpoint.
func InitMeterProvider(ctx context.Context, serviceName, otlpEndpoint string) (*metric.MeterProvider, func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, nil, err
	}

	var opts []otlpmetrichttp.Option
	opts = append(opts, otlpmetrichttp.WithInsecure())
	if otlpEndpoint != "" {
		clean := strings.TrimPrefix(strings.TrimPrefix(otlpEndpoint, "http://"), "https://")
		opts = append(opts, otlpmetrichttp.WithEndpoint(clean))
	}

	exporter, err := otlpmetrichttp.New(ctx, opts...)
	if err != nil {
		return nil, nil, err
	}

	mp := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(exporter, metric.WithInterval(10*time.Second))),
	)
	otel.SetMeterProvider(mp)

	return mp, mp.Shutdown, nil
}

