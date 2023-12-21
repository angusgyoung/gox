package telemetry

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

var meterProvider metric.MeterProvider

func Initialise() error {
	res, err := newResource()
	if err != nil {
		log.WithError(err).Warn("Failed to create resource")
		return err
	}

	meterProvider, err := newMeterProvider(res)
	if err != nil {
		log.WithError(err).Warn("Failed to create meter provider")
		return err
	}

	otel.SetMeterProvider(meterProvider)
	return nil
}

func Close() {
	log.Info("Closing telemetry...")
	err := meterProvider.Shutdown(context.Background())
	if err != nil {
		log.WithError(err).Warn("Failed to shutdown meter provider")
	}
}

func newResource() (*resource.Resource, error) {
	return resource.Merge(resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName("gox"),
		))
}

func newMeterProvider(res *resource.Resource) (*metric.MeterProvider, error) {
	otlpHttpExporter, err := otlpmetrichttp.New(context.Background())

	if err != nil {
		return nil, err
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(otlpHttpExporter)),
	)
	return meterProvider, nil
}
