use opentelemetry::sdk::trace::BatchConfig;
use opentelemetry::{global, KeyValue};

use opentelemetry::sdk::propagation::TraceContextPropagator;
use opentelemetry::sdk::{trace, Resource};
use opentelemetry_otlp::WithExportConfig;
use tracing::level_filters::LevelFilter;
use tracing_bunyan_formatter::JsonStorageLayer;
use tracing_subscriber::prelude::*;
use tracing_subscriber::Registry;

const SERVICE_NAME: &str = "rldb";

pub fn initialize_jaeger_subscriber(exporter_endpoint: &str) {
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(exporter_endpoint);

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(
            trace::config().with_resource(Resource::new(vec![KeyValue::new(
                opentelemetry_semantic_conventions::resource::SERVICE_NAME,
                SERVICE_NAME.to_string(),
            )])),
        )
        .with_batch_config(BatchConfig::default().with_max_queue_size(1024 * 1024))
        .install_batch(opentelemetry::runtime::Tokio)
        .expect("Error: Failed to initialize the tracer.");

    let subscriber = Registry::default();
    let tracing_layer = tracing_opentelemetry::layer().with_tracer(tracer);
    global::set_text_map_propagator(TraceContextPropagator::new());

    subscriber
        .with(LevelFilter::INFO)
        .with(tracing_layer)
        .with(JsonStorageLayer)
        .init();
}
